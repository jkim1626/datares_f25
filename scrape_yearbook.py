import logging, re, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import zipfile
from pathlib import Path
from helpers.db_manifest import DBManifest
from helpers.paths import get_yearbook_outdir

# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Configs
BASE = "https://ohss.dhs.gov"
ROOT = "https://ohss.dhs.gov/topics/immigration/yearbook"
FILE_EXTS = (".pdf", ".xlsx", ".xls", ".zip")

MODE = "safe"
POLITE_DELAY = 0.5

# HTTP helpers
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "DataResScraper/1.0"})
    return s

def retrying_get(session: requests.Session, url: str, *, timeout=(10, 30), stream=False):
    backoff = 1.0
    for i in range(4):
        try:
            r = session.get(url, timeout=timeout, stream=stream)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {r.status_code}")
            r.raise_for_status()
            return r
        except Exception as e:
            if i == 3:
                logger.error(f"GET failed {url}: {e}")
                raise
            logger.warning(f"GET retry {i+1} for {url}: {e}")
            time.sleep(backoff)
            backoff *= 2

# Utilities
def extract_zip_file(zip_path: Path):
    """Extract a zip file into its own subfolder within the same directory."""
    try:
        zip_filename = zip_path.name
        folder_name = zip_path.stem
        extract_folder = zip_path.parent / folder_name
        
        extract_folder.mkdir(parents=True, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        
        logger.info(f"  Extracted {zip_filename} to {folder_name}/")
        return True
        
    except zipfile.BadZipFile:
        logger.error(f"  Failed to extract {zip_path.name}: Not a valid zip file")
        return False
    except Exception as e:
        logger.error(f"  Failed to extract {zip_path.name}: {e}")
        return False

# Discovery functions
def discover_yearbooks(session: requests.Session) -> list:
    """
    Discover ALL available yearbooks from the website.
    Returns list of dicts: {"year": str, "url": str}
    """
    r = retrying_get(session, ROOT)
    soup = BeautifulSoup(r.text, "html.parser")
    
    yearbooks = {}
    
    # Find all yearbook links
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        text = a_tag.get_text(strip=True)
        
        # Try to extract year from URL pattern: /yearbook/YYYY or /yearbook/YYYY-YYYY
        url_year_match = re.search(r"/yearbook/(\d{4})(?:-\d{4})?/?$", href)
        if url_year_match:
            year = str(url_year_match.group(1))  # FIXED: Keep as string
            absolute_url = urljoin(ROOT, href)
            yearbooks[year] = {
                "year": year,
                "url": absolute_url,
            }
            continue
        
        # Try to extract year from text pattern: "Yearbook YYYY" or "Yearbook YYYY to YYYY"
        text_year_match = re.search(r"Yearbook\s+(\d{4})(?:\s+to\s+\d{4})?", text, re.I)
        if text_year_match:
            year = str(text_year_match.group(1))  # FIXED: Keep as string
            if year not in yearbooks:
                absolute_url = urljoin(ROOT, href)
                yearbooks[year] = {
                    "year": year,
                    "url": absolute_url,
                }
    
    # Sort by year (newest first)
    result = sorted(yearbooks.values(), key=lambda x: x["year"], reverse=True)
    logger.info(f"Found {len(result)} yearbook(s) on website")
    return result

def get_download_links(yearbook_url: str, session: requests.Session) -> list:
    """
    Extract all downloadable file links from a yearbook page.
    Returns list of dicts: {"url": str, "filename": str}
    """
    r = retrying_get(session, yearbook_url)
    soup = BeautifulSoup(r.text, "html.parser")
    
    links = []
    
    # Find all <a> tags
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        
        # Check if link ends with allowed extension
        url_path = urlparse(href).path.lower()
        if not any(url_path.endswith(ext) for ext in FILE_EXTS):
            continue
        
        # Construct absolute URL
        absolute_url = urljoin(BASE, href)
        filename = Path(url_path).name
        
        links.append({
            "url": absolute_url,
            "filename": filename,
        })
    
    # Deduplicate by URL
    seen_urls = set()
    unique_links = []
    for link in links:
        if link["url"] not in seen_urls:
            unique_links.append(link)
            seen_urls.add(link["url"])
    
    return unique_links

# Main
def main():
    session = get_session()
    manifest = DBManifest(source_id="dhsyearbook", file_type="yearbook", mode=MODE)

    counts = {"downloaded": 0, "versioned": 0, "skipped": 0, "unchanged": 0, "errors": 0}

    try:
        yearbooks = discover_yearbooks(session)
    except Exception as e:
        logger.error(f"[error] discovery failed ({e})")
        return

    if not yearbooks:
        logger.error("No yearbooks found on website")
        return

    for yb in yearbooks:
        year = yb["year"]
        yearbook_url = yb["url"]
        
        logger.info(f"Processing yearbook {year}")
        
        # Get all download links from this yearbook
        try:
            links = get_download_links(yearbook_url, session)
        except Exception as e:
            logger.error(f"Failed to get links for {year}: {e}")
            continue
        
        if not links:
            logger.info(f"Yearbook {year}: No files found")
            continue
        
        logger.info(f"Yearbook {year}: {len(links)} file(s) discovered")
        
        # Process each file
        for link in links:
            filename = link["filename"]
            file_url = link["url"]
            period = str(year)  # FIXED: Ensure period is string
            
            try:
                decision = manifest.plan(period, file_url)

                # Decision: skip
                if decision["decision"] == "skip":
                    counts["skipped"] += 1
                    logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
                    continue

                # Check if file already exists
                ydir = get_yearbook_outdir(year)
                expected_path = ydir / filename
                
                if expected_path.exists():
                    existing = manifest.get_existing(period, file_url)
                    if not existing:
                        # File exists but not in manifest - register it
                        if manifest.register_existing_file(period, file_url, str(expected_path)):
                            counts["downloaded"] += 1
                            logger.info(f"[registered] {period} {file_url} -> {expected_path}")
                        continue

                versioned = (decision["decision"] == "version")

                saved = manifest.download_and_record(
                    session, file_url, outdir=str(ydir), period=period, versioned=versioned
                )

                if saved:
                    if versioned:
                        counts["versioned"] += 1
                        logger.info(f"[new-version] {period} {file_url} -> {saved}")
                    else:
                        counts["downloaded"] += 1
                        logger.info(f"[downloaded] {period} {file_url} -> {saved}")
                    
                    # Extract zip files automatically
                    saved_path = Path(saved)
                    if saved_path.suffix.lower() == '.zip':
                        extract_zip_file(saved_path)
                else:
                    counts["unchanged"] += 1
                    logger.info(f"[unchanged] {period} {file_url} (no write)")

            except Exception as e:
                counts["errors"] += 1
                logger.error(f"[error] {period} {file_url} ({e})")

            time.sleep(POLITE_DELAY)
        
        time.sleep(POLITE_DELAY)  # Delay between yearbooks

    logger.info(
        "Yearbook summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )

if __name__ == "__main__":
    main()