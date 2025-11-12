import logging, re, time
from urllib.parse import urljoin, urlparse
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from db_manifest import DBManifest
from paths import get_uscis_outdir

# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Configs
BASE_URLS = {
    'h1b': 'https://www.uscis.gov/archive/h-1b-employer-data-hub-files',
    'h2a': 'https://www.uscis.gov/archive/h-2a-employer-data-hub-files',
    'h2b': 'https://www.uscis.gov/archive/h-2b-employer-data-hub-files'
}

FILE_EXTS = (".csv", ".xls", ".xlsx", ".zip")

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
def extract_year(filename: str) -> str:
    """
    Extract fiscal or calendar year from filename.
    Returns year string or None if not found.
    """
    # Try FY pattern: FY2024, FY24
    fy_match = re.search(r"FY\s*?(\d{2,4})", filename, re.IGNORECASE)
    if fy_match:
        year_str = fy_match.group(1)
        return f"20{year_str}" if len(year_str) == 2 else year_str
    
    # Try 4-digit year: 2024, 2023
    year_match = re.search(r"\b(20\d{2})\b", filename)
    if year_match:
        return year_match.group(1)
    
    return None

def make_period(visa_type: str, filename: str) -> str:
    """
    Create period string: "h1b/2024" or "h1b/misc" if no year found.
    """
    year = extract_year(filename)
    if year:
        return f"{visa_type}/{year}"
    else:
        return f"{visa_type}/misc"

# Discovery functions
def get_download_links(url: str, session: requests.Session) -> list:
    """
    Extract all downloadable file links from a USCIS data hub page.
    Returns list of dicts: {"url": str, "filename": str}
    """
    r = retrying_get(session, url)
    soup = BeautifulSoup(r.text, "html.parser")
    
    links = []
    
    # Find all links to CSV, Excel, or ZIP files
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        
        # Check if link ends with allowed extension
        if not any(href.lower().endswith(ext) for ext in FILE_EXTS):
            continue
        
        # Construct absolute URL
        full_url = urljoin(url, href)
        
        # Get filename from URL path
        filename = Path(urlparse(full_url).path).name
        if not filename:
            # Fallback: generate from URL hash
            import hashlib
            filename = f"file_{hashlib.md5(full_url.encode()).hexdigest()[:8]}.csv"
        
        links.append({
            "url": full_url,
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

    counts = {"downloaded": 0, "versioned": 0, "skipped": 0, "unchanged": 0, "errors": 0}

    # Process each visa type
    for visa_type, base_url in BASE_URLS.items():
        logger.info(f"Processing {visa_type.upper()} from {base_url}")
        
        # Create manifest for this visa type
        manifest = DBManifest(source_id="uscis", file_type="uscis", mode=MODE, program=visa_type)
        
        # Get all download links
        try:
            links = get_download_links(base_url, session)
        except Exception as e:
            logger.error(f"Failed to get links for {visa_type.upper()}: {e}")
            continue
        
        if not links:
            logger.info(f"{visa_type.upper()}: No files found")
            continue
        
        logger.info(f"{visa_type.upper()}: {len(links)} file(s) discovered")
        
        # Process each file
        for link in links:
            filename = link["filename"]
            file_url = link["url"]
            
            # Determine period (visa_type/year or visa_type/misc)
            period = make_period(visa_type, filename)
            
            try:
                decision = manifest.plan(period, file_url)

                # Decision: skip
                if decision["decision"] == "skip":
                    counts["skipped"] += 1
                    logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
                    continue

                # Check if file already exists
                # Extract year from period: "h1b/2024" -> "2024"
                year_or_misc = period.split('/')[1]
                vdir = get_uscis_outdir(visa_type, year_or_misc)
                expected_path = vdir / filename
                
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
                    session, file_url, outdir=str(vdir), period=period, versioned=versioned
                )

                if saved:
                    if versioned:
                        counts["versioned"] += 1
                        logger.info(f"[new-version] {period} {file_url} -> {saved}")
                    else:
                        counts["downloaded"] += 1
                        logger.info(f"[downloaded] {period} {file_url} -> {saved}")
                else:
                    counts["unchanged"] += 1
                    logger.info(f"[unchanged] {period} {file_url} (no write)")

            except Exception as e:
                counts["errors"] += 1
                logger.error(f"[error] {period} {file_url} ({e})")

            time.sleep(POLITE_DELAY)
        
        time.sleep(POLITE_DELAY)  # Delay between visa types

    logger.info(
        "USCIS summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )

if __name__ == "__main__":
    main()
