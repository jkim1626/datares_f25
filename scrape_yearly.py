import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from db_manifest import DBManifest
from paths import get_annual_outdir

# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Configs
ROOT = "https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics.html"
INDEX_ANCHOR_TEXT = "Report of the Visa Office"
FILE_EXTS = (".pdf", ".xlsx", ".xls", ".csv")

MODE = "safe"  # dedup mode
POLITE_DELAY = 0.5  # seconds between requests


# Helpers
def retry_get(session, url, stream=False, attempts=3, timeout=(10, 30)):
    backoff = 1.0
    for i in range(attempts):
        try:
            r = session.get(url, timeout=timeout, stream=stream)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"retryable {r.status_code}")
            r.raise_for_status()
            return r
        except Exception as e:
            if i == attempts - 1:
                logger.error(f"GET failed {url}: {e}")
                raise
            logger.warning(f"GET retry {i+1} for {url}: {e}")
            time.sleep(backoff)
            backoff *= 2


def extract_year(text):
    m = re.search(r"\b(20\d{2})\b", text)
    return m.group(1) if m else None


def discover_year_pages(session):
    """
    Returns [(year, year_page_url)] for every 'Report of the Visa Office {YEAR}' link.
    """
    resp = retry_get(session, ROOT)
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    
    for a in soup.select("a"):
        href = a.get("href")
        if not href:
            continue
        text = (a.get_text() or "").strip()
        if INDEX_ANCHOR_TEXT in text:
            year = extract_year(text) or extract_year(href) or ""
            if not year:
                continue
            results.append((year, urljoin(ROOT, href)))
    
    # Deduplicate and sort newest→oldest
    seen = set()
    uniq = []
    for y, u in results:
        if (y, u) in seen:
            continue
        seen.add((y, u))
        uniq.append((y, u))
    
    uniq.sort(key=lambda t: t[0], reverse=True)
    return uniq


def collect_files_for_year(session, year_page_url):
    resp = retry_get(session, year_page_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    
    for a in soup.select("a"):
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(year_page_url, href)
        if any(abs_url.lower().endswith(ext) for ext in FILE_EXTS):
            links.append(abs_url)
    
    # Deduplicate
    seen = set()
    uniq = []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    
    return uniq


# Main
def main():
    manifest = DBManifest(source_id="visastats", file_type="annual", mode=MODE)
    
    counts = {"downloaded": 0, "versioned": 0, "skipped": 0, "unchanged": 0, "errors": 0}
    
    with requests.Session() as session:
        year_pages = discover_year_pages(session)
        if not year_pages:
            raise RuntimeError("No 'Report of the Visa Office {YEAR}' links found on the index page")

        logger.info(f"Found {len(year_pages)} yearly pages on index")

        for year, year_url in year_pages:
            try:
                files = collect_files_for_year(session, year_url)
            except Exception as e:
                logger.error(f"Failed to collect files for {year} ({year_url}): {e}")
                continue

            if not files:
                logger.warning(f"No files found on {year} page: {year_url}")
                continue

            ydir = get_annual_outdir(year)
            logger.info(f"{year}: {len(files)} file(s) discovered")

            for file_url in files:
                try:
                    decision = manifest.plan(year, file_url)

                    # Decision: skip (based purely on manifest/metadata)
                    if decision["decision"] == "skip":
                        counts["skipped"] += 1
                        logger.info(f"[skipped] {year} {file_url} ({decision['reason']})")
                        continue

                    # Check if file already exists
                    url_name = file_url.split("?")[0].rstrip("/").split("/")[-1] or f"{year}.bin"
                    expected_path = ydir / url_name

                    if expected_path.exists():
                        existing = manifest.get_existing(year, file_url)
                        if not existing:
                            # File exists but not in manifest - register it
                            if manifest.register_existing_file(year, file_url, str(expected_path)):
                                counts["downloaded"] += 1
                                logger.info(f"[registered] {year} {file_url} -> {expected_path}")
                            continue

                    versioned = (decision["decision"] == "version")
                    saved = manifest.download_and_record(
                        session, file_url, outdir=str(ydir), period=year, versioned=versioned
                    )

                    if saved:
                        if versioned:
                            counts["versioned"] += 1
                            logger.info(f"[new-version] {year} {file_url} -> {saved}")
                        else:
                            counts["downloaded"] += 1
                            logger.info(f"[downloaded] {year} {file_url} -> {saved}")
                    else:
                        counts["unchanged"] += 1
                        logger.info(f"[unchanged] {year} {file_url} (no write)")

                except Exception as e:
                    counts["errors"] += 1
                    logger.error(f"[error] {year} {file_url} ({e})")
                
                time.sleep(POLITE_DELAY)

    logger.info(
        "Annual summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )


if __name__ == "__main__":
    main()
