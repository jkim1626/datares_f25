import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_dt
from helpers.db_manifest import DBManifest
from helpers.paths import get_monthly_outdir

# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Configs
ROOT = "https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics.html"
KEY_TEXT_IV = "Monthly Immigrant Visa (IV) Issuances"
KEY_TEXT_NIV = "Monthly Nonimmigrant Visa (NIV) Issuances"
FILE_EXTS = (".pdf", ".xlsx", ".xls", ".csv")

MODE = "safe"  # dedup mode
POLITE_DELAY = 0.4  # seconds between requests

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


# Parsing Helpers
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12
}


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def extract_period(text: str, href: str) -> str:
    """
    Return a period string like 'FY2024-10' if possible, else
    just 'FY2024' or fallback 'misc'.
    """
    t = normalize_whitespace(text)
    h = href or ""

    # Prefer FY style if present
    mfy = re.search(r"\bFY\s*?(20\d{2})\b", f"{t} {h}", flags=re.I)
    year = None
    if mfy:
        year = int(mfy.group(1))

    # Try to find month in text or href
    mmatch = None
    for k, v in MONTHS.items():
        if re.search(rf"\b{k}\b", t, flags=re.I) or re.search(rf"\b{k}\b", h, flags=re.I):
            mmatch = v
            break

    # If no explicit month words, try a general date parse
    if not mmatch:
        try:
            dt = parse_dt(t, fuzzy=True, default=None)
            if dt:
                mmatch = dt.month
                if not year:
                    year = dt.year
        except Exception:
            pass

    # Try to detect year if FY not present
    if not year:
        m2 = re.search(r"\b(20\d{2})\b", f"{t} {h}")
        if m2:
            year = int(m2.group(1))

    if year and mmatch:
        return f"FY{year}-{mmatch:02d}"
    if year:
        return f"FY{year}"
    return "misc"


def find_anchor_by_text(soup: BeautifulSoup, text_substring: str):
    for a in soup.find_all("a"):
        if text_substring.lower() in (a.get_text() or "").strip().lower():
            href = a.get("href")
            if href:
                return urljoin(ROOT, href)
    return None


def discover_monthly_files(session: requests.Session):
    """
    Returns list of tuples: (program, period, file_url)
    program: "IV" or "NIV"
    """
    r = retrying_get(session, ROOT)
    root = BeautifulSoup(r.text, "html.parser")

    pages = []
    iv_url = find_anchor_by_text(root, KEY_TEXT_IV)
    niv_url = find_anchor_by_text(root, KEY_TEXT_NIV)
    if iv_url:
        pages.append(("IV", iv_url))
    if niv_url:
        pages.append(("NIV", niv_url))

    results = []
    for program, page_url in pages:
        pr = retrying_get(session, page_url)
        ps = BeautifulSoup(pr.text, "html.parser")

        links = []
        for a in ps.find_all("a"):
            href = a.get("href") or ""
            if any(href.lower().endswith(ext) for ext in FILE_EXTS):
                links.append((a.get_text() or href, urljoin(page_url, href)))

        # Deduplicate by final URL
        seen = set()
        uniq = []
        for txt, href in links:
            if href not in seen:
                seen.add(href)
                uniq.append((txt, href))

        logger.info(f"{program}: {len(uniq)} file(s) discovered")

        for txt, href in uniq:
            period = extract_period(txt, href)
            results.append((program, period, href))

    return results


# Main
def main():
    session = get_session()
    
    # Separate manifest for IV and NIV
    iv_manifest = DBManifest(source_id="visastats", file_type="monthly", mode=MODE, program="IV")
    niv_manifest = DBManifest(source_id="visastats", file_type="monthly", mode=MODE, program="NIV")

    counts = {"downloaded": 0, "versioned": 0, "skipped": 0, "unchanged": 0, "errors": 0}

    try:
        candidates = discover_monthly_files(session)
    except Exception as e:
        logger.error(f"[error] discovery failed ({e})")
        return

    for program, period, file_url in candidates:
        # Choose the right manifest based on program
        manifest = iv_manifest if program == "IV" else niv_manifest
        
        try:
            decision = manifest.plan(period, file_url)

            # Decision: skip (based purely on manifest/metadata)
            if decision["decision"] == "skip":
                counts["skipped"] += 1
                logger.info(f"[skipped] {program} {period} {file_url} ({decision['reason']})")
                continue

            # Check if file already exists in expected location
            pdir = get_monthly_outdir(program, period)
            url_name = file_url.split("?")[0].rstrip("/").split("/")[-1] or f"{period}.bin"
            expected_path = pdir / url_name

            if expected_path.exists():
                existing = manifest.get_existing(period, file_url)
                if not existing:
                    # File exists but not in manifest - register it
                    if manifest.register_existing_file(period, file_url, str(expected_path)):
                        counts["downloaded"] += 1
                        logger.info(f"[registered] {program} {period} {file_url} -> {expected_path}")
                    continue

            versioned = (decision["decision"] == "version")

            # Download and record
            saved = manifest.download_and_record(
                session, file_url, outdir=str(pdir), period=period, versioned=versioned
            )

            if saved:
                if versioned:
                    counts["versioned"] += 1
                    logger.info(f"[new-version] {program} {period} {file_url} -> {saved}")
                else:
                    counts["downloaded"] += 1
                    logger.info(f"[downloaded] {program} {period} {file_url} -> {saved}")
            else:
                counts["unchanged"] += 1
                logger.info(f"[unchanged] {program} {period} {file_url} (no write)")

        except Exception as e:
            counts["errors"] += 1
            logger.error(f"[error] {program} {period} {file_url} ({e})")

        time.sleep(POLITE_DELAY)

    logger.info(
        "Monthly summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )


if __name__ == "__main__":
    main()
