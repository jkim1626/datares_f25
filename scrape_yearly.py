import argparse, os, re, time, hashlib, json
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_dt
from tqdm import tqdm

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
ROOT = "https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics.html"
ANNUAL_INDEX_HINT = "/legal/visa-law0/visa-statistics/annual-reports.html"

KEY_TEXT_ANNUAL = "Report of the Visa Office"

ALLOWED_EXTS = (".pdf", ".xlsx", ".xls")  # warn/skip others
POLITE_DELAY = 1.0
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Output paths relative to this script
BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_ROOT = BASE_DIR / "data" / "visa-statistics" / "annual" / "visa-office-report"
MANIFEST_PATH = BASE_DIR / "visa_annual_manifest.json"

# ------------------------------------------------------------
# DATA STRUCTURE
# ------------------------------------------------------------
@dataclass
class Record:
    url: str
    filetype: str
    stream_sha256: str
    bytes: int
    saved_to: str
    fy: str
    downloaded_at: str


# ------------------------------------------------------------
# HTTP + HELPERS
# ------------------------------------------------------------
def log(msg): print(f"[annual] {msg}", flush=True)

def get(session, url, stream=False, attempts=MAX_RETRIES):
    backoff = 1.0
    for i in range(attempts):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=stream)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable status {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception:
            if i == attempts - 1: raise
            time.sleep(backoff); backoff *= 2

def head(session, url, attempts=MAX_RETRIES):
    backoff = 1.0
    for i in range(attempts):
        try:
            resp = session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Retryable status {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception:
            if i == attempts - 1: return None
            time.sleep(backoff); backoff *= 2

def soup_from(session, url):
    r = get(session, url)
    return BeautifulSoup(r.text, "lxml")

def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {"records": [], "_url_meta": {}}

def save_manifest(m): MANIFEST_PATH.write_text(json.dumps(m, indent=2))


# ------------------------------------------------------------
# PAGE DISCOVERY
# ------------------------------------------------------------
def find_annual_overview(session):
    """
    Find the 'Annual Reports' overview page that lists 'Report of the Visa Office {YEAR}' links.
    Strategy:
      1) From ROOT (Visa Statistics), click 'Annual Reports'.
      2) If we already have a single-year page URL, follow the breadcrumb back to 'Annual Reports'.
    """
    # Step 1: start at ROOT and find 'Annual Reports'
    root = soup_from(session, ROOT)
    for a in root.select("a[href]"):
        txt = (a.get_text() or "").strip().lower()
        href = a.get("href")
        if not href: 
            continue
        if "annual reports" in txt:
            return urljoin(ROOT, href)

    # Fallback: guess by known path segment (site is stable)
    return urljoin(ROOT, ANNUAL_INDEX_HINT)

FY_LINK_RE = re.compile(r"Report of the Visa Office\s*(\d{4})", re.IGNORECASE)

def list_fy_pages(session, annual_overview_url):
    """
    Return [(FYxxxx, url), ...] by scanning the Annual Reports overview.
    Matches link text like 'Report of the Visa Office 2024'.
    """
    s = soup_from(session, annual_overview_url)
    out = []
    seen = set()
    for a in s.select("a[href]"):
        txt = (a.get_text() or "").strip()
        m = FY_LINK_RE.search(txt or "")
        if not m:
            continue
        year = m.group(1)
        fy = f"FY{year}"
        u = urljoin(annual_overview_url, a.get("href"))
        if u not in seen:
            seen.add(u)
            out.append((fy, u))
    return out

def list_files_for_fy(session, fy_url):
    """Return all allowed file links (pdf/xlsx/xls) on the FY page."""
    s = soup_from(session, fy_url)
    results = []
    for a in s.select('a[href$=".pdf"], a[href$=".xlsx"], a[href$=".xls"], a[href$=".PDF"], a[href$=".XLSX"], a[href$=".XLS"]'):
        href = a.get("href")
        if not href:
            continue
        absu = urljoin(fy_url, href)
        ext = os.path.splitext(absu)[1].lower()
        if ext in ALLOWED_EXTS:
            fname = os.path.basename(urlparse(absu).path)
            results.append((absu, fname, ext))
        else:
            # warn on other file types with an extension
            if "." in os.path.basename(urlparse(absu).path):
                log(f"WARNING: skipping unsupported file type ({ext}) at {absu}")
    # de-dupe
    uniq, seen = [], set()
    for r in results:
        if r[0] in seen: 
            continue
        seen.add(r[0]); uniq.append(r)
    return uniq


# ------------------------------------------------------------
# DOWNLOAD LOGIC
# ------------------------------------------------------------
def check_changed(session, url, manifest):
    h = head(session, url)
    if not h: return True  # allow once if HEAD fails
    etag = h.headers.get("ETag"); lm = h.headers.get("Last-Modified")
    key = f"{etag}|{lm}"
    prev = manifest["_url_meta"].get(url)
    if prev == key: return False
    manifest["_url_meta"][url] = key
    return True

def stream_download(session, url, dest):
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = get(session, url, stream=True)
    total = int(r.headers.get("Content-Length", 0) or 0)
    sha, size = hashlib.sha256(), 0
    with open(dest, "wb") as f, tqdm(total=total or None, unit="B", unit_scale=True, desc=dest.name) as p:
        for chunk in r.iter_content(1024 * 256):
            if not chunk: continue
            f.write(chunk); sha.update(chunk); size += len(chunk)
            if p.total: p.update(len(chunk))
    ts = r.headers.get("Last-Modified")
    try:
        ts = parse_dt(ts).isoformat() if ts else time.strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        ts = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    return sha.hexdigest(), size, ts


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=POLITE_DELAY)
    args = ap.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "GaleVisaScraper/Annual/1.1 (+research use)"})

    # robots.txt (informational)
    try:
        robots = urljoin(ROOT, "/robots.txt")
        r = get(session, robots)
        if r.status_code == 200:
            log("robots.txt checked (polite crawling enabled).")
    except Exception:
        pass

    manifest = load_manifest()

    # Find the Annual Reports overview page that lists all years
    annual_overview = find_annual_overview(session)
    log(f"Annual reports overview: {annual_overview}")

    fy_pages = list_fy_pages(session, annual_overview)
    if not fy_pages:
        log("No FY links found on the overview page. Site layout may have changed.")
        return

    log(f"Discovered {len(fy_pages)} fiscal year pages.")

    # Iterate all FY pages and download allowed files
    for fy, fy_url in fy_pages:
        log(f"\n=== Processing {fy} ===")
        try:
            files = list_files_for_fy(session, fy_url)
        except Exception as e:
            log(f"ERROR parsing FY page {fy_url}: {e}")
            continue

        if not files:
            log(f"No files found for {fy}.")
            continue

        for url, fname, ext in files:
            if not check_changed(session, url, manifest):
                log(f"Skip unchanged: {url}")
                continue

            dest = DOWNLOAD_ROOT / fy / fname
            try:
                sha, size, ts = stream_download(session, url, dest)
                rec = Record(
                    url=url, filetype=ext.lstrip("."), stream_sha256=sha,
                    bytes=size, saved_to=str(dest), fy=fy, downloaded_at=ts
                )
                manifest["records"].append(asdict(rec))
                save_manifest(manifest)
                log(f"Saved {dest}")
            except Exception as e:
                log(f"ERROR downloading {url}: {e}")
            time.sleep(args.delay)

    log(f"\nDone. Manifest written to {MANIFEST_PATH}")

if __name__ == "__main__":
    main()
