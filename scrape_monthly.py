import argparse, os, re, time, hashlib, json
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_dt
from tqdm import tqdm

# ---------- SITE CONSTANTS (adjustable if State.gov changes) ----------
ROOT = "https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics.html"
KEY_TEXT_IV  = "Monthly Immigrant Visa (IV) Issuances"      # anchor text to IV landing
KEY_TEXT_NIV = "Monthly Nonimmigrant Visa (NIV) Issuances"   # anchor text to NIV landing

KW_IV_FSC   = ("FSC", "Foreign State of Chargeability", "Place of Birth")
KW_IV_POST  = ("Post",)
KW_NIV_NATL = ("Nationality",)
KW_NIV_POST = ("Post",)

RE_MONTH = re.compile(r"(January|February|March|April|May|June|July|August|September|October|November|December)|\b(20\d{2})[-_](0[1-9]|1[0-2])", re.IGNORECASE)
RE_FY    = re.compile(r"\bFY\s*([12]\d{3})\b|\b(FY)?(20\d{2})\b", re.IGNORECASE)

ALLOWED_EXTS = (".xlsx", ".xls", ".pdf")
POLITE_DELAY = 1.0
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# Output paths relative to script directory
BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_ROOT = BASE_DIR / "data" / "visa-statistics" / "monthly"
MANIFEST_PATH = BASE_DIR / "visa_monthlies_manifest.json"

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
    ym: str
    program: str     # iv | niv
    variant: str     # by-fsc | by-post | by-nationality
    downloaded_at: str


# ------------------------------------------------------------
# UTILS
# ------------------------------------------------------------
def log(msg): print(f"[monthly] {msg}", flush=True)

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

def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {"records": [], "_url_meta": {}}

def save_manifest(m): MANIFEST_PATH.write_text(json.dumps(m, indent=2))

def soup_from(session, url):
    r = get(session, url)
    return BeautifulSoup(r.text, "lxml")

def find_section_link(soup, key_text):
    for a in soup.select("a"):
        txt = (a.get_text() or "").strip()
        if key_text.lower() in txt.lower():
            href = a.get("href")
            if href:
                return urljoin(ROOT, href)
    return None

def list_links_recursive(session, index_url):
    """Crawl landing and FY pages; return (abs_url, link_text, context_text)."""
    seen, queue, results = set(), [index_url], []
    while queue:
        url = queue.pop(0)
        if url in seen: continue
        seen.add(url)

        s = soup_from(session, url)

        # enqueue likely FY subpages
        for a in s.select("a"):
            href = a.get("href"); 
            if not href: continue
            absu = urljoin(url, href)
            if absu.startswith("https://travel.state.gov/") and any(y in (a.get_text() or "") for y in ["FY","Fiscal","20"]):
                if urlparse(absu).path != urlparse(url).path:
                    queue.append(absu)

        # collect files (xlsx/xls/pdf)
        for a in s.select('a[href]'):
            href = a.get("href"); 
            if not href: continue
            absu = urljoin(url, href)
            if not absu.lower().endswith(ALLOWED_EXTS): 
                continue
            text = " ".join((a.get_text() or "").split())
            ctx = text
            parent = a.find_parent()
            if parent:
                ctx = (ctx + " " + " ".join(parent.get_text(" ").split())).strip()
            results.append((absu, text, ctx))

    uniq, seenu = [], set()
    for r in results:
        if r[0] in seenu: continue
        seenu.add(r[0]); uniq.append(r)
    return uniq

def infer_fy(text):
    m = RE_FY.search(text or "")
    if not m: return None
    year = m.group(1) or m.group(3)
    return f"FY{year}"

def infer_month_ym(text):
    t = text or ""
    m = RE_MONTH.search(t)
    if not m: return None
    if m.group(1):  # month name + year
        mon = m.group(1).lower()
        yearm = re.search(r"(20\d{2})", t)
        if not yearm: return None
        yr = yearm.group(1)
        mm = {
            "october":"10","november":"11","december":"12","january":"01","february":"02","march":"03",
            "april":"04","may":"05","june":"06","july":"07","august":"08","september":"09"
        }[mon]
        return f"{yr}-{mm}"
    else:
        yr, mm = m.group(2), m.group(3)
        return f"{yr}-{mm}"

def classify_variant(program, text):
    t = (text or "").lower()
    if program == "iv":
        if any(k.lower() in t for k in KW_IV_FSC):  return "by-fsc"
        if any(k.lower() in t for k in KW_IV_POST): return "by-post"
    else:
        if any(k.lower() in t for k in KW_NIV_NATL): return "by-nationality"
        if any(k.lower() in t for k in KW_NIV_POST): return "by-post"
    return None

def check_changed(session, url, manifest):
    h = head(session, url)
    if not h: return True
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

def ensure_month_pairs(found_by_variant, expect_variants, label):
    missing = [v for v in expect_variants if v not in found_by_variant]
    if missing:
        log(f"WARNING: {label} missing variants: {', '.join(missing)}")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=POLITE_DELAY)
    args = ap.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "GaleVisaScraper/Monthly/1.1 (+research use)"})

    # robots.txt (informational)
    try:
        robots = urljoin(ROOT, "/robots.txt")
        r = get(session, robots)
        if r.status_code == 200:
            log("robots.txt checked (1s delay + polite rate limiting).")
    except Exception:
        pass

    manifest = load_manifest()
    root_soup = soup_from(session, ROOT)
    links = {
        "iv":  find_section_link(root_soup, KEY_TEXT_IV),
        "niv": find_section_link(root_soup, KEY_TEXT_NIV),
    }
    if not links["iv"] or not links["niv"]:
        log("Could not locate IV/NIV landing links—site layout may have changed.")
        return

    for program in ("iv","niv"):
        landing = links[program]
        log(f"Scanning {program.upper()} landing: {landing}")
        items = list_links_recursive(session, landing)

        index = {}
        for url, text, ctx in items:
            if not url.lower().endswith(ALLOWED_EXTS): 
                continue
            fy  = infer_fy(ctx) or infer_fy(text) or "FY-unknown"
            ym  = infer_month_ym(ctx) or infer_month_ym(text) or "unknown"
            variant = classify_variant(program, ctx + " " + text)
            if not variant: 
                continue
            fname = os.path.basename(urlparse(url).path)
            index.setdefault((fy, ym), {}).setdefault(variant, []).append((url, fname))

        expect = ("by-fsc","by-post") if program=="iv" else ("by-nationality","by-post")

        for (fy, ym), variants in sorted(index.items()):
            ensure_month_pairs(variants, expect, f"{program.upper()} {fy} {ym}")
            for v in expect:
                if v not in variants: 
                    continue
                url, fname = variants[v][0]

                if not check_changed(session, url, manifest):
                    log(f"Skip unchanged: {url}")
                    continue

                ext = os.path.splitext(fname)[1].lower().lstrip(".")
                dest = DOWNLOAD_ROOT / program / v / fy / ym / fname

                try:
                    sha, size, ts = stream_download(session, url, dest)
                    rec = Record(
                        url=url, filetype=ext, stream_sha256=sha, bytes=size,
                        saved_to=str(dest), fy=fy, ym=ym, program=program, variant=v,
                        downloaded_at=ts
                    )
                    manifest["records"].append(asdict(rec))
                    save_manifest(manifest)
                    log(f"Saved {dest}")
                except Exception as e:
                    log(f"ERROR downloading {url}: {e}")
                time.sleep(args.delay)

    log(f"Done. Manifest at {MANIFEST_PATH}")

if __name__ == "__main__":
    main()
