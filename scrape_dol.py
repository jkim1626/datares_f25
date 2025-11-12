import logging, re, time
from urllib.parse import urljoin, urlsplit, urlunsplit
import requests
from bs4 import BeautifulSoup
from db_manifest import DBManifest
from paths import get_dol_outdir

# Configure logging to stdout for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Configs
ROOT = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
FILE_EXTS = (".pdf", ".xlsx", ".xls", ".csv", ".docx", ".doc", ".zip")

MODE = "safe"
POLITE_DELAY = 0.5

# Skip deprecated annual reports
SKIP_PATTERNS = [
    "annual performance report",
    "fy 2016 report", "fy 2015 report", "fy 2014 report", "fy 2013 report",
    "fy 2012 report", "fy 2011 report", "fy 2010 report", "fy 2009 report",
    "fy 2007 report", "fy 2006 report",
]

# Program detection mapping
PROGRAM_MAP = {
    "perm": "PERM Program",
    "lca": "LCA Program",
    "h-1b": "LCA Program",
    "h1b": "LCA Program",
    "pw": "Prevailing Wage Program",
    "prevailing": "Prevailing Wage Program",
    "h-2a": "H-2A Program",
    "h2a": "H-2A Program",
    "h-2b": "H-2B Program",
    "h2b": "H-2B Program",
    "cw-1": "CW-1 Program",
    "cw1": "CW-1 Program",
}

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
def normalize_url(url: str) -> str:
    """Normalize URL so duplicates always match."""
    parsed = urlsplit(url.strip())
    return urlunsplit((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path.rstrip('/'),
        parsed.query,
        parsed.fragment,
    ))

def clean_program_name(name: str) -> str:
    """Sanitize folder names."""
    name = re.sub(r'[\\/*?:"<>|]', "_", name.strip())
    if len(name) > 80:
        name = name[:80]
    return name

def extract_year(filename: str) -> str:
    """Extract fiscal or calendar year from filename."""
    match = re.search(r"fy\s*(\d{2,4})", filename, re.IGNORECASE)
    if match:
        token = match.group(1)
        return f"20{token}" if len(token) == 2 else token
    match = re.search(r"(19|20)\d{2}", filename)
    return match.group(0) if match else "unknown_year"

def detect_program_from_filename(filename: str) -> str:
    """Detect program from filename as fallback."""
    filename_lower = filename.lower()
    for key, val in PROGRAM_MAP.items():
        if key in filename_lower:
            return val
    return None

def should_skip_file(filename: str, text_context: str = "") -> bool:
    """Check if file should be skipped (deprecated annual reports)."""
    combined = (filename + " " + text_context).lower()
    
    # Skip if matches deprecated patterns (but not "record layout" PDFs)
    if "record layout" not in combined and "record_layout" not in combined:
        for pattern in SKIP_PATTERNS:
            if pattern in combined:
                return True
        
        # Skip PDF annual reports specifically (but not layouts)
        if "annual" in combined and "report" in combined and filename.lower().endswith(".pdf"):
            return True
    
    return False

def parse_table_links(soup: BeautifulSoup) -> list:
    """Parse download links from table format (Latest Quarterly Updates)."""
    table_links = []
    
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            
            # First cell typically has program name
            program_cell = cells[0].get_text(strip=True)
            
            # Remaining cells have file links
            for cell in cells[1:]:
                for link in cell.find_all("a", href=True):
                    href = link["href"]
                    if not any(href.lower().endswith(ext) for ext in FILE_EXTS):
                        continue
                    
                    filename = href.split("/")[-1]
                    
                    # Skip deprecated files
                    if should_skip_file(filename, program_cell):
                        logger.info(f"Skipping deprecated: {filename}")
                        continue
                    
                    full_url = urljoin(ROOT, href)
                    year = extract_year(filename)
                    
                    # Detect program from table cell or filename
                    current_program = None
                    for key, val in PROGRAM_MAP.items():
                        if key in program_cell.lower() or key in filename.lower():
                            current_program = val
                            break
                    
                    if not current_program:
                        current_program = detect_program_from_filename(filename)
                    
                    if not current_program:
                        current_program = "Uncategorized"
                    
                    table_links.append({
                        "program": current_program,
                        "url": normalize_url(full_url),
                        "filename": filename,
                        "year": year
                    })
    
    return table_links

def discover_files(session: requests.Session) -> list:
    """
    Returns list of dicts: {"program": str, "year": str, "url": str, "filename": str}
    """
    r = retrying_get(session, ROOT)
    soup = BeautifulSoup(r.text, "html.parser")
    
    download_links = []
    
    # Parse table-based links
    logger.info("Parsing table-based links...")
    table_links = parse_table_links(soup)
    logger.info(f"Found {len(table_links)} file(s) from tables")
    download_links.extend(table_links)
    
    # Parse all links on page
    logger.info("Scanning all links on page...")
    all_links = soup.find_all("a", href=True)
    
    for link in all_links:
        href = link["href"]
        href_lower = href.lower()
        
        if not any(href_lower.endswith(ext) for ext in FILE_EXTS):
            continue
        
        filename = href.split("/")[-1]
        
        if should_skip_file(filename):
            continue
        
        program = detect_program_from_filename(filename)
        
        # Try parent context
        if not program:
            parent = link.find_parent(["td", "p", "li", "div"])
            if parent:
                context = parent.get_text(strip=True)
                for key, val in PROGRAM_MAP.items():
                    if key in context.lower():
                        program = val
                        break
        
        # Try preceding headings
        if not program:
            for prev in link.find_all_previous(["h2", "h3", "h4", "strong", "b"]):
                text = prev.get_text(strip=True).lower()
                for key, val in PROGRAM_MAP.items():
                    if key in text and "annual" not in text:
                        program = val
                        break
                if program:
                    break
        
        if not program:
            program = "Uncategorized"
        
        full_url = urljoin(ROOT, href)
        normalized_url = normalize_url(full_url)
        year = extract_year(filename)
        
        # Deduplicate by URL
        if any(item["url"] == normalized_url for item in download_links):
            continue
        
        download_links.append({
            "program": program,
            "url": normalized_url,
            "filename": filename,
            "year": year
        })
    
    logger.info(f"Found {len(download_links)} total file(s) discovered")
    return download_links

# Main
def main():
    session = get_session()
    
    counts = {"downloaded": 0, "versioned": 0, "skipped": 0, "unchanged": 0, "errors": 0}

    try:
        candidates = discover_files(session)
    except Exception as e:
        logger.error(f"[error] discovery failed ({e})")
        return

    # Group by program for separate manifests
    programs = {}
    for item in candidates:
        prog = item["program"]
        if prog not in programs:
            programs[prog] = []
        programs[prog].append(item)
    
    # Process each program
    for program, items in programs.items():
        logger.info(f"Processing {program}: {len(items)} file(s)")
        
        # Create manifest for this program
        manifest = DBManifest(source_id="dolstats", file_type="dol", mode=MODE, program=program)
        
        for item in items:
            year = item["year"]
            file_url = item["url"]
            filename = item["filename"]
            
            # Period format: "PERM Program/2024"
            period = f"{program}/{year}"
            
            try:
                decision = manifest.plan(period, file_url)

                # Decision: skip
                if decision["decision"] == "skip":
                    counts["skipped"] += 1
                    logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
                    continue

                # Check if file already exists
                pdir = get_dol_outdir(program, year)
                expected_path = pdir / filename
                
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
                    session, file_url, outdir=str(pdir), period=period, versioned=versioned
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

    logger.info(
        "DOL summary: "
        f"downloaded={counts['downloaded']}, new_versions={counts['versioned']}, "
        f"skipped={counts['skipped']}, unchanged={counts['unchanged']}, errors={counts['errors']}"
    )

if __name__ == "__main__":
    main()
