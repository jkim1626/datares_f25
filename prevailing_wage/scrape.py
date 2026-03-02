"""
Scrape all downloadable files by program (PERM, LCA, H-2A, etc.)
from: https://www.dol.gov/agencies/eta/foreign-labor/performance

Features:
- Groups files by Program // Year // File
- Deduplicates using manifest.json
- Skips deprecated Annual Reports
- Atomic writes with rollback support
- Versioned logging
- Manifest recovery (delegates cleanup to cleanup.py)
"""

import os
import re
import json
import hashlib
import requests
import shutil
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, urlunsplit
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional
import tempfile

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

BASE_URL = "https://www.dol.gov/agencies/eta/foreign-labor/performance"
SAVE_DIR = "data"
VALID_EXTS = (".xlsx", ".csv", ".pdf", ".docx", ".doc", ".zip", ".xls")

# Skip deprecated annual reports
SKIP_PATTERNS = [
    "annual performance report",
    "fy 2016 report",
    "fy 2015 report",
    "fy 2014 report",
    "fy 2013 report",
    "fy 2012 report",
    "fy 2011 report",
    "fy 2010 report",
    "fy 2009 report",
    "fy 2007 report",
    "fy 2006 report",
]

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

os.makedirs(SAVE_DIR, exist_ok=True)
manifest_path = os.path.join(SAVE_DIR, "manifest.json")
log_dir = os.path.join(SAVE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)

# ---------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------

def setup_logging():
    """Setup versioned logging to file and console."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"scrape_{timestamp}.log")
    
    logger = logging.getLogger("scraper")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    
    # File handler with detailed format
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler with simpler format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging to: {log_file}")
    return logger

logger = setup_logging()

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------

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

def load_manifest() -> Dict:
    """
    Load manifest.json with fallback to backup.
    
    Note: Manifest validation/cleanup is handled by cleanup.py.
    This function just loads the manifest or creates empty one.
    """
    # Try primary manifest
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
                logger.info(f"Loaded {len(manifest)} entries from manifest")
                return manifest
        except json.JSONDecodeError as e:
            logger.error(f"Manifest corrupted: {e}")
            
            # Try backup
            backup_path = f"{manifest_path}.bak"
            if os.path.exists(backup_path):
                logger.info("Attempting to restore from backup...")
                try:
                    with open(backup_path, "r") as f:
                        manifest = json.load(f)
                        logger.info(f"Restored {len(manifest)} entries from backup")
                        return manifest
                except json.JSONDecodeError:
                    logger.error("Backup also corrupted")
    
    # Return empty manifest if nothing exists or all corrupted
    logger.warning("No valid manifest found - starting fresh")
    logger.info("Tip: Run cleanup.py after scraping to validate manifest")
    return {}

def save_manifest(manifest: Dict):
    """
    Atomically save manifest.json with backup.
    Uses atomic write pattern: write to temp file, then rename.
    """
    # Create backup of existing manifest
    if os.path.exists(manifest_path):
        backup_path = f"{manifest_path}.bak"
        try:
            shutil.copy2(manifest_path, backup_path)
            logger.debug(f"Created manifest backup")
        except Exception as e:
            logger.error(f"Failed to create manifest backup: {e}")
    
    # Write to temporary file first (atomic write pattern)
    temp_fd, temp_path = tempfile.mkstemp(
        dir=SAVE_DIR,
        prefix=".manifest_",
        suffix=".json.tmp"
    )
    
    try:
        with os.fdopen(temp_fd, 'w') as f:
            json.dump(manifest, f, indent=2)
        
        # Atomic rename
        shutil.move(temp_path, manifest_path)
        logger.debug("Manifest saved atomically")
        
    except Exception as e:
        # Clean up temp file on error
        try:
            os.unlink(temp_path)
        except:
            pass
        logger.error(f"Failed to save manifest: {e}")
        raise

def clean_program_name(name):
    """Sanitize and truncate overly long folder names."""
    name = re.sub(r"[\\/*?:\"<>|]", "_", name.strip())
    if len(name) > 80:
        name = name[:80]
    return name

def extract_year(filename):
    """Extract fiscal or calendar year from filename."""
    match = re.search(r"fy\s*(\d{2,4})", filename, re.IGNORECASE)
    if match:
        token = match.group(1)
        return f"20{token}" if len(token) == 2 else token
    match = re.search(r"(19|20)\d{2}", filename)
    return match.group(0) if match else "unknown_year"

def detect_program_from_filename(filename):
    """Detect program from filename as fallback."""
    filename_lower = filename.lower()
    
    for key, val in PROGRAM_MAP.items():
        if key in filename_lower:
            return val
    
    return None

def should_skip_file(filename, text_context=""):
    """Check if file should be skipped (deprecated annual reports)."""
    combined = (filename + " " + text_context).lower()
    
    # Skip if it matches deprecated patterns (but not "record layout" PDFs)
    if "record layout" not in combined and "record_layout" not in combined:
        for pattern in SKIP_PATTERNS:
            if pattern in combined:
                return True
        
        # Skip PDF annual reports specifically (but not layouts)
        if "annual" in combined and "report" in combined and filename.lower().endswith(".pdf"):
            return True
    
    return False

def parse_table_links(soup):
    """Parse download links from table format (used in Latest Quarterly Updates)."""
    table_links = []
    
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            
            program_cell = cells[0].get_text(strip=True)
            
            for cell in cells[1:]:
                for link in cell.find_all("a", href=True):
                    href = link["href"]
                    if not any(href.lower().endswith(ext) for ext in VALID_EXTS):
                        continue
                    
                    filename = href.split("/")[-1]
                    
                    if should_skip_file(filename, program_cell):
                        logger.debug(f"[TABLE] Skipping deprecated: {filename}")
                        continue
                    
                    full_url = urljoin(BASE_URL, href)
                    year = extract_year(filename)
                    
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

def download_file_atomic(url: str, filepath: str) -> tuple:
    """
    Download file atomically to temporary location, then move to final location.
    Returns (content, headers_dict) on success.
    """
    temp_fd, temp_path = tempfile.mkstemp(
        dir=os.path.dirname(filepath),
        prefix=".download_",
        suffix=os.path.splitext(filepath)[1]
    )
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        
        # Write to temp file
        with os.fdopen(temp_fd, 'wb') as f:
            f.write(r.content)
        
        # Atomic move to final location
        shutil.move(temp_path, filepath)
        
        return r.content, dict(r.headers)
        
    except Exception as e:
        # Clean up temp file on error
        try:
            os.unlink(temp_path)
        except:
            pass
        raise e

# ---------------------------------------------------------------------
# Main Scraping Logic
# ---------------------------------------------------------------------

def main():
    logger.info("="*60)
    logger.info(f"Starting scrape at {datetime.now()}")
    logger.info("="*60)
    
    # Note about hash-based deduplication
    logger.info("Note: Deduplication uses ETag/Last-Modified headers")
    logger.info("Future improvement: Add hash-based dedup for identical files with different URLs")
    logger.info("")
    
    logger.info(f"Fetching: {BASE_URL}")
    try:
        response = requests.get(BASE_URL, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch main page: {e}")
        return 1
    
    soup = BeautifulSoup(response.text, "html.parser")
    
    manifest = load_manifest()
    download_links = []
    
    # Parse table-based links
    logger.info("Parsing table-based links...")
    table_links = parse_table_links(soup)
    logger.info(f"Found {len(table_links)} files from tables")
    download_links.extend(table_links)
    
    # Parse all links on page
    logger.info("Scanning all links on page...")
    all_links = soup.find_all("a", href=True)
    
    for link in all_links:
        href = link["href"]
        href_lower = href.lower()
        
        if not any(href_lower.endswith(ext) for ext in VALID_EXTS):
            continue
        
        filename = href.split("/")[-1]
        
        if should_skip_file(filename):
            continue
        
        program = detect_program_from_filename(filename)
        
        if not program:
            parent = link.find_parent(["td", "p", "li", "div"])
            if parent:
                context = parent.get_text(strip=True)
                for key, val in PROGRAM_MAP.items():
                    if key in context.lower():
                        program = val
                        break
        
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
        
        full_url = urljoin(BASE_URL, href)
        normalized_url = normalize_url(full_url)
        year = extract_year(filename)
        
        if any(item["url"] == normalized_url for item in download_links):
            continue
        
        download_links.append({
            "program": program,
            "url": normalized_url,
            "filename": filename,
            "year": year
        })
    
    logger.info(f"Found {len(download_links)} total downloadable files")
    
    # Download files
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0
    
    for item in download_links:
        program = item["program"]
        url = item["url"]
        filename = item["filename"]
        year = item["year"]
        
        safe_program = clean_program_name(program)
        
        program_dir = os.path.join(SAVE_DIR, safe_program)
        year_dir = os.path.join(program_dir, year)
        os.makedirs(year_dir, exist_ok=True)
        filepath = os.path.join(year_dir, filename)
        
        # Check if we should skip this file
        should_skip = False
        
        # Skip if URL in manifest and unchanged
        if url in manifest:
            try:
                head = requests.head(url, timeout=10)
                etag = head.headers.get("ETag")
                last_modified = head.headers.get("Last-Modified")
                
                cached = manifest[url]
                
                if etag and cached.get("etag") == etag:
                    logger.debug(f"Skipping (ETag match): {filename}")
                    should_skip = True
                elif last_modified and cached.get("last_modified") == last_modified:
                    logger.debug(f"Skipping (Last-Modified match): {filename}")
                    should_skip = True
            except Exception as e:
                logger.warning(f"HEAD request failed for {url}: {e}")
                # If HEAD fails, check if file exists on disk
                if os.path.exists(filepath):
                    logger.debug(f"Skipping (file exists, HEAD failed): {filename}")
                    should_skip = True
        
        # Skip if file path already exists in manifest (different URL, same file)
        elif any(entry.get("saved_path") == filepath for entry in manifest.values()):
            logger.debug(f"Skipping (path exists): {filepath}")
            should_skip = True
        
        if should_skip:
            skipped_count += 1
            continue
        
        # Download
        logger.info(f"Downloading ({safe_program}/{year}): {filename}")
        try:
            content, headers = download_file_atomic(url, filepath)
            digest = hashlib.sha256(content).hexdigest()
            
            manifest[url] = {
                "program": safe_program,
                "filename": filename,
                "year": year,
                "saved_path": filepath,
                "sha256": digest,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
            }
            
            # Save manifest after each successful download
            save_manifest(manifest)
            logger.info(f"✓ Saved to: {filepath}")
            downloaded_count += 1
            
        except Exception as e:
            logger.error(f"✗ Failed to download {url}: {e}")
            failed_count += 1
    
    # Final summary
    logger.info("="*60)
    logger.info("Scrape completed!")
    logger.info(f"Downloaded: {downloaded_count} files")
    logger.info(f"Skipped: {skipped_count} files")
    logger.info(f"Failed: {failed_count} files")
    logger.info(f"Total in manifest: {len(manifest)} files")
    logger.info("")
    logger.info("Tip: Run cleanup.py to validate manifest and remove stale entries")
    logger.info("="*60)
    
    return 0 if failed_count == 0 else 1

if __name__ == "__main__":
    exit(main())