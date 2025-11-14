import logging
from scrape_yearbook import discover_yearbooks, get_download_links, get_session, MODE
from helpers.db_manifest import DBManifest
from helpers.paths import get_yearbook_outdir
from pathlib import Path
import zipfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("TEST MODE: Yearbook Scraper (1 file only)")
    logger.info("=" * 60)
    
    session = get_session()
    manifest = DBManifest(source_id="dhsyearbook", file_type="yearbook", mode=MODE)
    
    try:
        yearbooks = discover_yearbooks(session)
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        return
    
    if not yearbooks:
        logger.warning("No yearbooks discovered!")
        return
    
    # Use first yearbook
    yb = yearbooks[0]
    year = yb["year"]  # Now correctly a string
    yearbook_url = yb["url"]
    
    logger.info(f"\n🧪 TEST: Using ONLY first yearbook: {year}")
    logger.info(f"   URL: {yearbook_url}\n")
    
    try:
        links = get_download_links(yearbook_url, session)
    except Exception as e:
        logger.error(f"Failed to get links: {e}")
        return
    
    if not links:
        logger.warning("No files found!")
        return
    
    # ONLY DOWNLOAD FIRST FILE
    link = links[0]
    filename = link["filename"]
    file_url = link["url"]
    period = str(year)  # FIXED: Ensure period is string
    
    logger.info(f"🧪 TEST: Downloading ONLY first file:")
    logger.info(f"   Year: {year}")
    logger.info(f"   File: {filename}")
    logger.info(f"   URL: {file_url}\n")
    
    try:
        decision = manifest.plan(period, file_url)
        
        if decision["decision"] == "skip":
            logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
            logger.info("\n✅ File already exists in database!")
            return
        
        ydir = get_yearbook_outdir(year)  # Now works because year is a string
        expected_path = ydir / filename
        
        if expected_path.exists():
            existing = manifest.get_existing(period, file_url)
            if not existing:
                if manifest.register_existing_file(period, file_url, str(expected_path)):
                    logger.info(f"[registered] {period} {file_url} -> {expected_path}")
                    logger.info("\n✅ File registered in database!")
                return
        
        versioned = (decision["decision"] == "version")
        saved = manifest.download_and_record(
            session, file_url, outdir=str(ydir), period=period, versioned=versioned
        )
        
        if saved:
            logger.info(f"[downloaded] {period} {file_url} -> {saved}")
            
            # Extract zip if applicable
            saved_path = Path(saved)
            if saved_path.suffix.lower() == '.zip':
                try:
                    folder_name = saved_path.stem
                    extract_folder = saved_path.parent / folder_name
                    extract_folder.mkdir(parents=True, exist_ok=True)
                    with zipfile.ZipFile(saved_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_folder)
                    logger.info(f"  Extracted {saved_path.name} to {folder_name}/")
                except Exception as e:
                    logger.warning(f"  Failed to extract: {e}")
            
            logger.info("\n✅ TEST COMPLETE!")
            logger.info(f"   File saved to: {saved}")
            logger.info(f"   Database record created: Yes")
        else:
            logger.info(f"[unchanged] {period} {file_url}")
            
    except Exception as e:
        logger.error(f"[error] {period} {file_url} ({e})")
        logger.error("\n❌ TEST FAILED!")

if __name__ == "__main__":
    main()