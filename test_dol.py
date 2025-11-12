import logging
from scrape_dol import discover_files, get_session, MODE
from db_manifest import DBManifest
from paths import get_dol_outdir

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("TEST MODE: DOL Scraper (1 file only)")
    logger.info("=" * 60)
    
    session = get_session()
    
    try:
        candidates = discover_files(session)
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        return
    
    if not candidates:
        logger.warning("No files discovered!")
        return
    
    # ONLY DOWNLOAD FIRST FILE
    item = candidates[0]
    program = item["program"]
    year = item["year"]
    file_url = item["url"]
    filename = item["filename"]
    
    logger.info(f"\n🧪 TEST: Downloading ONLY first file found:")
    logger.info(f"   Program: {program}")
    logger.info(f"   Year: {year}")
    logger.info(f"   URL: {file_url}\n")
    
    # Create manifest
    manifest = DBManifest(source_id="dolstats", file_type="dol", mode=MODE, program=program)
    
    period = f"{program}/{year}"
    
    try:
        decision = manifest.plan(period, file_url)
        
        if decision["decision"] == "skip":
            logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
            logger.info("\n✅ File already exists in database!")
            return
        
        pdir = get_dol_outdir(program, year)
        expected_path = pdir / filename
        
        if expected_path.exists():
            existing = manifest.get_existing(period, file_url)
            if not existing:
                if manifest.register_existing_file(period, file_url, str(expected_path)):
                    logger.info(f"[registered] {period} {file_url} -> {expected_path}")
                    logger.info("\n✅ File registered in database!")
                return
        
        versioned = (decision["decision"] == "version")
        saved = manifest.download_and_record(
            session, file_url, outdir=str(pdir), period=period, versioned=versioned
        )
        
        if saved:
            logger.info(f"[downloaded] {period} {file_url} -> {saved}")
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
