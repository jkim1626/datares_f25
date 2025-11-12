import logging
from scrape_uscis import BASE_URLS, get_download_links, get_session, MODE, make_period
from db_manifest import DBManifest
from paths import get_uscis_outdir

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 60)
    logger.info("TEST MODE: USCIS Scraper (1 file only)")
    logger.info("=" * 60)
    
    session = get_session()
    
    # Use first visa type (h1b)
    visa_type = 'h1b'
    base_url = BASE_URLS[visa_type]
    
    logger.info(f"\n🧪 TEST: Using ONLY first visa type: {visa_type.upper()}")
    logger.info(f"   URL: {base_url}\n")
    
    manifest = DBManifest(source_id="uscis", file_type="uscis", mode=MODE, program=visa_type)
    
    try:
        links = get_download_links(base_url, session)
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
    
    period = make_period(visa_type, filename)
    
    logger.info(f"🧪 TEST: Downloading ONLY first file:")
    logger.info(f"   Visa Type: {visa_type.upper()}")
    logger.info(f"   Period: {period}")
    logger.info(f"   File: {filename}")
    logger.info(f"   URL: {file_url}\n")
    
    try:
        decision = manifest.plan(period, file_url)
        
        if decision["decision"] == "skip":
            logger.info(f"[skipped] {period} {file_url} ({decision['reason']})")
            logger.info("\n✅ File already exists in database!")
            return
        
        year_or_misc = period.split('/')[1]
        vdir = get_uscis_outdir(visa_type, year_or_misc)
        expected_path = vdir / filename
        
        if expected_path.exists():
            existing = manifest.get_existing(period, file_url)
            if not existing:
                if manifest.register_existing_file(period, file_url, str(expected_path)):
                    logger.info(f"[registered] {period} {file_url} -> {expected_path}")
                    logger.info("\n✅ File registered in database!")
                return
        
        versioned = (decision["decision"] == "version")
        saved = manifest.download_and_record(
            session, file_url, outdir=str(vdir), period=period, versioned=versioned
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
