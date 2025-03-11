import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

def scraper_data():
    logger.info('Starting data scraping process')