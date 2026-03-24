import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def test_google_maps_scraper():
    api_key = os.getenv("APIFY_API_TOKEN")
    if not api_key:
        logger.error("No APIFY_API_TOKEN found in .env")
        return

    # Initialize the ApifyClient with your API token
    client = ApifyClient(api_key)

    # Prepare the Actor input for Production House in Mumbai
    # Coordinates for Mumbai approximately: 19.0760, 72.8777
    run_input = {
        "area_height": 20,
        "area_width": 20,
        "gmaps_url": "https://www.google.com/maps/search/production+house/@19.0760,72.8777,13z",
        "max_results": 100,
        "search_query": "production house"
    }

    logger.info("Starting Apify Actor: xmiso_scrapers/google-maps-scraper...")
    logger.info(f"Input: {json.dumps(run_input, indent=2)}")

    try:
        # Run the Actor and wait for it to finish
        run = client.actor("xmiso_scrapers/google-maps-scraper").call(run_input=run_input)
        
        logger.info(f"Actor run finished. Run ID: {run.get('id')}")
        logger.info("Fetching results from dataset...")

        # Fetch and print Actor results from the run's dataset
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            logger.error("No default dataset ID found in run results.")
            return

        results = list(client.dataset(dataset_id).iterate_items())
        
        logger.info(f"\nFound {len(results)} businesses:")
        logger.info("-" * 50)
        
        for idx, item in enumerate(results, start=1):
            name = item.get('name', item.get('title', 'N/A'))
            website = item.get('website_url', item.get('website', 'N/A'))
            phone = item.get('phone_number', item.get('phone', 'N/A'))
            
            logger.info(f"{idx}. {name}")
            logger.info(f"   Website: {website}")
            logger.info(f"   Phone: {phone}")
            logger.info("-" * 50)

    except Exception as e:
        logger.error(f"Error running Apify Actor: {e}")

if __name__ == "__main__":
    test_google_maps_scraper()
