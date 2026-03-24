#!/usr/bin/env python3
"""
Apify Search — Find businesses via Google Maps using Apify actor.
Uses 'blueorion/free-google-maps-scraper-extensive'.

Usage:
    python -m execution.apify_search --query "production house" --location "mumbai" --num 100
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from apify_client import ApifyClient
from supabase import create_client

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_country_code(location: str) -> str:
    """Simple mapping of common locations to country codes."""
    loc = location.lower()
    if any(x in loc for x in ['india', 'mumbai', 'delhi', 'bangalore', 'pune']):
        return "IN"
    if any(x in loc for x in ['usa', 'ny', 'california', 'texas', 'york', 'angeles']):
        return "US"
    if any(x in loc for x in ['uk', 'london', 'manchester']):
        return "GB"
    if any(x in loc for x in ['uae', 'dubai']):
        return "AE"
    return "US" # Default

def run_apify_maps_search(query: str, location: str, num: int = 100, project_id: str = None):
    """Run the Apify actor and store results in Supabase."""
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        logger.error("No APIFY_API_KEY found in environment")
        return None

    client = ApifyClient(api_key)
    country_code = get_country_code(location)
    
    run_input = {
        "maxItems": num,
        "searchTerms": [query],
        "startingLocations": [location],
        "countryCode": country_code,
        "language": "en",
        "mode": "aggressive"
    }

    logger.info(f"Starting Apify Actor: blueorion/free-google-maps-scraper-extensive...")
    logger.info(f"Query: {query}, Location: {location}, Country: {country_code}, Max: {num}")

    try:
        run = client.actor("blueorion/free-google-maps-scraper-extensive").call(run_input=run_input)
        logger.info(f"Actor run finished. ID: {run.get('id')}")
        
        dataset_id = run.get("defaultDatasetId")
        results = list(client.dataset(dataset_id).iterate_items())
        logger.info(f"Fetched {len(results)} results from Apify.")

        if not results:
            return {'inserted': 0, 'skipped': 0}

        return store_apify_results(results, query, location, project_id)

    except Exception as e:
        logger.error(f"Apify execution failed: {e}")
        return None

def store_apify_results(results: list[dict], query: str, location: str, project_id: str = None):
    """Clean and store Apify results in Supabase contacts table."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials missing")
        return None

    supabase = create_client(supabase_url, supabase_key)
    source_str = f"apify_maps: {query} in {location}"
    
    # Simple name cleaning for Apify (Apify usually gives cleaner names than Serper title tags)
    from execution.serper_search import _extract_brand_from_url

    # Fetch existing for deduplication within project
    existing_companies = set()
    existing_phones = set()
    if project_id:
        res = supabase.table('contacts').select('company, phone').eq('project_id', project_id).execute()
        for r in res.data:
            if r.get('company'): existing_companies.add(r['company'].lower())
            if r.get('phone'): existing_phones.add(r['phone'])

    leads_to_insert = []
    for item in results:
        name = item.get('title') or item.get('name') or "Unknown Business"
        phone = item.get('phoneNumber') or item.get('phone') or ""
        website = item.get('website') or ""
        
        # Dedupe by company name or phone
        if name.lower() in existing_companies or (phone and phone in existing_phones):
            continue
            
        existing_companies.add(name.lower())
        if phone: existing_phones.add(phone)

        leads_to_insert.append({
            'name': name,
            'company': name,
            'phone': phone,
            'source_url': website, # Apify sometimes doesn't give website, that's fine
            'project_id': project_id,
            'status': 'new',
            'source': source_str,
            'enrichment_data': {
                'address': item.get('address'),
                'category': item.get('categoryName'),
                'reviews': item.get('reviewsCount'),
                'stars': item.get('stars'),
                'apify_data': item
            }
        })

    if leads_to_insert:
        logger.info(f"Inserting {len(leads_to_insert)} new leads from Apify...")
        for i in range(0, len(leads_to_insert), 100):
            batch = leads_to_insert[i:i+100]
            supabase.table('contacts').insert(batch).execute()
        return {'inserted': len(leads_to_insert), 'skipped': len(results) - len(leads_to_insert)}
    
    logger.info("No new unique leads to insert.")
    return {'inserted': 0, 'skipped': len(results)}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search Google Maps via Apify')
    parser.add_argument('--query', required=True, help='Search term')
    parser.add_argument('--location', required=True, help='Location')
    parser.add_argument('--num', type=int, default=100, help='Max results')
    parser.add_argument('--project_id', help='Supabase Project ID')

    args = parser.parse_args()
    
    # Try to find default project if not provided
    pid = args.project_id
    if not pid:
        try:
            url = os.getenv('SUPABASE_URL')
            key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
            sb = create_client(url, key)
            res = sb.table('projects').select('id').limit(1).execute()
            if res.data: pid = res.data[0]['id']
        except: pass

    stats = run_apify_maps_search(args.query, args.location, args.num, pid)
    print(json.dumps(stats, indent=2))
