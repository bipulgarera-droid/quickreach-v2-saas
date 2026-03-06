#!/usr/bin/env python3
"""
Serper Search — Google search via Serper API for film festival contacts.

Inputs:
    - queries: list of search queries
    - num_results: number of results per query (default 100)

Outputs:
    - List of {title, link, snippet} dicts
    - Inserts a search_run record per query into Supabase

Usage:
    python -m execution.serper_search --queries "programmer film festival site:linkedin.com" "film critic India"
"""

import os
import sys
import json
import argparse
import requests
import logging
from datetime import datetime

# Add parent dir to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

# Load env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'


def search_serper(query: str, num_results: int = 100) -> list[dict]:
    """
    Search Google via Serper API.
    
    Args:
        query: Search query string
        num_results: Number of results to fetch (max 100 per request)
    
    Returns:
        List of result dicts: {title, link, snippet}
    """
    if not SERPER_API_KEY:
        logger.error("SERPER_API_KEY not set in .env")
        return []

    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }

    all_results = []
    # Serper often blocks num=100 for site:linkedin.com or certain accounts
    # We will use num=10 (the default) and paginate using 'page' parameter
    pages = (num_results + 9) // 10
    
    # Cap total pages to prevent runaway loops (Serper max is usually 1000 results for organic anyway, but let's allow up to 10000/10 = 1000 pages)
    if pages > 1000:
        pages = 1000
    
    for page in range(pages):
        payload = {
            'q': query,
            'num': 10,
        }
        
        # Serper 'page' parameter is 1-indexed, default is 1
        if page > 0:
            payload['page'] = page + 1

        try:
            logger.info(f"Searching: '{query}' (page {page + 1}/{pages})")
            response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=30)
            
            # Print specific error message from Serper if bad request
            if response.status_code != 200:
                logger.error(f"Serper API error ({response.status_code}): {response.text}")
                break
                
            data = response.json()
            
            organic = data.get('organic', [])
            for result in organic:
                all_results.append({
                    'title': result.get('title', ''),
                    'link': result.get('link', ''),
                    'snippet': result.get('snippet', ''),
                    'position': result.get('position', 0)
                })
            
            logger.info(f"  Got {len(organic)} results (total: {len(all_results)})")
            
            # If we hit our target number, trim and stop
            if len(all_results) >= num_results:
                all_results = all_results[:num_results]
                break
                
            # If we got fewer than 10 results, Google has no more pages
            if len(organic) < 10:
                logger.info("  No more results available from Google.")
                break
            
            # Rate limit protection for large scrapes
            if page < pages - 1:
                import time
                time.sleep(1)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Serper HTTP error: {e}")
            break
    
    return all_results


def save_search_run(supabase_client, query: str, results: list[dict], status: str = 'completed', error: str = None, project_id: str = None):
    """Save a search run record to Supabase."""
    try:
        record = {
            'query': query,
            'results_count': len(results),
            'status': status,
            'error_message': error,
        }
        if project_id:
            record['project_id'] = project_id
            
        if status == 'completed':
            record['completed_at'] = datetime.utcnow().isoformat()
        
        result = supabase_client.table('search_runs').insert(record).execute()
        logger.info(f"Search run saved: {result.data[0]['id']}")
        return result.data[0]
    except Exception as e:
        logger.error(f"Failed to save search run: {e}")
        return None


def run_search_pipeline(queries: list[str], num_results: int = 100, project_id: str = None) -> list[dict]:
    """
    Run the full search pipeline:
    1. Search via Serper for each query
    2. Record search runs in Supabase
    3. Return all results
    
    Args:
        queries: List of search query strings
        num_results: Results per query
    
    Returns:
        All search results combined
    """
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    supabase = None
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
    else:
        logger.warning("Supabase not configured — results won't be saved to DB")
    
    all_results = []
    
    for query in queries:
        try:
            results = search_serper(query, num_results)
            all_results.extend(results)
            
            if supabase:
                save_search_run(supabase, query, results, project_id=project_id)
        except Exception as e:
            logger.error(f"Error searching '{query}': {e}")
            if supabase:
                save_search_run(supabase, query, [], status='failed', error=str(e), project_id=project_id)
    
    logger.info(f"Total results across all queries: {len(all_results)}")
    return all_results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search Google via Serper API')
    parser.add_argument('--queries', nargs='+', required=True, help='Search queries')
    parser.add_argument('--num', type=int, default=100, help='Results per query')
    parser.add_argument('--output', type=str, help='Output JSON file path')
    
    args = parser.parse_args()
    
    results = run_search_pipeline(args.queries, args.num)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {args.output}")
    else:
        print(json.dumps(results, indent=2))
