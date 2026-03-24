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
import re
import time
from datetime import datetime
from urllib.parse import urlparse

# Add parent dir to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = '466022051ded94f7fd2aa2132279a1ffa57a500b'
SERPER_URL = 'https://google.serper.dev/search'

def _extract_brand_from_url(url: str) -> str:
    """Extract a clean brand name from a URL with robust word splitting."""
    if not url: return ""
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # 1. Remove common extensions
        brand = re.sub(r'\.(com|in|org|net|biz|co\.in|me|tv|us|ae|io|ai|live)$', '', domain, flags=re.IGNORECASE)
        
        # 2. Handle hyphens and underscores
        brand = brand.replace('-', ' ').replace('_', ' ')
        
        # 3. Smart Spacing Logic
        brand = re.sub(r'([a-z])([A-Z])', r'\1 \2', brand)
        
        keywords = [
            'entertainment', 'motionpictures', 'productions', 'production', 
            'studios', 'studio', 'films', 'film', 'media', 'works', 'creative', 
            'solutions', 'digital', 'global', 'agency', 'group', 'services', 
            'official', 'vfx', 'corp', 'company', 'pictures', 'house', 'collective',
            'mantra', 'wadi', 'power', 'hour', 'baba', 'chillies', 'view', 'point',
            'stories', 'maverick', 'jugaad', 'zoom', 'cine', 'that', 'matter', 'away', 'frames', 'action', 'rodey', 'laser'
        ]
        prefixes = ['the', 'wild', 'magic', 'stories', 'red', 'zoom', 'cine', 'jugaad', 'maverick', 'goodfellas', 'star', 'grand', 'royal', 'nishant']
        
        for _ in range(3):
            old_brand = brand
            words = brand.split()
            cleaned_words = []
            for word in words:
                if len(word) >= 3:
                    for p in prefixes:
                        if word.lower().startswith(p) and len(word) > len(p) + 2:
                            word = word[:len(p)] + ' ' + word[len(p):]
                            break
                    for k in keywords:
                        low = word.lower()
                        if k in low:
                            idx = low.find(k)
                            if idx > 0 and word[idx-1] != ' ':
                                word = word[:idx] + ' ' + word[idx:]
                                break
                            elif idx == 0 and len(word) > len(k) + 2:
                                word = word[:len(k)] + ' ' + word[len(k):]
                                break
                cleaned_words.append(word)
            brand = ' '.join(cleaned_words)
            if brand == old_brand: break

        return brand.title().strip()
    except:
        return ""


def search_serper(query: str, num_results: int = 100, location: str = None) -> list[dict]:
    """
    Search Google via Serper API.
    
    Args:
        query: Search query string
        num_results: Number of results to fetch (max 100 per request)
        location: Optional location for dynamic GL/location logic
    
    Returns:
        List of result dicts: {title, link, snippet}
    """
    global SERPER_API_KEY
    if not SERPER_API_KEY:
        load_dotenv(Path(__file__).resolve().parent.parent / '.env')
        SERPER_API_KEY = os.getenv('SERPER_API_KEY')
        
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
    
    # No cap for Bulk - get maximum results
    # pages = (num_results + 9) // 10
    
    # 1. Dynamic Location Detection
    gl = 'us'  # Default
    loc_param = None
    
    # Check both location param AND query string for context
    context_str = (location or "") + " " + query
    context_lower = context_str.lower()
    
    if 'india' in context_lower or 'mumbai' in context_lower or 'delhi' in context_lower or 'bangalore' in context_lower:
        gl = 'in'
        loc_param = f"{location}, India" if location else "India"
    elif 'uk' in context_lower or 'london' in context_lower or 'united kingdom' in context_lower:
        gl = 'gb'
        loc_param = f"{location}, United Kingdom" if location else "United Kingdom"
    elif 'canada' in context_lower or 'toronto' in context_lower:
        gl = 'ca'
        loc_param = f"{location}, Canada" if location else "Canada"
    elif 'australia' in context_lower or 'sydney' in context_lower:
        gl = 'au'
        loc_param = f"{location}, Australia" if location else "Australia"
    elif location:
        # Fallback to provided location if specified but not in common list
        loc_param = location

    for page in range(pages):
        payload = {
            'q': query,
            'num': 10,
            'gl': gl
        }
        if loc_param:
            payload['location'] = loc_param
        
        # Serper 'page' parameter is 1-indexed, default is 1
        if page > 0:
            payload['page'] = page + 1

        try:
            logger.info(f"Searching: '{query}' (page {page + 1}/{pages})")
            response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=30)
            
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
            'project_id': project_id
        }
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
    2. Extract and store businesses
    3. Record search runs in Supabase
    4. Return total stats
    """
    from supabase import create_client
    from execution.business_search import extract_and_store_businesses
    import time # Added import for time.sleep
    
    supabase_url = 'https://rbkrtmzqubwrvkrvcebr.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJia3J0bXpxdWJ3cnZrcnZjZWJyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3NDM2MTAsImV4cCI6MjA4ODMxOTYxMH0.WMnotMf_h6wjT5DgZxhliTIdmxdl4DFjvHzfvI80QHA'
    
    total_stats = {'inserted': 0, 'skipped': 0, 'errors': 0}
    
    supabase = None
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
    
    # 1. Fetch search history to skip already completed queries
    existing_queries = set()
    if supabase and project_id:
        try:
            response = supabase.table("search_runs") \
                .select("query") \
                .eq("project_id", project_id) \
                .eq("status", "completed") \
                .execute()
            if response.data:
                existing_queries = {row['query'] for row in response.data}
            logger.info(f"Found {len(existing_queries)} already completed queries. They will be skipped.")
        except Exception as e:
            logger.error(f"Error fetching search history: {e}")

    for i, query in enumerate(queries):
        if query in existing_queries:
            logger.info(f"--- Skipping Query {i+1}/{len(queries)}: '{query}' (Already searched) ---")
            continue

        logger.info(f"--- Processing Query {i+1}/{len(queries)}: '{query}' ---")
        try:
            # 1. Search Serper
            results = search_serper(query, num_results=pages_per_query * 10, location=location)
            
            # 2. Extract and Store as Businesses
            if results:
                stats = extract_and_store_businesses(results, source_query=query, project_id=project_id, niche=niche, location=location)
                total_stats['inserted'] += stats['inserted']
                total_stats['skipped'] += stats['skipped']
                total_stats['errors'] += stats['errors']
                
                # 3. Mark query as completed in search_runs
                if supabase:
                    save_search_run(supabase, query, results, status='completed', project_id=project_id)
                
                logger.info(f"Current Stats: {total_stats}")
            
            # Rate limit
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing query '{query}': {e}")
            if supabase:
                save_search_run(supabase, query, [], status='failed', error=str(e), project_id=project_id)
    
    return total_stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Search Google via Serper API')
    parser.add_argument('--queries', nargs='+', required=True, help='Search queries')
    parser.add_argument('--num', type=int, default=100, help='Results per query')
    parser.add_argument('--output', type=str, help='Output JSON file path')
    parser.add_argument('--project_id', type=str, help='Target project ID')
    
    args = parser.parse_args()
    
    # If no project_id provided, try to find the first one from Supabase
    pid = args.project_id
    if not pid:
        try:
            from supabase import create_client
            url = os.getenv('SUPABASE_URL')
            key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
            if url and key:
                sb = create_client(url, key)
                res = sb.table('projects').select('id').limit(1).execute()
                if res.data:
                    pid = res.data[0]['id']
                    logger.info(f"Using default project_id: {pid}")
        except:
            pass

    results = run_search_pipeline(args.queries, args.num, project_id=pid)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {args.output}")
    else:
        # Only print summary to stdout to keep it clean, or full JSON if requested
        logger.info(f"Search pipeline finished with {len(results)} results.")
