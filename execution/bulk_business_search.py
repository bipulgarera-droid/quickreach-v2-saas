#!/usr/bin/env python3
"""
Bulk Business Search — Generate 20+ query variants and paginate Serper for bulk results.

Core logic:
1. Generate 20+ query variants: niche + location, niche + companies + location, etc.
2. For each query, paginate Serper API: up to 10 pages per query (100 results/query).
3. Extract/parse each result: title → company name, link → website.
4. Dedupe by domain/name, filter valid businesses, save to Supabase.
5. Rate limit: 1 query/sec to avoid serper limits.

Usage:
    python -m execution.bulk_business_search --niche "production house" --location "Mumbai" --project_id "YOUR_PROJECT_ID"
"""

import os
import sys
import json
import logging
import argparse
import time
import re
from datetime import datetime
from urllib.parse import urlparse

# Add parent dir to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

# Load env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from execution.serper_search import search_serper, save_search_run
from execution.business_search import extract_and_store_businesses

def generate_queries(niche: str, location: str) -> list[str]:
    """Generate 20+ query variants for the given niche and location."""
    # Base templates for maximum coverage
    templates = [
        # Niche + Location variants
        f"{niche} {location}",
        f"{niche} company {location}",
        f"{niche} companies {location}",
        f"{niche} agency {location}",
        f"{niche} agencies {location}",
        f"{niche} firm {location}",
        f"{niche} firms {location}",
        f"{niche} studio {location}",
        f"{niche} studios {location}",
        f"{niche} business {location}",
        f"{niche} businesses {location}",
        f"{niche} group {location}",
        f"{niche} services {location}",
        f"best {niche} {location}",
        f"top {niche} {location}",
        f"leading {niche} {location}",
        f"local {niche} {location}",
        f"{niche} in {location}",
        f"list of {niche} in {location}",
        f"directory of {niche} in {location}",
        
        # Operational variants
        f"{niche} office {location}",
        f"{niche} headquarters {location}",
        f"{niche} contact {location}",
        f"{niche} team {location}",
        f"{niche} portfolio {location}",
        f"{niche} projects {location}",
        f"{niche} works {location}",
        
        # Industry specific / Long tail
        f"independent {niche} {location}",
        f"boutique {niche} {location}",
        f"creative {niche} {location}",
        f"professional {niche} {location}",
        f"award winning {niche} {location}",
        f"full service {niche} {location}",
        f"specialized {niche} {location}",
        f"certified {niche} {location}",
        
        # Combinations and complex queries
        f'"{niche}" AND "{location}"',
        f'"{niche}" {location} -directory -list',
        f'"{niche}" {location} "contact us"',
        f'"{niche}" {location} "about us"',
        f'"{niche}" {location} "our team"',
        f'"{niche}" {location} "clients"',
        
        # Site searches (if applicable but here general)
        f'site:.com "{niche}" {location}',
        f'site:.in "{niche}" {location}',
        f'site:.co.in "{niche}" {location}',
        f'site:.net "{niche}" {location}',
        f'site:.org "{niche}" {location}',
        
        # Maps-style
        f"{niche} near {location}",
        f"{niche} around {location}",
    ]
    
    # Add OR combinations for broad coverage
    # Example: if location is Mumbai, also search for Bombay
    if location.lower() == 'mumbai':
        templates.append(f'"{niche}" {location} OR Bombay')
    
    # Dedupe and limit to top 50 unique variants
    unique_queries = []
    for q in templates:
        q = q.strip()
        if q not in unique_queries:
            unique_queries.append(q)
    
    return unique_queries[:50]

def run_bulk_search(niche: str, location: str, project_id: str = None, pages_per_query: int = 10):
    """
    1. Generate queries
    2. Loop queries
    3. Loop pages (1-10)
    4. Collect results
    5. Store as businesses
    """
    from supabase import create_client
    
    supabase_url = 'https://rbkrtmzqubwrvkrvcebr.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJia3J0bXpxdWJ3cnZrcnZjZWJyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3NDM2MTAsImV4cCI6MjA4ODMxOTYxMH0.WMnotMf_h6wjT5DgZxhliTIdmxdl4DFjvHzfvI80QHA'
    
    supabase = None
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
    
    queries = generate_queries(niche, location)
    logger.info(f"Generated {len(queries)} query variants for '{niche}' in '{location}'")

    total_stats = {'inserted': 0, 'skipped': 0, 'errors': 0}

    # 1. Fetch search history for this project to avoid redundant costs
    existing_queries = set()
    if project_id and supabase:
        try:
            response = supabase.table("search_runs") \
                .select("query") \
                .eq("project_id", project_id) \
                .eq("status", "completed") \
                .execute()
            if response.data:
                existing_queries = {row['query'] for row in response.data}
            logger.info(f"Found {len(existing_queries)} already completed queries for this project. They will be skipped.")
        except Exception as e:
            logger.error(f"Error fetching search history: {e}")

    # 2. Main loop through generated queries
    target_count = 10000 # Default or from args if added
    for i, query in enumerate(queries):
        # Skip if already reached target
        if target_count > 0 and total_stats['inserted'] >= target_count:
            logger.info(f"--- Reached target count of {target_count}. Stopping early. ---")
            break
            
        # Skip if already searched successfully
        if query in existing_queries:
            logger.info(f"--- Skipping Query {i+1}/{len(queries)}: '{query}' (Already searched) ---")
            continue

        logger.info(f"--- Processing Query {i+1}/{len(queries)}: '{query}' ---")
        
        try:
            # 1. Search Serper
            results = search_serper(query, num_results=pages_per_query * 10, location=location)
            
            # 2. Extract and Store as Businesses
            if results:
                # This function already dedupes and filters via the new "Iron-Clad" logic
                stats = extract_and_store_businesses(results, source_query=query, project_id=project_id, niche=niche, location=location)
                total_stats['inserted'] += stats['inserted']
                total_stats['skipped'] += stats['skipped']
                total_stats['errors'] += stats['errors']
                
                # 3. Mark query as completed in search_runs
                if supabase:
                    save_search_run(supabase, query, results, status='completed', project_id=project_id)
                
                logger.info(f"Current Stats: {total_stats}")
            
            # Rate limit between queries
            logger.info("Waiting 1 second before next query...")
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing query '{query}': {e}")
            if supabase:
                save_search_run(supabase, query, [], status='failed', error=str(e), project_id=project_id)
    
    return total_stats

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bulk Business Search via Serper')
    parser.add_argument('--niche', type=str, required=True, help='Business niche (e.g. production house)')
    parser.add_argument('--location', type=str, required=True, help='Location (e.g. Mumbai)')
    parser.add_argument('--project_id', type=str, help='Project ID to store contacts under')
    parser.add_argument('--pages', type=int, default=10, help='Pages to fetch per query (default 10 = 100 results)')
    
    args = parser.parse_args()
    
    logger.info(f"Starting BULK business search: {args.niche} in {args.location}")
    stats = run_bulk_search(args.niche, args.location, args.project_id, args.pages)
    
    print("\n" + "="*40)
    print("FINAL BULK SEARCH RESULTS")
    print("="*40)
    print(json.dumps(stats, indent=2))
    print("="*40)
