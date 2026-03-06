#!/usr/bin/env python3
"""
Scrape Contacts — Extract names, bios, LinkedIn URLs from Serper search results.

Takes raw Serper results and intelligently extracts contact information,
deduplicates, and inserts into Supabase contacts table.

Usage:
    python -m execution.scrape_contacts --input .tmp/search_results.json
    # Or call programmatically: extract_and_store_contacts(results)
"""

import os
import sys
import json
import re
import argparse
import logging
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_name_from_linkedin(title: str) -> str:
    """
    Extract a person's name from a LinkedIn title.
    Handles profile pages and post pages.
    """
    # Remove LinkedIn suffixes
    name = re.sub(r"\s*('s)?\s*(Post|Article|Activity).*$", "", title, flags=re.IGNORECASE)
    name = re.sub(r'\s*[\|\-–—]\s*LinkedIn.*$', '', name, flags=re.IGNORECASE)
    # Remove common suffixes like titles
    name = re.sub(r'\s*[\|\-–—]\s*(Film|Festival|Critic|Director|Programmer|Producer|Curator|Writer).*$', '', name, flags=re.IGNORECASE)
    # Clean up
    name = name.strip(' -|–—')
    
    # If the title was something like "Job-Posting...", the name might be junk
    if "Job" in name or "PDF" in name.upper() or "Call For" in name:
        # Fall back to splitting by ' | ' or ' - ' to try to find a name
        parts = title.split(' | ')
        if len(parts) > 1:
            name = parts[-1]
        else:
            parts = title.split(' - ')
            if len(parts) > 1:
                name = parts[-1]
    
    name = name.strip()
    
    # Validate: should be 1-5 words, no special chars
    words = name.split()
    if 1 <= len(words) <= 6:
        return name
    
    return title.split(' - ')[0].split(' | ')[0].strip()


def extract_bio_from_snippet(snippet: str) -> str:
    """Extract a meaningful bio from the search snippet."""
    if not snippet:
        return ''
    
    # Clean up common LinkedIn snippet patterns
    bio = re.sub(r'^(View|See)\s+\w+.*?profile\s*\.?\s*', '', snippet, flags=re.IGNORECASE)
    bio = re.sub(r'Join LinkedIn today.*$', '', bio, flags=re.IGNORECASE)
    bio = re.sub(r'\d+\s*connections?.*$', '', bio, flags=re.IGNORECASE)
    bio = bio.strip('. ')
    
    # Truncate to reasonable bio length
    if len(bio) > 300:
        bio = bio[:297] + '...'
    
    return bio


def is_linkedin_profile(url: str) -> bool:
    """Check if URL is a LinkedIn profile (not company/post). Handles country paths like uk.linkedin.com."""
    if not url:
        return False
    return bool(re.search(r'linkedin\.com/.*in/', url))


def is_any_linkedin_url(url: str) -> bool:
    """Check if URL is any type of LinkedIn link (post, pulse, profile)."""
    return 'linkedin.com' in url.lower()


def is_valid_contact(result: dict) -> bool:
    """Check if a search result looks like a valid person contact."""
    title = result.get('title', '').lower()
    link = result.get('link', '')
    
    # Must have a link
    if not link:
        return False
    
    # LinkedIn links are usually valid
    if is_any_linkedin_url(link):
        return True
    
    # For non-LinkedIn results, look for person indicators
    person_indicators = ['critic', 'director', 'programmer', 'curator', 'festival', 'film', 'review']
    return any(ind in title for ind in person_indicators)


def parse_search_results(results: list[dict], source_query: str = '') -> list[dict]:
    """
    Parse raw Serper results into contact records.
    
    Args:
        results: List of {title, link, snippet} from Serper
        source_query: The search query that produced these results
    
    Returns:
        List of contact dicts ready for Supabase insertion
    """
    contacts = []
    seen_urls = set()
    seen_names = set()
    
    for result in results:
        if not is_valid_contact(result):
            continue
        
        link = result.get('link', '')
        
        # Deduplicate by URL
        if link in seen_urls:
            continue
        seen_urls.add(link)
        
        # Extract fields
        title = result.get('title', '')
        snippet = result.get('snippet', '')
        
        if is_any_linkedin_url(link):
            name = extract_name_from_linkedin(title)
            linkedin_url = link
        else:
            name = title.split(' - ')[0].split(' | ')[0].strip()
            linkedin_url = None
        
        # Deduplicate by name (case-insensitive)
        name_lower = name.lower().strip()
        if name_lower in seen_names or len(name_lower) < 3:
            continue
        seen_names.add(name_lower)
        
        bio = extract_bio_from_snippet(snippet)
        
        contact = {
            'name': name,
            'bio': bio,
            'linkedin_url': linkedin_url,
            'source': source_query,
            'source_url': link,
            'status': 'new'
        }
        
        contacts.append(contact)
    
    logger.info(f"Parsed {len(contacts)} contacts from {len(results)} results")
    return contacts


def store_contacts(contacts: list[dict], project_id: str = None) -> dict:
    """
    Store contacts in Supabase contacts table.
    Deduplicates by LinkedIn URL or name using bulk fetching for efficiency.
    
    Returns:
        Stats dict: {inserted, skipped, errors}
    """
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not configured")
        return {'inserted': 0, 'skipped': 0, 'errors': len(contacts)}
    
    supabase = create_client(supabase_url, supabase_key)
    
    stats = {'inserted': 0, 'skipped': 0, 'errors': 0}
    
    if not contacts:
        return stats
        
    try:
        # 1. Fetch existing names and LinkedIn URLs to deduplicate efficiently locally
        logger.info("Fetching existing contacts for deduplication...")
        existing_names = set()
        existing_urls = set()
        
        # Paginate to get all existing records (Supabase limits to 1000 per request normally)
        has_more = True
        offset = 0
        limit = 1000
        
        query = supabase.table('contacts').select('name, linkedin_url')
        if project_id:
            query = query.eq('project_id', project_id)
            
        while has_more:
            res = query.range(offset, offset + limit - 1).execute()
            if res.data:
                for row in res.data:
                    if row.get('name'):
                        existing_names.add(row['name'].lower())
                    if row.get('linkedin_url'):
                        existing_urls.add(row['linkedin_url'].lower())
                offset += limit
                has_more = len(res.data) == limit
            else:
                has_more = False

        # 2. Filter contacts
        new_contacts = []
        for contact in contacts:
            name_lower = (contact.get('name') or '').lower()
            url_lower = (contact.get('linkedin_url') or '').lower()
            
            if (url_lower and url_lower in existing_urls) or (name_lower and name_lower in existing_names):
                stats['skipped'] += 1
            else:
                if project_id:
                    contact['project_id'] = project_id
                new_contacts.append(contact)
                # Add to local sets immediately so we don't insert duplicates within the same batch
                if name_lower: existing_names.add(name_lower)
                if url_lower: existing_urls.add(url_lower)

        # 3. Bulk Insert
        if new_contacts:
            logger.info(f"Bulk inserting {len(new_contacts)} new contacts...")
            # Supabase prefers batches of ~500 for inserts
            batch_size = 500
            for i in range(0, len(new_contacts), batch_size):
                batch = new_contacts[i:i + batch_size]
                supabase.table('contacts').insert(batch).execute()
                stats['inserted'] += len(batch)
        
    except Exception as e:
        logger.error(f"Error in store_contacts batch operation: {e}")
        stats['errors'] = len(contacts) - stats['inserted'] - stats['skipped']

    logger.info(f"Storage results: {stats}")
    return stats


def extract_and_store_contacts(results: list[dict], source_query: str = '', project_id: str = None) -> dict:
    """
    Full pipeline: parse results → deduplicate → store in Supabase.
    
    Args:
        results: Raw Serper search results
        source_query: The search query used
    
    Returns:
        Stats dict
    """
    contacts = parse_search_results(results, source_query)
    return store_contacts(contacts, project_id=project_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract contacts from search results')
    parser.add_argument('--input', type=str, required=True, help='Input JSON file from serper_search')
    parser.add_argument('--query', type=str, default='', help='Source query for tagging')
    
    args = parser.parse_args()
    
    with open(args.input, 'r') as f:
        results = json.load(f)
    
    stats = extract_and_store_contacts(results, args.query)
    print(json.dumps(stats, indent=2))
