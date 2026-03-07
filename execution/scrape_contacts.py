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
    # Remove LinkedIn suffix first
    name = re.sub(r'\s*[\|\-–—]\s*LinkedIn.*$', '', title, flags=re.IGNORECASE)
    # Remove post/article markers
    name = re.sub(r"\s*('s)?\s*(Post|Article|Activity|Likes).*$", "", name, flags=re.IGNORECASE)
    # Remove role/title suffixes after dash/pipe (e.g. "John Doe - Film Programmer at XYZ")
    name = re.sub(r'\s*[\|\-–—]\s*.+$', '', name)
    # Remove degree/credential suffixes (e.g. "John Doe, PhD" or "John Doe MFA")
    name = re.sub(r',\s*(PhD|MFA|MBA|MPA|MA|BA|BS|MS|Jr|Sr|III|II|IV).*$', '', name, flags=re.IGNORECASE)
    # Clean up
    name = name.strip(' -|–—,."\'')
    
    # If the title was something like "Job posting" or "PDF", it's junk
    junk_indicators = ['job', 'pdf', 'call for', 'submission', 'deadline', 'apply', 'hiring', 'vacancy']
    if any(junk in name.lower() for junk in junk_indicators):
        return ''
    
    name = name.strip()
    
    # Validate: should be 2-6 words, primarily alphabetic
    words = name.split()
    if 2 <= len(words) <= 6:
        # Each word should be mostly alphabetic
        alpha_words = [w for w in words if re.match(r'^[A-Za-z\'\-\.]+$', w)]
        if len(alpha_words) >= 2:
            return ' '.join(alpha_words)
    
    # Last resort: grab the first part before any delimiter
    fallback = title.split(' - ')[0].split(' | ')[0].strip()
    fallback_words = fallback.split()
    if 2 <= len(fallback_words) <= 6:
        return fallback
    
    return ''


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
    """Check if URL is a LinkedIn personal profile (/in/username). Rejects company, pulse, and post pages."""
    if not url:
        return False
    # Must be linkedin.com/in/some-username (with optional trailing slash/query)
    return bool(re.search(r'linkedin\.com/in/[a-zA-Z0-9\-_%]+', url))


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
    
    # Reject obvious non-person pages
    reject_patterns = ['job posting', 'careers', 'apply now', 'call for', 'submission',
                       'deadline', 'vacancy', 'company page', '/company/', '/jobs/']
    for pattern in reject_patterns:
        if pattern in link.lower() or pattern in title:
            return False
    
    # LinkedIn /in/ profiles are highest quality
    if is_linkedin_profile(link):
        return True
    
    # Other LinkedIn links (posts, pulse articles) — accept but they may have weaker data
    if is_any_linkedin_url(link):
        return True
    
    # For non-LinkedIn results, look for person indicators
    person_indicators = ['critic', 'director', 'programmer', 'curator', 'festival',
                         'film', 'review', 'ceo', 'founder', 'producer', 'journalist']
    return any(ind in title for ind in person_indicators)


def is_person_name(name: str) -> bool:
    """
    Validate that a string looks like an actual human name, not an org or article title.
    Reject things like 'Sundance Film Fest News', 'Dear Fellow Film Programmers', etc.
    """
    if not name or len(name) < 3:
        return False
    
    name_lower = name.lower().strip()
    words = name_lower.split()
    
    # Too many words = probably a title/article, not a name
    if len(words) > 5 or len(words) < 2:
        return False
    
    # Reject if it starts with common non-name words
    non_name_starts = [
        'dear', 'the', 'a', 'an', 'all', 'our', 'my', 'your', 'this', 'that',
        'how', 'why', 'what', 'when', 'where', 'who', 'which',
        'top', 'best', 'new', 'free', 'meet', 'join', 'about',
        'welcome', 'hello', 'hi', 'hey',
    ]
    if words[0] in non_name_starts:
        return False
    
    # Reject if name contains org/brand/article indicator words
    org_indicators = [
        'news', 'festival', 'magazine', 'network', 'association', 'society',
        'foundation', 'institute', 'university', 'college', 'school',
        'company', 'inc', 'llc', 'ltd', 'corp', 'group', 'team',
        'award', 'awards', 'prize', 'committee', 'board', 'council',
        'fellow', 'fellows', 'programmers', 'directors', 'critics',
        'review', 'reviews', 'journal', 'weekly', 'daily', 'times',
        'international', 'world', 'global', 'national',
        'underground', 'independent', 'online', 'digital',
        'submissions', 'entries', 'open', 'call',
    ]
    if any(ind in words for ind in org_indicators):
        return False
    
    # Reject if any word contains '#' or '@' (hashtag/social scraps)
    if any('#' in w or '@' in w for w in words):
        return False
    
    # Reject names that are all caps (typically acronyms/orgs like "TIFF" or "BFI")
    if name == name.upper() and len(name) < 10:
        return False
    
    # Each word in a real name should be mostly alphabetic
    alpha_count = sum(1 for w in words if re.match(r'^[A-Za-z\'\-\.]+$', w))
    if alpha_count < 2:
        return False
    
    return True


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
        
        if is_linkedin_profile(link):
            # Best case: this is a /in/ profile page
            name = extract_name_from_linkedin(title)
            linkedin_url = link
        elif is_any_linkedin_url(link):
            # It's a LinkedIn post/article — still try to extract name
            name = extract_name_from_linkedin(title)
            linkedin_url = None  # Don't store non-profile URLs as the profile link
        else:
            name = title.split(' - ')[0].split(' | ')[0].strip()
            linkedin_url = None
        
        # Skip if we couldn't extract a valid name or it's not a person
        if not name or len(name) < 3:
            continue
        if not is_person_name(name):
            logger.info(f"  Rejected non-person: '{name}'")
            continue
        
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
