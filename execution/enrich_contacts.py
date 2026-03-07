#!/usr/bin/env python3
"""
Enrich Contacts — Find emails and Instagram handles via Serper Google Search.

Reads contacts with status='new' from Supabase,
enriches with email and Instagram using smart Google queries.

Usage:
    python -m execution.enrich_contacts --limit 50
"""

import os
import sys
import json
import re
import argparse
import requests
import logging
import time
from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'

# Common role keywords to help Google narrow results
ROLE_KEYWORDS_MAP = {
    'programmer': 'festival programmer',
    'curator': 'festival curator',
    'director': 'festival director',
    'producer': 'producer',
    'critic': 'film critic',
    'writer': 'writer',
    'journalist': 'journalist',
    'ceo': 'ceo',
    'founder': 'founder',
    'marketing': 'marketing',
    'manager': 'manager',
}


def guess_role_keyword(contact: dict) -> str:
    """
    Try to extract a role keyword from the contact's bio or source query.
    This helps narrow down Serper searches (e.g. 'John Doe festival programmer email').
    """
    bio = (contact.get('bio') or '').lower()
    source = (contact.get('source') or '').lower()
    combined = f"{bio} {source}"
    
    for keyword, role_phrase in ROLE_KEYWORDS_MAP.items():
        if keyword in combined:
            return role_phrase
    
    # Default fallback: just use empty string so search stays broad
    return ''


def extract_domain_serper(company_name: str) -> Optional[str]:
    """Find the official website domain for a company using Serper."""
    if not SERPER_API_KEY or not company_name:
        return None
        
    try:
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        # Dork to find official website, excluding linkedin
        query = f'"{company_name}" ("official website" OR site OR .com OR .org OR .io) -inurl:linkedin'
        payload = {'q': query, 'num': 3}
        
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        for result in data.get('organic', []):
            url = result.get('link', '')
            # Extract domain from url
            match = re.search(r'https?://(?:www\.)?([^/]+)', url)
            if match:
                domain = match.group(1).lower()
                # Skip known directories/socials
                if not any(skip in domain for skip in ['facebook', 'instagram', 'twitter', 'wikipedia', 'imdb']):
                    return domain
    except Exception as e:
        logger.warning(f"Domain extraction error for {company_name}: {e}")
    return None


def find_emails_serper(name: str, role_keyword: str = '', domain: str = None) -> list[str]:
    """
    Find email addresses by searching Google via Serper.
    Runs multiple targeted dorks based on available info.
    Returns ALL valid emails found across results.
    """
    if not SERPER_API_KEY:
        return []
    
    found_emails = []
    seen_emails = set()
    
    queries_to_run = []
    
    # 1. Broad search: name + role + email
    broad_parts = [name]
    if role_keyword:
        broad_parts.append(role_keyword)
    broad_parts.append('email')
    queries_to_run.append(' '.join(broad_parts))
    
    # 2. Domain-specific searches
    if domain:
        # e.g., site:sundance.org "John Doe" email
        queries_to_run.append(f'site:{domain} "{name}" email')
        # e.g., site:sundance.org contact OR email
        queries_to_run.append(f'site:{domain} contact OR email')
        # e.g., "John Doe" "@sundance.org"
        queries_to_run.append(f'"{name}" "@{domain}"')
    
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    for query in queries_to_run:
        try:
            payload = {'q': query, 'num': 10}
            logger.info(f"  Email search query: {query}")
            
            response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
            data = response.json()
            
            extract_emails_from_serper_data(data, found_emails, seen_emails)
            
            # If we already found emails for this person, we don't necessarily need to keep blasting queries
            # but getting more candidates is better for fallback. Let's run all queries to maximize candidates.
            
        except Exception as e:
            logger.warning(f"Serper email search error for query '{query}': {e}")
    
    return found_emails

def extract_emails_from_serper_data(data: dict, found_emails: list, seen_emails: set):
    """Helper to regex emails out of Serper JSON response."""
    # Check AI snippet / knowledge graph first (Google's AI answer)
    ai_snippet = data.get('answerBox', {}).get('snippet', '') or ''
    ai_answer = data.get('answerBox', {}).get('answer', '') or ''
    knowledge_desc = data.get('knowledgeGraph', {}).get('description', '') or ''
    
    for text_block in [ai_snippet, ai_answer, knowledge_desc]:
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text_block)
        for email in emails:
            email_lower = email.lower()
            if email_lower not in seen_emails and _is_valid_email(email_lower):
                found_emails.append(email)
                seen_emails.add(email_lower)
    
    # Scan organic results
    for result in data.get('organic', []):
        text = f"{result.get('title', '')} {result.get('snippet', '')}"
        emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
        for email in emails:
            email_lower = email.lower()
            if email_lower not in seen_emails and _is_valid_email(email_lower):
                found_emails.append(email)
                seen_emails.add(email_lower)


def _is_valid_email(email: str) -> bool:
    """Filter out junk/generic emails."""
    skip_patterns = [
        'example.com', 'email.com', 'noreply', 'support@', 'info@',
        'admin@', 'webmaster@', 'no-reply', 'donotreply', 'test@',
        'sentry.io', 'github.com', 'placeholder', 'domain.com',
        'yourname@', 'name@', 'user@', 'sample'
    ]
    return not any(skip in email for skip in skip_patterns)


def find_instagram_serper(name: str, role_keyword: str = '') -> Optional[str]:
    """
    Find Instagram handle via Serper search.
    Uses unquoted name + role keyword + site:instagram.com.
    Extracts handle from ANY Instagram URL (profiles, posts, reels).
    """
    if not SERPER_API_KEY:
        return None
    
    try:
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        
        # Build query: name (unquoted) + role keyword + site:instagram.com
        query_parts = [name]
        if role_keyword:
            query_parts.append(role_keyword)
        query_parts.append('site:instagram.com')
        query = ' '.join(query_parts)
        
        payload = {
            'q': query,
            'num': 5
        }
        
        logger.info(f"  Instagram search query: {query}")
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        # Generic IG pages that are NOT handles
        skip_handles = {'explore', 'accounts', 'about', 'tags', 'locations', 'stories', 'directory'}
        
        for result in data.get('organic', []):
            url = result.get('link', '')
            
            # If it's a post or reel, return the full URL instead of the poster's handle
            # (since the prospect is likely just tagged in the caption)
            if '/p/' in url or '/reel/' in url or '/tv/' in url:
                return url
                
            # Otherwise, extract the profile handle
            match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', url)
            if match:
                handle = match.group(1)
                # Filter out generic Instagram pages
                if handle.lower() not in skip_handles and handle.lower() not in ['p', 'reel', 'tv']:
                    return f"@{handle}"
    except Exception as e:
        logger.warning(f"Instagram search error for {name}: {e}")
    
    return None


def enrich_single_contact(contact: dict) -> dict:
    """
    Enrich a single contact with email(s) and Instagram.
    
    Returns:
        Dict of enriched fields to update
    """
    name = contact.get('name', '')
    role_keyword = guess_role_keyword(contact)
    company_name = contact.get('company') or contact.get('source') or ''
    
    updates = {
        'enrichment_data': {},
        'status': 'enriched',
        'updated_at': datetime.utcnow().isoformat()
    }
    
    # 1. Extract domain
    domain = None
    if company_name:
        domain = extract_domain_serper(company_name)
        if domain:
            updates['enrichment_data']['company_domain'] = domain
            logger.info(f"  Extracted domain: {domain}")
    
    # 2. Find emails via Serper (runs broad + domain-specific queries)
    emails = find_emails_serper(name, role_keyword, domain)
    if emails:
        # Store the first email as primary, all candidates in enrichment_data
        updates['email'] = emails[0]
        updates['enrichment_data']['email_source'] = 'serper_enhanced'
        updates['enrichment_data']['email_candidates'] = emails
        logger.info(f"  Found {len(emails)} email(s): {emails}")
    
    # 2. Find Instagram via Serper
    instagram = find_instagram_serper(name, role_keyword)
    if instagram:
        updates['instagram'] = instagram
        updates['enrichment_data']['instagram_source'] = 'serper'
        logger.info(f"  Found Instagram: {instagram}")
    
    updates['enrichment_data'] = json.dumps(updates['enrichment_data'])
    
    return updates


def enrich_contacts(limit: int = 50, contact_ids: list = None, dry_run: bool = False) -> dict:
    """
    Enrich contacts in batch.
    
    Args:
        limit: Max contacts to enrich per run
        contact_ids: Optional list of specific contact IDs to enrich (bypasses status='new' check)
        dry_run: If True, don't update Supabase
    """
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not configured")
        return {'error': 'No Supabase credentials'}
    
    supabase = create_client(supabase_url, supabase_key)
    
    # Fetch contacts needing enrichment
    query = supabase.table('contacts').select('*')
    if contact_ids and len(contact_ids) > 0:
        query = query.in_('id', contact_ids)
    else:
        query = query.eq('status', 'new').limit(limit)
        
    result = query.execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts to enrich")
    
    stats = {'processed': 0, 'emails_found': 0, 'ig_found': 0, 'errors': 0}
    
    for i, contact in enumerate(contacts):
        try:
            logger.info(f"[{i+1}/{len(contacts)}] Enriching: {contact['name']}")
            
            updates = enrich_single_contact(contact)
            
            if not dry_run:
                supabase.table('contacts').update(updates).eq('id', contact['id']).execute()
            
            if updates.get('email'):
                stats['emails_found'] += 1
            if updates.get('instagram'):
                stats['ig_found'] += 1
            
            stats['processed'] += 1
            
            # Rate limiting: 1 second between contacts (2 Serper calls per contact)
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error enriching {contact.get('name', '?')}: {e}")
            stats['errors'] += 1
    
    logger.info(f"Enrichment complete: {stats}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Enrich contacts with emails and Instagram')
    parser.add_argument('--limit', type=int, default=50, help='Max contacts to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    
    args = parser.parse_args()
    
    stats = enrich_contacts(limit=args.limit, dry_run=args.dry_run)
    print(json.dumps(stats, indent=2))
