#!/usr/bin/env python3
"""
Enrich Contacts — Find emails and Instagram handles via Apify LinkedIn + Serper Google Search.

Reads contacts from Supabase,
enriches with LinkedIn profile data (via Apify), email, and Instagram.

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

try:
    from apify_client import ApifyClient
except ImportError:
    ApifyClient = None

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'
APIFY_API_KEY = os.getenv('APIFY_API_KEY')
APIFY_ACTOR = 'apimaestro/linkedin-profile-full-sections-scraper'

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


def extract_linkedin_slug(linkedin_url: str) -> Optional[str]:
    """
    Extract the username slug from a LinkedIn profile URL.
    e.g. 'https://ca.linkedin.com/in/carolyn-mauricette-0438321' -> 'carolyn-mauricette-0438321'
    Returns None if the URL is not a profile URL (e.g. posts, activity).
    """
    if not linkedin_url:
        return None
    # Match /in/some-slug pattern
    match = re.search(r'linkedin\.com/in/([a-zA-Z0-9_-]+)', linkedin_url)
    if match:
        return match.group(1).rstrip('/')
    return None


def scrape_linkedin_apify(username_slug: str) -> dict:
    """
    Scrape a LinkedIn profile using apimaestro/linkedin-profile-full-sections-scraper.
    
    Returns a dict with:
        - 'email': str or None
        - 'headline': str or None
        - 'current_company': str or None
        - 'current_title': str or None
        - 'about': str or None
        - 'location': str or None
        - 'raw': full response dict
    """
    result = {
        'email': None, 'headline': None, 'current_company': None,
        'current_title': None, 'about': None, 'location': None, 'raw': None
    }
    
    if not APIFY_API_KEY or not ApifyClient:
        logger.warning("  Apify not available (missing API key or apify-client package)")
        return result
    
    try:
        client = ApifyClient(APIFY_API_KEY)
        run_input = {
            "includeEmail": True,
            "usernames": [username_slug]
        }
        
        logger.info(f"  Apify: scraping LinkedIn profile '{username_slug}'...")
        run = client.actor(APIFY_ACTOR).call(run_input=run_input, timeout_secs=120)
        
        items = list(client.dataset(run['defaultDatasetId']).iterate_items())
        if not items:
            logger.warning(f"  Apify: no data returned for '{username_slug}'")
            return result
        
        item = items[0]
        
        # Check for errors
        if item.get('message') and 'No profile found' in item.get('message', ''):
            logger.warning(f"  Apify: profile not found for '{username_slug}'")
            return result
        
        basic = item.get('basic_info', {})
        result['raw'] = item
        result['headline'] = basic.get('headline')
        result['about'] = basic.get('about')
        result['current_company'] = basic.get('current_company')
        result['email'] = basic.get('email')  # may be None
        
        loc = basic.get('location', {})
        if isinstance(loc, dict):
            result['location'] = loc.get('full') or loc.get('city')
        elif isinstance(loc, str):
            result['location'] = loc
        
        # Extract current title from experience
        experiences = item.get('experience', [])
        for exp in experiences:
            if exp.get('is_current'):
                result['current_title'] = exp.get('title')
                # If basic_info didn't have current_company, grab from experience
                if not result['current_company']:
                    result['current_company'] = exp.get('company')
                break
        
        logger.info(f"  Apify: got profile data. Email={result['email']}, Company={result['current_company']}")
        
    except Exception as e:
        logger.warning(f"  Apify scraping error for '{username_slug}': {e}")
    
    return result


def scrape_contact_page_apify(domain: str) -> list:
    """
    Use vdrmota/contact-info-scraper to crawl a company website's contact page
    and extract email addresses.
    
    Args:
        domain: Company domain (e.g. 'sundance.org')
    
    Returns:
        List of email addresses found
    """
    if not APIFY_API_KEY or not ApifyClient or not domain:
        return []
    
    try:
        client = ApifyClient(APIFY_API_KEY)
        
        # Build URLs to scrape — the main domain and common contact pages
        start_urls = [
            {'url': f'https://{domain}'},
            {'url': f'https://{domain}/contact'},
            {'url': f'https://{domain}/about'},
            {'url': f'https://www.{domain}/contact'},
        ]
        
        run_input = {
            'startUrls': start_urls,
            'maxDepth': 1,
            'maxRequestsPerStartUrl': 3,
            'sameDomain': True,
        }
        
        logger.info(f"  Apify: scraping contact page for domain '{domain}'...")
        run = client.actor('vdrmota/contact-info-scraper').call(run_input=run_input, timeout_secs=120)
        
        emails = []
        seen = set()
        for item in client.dataset(run['defaultDatasetId']).iterate_items():
            for email in item.get('emails', []):
                email_lower = email.lower()
                if email_lower not in seen and _is_valid_email(email_lower):
                    emails.append(email)
                    seen.add(email_lower)
        
        if emails:
            logger.info(f"  Apify contact scraper found {len(emails)} email(s): {emails}")
        else:
            logger.info(f"  Apify contact scraper: no emails found on {domain}")
        
        return emails
        
    except Exception as e:
        logger.warning(f"  Apify contact scraper error for '{domain}': {e}")
        return []


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
            
            # If we found valid emails from the primary broad search, no need to keep spamming targeted 
            # domain searches which might pull in unrelated company emails like info@ or jobs@
            if len(found_emails) > 0:
                logger.info(f"    -> Found {len(found_emails)} emails, stopping further queries.")
                break
            
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


def _score_email(email: str, name: str, domain: str, source: str) -> int:
    """
    Score an email candidate on a 0-100 confidence scale.
    
    Scoring factors:
        - Source reliability: apify_linkedin > apify_contact_page > serper
        - Name match: does the email contain the prospect's first/last name?
        - Domain match: does the email domain match the company domain?
        - Generic penalty: info@, submissions@, admin@ get penalized
    """
    score = 0
    email_lower = email.lower()
    local_part = email_lower.split('@')[0]
    email_domain = email_lower.split('@')[1] if '@' in email_lower else ''
    
    # Source base score
    source_scores = {
        'apify_linkedin': 40,      # Directly from their LinkedIn profile
        'apify_contact_page': 25,  # From company website
        'serper': 15,              # Regex from Google snippets
    }
    score += source_scores.get(source, 10)
    
    # Name match bonus (huge signal — email contains their name)
    name_parts = name.lower().split()
    first_name = name_parts[0] if name_parts else ''
    last_name = name_parts[-1] if len(name_parts) > 1 else ''
    
    if first_name and last_name:
        if first_name in local_part and last_name in local_part:
            score += 35  # firstname.lastname@ — very high confidence
        elif first_name in local_part or last_name in local_part:
            score += 20  # partial name match
        elif first_name[0] in local_part and last_name in local_part:
            score += 15  # initial + lastname (e.g. cmauricette@)
    
    # Domain match bonus
    if domain and email_domain and domain.lower() in email_domain:
        score += 15  # email is @companydomain.com
    
    # Generic email penalty
    generic_prefixes = ['info', 'contact', 'hello', 'admin', 'support', 
                        'submissions', 'general', 'office', 'team', 'press',
                        'media', 'marketing', 'sales', 'jobs', 'careers', 'hr']
    if any(local_part.startswith(g) for g in generic_prefixes):
        score -= 20  # It's a shared/generic inbox, not personal
    
    return max(0, min(100, score))


def enrich_single_contact(contact: dict) -> dict:
    """
    Enrich a single contact with LinkedIn profile data, email(s), and Instagram.
    
    Flow:
        1. If contact has a LinkedIn URL → scrape via Apify (profile data + email attempt)
        2. Extract company domain
        3. Run ALL email sources in parallel, collect candidates
        4. Score each candidate and pick the best
        5. Find Instagram via Serper
    
    Returns:
        Dict of enriched fields to update
    """
    name = contact.get('name', '')
    role_keyword = guess_role_keyword(contact)
    company_name = contact.get('company') or contact.get('source') or ''
    linkedin_url = contact.get('linkedin_url') or ''
    
    updates = {
        'enrichment_data': {},
        'status': 'enriched',
        'updated_at': datetime.utcnow().isoformat()
    }
    
    # Collect ALL email candidates from ALL sources: (email, source_tag)
    all_candidates = []
    
    # ── Step 0: Apify LinkedIn Scrape ──────────────────────────────────────
    slug = extract_linkedin_slug(linkedin_url)
    if slug:
        apify_data = scrape_linkedin_apify(slug)
        
        # Store profile data in enrichment_data
        if apify_data.get('headline'):
            updates['enrichment_data']['linkedin_headline'] = apify_data['headline']
        if apify_data.get('current_company'):
            updates['enrichment_data']['linkedin_company'] = apify_data['current_company']
            if not company_name:
                company_name = apify_data['current_company']
        if apify_data.get('current_title'):
            updates['enrichment_data']['linkedin_title'] = apify_data['current_title']
        if apify_data.get('about'):
            updates['enrichment_data']['linkedin_about'] = apify_data['about']
        if apify_data.get('location'):
            updates['enrichment_data']['linkedin_location'] = apify_data['location']
        
        if apify_data.get('email'):
            all_candidates.append((apify_data['email'], 'apify_linkedin'))
    
    # ── Step 1: Domain Extraction ──────────────────────────────────────────
    domain = None
    if company_name:
        domain = extract_domain_serper(company_name)
        if domain:
            updates['enrichment_data']['company_domain'] = domain
            logger.info(f"  Extracted domain: {domain}")
    
    # ── Step 2: Serper Email Dorks (always runs) ──────────────────────────
    serper_emails = find_emails_serper(name, role_keyword, domain)
    for em in serper_emails:
        all_candidates.append((em, 'serper'))
    
    # ── Step 2.5: Apify Contact Page Scraper (always runs if we have a domain)
    if domain:
        page_emails = scrape_contact_page_apify(domain)
        for em in page_emails:
            all_candidates.append((em, 'apify_contact_page'))
    
    # ── Confidence Scoring: pick the best email ───────────────────────────
    if all_candidates:
        # Deduplicate while preserving best source
        seen = {}
        for em, src in all_candidates:
            em_lower = em.lower()
            if em_lower not in seen:
                seen[em_lower] = (em, src)
            else:
                # Keep the one from the more reliable source
                existing_src = seen[em_lower][1]
                source_rank = {'apify_linkedin': 3, 'apify_contact_page': 2, 'serper': 1}
                if source_rank.get(src, 0) > source_rank.get(existing_src, 0):
                    seen[em_lower] = (em, src)
        
        # Score all unique candidates
        scored = []
        for em_lower, (em, src) in seen.items():
            score = _score_email(em, name, domain, src)
            scored.append((score, em, src))
            logger.info(f"    Email candidate: {em} (source={src}, score={score})")
        
        # Sort by score descending
        scored.sort(key=lambda x: x[0], reverse=True)
        
        best_score, best_email, best_source = scored[0]
        updates['email'] = best_email
        updates['enrichment_data']['email_source'] = best_source
        updates['enrichment_data']['email_confidence'] = best_score
        updates['enrichment_data']['email_candidates'] = [
            {'email': em, 'source': src, 'confidence': sc}
            for sc, em, src in scored
        ]
        logger.info(f"  ✅ Best email: {best_email} (source={best_source}, confidence={best_score})")
    
    # ── Step 3: Instagram via Serper ───────────────────────────────────────
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
