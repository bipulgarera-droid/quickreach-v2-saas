#!/usr/bin/env python3
"""
Enrich Contacts — 2-Step Serper Flow:
  1. Find company website (if missing) via: {company} {niche}
  2. Find emails via: "@{domain}" AND ("email" OR "contact")

Also finds Instagram handles.

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
from urllib.parse import urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

from execution.verify_email import check_email

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'

# Domains to skip when discovering a company website
SKIP_DOMAINS = {
    'linkedin.com', 'facebook.com', 'twitter.com', 'x.com',
    'instagram.com', 'youtube.com', 'wikipedia.org', 'imdb.com',
    'crunchbase.com', 'glassdoor.com', 'indeed.com', 'yelp.com',
    'reddit.com', 'quora.com', 'pinterest.com', 'tiktok.com',
    'google.com', 'apple.com', 'amazon.com', 'github.com',
    'medium.com', 'forbes.com', 'bloomberg.com',
    'justdial.com', 'zaubacorp.com', 'tofler.in', 'tracxn.com',
}


def _extract_domain(url: str) -> str:
    """Extract clean domain from URL (no www, no path)."""
    if not url:
        return ""
    try:
        parsed = urlparse(url if '://' in url else f'https://{url}')
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def _is_company_site(url: str) -> bool:
    """Check if URL looks like a legit company website (not social media, etc)."""
    domain = _extract_domain(url)
    if not domain:
        return False
    for skip in SKIP_DOMAINS:
        if domain == skip or domain.endswith('.' + skip):
            return False
    return True


def _extract_brand_from_url(url: str) -> str:
    """Extract a clean brand name from a URL."""
    if not url:
        return ""
    try:
        domain = _extract_domain(url)
        # Remove TLD
        brand = re.sub(r'\.(com|in|org|net|biz|co\.in|me|tv|us|ae|io|ai|live|co)$', '', domain, flags=re.IGNORECASE)
        brand = brand.replace('-', ' ').replace('_', ' ')
        # CamelCase split
        brand = re.sub(r'([a-z])([A-Z])', r'\1 \2', brand)
        return brand.title().strip()
    except Exception:
        return ""


def _extract_human_name_from_email(email: str) -> Optional[str]:
    """Extract a human name from a personal-looking email address."""
    if not email:
        return None
    local_part = email.split('@')[0].lower()
    
    generic = ['info', 'contact', 'hello', 'admin', 'support', 'office', 'team',
               'press', 'mail', 'projects', 'careers', 'vfx', 'hr', 'sales',
               'marketing', 'general', 'submissions', 'media', 'jobs']
    if any(local_part == g or local_part.startswith(g + '.') or local_part.startswith(g + '_') for g in generic):
        return None
        
    name_parts = re.split(r'[\._-]', local_part)
    clean_parts = [re.sub(r'\d+', '', p).title() for p in name_parts if len(re.sub(r'\d+', '', p)) > 2]
    
    if clean_parts:
        return ' '.join(clean_parts[:2])
    return None


# =============================================================================
# STEP 1: Find Company Website
# =============================================================================

def find_website_serper(company: str, niche: str = '') -> Optional[str]:
    """
    Find company website via Serper search.
    Query: {company} {niche}
    Returns the first organic result URL that looks like a real company site.
    """
    if not SERPER_API_KEY or not company:
        return None
    
    query_parts = [company.strip()]
    if niche:
        query_parts.append(niche.strip())
    query = ' '.join(query_parts)
    
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    try:
        payload = {'q': query, 'num': 10}
        logger.info(f"  🔍 Website search: {query}")
        
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        # Check knowledge graph first (often has the official website)
        kg = data.get('knowledgeGraph', {})
        kg_website = kg.get('website', '')
        if kg_website and _is_company_site(kg_website):
            logger.info(f"  🌐 Found website (KG): {kg_website}")
            return kg_website
        
        # Scan organic results
        for result in data.get('organic', []):
            url = result.get('link', '')
            if _is_company_site(url):
                logger.info(f"  🌐 Found website (organic): {url}")
                return url
                
    except Exception as e:
        logger.warning(f"Website search error for '{company}': {e}")
    
    return None


# =============================================================================
# STEP 2: Find Emails by Domain
# =============================================================================

def find_emails_by_domain(domain: str) -> list[str]:
    """
    Find email addresses associated with a domain via Serper.
    Query: "@{domain}" AND ("email" OR "contact")
    """
    if not SERPER_API_KEY or not domain:
        return []
    
    query = f'"@{domain}" AND ("email" OR "contact")'
    
    headers = {
        'X-API-KEY': SERPER_API_KEY,
        'Content-Type': 'application/json'
    }
    
    found_emails = []
    seen_emails = set()
    
    try:
        payload = {'q': query, 'num': 10}
        logger.info(f"  📧 Email search: {query}")
        
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        # Check AI snippet / answer box / knowledge graph
        for text_block in [
            data.get('answerBox', {}).get('snippet', '') or '',
            data.get('answerBox', {}).get('answer', '') or '',
            data.get('knowledgeGraph', {}).get('description', '') or '',
        ]:
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text_block)
            for email in emails:
                email = email.strip().rstrip('.,;:)!% ]').strip()
                email_lower = email.lower()
                if email_lower not in seen_emails and _is_valid_email(email_lower):
                    found_emails.append(email)
                    seen_emails.add(email_lower)
        
        # Scan organic results
        for result in data.get('organic', []):
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            for email in emails:
                email = email.strip().rstrip('.,;:)!% ]').strip()
                email_lower = email.lower()
                if email_lower not in seen_emails and _is_valid_email(email_lower):
                    found_emails.append(email)
                    seen_emails.add(email_lower)
        
        if found_emails:
            logger.info(f"    -> Found {len(found_emails)} emails for @{domain}")
            
    except Exception as e:
        logger.warning(f"Email search error for domain '{domain}': {e}")
    
    return found_emails


def _is_valid_email(email: str) -> bool:
    """Filter out junk/generic emails."""
    skip_patterns = [
        'example.com', 'email.com', 'noreply', 'no-reply', 'donotreply',
        'test@', 'sentry.io', 'github.com', 'placeholder', 'domain.com',
        'yourname@', 'name@', 'user@', 'sample', 'wix.com', 'squarespace.com'
    ]
    return not any(skip in email for skip in skip_patterns)


def _score_email(email: str, company: str = '', domain: str = '') -> int:
    """Score an email candidate on a 0-100 scale."""
    score = 20  # Base score
    email_lower = email.lower()
    local_part = email_lower.split('@')[0]
    email_domain = email_lower.split('@')[1] if '@' in email_lower else ''
    
    # Domain match bonus — email from the company's own domain is best
    if domain and email_domain == domain.lower():
        score += 30
    
    # Personal email bonus (has a name, not generic)
    generic_prefixes = ['info', 'contact', 'hello', 'admin', 'support',
                        'submissions', 'general', 'office', 'team', 'press',
                        'media', 'marketing', 'sales', 'jobs', 'careers', 'hr']
    if any(local_part.startswith(g) for g in generic_prefixes):
        score -= 10  # Still useful, just lower priority
    else:
        score += 15  # Likely a personal email
    
    # Free email penalty (gmail, yahoo, etc)
    free_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com']
    if email_domain in free_domains:
        score -= 15
    
    return max(0, min(100, score))


# =============================================================================
# Instagram Search (unchanged)
# =============================================================================

def find_instagram_serper(name: str, niche: str = '') -> Optional[str]:
    """Find Instagram handle via Serper search."""
    if not SERPER_API_KEY:
        return None
        
    try:
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        
        query_parts = [name]
        if niche:
            query_parts.append(niche)
        query_parts.append('instagram')
        query = ' '.join(query_parts)
        
        payload = {'q': query, 'num': 5}
        
        logger.info(f"  📸 Instagram search: {query}")
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        skip_handles = {'explore', 'accounts', 'about', 'tags', 'locations', 'stories', 'directory'}
        
        for result in data.get('organic', []):
            url = result.get('link', '')
            
            if '/p/' in url or '/reel/' in url or '/tv/' in url:
                return url
                
            match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', url)
            if match:
                handle = match.group(1)
                if handle.lower() not in skip_handles and handle.lower() not in ['p', 'reel', 'tv']:
                    return f"@{handle}"
                    
    except Exception as e:
        logger.warning(f"Instagram search error for {name}: {e}")
        
    return None


# =============================================================================
# Main Enrichment Logic
# =============================================================================

def enrich_single_contact(contact: dict, project_info: dict = None) -> Optional[dict]:
    """
    Enrich a contact with the 2-step Serper flow:
      1. Find website if missing (company + niche query)
      2. Find emails using domain (if website found)
      3. Find Instagram (company + niche query)
    """
    company = contact.get('company') or contact.get('name') or ''
    niche = ''
    
    # Determine niche: contact niche > project niche > project name
    if contact.get('niche'):
        niche = contact['niche']
    elif project_info:
        niche = project_info.get('niche') or project_info.get('name') or ''
    
    existing_email = (contact.get('email') or '').strip()
    existing_instagram = (contact.get('instagram') or '').strip()
    existing_website = (contact.get('website') or '').strip()
    
    existing_enrichment = contact.get('enrichment_data')
    if isinstance(existing_enrichment, str):
        try:
            existing_enrichment = json.loads(existing_enrichment)
        except Exception:
            existing_enrichment = {}
    elif not isinstance(existing_enrichment, dict):
        existing_enrichment = {}
    
    # Also check enrichment_data for website
    if not existing_website:
        existing_website = (existing_enrichment.get('website') or '').strip()
    
    # ── SMART SKIP ──
    has_verif = existing_enrichment.get('verification_status') is not None
    if existing_email and existing_instagram and has_verif:
        logger.info(f"  ⏭️  Skipping — Email, Instagram, and Verification all present.")
        return None

    updates = {
        'enrichment_data': existing_enrichment.copy(),
        'status': 'enriched',
        'updated_at': datetime.utcnow().isoformat()
    }

    # ══════════════════════════════════════════════════════
    # STEP 1: Find Website (if missing)
    # ══════════════════════════════════════════════════════
    website = existing_website
    if not website:
        website = find_website_serper(company, niche)
        if website:
            updates['website'] = website
            updates['enrichment_data']['website'] = website
            updates['enrichment_data']['website_source'] = 'serper'
            logger.info(f"  ✅ Website found: {website}")
        else:
            logger.warning(f"  ❌ No website found for: {company}")
    
    domain = _extract_domain(website) if website else ''
    
    # ══════════════════════════════════════════════════════
    # STEP 2: Find Emails by Domain
    # ══════════════════════════════════════════════════════
    if not existing_email and domain:
        emails = find_emails_by_domain(domain)
        
        if emails:
            scored = [(email, _score_email(email, company, domain)) for email in emails]
            scored.sort(key=lambda x: x[1], reverse=True)
            
            for em_candidate, em_score in scored:
                logger.info(f"    Email candidate: {em_candidate} (score={em_score})")
            
            valid_found = False
            for best_email, best_score in scored:
                if best_score < 10:
                    break
                    
                logger.info(f"  Verifying: {best_email} (score={best_score})...")
                v_status, v_reason = check_email(best_email)
                logger.info(f"  Verification: {v_status} ({v_reason})")
                
                if v_status == 'invalid':
                    logger.warning(f"  ❌ Invalid: {best_email}")
                    continue
                
                # Found a valid/risky email
                updates['email'] = best_email
                logger.info(f"  ✅ Email saved: {best_email} (confidence={best_score}, status={v_status})")
                
                # Try to extract human name from email
                current_name = (contact.get('name') or '').lower().strip()
                current_company = (contact.get('company') or '').lower().strip()
                if current_name == current_company or not current_name:
                    human_name = _extract_human_name_from_email(best_email)
                    if human_name:
                        updates['name'] = human_name
                        logger.info(f"  👤 Human name from email: {human_name}")
                
                updates['enrichment_data']['email_source'] = 'serper_domain'
                updates['enrichment_data']['email_confidence'] = best_score
                updates['enrichment_data']['verification_status'] = v_status
                updates['enrichment_data']['verification_reason'] = v_reason
                valid_found = True
                break
            
            if not valid_found:
                logger.warning(f"  ❌ All email candidates invalid or below threshold.")
                updates['email'] = None
            
            updates['enrichment_data']['email_candidates'] = [
                {'email': em, 'source': 'serper_domain', 'confidence': sc}
                for em, sc in scored
            ]
        else:
            logger.warning(f"  ❌ No emails found for domain: {domain}")
    elif existing_email and not has_verif:
        # Verify existing email
        logger.info(f"  Verifying existing email: {existing_email}...")
        v_status, v_reason = check_email(existing_email)
        logger.info(f"  Verification: {v_status} ({v_reason})")
        updates['enrichment_data']['verification_status'] = v_status
        updates['enrichment_data']['verification_reason'] = v_reason
    elif not domain and not existing_email:
        logger.warning(f"  ⚠️ No website/domain — can't search for emails.")

    # ══════════════════════════════════════════════════════
    # STEP 3: Instagram
    # ══════════════════════════════════════════════════════
    if not existing_instagram:
        instagram = find_instagram_serper(company, niche)
        if instagram:
            updates['instagram'] = instagram
            updates['enrichment_data']['instagram_source'] = 'serper'
            logger.info(f"  📸 Instagram: {instagram}")
    
    # ── Brand / Company extraction from URL ──
    if contact.get('status') in ['new', 'enriched'] and website:
        clean_brand = _extract_brand_from_url(website)
        if clean_brand and len(clean_brand) > 2:
            updates['name'] = updates.get('name') or clean_brand
            updates['company'] = clean_brand
            logger.info(f"  🏢 Brand from URL: {clean_brand}")
    
    return updates


def enrich_contacts(limit: int = 50, project_id: str = None, contact_ids: list = None, dry_run: bool = False) -> dict:
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not configured")
        return {'error': 'No Supabase credentials'}
        
    supabase = create_client(supabase_url, supabase_key)
    
    # Fetch project info (including niche) for context
    project_info = {}
    if project_id:
        p_res = supabase.table('projects').select('name, description, niche').eq('id', project_id).limit(1).execute()
        if p_res.data:
            project_info = p_res.data[0]
        
    query = supabase.table('contacts').select('*')
    if contact_ids and len(contact_ids) > 0:
        logger.info(f"  Fetching specific contact IDs: {len(contact_ids)}")
        query = query.in_('id', contact_ids).range(0, 1000)
    elif project_id:
        logger.info(f"  Fetching pending contacts for project {project_id} (limit {limit})")
        query = query.eq('project_id', project_id).eq('status', 'new').limit(limit)
    else:
        logger.info(f"  Fetching pending contacts with limit: {limit}")
        query = query.eq('status', 'new').limit(limit)
        
    result = query.execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts to enrich")
    
    stats = {'processed': 0, 'emails_found': 0, 'websites_found': 0, 'ig_found': 0, 'errors': 0}
    
    for i, contact in enumerate(contacts):
        try:
            pid = project_id or contact.get('project_id')
            if pid and not project_info:
                p_res = supabase.table('projects').select('name, description, niche').eq('id', pid).limit(1).execute()
                if p_res.data:
                    project_info = p_res.data[0]

            logger.info(f"[{i+1}/{len(contacts)}] Enriching: {contact.get('name', '?')} / {contact.get('company', '?')}")
            updates = enrich_single_contact(contact, project_info)
            
            if updates and not dry_run:
                supabase.table('contacts').update(updates).eq('id', contact['id']).execute()
            
            if updates:
                if updates.get('email'):
                    stats['emails_found'] += 1
                if updates.get('website'):
                    stats['websites_found'] += 1
                if updates.get('instagram'):
                    stats['ig_found'] += 1
                stats['processed'] += 1
            
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error enriching {contact.get('name', '?')}: {e}")
            stats['errors'] += 1
            
    logger.info(f"Enrichment complete: {stats}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Enrich contacts — 2-step Serper flow")
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--id', type=str, help="Specific contact ID to enrich")
    args = parser.parse_args()
    
    ids = [args.id] if args.id else None
    res = enrich_contacts(limit=args.limit, contact_ids=ids)
    print(json.dumps(res, indent=2))
