#!/usr/bin/env python3
"""
Enrich Contacts — Find emails and Instagram handles via Serper/Hunter.

Reads contacts with status='new' from Supabase,
enriches with email (Hunter.io or Serper fallback) and Instagram.

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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERPER_API_KEY = os.getenv('SERPER_API_KEY')
HUNTER_API_KEY = os.getenv('HUNTER_API_KEY')
SERPER_URL = 'https://google.serper.dev/search'


def find_email_hunter(name: str, domain: str = None) -> str | None:
    """Find email via Hunter.io email finder API."""
    if not HUNTER_API_KEY:
        return None
    
    try:
        parts = name.split()
        if len(parts) < 2:
            return None
        
        params = {
            'api_key': HUNTER_API_KEY,
            'first_name': parts[0],
            'last_name': ' '.join(parts[1:]),
        }
        if domain:
            params['domain'] = domain
        
        response = requests.get('https://api.hunter.io/v2/email-finder', params=params, timeout=15)
        data = response.json()
        
        if data.get('data', {}).get('email'):
            return data['data']['email']
    except Exception as e:
        logger.warning(f"Hunter.io error for {name}: {e}")
    
    return None


def find_email_serper(name: str) -> str | None:
    """Find email by searching Google via Serper."""
    if not SERPER_API_KEY:
        return None
    
    try:
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        payload = {
            'q': f'"{name}" email contact',
            'num': 10
        }
        
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        # Look for email patterns in results
        for result in data.get('organic', []):
            text = f"{result.get('title', '')} {result.get('snippet', '')}"
            emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
            for email in emails:
                # Filter out generic/junk emails
                if not any(skip in email.lower() for skip in ['example.com', 'email.com', 'noreply', 'support@', 'info@']):
                    return email
    except Exception as e:
        logger.warning(f"Serper email search error for {name}: {e}")
    
    return None


def find_instagram_serper(name: str) -> str | None:
    """Find Instagram handle via Serper search."""
    if not SERPER_API_KEY:
        return None
    
    try:
        headers = {
            'X-API-KEY': SERPER_API_KEY,
            'Content-Type': 'application/json'
        }
        payload = {
            'q': f'"{name}" site:instagram.com',
            'num': 5
        }
        
        response = requests.post(SERPER_URL, headers=headers, json=payload, timeout=15)
        data = response.json()
        
        for result in data.get('organic', []):
            url = result.get('link', '')
            # Extract IG handle from instagram.com/username
            match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', url)
            if match:
                handle = match.group(1)
                # Filter out generic pages
                if handle.lower() not in ['p', 'explore', 'reel', 'stories', 'accounts', 'about']:
                    return f"@{handle}"
    except Exception as e:
        logger.warning(f"Instagram search error for {name}: {e}")
    
    return None


def enrich_single_contact(contact: dict) -> dict:
    """
    Enrich a single contact with email and Instagram.
    
    Returns:
        Dict of enriched fields to update
    """
    name = contact.get('name', '')
    updates = {
        'enrichment_data': {},
        'status': 'enriched',
        'updated_at': datetime.utcnow().isoformat()
    }
    
    # 1. Find email: try Hunter first, then Serper
    email = find_email_hunter(name)
    if email:
        updates['email'] = email
        updates['enrichment_data']['email_source'] = 'hunter'
    else:
        email = find_email_serper(name)
        if email:
            updates['email'] = email
            updates['enrichment_data']['email_source'] = 'serper'
    
    # 2. Find Instagram
    instagram = find_instagram_serper(name)
    if instagram:
        updates['instagram'] = instagram
        updates['enrichment_data']['instagram_source'] = 'serper'
    
    updates['enrichment_data'] = json.dumps(updates['enrichment_data'])
    
    return updates


def enrich_contacts(limit: int = 50, dry_run: bool = False) -> dict:
    """
    Enrich contacts in batch.
    
    Args:
        limit: Max contacts to enrich per run
        dry_run: If True, don't update Supabase
    
    Returns:
        Stats dict: {processed, emails_found, ig_found, errors}
    """
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase credentials not configured")
        return {'error': 'No Supabase credentials'}
    
    supabase = create_client(supabase_url, supabase_key)
    
    # Fetch contacts needing enrichment
    result = supabase.table('contacts').select('*').eq('status', 'new').limit(limit).execute()
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
            
            # Rate limiting: 1 second between contacts
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
