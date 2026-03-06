#!/usr/bin/env python3
"""
Generate Icebreakers — Use Perplexity API to create personalized icebreakers.

Reads contacts with status='enriched' and generates a 2-sentence
personalized icebreaker referencing their work for film outreach.

Usage:
    python -m execution.generate_icebreakers --limit 50
"""

import os
import sys
import json
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

PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
PERPLEXITY_URL = 'https://api.perplexity.ai/chat/completions'


def generate_icebreaker(name: str, bio: str, linkedin_url: str = None) -> str | None:
    """
    Generate a personalized icebreaker using Perplexity API.
    
    Args:
        name: Contact's full name
        bio: Their bio/description
        linkedin_url: Optional LinkedIn profile for context
    
    Returns:
        Icebreaker string or None on error
    """
    if not PERPLEXITY_API_KEY:
        logger.error("PERPLEXITY_API_KEY not set")
        return None
    
    context = f"Name: {name}"
    if bio:
        context += f"\nBio: {bio}"
    if linkedin_url:
        context += f"\nLinkedIn: {linkedin_url}"
    
    prompt = f"""Generate a 2-sentence personalized icebreaker for cold emailing this person about an indie film.
The first sentence should reference something specific about their work or background.
The second sentence should naturally bridge to why you're reaching out about your film.

Keep it warm, genuine, and NOT salesy. No generic flattery.

{context}

Reply with ONLY the 2-sentence icebreaker, nothing else."""

    headers = {
        'Authorization': f'Bearer {PERPLEXITY_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'model': 'sonar',
        'messages': [
            {
                'role': 'system',
                'content': 'You are a film publicist writing personalized outreach emails. Be specific, warm, and concise.'
            },
            {
                'role': 'user',
                'content': prompt
            }
        ],
        'max_tokens': 200,
        'temperature': 0.7
    }
    
    try:
        response = requests.post(PERPLEXITY_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        icebreaker = data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
        
        if icebreaker:
            logger.info(f"Generated icebreaker for {name}: {icebreaker[:80]}...")
            return icebreaker
        
    except Exception as e:
        logger.error(f"Perplexity error for {name}: {e}")
    
    return None


def generate_icebreakers_batch(limit: int = 50, dry_run: bool = False) -> dict:
    """
    Generate icebreakers for enriched contacts in batch.
    
    Args:
        limit: Max contacts to process
        dry_run: If True, don't update Supabase
    
    Returns:
        Stats dict
    """
    from supabase import create_client
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase not configured")
        return {'error': 'No Supabase credentials'}
    
    supabase = create_client(supabase_url, supabase_key)
    
    # Fetch enriched contacts without icebreakers
    result = supabase.table('contacts').select('*').eq('status', 'enriched').limit(limit).execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts needing icebreakers")
    
    stats = {'processed': 0, 'generated': 0, 'errors': 0}
    
    for i, contact in enumerate(contacts):
        try:
            logger.info(f"[{i+1}/{len(contacts)}] Generating for: {contact['name']}")
            
            icebreaker = generate_icebreaker(
                name=contact['name'],
                bio=contact.get('bio', ''),
                linkedin_url=contact.get('linkedin_url', '')
            )
            
            if icebreaker:
                if not dry_run:
                    supabase.table('contacts').update({
                        'icebreaker': icebreaker,
                        'status': 'icebreaker_ready',
                        'updated_at': datetime.utcnow().isoformat()
                    }).eq('id', contact['id']).execute()
                
                stats['generated'] += 1
            
            stats['processed'] += 1
            
            # Rate limiting: 2 seconds between API calls
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error generating icebreaker for {contact.get('name', '?')}: {e}")
            stats['errors'] += 1
    
    logger.info(f"Icebreaker generation complete: {stats}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate personalized icebreakers')
    parser.add_argument('--limit', type=int, default=50, help='Max contacts to process')
    parser.add_argument('--dry-run', action='store_true', help='Preview without saving')
    
    args = parser.parse_args()
    
    stats = generate_icebreakers_batch(limit=args.limit, dry_run=args.dry_run)
    print(json.dumps(stats, indent=2))
