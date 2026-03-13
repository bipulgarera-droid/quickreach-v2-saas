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


def generate_icebreaker(name: str, bio: str, linkedin_url: str = None, enrichment_data: dict = None) -> str | None:
    """
    Generate a personalized icebreaker using Perplexity API.
    
    Args:
        name: Contact's full name
        bio: Their bio/description
        linkedin_url: Optional LinkedIn profile for context
        enrichment_data: Optional dict with LinkedIn-scraped profile fields
    
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
    
    # Add rich LinkedIn data if available
    if enrichment_data:
        if enrichment_data.get('linkedin_headline'):
            context += f"\nLinkedIn Headline: {enrichment_data['linkedin_headline']}"
        if enrichment_data.get('linkedin_company'):
            context += f"\nCurrent Company: {enrichment_data['linkedin_company']}"
        if enrichment_data.get('linkedin_title'):
            context += f"\nCurrent Title: {enrichment_data['linkedin_title']}"
        if enrichment_data.get('linkedin_about'):
            # Truncate very long bios to avoid blowing up the token limit
            about = enrichment_data['linkedin_about'][:1500]
            context += f"\nLinkedIn About: {about}"
        if enrichment_data.get('linkedin_location'):
            context += f"\nLocation: {enrichment_data['linkedin_location']}"
    
    prompt = f"""Generate a 1-2 sentence heavily personalized icebreaker for cold emailing this person.
The icebreaker MUST observe or compliment something specific about their CURRENT job, current company, or most recent publicly shared achievement. 
CRITICAL: Do NOT hallucinate or confuse their current job with past roles. Stick strictly to their present situation.
CRITICAL: Do NOT mention your own project, film, business, or reason for reaching out. The icebreaker must be 100% about THEM and their work.

Keep it warm, genuine, and NOT salesy. No generic flattery.
CRITICAL INSTRUCTIONS:
1. NEVER include academic citations, footnotes, or bracketed numbers like [1] or [2] in your response.
2. DO NOT use HTML tags like <p> or <br>. Use standard text line breaks if needed.

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
                'content': 'You are an elite B2B and cold-email publicist writing personalized outreach emails. Be specific, accurate about their current role, warm, and concise.'
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


def generate_icebreakers_batch(limit: int = 50, project_id: str | None = None, contact_ids: list | None = None, dry_run: bool = False) -> dict:
    """
    Generate icebreakers for enriched contacts in batch.
    
    Args:
        limit: Max contacts to process
        project_id: Optional project to scope the generation
        contact_ids: Optional specific contacts to generate for
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
    query = supabase.table('contacts').select('*').eq('status', 'enriched')
    
    if contact_ids:
        query = query.in_('id', contact_ids)
    elif project_id:
        query = query.eq('project_id', project_id)
        
    result = query.limit(limit).execute()
    contacts = result.data or []
    
    logger.info(f"Found {len(contacts)} contacts needing icebreakers")
    
    stats = {'processed': 0, 'generated': 0, 'errors': 0}
    
    for i, contact in enumerate(contacts):
        try:
            logger.info(f"[{i+1}/{len(contacts)}] Generating for: {contact['name']}")
            
            # Parse enrichment_data JSON if available
            enrichment_data = contact.get('enrichment_data')
            if isinstance(enrichment_data, str):
                try:
                    enrichment_data = json.loads(enrichment_data)
                except (json.JSONDecodeError, TypeError):
                    enrichment_data = {}
            elif not isinstance(enrichment_data, dict):
                enrichment_data = {}
            
            icebreaker = generate_icebreaker(
                name=contact['name'],
                bio=contact.get('bio', ''),
                linkedin_url=contact.get('linkedin_url', ''),
                enrichment_data=enrichment_data
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
