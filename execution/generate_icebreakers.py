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

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
# We import google.genai inside the function to avoid global import errors if not installed
SERPER_API_KEY = os.getenv('SERPER_API_KEY')


def _fetch_website_context(name: str, location: str = None, website: str = None) -> str:
    """
    Use Serper to find the business website, then Jina to scrape it.
    Returns scraped homepage text or empty string on failure.
    """
    try:
        # Step 1: Build search query — strip team/business suffixes for cleaner search
        import re as _re
        clean_name = _re.sub(r'\s*-\s*(Team|Business|Staff|Group|Page|Hub|Official)\s*$', '', name, flags=_re.IGNORECASE).strip()
        query = clean_name if not location else f"{clean_name} {location}"
        
        # If we already have a website, skip Serper and go straight to scraping
        target_url = website
        
        if not target_url and SERPER_API_KEY:
            headers = {'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json'}
            resp = requests.post('https://google.serper.dev/search', headers=headers,
                                 json={'q': query, 'num': 3}, timeout=10)
            resp.raise_for_status()
            results = resp.json().get('organic', [])
            for r in results:
                link = r.get('link', '')
                # Avoid directories, review sites etc
                skip = ['yelp.com', 'tripadvisor.com', 'facebook.com', 'instagram.com', 'google.com', 'linkedin.com', 'twitter.com']
                if link and not any(s in link for s in skip):
                    target_url = link
                    break

        if not target_url:
            return ''
        
        # Step 2: Scrape the website via Jina Reader
        jina_url = f"https://r.jina.ai/{target_url}"
        scrape_res = requests.get(jina_url, headers={'Accept': 'text/plain'}, timeout=20)
        scrape_res.raise_for_status()
        content = scrape_res.text[:3000]  # Keep first 3000 chars (plenty for icebreaker)
        logger.info(f"Scraped website for {name} ({target_url}): {len(content)} chars")
        return content
        
    except Exception as e:
        logger.warning(f"Failed to fetch website context for {name}: {e}")
        return ''


def generate_icebreaker(name: str, bio: str, linkedin_url: str = None, enrichment_data: dict = None) -> str | None:
    """
    Generate a personalized icebreaker using Gemini 2.5 Pro.
    First scrapes the business website via Serper + Jina to gather real context.
    
    Args:
        name: Contact's full name / business name
        bio: Their bio/description
        linkedin_url: Optional LinkedIn profile for context
        enrichment_data: Optional dict with LinkedIn-scraped profile fields
    
    Returns:
        Icebreaker string or None on error
    """
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY not set")
        return None
    
    if not isinstance(enrichment_data, dict):
        enrichment_data = {}
    
    # Pull location and website from enrichment_data
    location = enrichment_data.get('city') or enrichment_data.get('location') or enrichment_data.get('linkedin_location', '')
    website = enrichment_data.get('website') or enrichment_data.get('domain', '')
    
    # Use company field for search if available (avoids "Jasmine - Team" style noise)
    search_name = enrichment_data.get('company') or enrichment_data.get('linkedin_company') or name
    
    # Step 1: Scrape the business website for real context
    web_content = _fetch_website_context(search_name, location=location, website=website)
    
    context = f"Business Name: {name}"
    if location:
        context += f"\nLocation: {location}"
    if bio:
        context += f"\nBio: {bio}"
    if linkedin_url:
        context += f"\nLinkedIn: {linkedin_url}"
    
    # Add rich LinkedIn data if available
    if enrichment_data.get('linkedin_headline'):
        context += f"\nLinkedIn Headline: {enrichment_data['linkedin_headline']}"
    if enrichment_data.get('linkedin_company'):
        context += f"\nCurrent Company: {enrichment_data['linkedin_company']}"
    if enrichment_data.get('linkedin_title'):
        context += f"\nCurrent Title: {enrichment_data['linkedin_title']}"
    if enrichment_data.get('linkedin_about'):
        about = enrichment_data['linkedin_about'][:1500]
        context += f"\nLinkedIn About: {about}"
    
    # If we have no scraped context, give Gemini a clear fallback instruction
    no_context_instruction = ''
    if not web_content:
        no_context_instruction = f"""\nIMPORTANT: No website content was found. You MUST still write a icebreaker.
    Write a warm, professional 1-2 sentence opener based ONLY on the business name and type.
    Example: "The dedication it takes to build a standout spa brand like {{name}} is impressive — your team clearly cares about the client experience."
    Never ask for more info. Never say you lack information. Always produce a warm sentence."""
    
    if web_content:
        context += f"\n\nSCRAPED WEBSITE CONTENT:\n{web_content}"
    
    prompt = f"""Generate a 1-2 sentence personalized icebreaker for cold emailing this business.

CRITICAL RULES (non-negotiable):
1. ALWAYS produce exactly 1-2 warm, genuine sentences. NEVER refuse. NEVER ask questions. NEVER say you lack information.
2. If website content is provided, reference something SPECIFIC from it — a service, a product, a value, a philosophy.
3. If NO website content is provided, write a warm professional opener based on the business name — keep it genuine.
4. Do NOT mention the sender's service, product, or reason for reaching out. 100% about THEM.
5. Do NOT include ANY citations, footnotes, or numbers in brackets like [1] or [2].
6. Do NOT add closing phrases like "Best," or "Regards," — just the icebreaker sentence(s).
7. Do NOT mention any city or location unless explicitly in the data below.
8. NEVER mention awards, rankings, competitions, innovation titles, or achievements (e.g. "award-winning", "recognized", "top 10", "innovative company of the year") UNLESS those EXACT words appear in the scraped content below. If unsure, leave awards out entirely.
{no_context_instruction}

{context}

Reply with ONLY the icebreaker (1-2 sentences). No intro, no explanation, no questions."""

    try:
        from google import genai
        from google.genai import types
        
        client = genai.Client(api_key=GEMINI_API_KEY)
        
        system_instruction = (
            'You are an elite cold-email publicist. Your ONLY job is to output 1-2 warm icebreaker sentences. '
            'You NEVER refuse, NEVER ask questions, and NEVER say you lack information. '
            'If you have website content, reference something specific. If you have nothing, write a warm professional opener based on the business name. '
            'Never hallucinate unverified facts, but always produce warm copy.'
        )
        
        # No google_search grounding — we already scraped manually with Serper+Jina
        config = types.GenerateContentConfig(
            temperature=0.3,  # Low temp = less hallucination
            max_output_tokens=350,
            system_instruction=system_instruction,
        )
        
        response = client.models.generate_content(
            model='gemini-2.5-pro',
            contents=prompt,
            config=config
        )
        
        if not response.text:
            logger.error(f"Gemini error for {name}: Response text is empty or blocked by safety filters.")
            return None
        
        import re
        
        icebreaker = response.text.strip()
        # Forcefully strip any citation brackets like [1] or [3]
        icebreaker = re.sub(r'\[\d+\]', '', icebreaker).strip()
        
        # Detect refusal / question responses — replace with a safe fallback
        refusal_signals = [
            'could you provide', 'i don\'t have', 'i do not have', 'i need more',
            'please provide', 'more information', 'to write a', 'i would need',
            'i cannot', 'unfortunately', 'no information', 'not enough information',
        ]
        # Detect hallucinated award/ranking claims not grounded in scraped content
        hallucination_signals = [
            'alongside nvidia', 'alongside spacex', 'innovative company winner',
            'innovative company of the year', 'top 10', 'top 50', 'top 100',
            'innovation award', 'award-winning recognition', '2026 winner',
            '2025 winner', 'trailblazer award',
        ]
        lower = icebreaker.lower()
        is_refusal = any(sig in lower for sig in refusal_signals) or icebreaker.endswith('?')
        is_hallucination = any(sig in lower for sig in hallucination_signals)
        
        if is_refusal or is_hallucination:
            business_display = search_name or name
            reason = 'hallucination' if is_hallucination else 'refusal'
            icebreaker = f"The work the team at {business_display} is doing really caught our eye — it's clear you put genuine care into what you offer."
            logger.warning(f"{reason.capitalize()} detected for {name}, using fallback icebreaker.")
        
        if icebreaker:
            logger.info(f"Generated icebreaker for {name}: {icebreaker[:80]}...")
            return icebreaker
        
    except Exception as e:
        logger.error(f"Gemini error for {name}: {e}")
    
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
    
    # Fetch contacts needing icebreakers
    query = supabase.table('contacts').select('*')
    
    if contact_ids:
        # If user explicitly selected them, allow regenerating existing icebreakers
        query = query.in_('status', ['enriched', 'icebreaker_ready']).in_('id', contact_ids)
    elif project_id:
        query = query.eq('status', 'enriched').eq('project_id', project_id)
    else:
        query = query.eq('status', 'enriched')
        
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
