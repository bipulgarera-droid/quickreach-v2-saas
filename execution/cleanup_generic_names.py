import os
import sys
import re
import json
import logging
from urllib.parse import urlparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path
from supabase import create_client

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _extract_brand_from_url(url: str) -> str:
    """Extract a clean brand name from a URL with robust word splitting."""
    if not url: return ""
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # 1. Remove common extensions
        brand = re.sub(r'\.(com|in|org|net|biz|co\.in|me|tv|us|ae|io|ai|live)$', '', domain, flags=re.IGNORECASE)
        
        # 2. Handle hyphens and underscores (direct separators)
        brand = brand.replace('-', ' ').replace('_', ' ')
        
        # 3. ─── SMART SPACING ───
        # A. CamelCase split (if any casing survived)
        brand = re.sub(r'([a-z])([A-Z])', r'\1 \2', brand)
        
        # B. Multi-Pass Industry Split (Split mashed words like 'laseraway' or 'framesinaction')
        keywords = [
            'entertainment', 'motionpictures', 'productions', 'production', 
            'studios', 'studio', 'films', 'film', 'media', 'works', 'creative', 
            'solutions', 'digital', 'global', 'agency', 'group', 'services', 
            'official', 'vfx', 'corp', 'company', 'pictures', 'house', 'collective',
            'mantra', 'wadi', 'power', 'hour', 'baba', 'chillies', 'view', 'point',
            'stories', 'maverick', 'jugaad', 'zoom', 'cine', 'that', 'matter', 'away', 'frames', 'action', 'rodey', 'laser'
        ]
        prefixes = ['the', 'wild', 'magic', 'stories', 'red', 'zoom', 'cine', 'jugaad', 'maverick', 'goodfellas', 'star', 'grand', 'royal', 'nishant']
        
        for _ in range(3):
            old_brand = brand
            words = brand.split()
            cleaned_words = []
            for word in words:
                if len(word) >= 3:
                    # Prefix split
                    for p in prefixes:
                        if word.lower().startswith(p) and len(word) > len(p) + 2:
                            word = word[:len(p)] + ' ' + word[len(p):]
                            break
                    # Keyword split
                    for k in keywords:
                        low = word.lower()
                        if k in low:
                            idx = low.find(k)
                            if idx > 0 and word[idx-1] != ' ':
                                word = word[:idx] + ' ' + word[idx:]
                                break
                            elif idx == 0 and len(word) > len(k) + 2:
                                word = word[:len(k)] + ' ' + word[len(k):]
                                break
                cleaned_words.append(word)
            brand = ' '.join(cleaned_words)
            if brand == old_brand: break

        return brand.title().strip()
    except:
        return ""

def cleanup_generic_names(project_id=None):
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    supabase = create_client(url, key)

    logger.info("Cleaning up names/companies for non-nurtured leads...")
    
    # Target leads that haven't been nurtured yet
    query = supabase.table('contacts').select('id, name, company, enrichment_data, status').in_('status', ['new', 'enriched'])
    if project_id:
        query = query.eq('project_id', project_id)
    
    res = query.execute()
    contacts = res.data or []
    logger.info(f"Found {len(contacts)} leads to process.")
    
    stats = {'fixed': 0, 'skipped': 0}

    for c in contacts:
        ed = c.get('enrichment_data')
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except: ed = {}
        else: ed = ed or {}
        
        # Priority: enrichment_data['website'] -> enrichment_data['url'] -> c['website'] -> c['source_url']
        website = ed.get('website') or ed.get('url') or c.get('website') or c.get('source_url')
        
        if website:
            clean_brand = _extract_brand_from_url(website)
            if clean_brand and len(clean_brand) > 2:
                # If name is current company or generic, update it
                updates = {'company': clean_brand}
                
                # Update name if it's currently a placeholder or generic
                current_name = (c.get('name') or '').lower()
                generic_placeholders = ['film production', 'video production', 'production house', 'agency', 'company']
                if not current_name or any(p in current_name for p in generic_placeholders) or current_name == (c.get('company') or '').lower():
                    updates['name'] = clean_brand
                
                if updates.get('name') != c.get('name') or updates.get('company') != c.get('company'):
                    logger.info(f"Updating ID {c['id']}: -> {clean_brand}")
                    supabase.table('contacts').update(updates).eq('id', c['id']).execute()
                    stats['fixed'] += 1
                    continue
        
        stats['skipped'] += 1

    logger.info(f"Cleanup finished: {stats}")

if __name__ == '__main__':
    cleanup_generic_names()
