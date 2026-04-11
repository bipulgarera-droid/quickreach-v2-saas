import os
import re
import logging
from urllib.parse import urlparse
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment - more robustly find .env in project root
BASE_DIR = Path(__file__).resolve().parent.parent
env_path = BASE_DIR / '.env'
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
# Fallback to SUPABASE_KEY if SERVICE_ROLE is empty (it's empty in user's .env)
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Missing SUPABASE_URL or SUPABASE_KEY in environment")

if SUPABASE_URL and SUPABASE_KEY:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
else:
    supabase = None

# Junk domains to remove entirely
JUNK_ROOT_DOMAINS = {
    'glassdoor.com', 'glassdoor.co.in', 'glassdoor.co.uk',
    'd7leadfinder.com', 'd7finder.com',
    'behance.net', 'tumblr.com',
    'asklaila.com', 'urbanpro.com', 'justdial.com',
    'indiamart.com', 'yellowpages.com', 'sulekha.com',
    'facebook.com', 'instagram.com', 'linkedin.com', 'twitter.com', 'youtube.com',
    'pinterest.com', 'vimeo.com', 'flickr.com', 'medium.com',
    'infinityfreeapp.com', 'wordpress.com', 'blogspot.com', 'github.io',
    'grotal.com', 'f6s.com', 'startupindia.gov.in', 'zaubacorp.com', 'tofler.in',
    'dir.indiamart.com', 'justdial.com'
}

# Industry keywords for splitting mashed words
SPLIT_KEYWORDS = [
    'production', 'productions', 'produce', 'produces', 'filmmaking',
    'house', 'houses', 'studios', 'studio', 'medias', 'media',
    'digital', 'creatives', 'creative', 'contents', 'content',
    'videos', 'video', 'cinemas', 'cinema', 'entertainments', 'entertainment',
    'india', 'indian', 'agency', 'agencies', 'marketing', 'marketings',
    'communications', 'communication', 'visuals', 'visual', 'motions', 'motion',
    'pictures', 'picture', 'stories', 'story', 'collective', 'collectives',
    'acting', 'onlines', 'online', 'bollywood', 'hollywood', 'district',
    'superfly', 'productions', 'foundation', 'group', 'works', 'work',
    'kala', 'chashma', 'bring', 'it', 'online', 'glass', 'door', 'jobs', 'and', 'auditions',
    'infinity', 'free', 'app', 'film', 'district', 'india', 'story', 'comm', 'lab', 'box',
    'mumbai', 'delhi', 'chennai', 'bombay', 'kailash', 'company', 'pvt', 'ltd', 'art', 'direction',
    'films', 'entertainment', 'city', 'harkat', 'talkies', 'natak', 'chitra', 'katha', 'nama', 'vision'
]

# Conjunctions/Short words
BRIDGE_WORDS = { 'and', 'with', 'for', 'in', 'the', 'of', 'at', 'by' }

def get_root_domain(url):
    if not url: return ""
    try:
        netloc = urlparse(url).netloc.lower()
        if netloc.startswith('www.'): netloc = netloc[4:]
        parts = netloc.split('.')
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return netloc
    except:
        return ""

def extract_from_bio(raw_domain, bio):
    """Attempt to find the properly formatted name directly from the bio text."""
    if not bio or not raw_domain: return None
    
    words = bio.split()
    # Check phrases up to 6 words long
    for i in range(len(words)):
        for j in range(i+1, min(i+1+6, len(words)+1)):
            phrase = " ".join(words[i:j])
            # Strip punctuation for comparison
            phrase_clean = re.sub(r'[^a-zA-Z0-9]', '', phrase).lower()
            if phrase_clean == raw_domain.lower():
                # Found the exact domain name as a sequence of words in the bio!
                # Clean up punctuation from the edges of the phrase
                return phrase.strip(',.()"\':;')
    return None

def is_irrelevant(name, source):
    """Check if the extracted name strongly contradicts the search intent."""
    if not source or not name: return False
    
    name_lower = name.lower()
    source_lower = source.lower()
    
    # If the search was specifically for production houses / film
    if 'production' in source_lower or 'film' in source_lower or 'studio' in source_lower:
        # If the actual resulting contact is just a generic job board or directory
        bad_keywords = ['job', 'audition', 'directory', 'portal', 'classifieds']
        for kw in bad_keywords:
            if kw in name_lower:
                return True
    return False

def extract_from_url(url_string, bio=None):
    """Extract name cleanly from URL and insert spaces, checking bio if provided."""
    if not url_string: return ""
    
    # Extract raw name
    raw_name = ""
    try:
        path_parts = urlparse(url_string).netloc.split('.')
        if not path_parts: return ""
        if path_parts[0] == 'www': path_parts.pop(0)
        raw_name = path_parts[0]
        
        # Special case for infinityfree/wordpress subdomains
        if len(path_parts) > 2 and path_parts[1] in ['infinityfreeapp', 'wordpress', 'blogspot']:
            raw_name = path_parts[0]
    except:
        return ""
        
    if not raw_name: return ""
    
    # 0. Check Bio First!
    bio_name = extract_from_bio(raw_name, bio)
    if bio_name:
         # Capitalize each word properly just in case
         return " ".join([w.capitalize() for w in bio_name.split()]).strip()

    # 1. Check if there's any capitalization already present in the raw domain string
    # If there is like FilmCityMumbai, we can just split by capital letters
    if any(c.isupper() for c in url_string):
      # Find original casing from the URL string
      match = re.search(raw_name, url_string, re.IGNORECASE)
      if match:
          original_cased = match.group(0)
          # Split by Capital letters
          spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', original_cased)
          return spaced.title().strip()
    
    # 2. If it's all lowercase like filmcitymumbai.com
    # We use our keyword dictionary
    low_word = raw_name.lower()
    parts = []
    current_pos = 0
    current_unmatched = ""
    sorted_keywords = sorted(SPLIT_KEYWORDS + list(BRIDGE_WORDS), key=len, reverse=True)

    while current_pos < len(low_word):
        match_found = False
        
        for k in sorted_keywords:
            if low_word[current_pos:].startswith(k):
                # Flush the unmatched buffer if any
                if current_unmatched:
                    parts.append(current_unmatched)
                    current_unmatched = ""
                # Add the matched keyword
                parts.append(low_word[current_pos:current_pos+len(k)])
                current_pos += len(k)
                match_found = True
                break
        
        if not match_found:
             # Just add the character to our buffer for unrecognized words
             current_unmatched += raw_name[current_pos]
             current_pos += 1

    # Flush the final buffer
    if current_unmatched:
        parts.append(current_unmatched)

    # Format output nicely
    final_name = " ".join(parts).title().strip()
    return final_name

def cleanup_contacts(project_id=None):
    if not supabase:
        logger.error("Supabase not initialized")
        return {"error": "Supabase not initialized"}

    logger.info(f"Starting contact cleanup for project: {project_id if project_id else 'ALL'}")
    
    query = supabase.table('contacts').select('*')
    if project_id:
        query = query.eq('project_id', project_id)
    
    contacts = query.execute().data or []
    logger.info(f"Found {len(contacts)} contacts to process")
    
    updates = []
    deletes = []
    
    # Dedup tracking: prefer email-based, fallback to domain-only if no email
    seen_emails = {}    # email -> id (keep first seen)
    seen_domains = {}   # domain -> id (only used for contacts WITHOUT email)
    
    for c in contacts:
        cid = c['id']
        email_addr = (c.get('email') or '').strip().lower()
        url = c.get('source_url') or c.get('website') or c.get('url')
        domain = get_root_domain(url)
        
        # 1. Filter Junk domains
        if domain in JUNK_ROOT_DOMAINS:
            logger.info(f"Deleting junk contact: {c.get('email')} ({domain})")
            deletes.append(cid)
            continue
            
        # 1.5 Filter Placeholder Emails
        if email_addr and any(placeholder in email_addr for placeholder in ['jane.doe@', 'john.doe@', 'johndoe@', 'janedoe@', 'email@', 'yourname@', 'name@']):
            logger.info(f"Deleting placeholder/dummy contact: {email_addr}")
            deletes.append(cid)
            continue
            
        # 2. Extract Clean Name
        if not url:
            new_name = c.get('name', '')
            new_company = c.get('company', '')
        else:
            new_name = extract_from_url(url, c.get('bio'))
            if not new_name:
                new_name = c.get('name', '')
            new_company = new_name
            
        # 2.5 Filter Irrelevant based on extracted name and search intent
        if is_irrelevant(new_name, c.get('source')):
            logger.info(f"Deleting irrelevant contact: {new_name} (Source: {c.get('source')})")
            deletes.append(cid)
            continue
            
        # 3. Deduplicate
        if email_addr:
            # Primary: deduplicate by exact email
            if email_addr in seen_emails:
                logger.info(f"Marking duplicate email for deletion: {email_addr}")
                deletes.append(cid)
                continue
            seen_emails[email_addr] = cid
            
        if domain:
            # Secondary: enforce 1-prospect-per-domain per project
            if domain in seen_domains:
                logger.info(f"Marking duplicate domain/company for deletion: {domain} (Strict Dedupe)")
                deletes.append(cid)
                continue
            seen_domains[domain] = cid
 
        # 4. Stage Update if name/company changed
        if new_name != c.get('name') or new_company != c.get('company'):
            updates.append({
                'id': cid,
                'name': new_name,
                'company': new_company,
            })

    # Execute Deletes
    if deletes:
        logger.info(f"Executing {len(deletes)} deletions...")
        for i in range(0, len(deletes), 100):
            chunk = deletes[i:i+100]
            supabase.table('contacts').delete().in_('id', chunk).execute()
            
    # Execute Updates
    if updates:
        logger.info(f"Executing {len(updates)} updates...")
        for up in updates:
            supabase.table('contacts').update({
                'name': up['name'],
                'company': up['company']
            }).eq('id', up['id']).execute()
            
    logger.info("Cleanup complete.")
    return {
        'deleted': len(deletes),
        'updated': len(updates)
    }

if __name__ == "__main__":
    import sys
    pid = sys.argv[1] if len(sys.argv) > 1 else None
    cleanup_contacts(pid)
