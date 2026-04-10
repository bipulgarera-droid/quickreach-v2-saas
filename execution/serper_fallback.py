import os
import re
import json
import logging
import requests
import concurrent.futures
from collections import defaultdict

logger = logging.getLogger(__name__)


def _is_exact_email_match(email_lower: str, text: str) -> bool:
    """Check if the email appears as a standalone string in text, not as part of a URL path or larger token.
    
    We use a regex word-boundary approach:
    - The character before the email must be a space, punctuation, or start-of-string
    - The character after must be a space, punctuation, or end-of-string
    - This prevents matching 'chris@csp.agency' inside 'https://site.com/chris@csp.agency/profile'
    """
    pattern = r'(?:^|[\s,;:\'\"<>()\[\]{}|])' + re.escape(email_lower) + r'(?:$|[\s,;:\'\"<>()\[\]{}|.!?])'
    return bool(re.search(pattern, text))


def verify_risky_contacts_bulk(contacts: list[dict], supabase_client, job_logger=None) -> tuple[int, int]:
    """
    Runs risky emails through Serper.dev Google Search with strict exact-match,
    updates enrichment_data with serper_verified, and returns (recovered, dropped) counts.
    """
    to_verify = []
    
    for c in contacts:
        ed = c.get('enrichment_data') or {}
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except: ed = {}
            c['enrichment_data'] = ed
            
        v_status = ed.get('verification_status')
        
        # Only risky emails need OSINT verification
        if v_status == 'risky':
            # Skip OSINT check if GrowthScout push (it already did Serper)
            if ed.get('source_app') == 'growthscout':
                continue
                
            email = c.get('email') or ''
            email = str(email).strip()
            if email and '@' in email:
                to_verify.append(c)
                    
    if not to_verify:
        return 0, 0

    logger.info(f"OSINT FALLBACK: Found {len(to_verify)} unverified risky leads. Preparing bulk Serper check...")

    serper_key = os.getenv('SERPER_API_KEY')
    if not serper_key:
        logger.error("OSINT FALLBACK: No SERPER_API_KEY found in environment or .env. Skipping deep verification.")
        if job_logger:
            job_logger.info("⚠️ OSINT SKIPPED: No SERPER_API_KEY configured")
        return 0, 0

    # Use a dict to map the exact email string we send to Serper back to the contact objects
    email_to_contacts = defaultdict(list)
    emails_to_test = []
    
    for c in to_verify:
        email = (c.get('email') or '').strip()
        if not email:
            continue
        email_to_contacts[email].append(c)
        
    for e, contact_list in email_to_contacts.items():
        comp = (contact_list[0].get('company') or '').strip()
        emails_to_test.append((e, comp))
    
    logger.info(f"OSINT FALLBACK: Querying Serper API for {len(emails_to_test)} unique emails...")
    
    def test_serper(lead_data):
        """Search Google for the exact email in quotes and verify it appears as a standalone match."""
        email, company = lead_data
        query = f'"{email}"'
        
        payload = json.dumps({"q": query, "num": 10})
        headers = {
            'X-API-KEY': serper_key,
            'Content-Type': 'application/json'
        }
        import time
        for attempt in range(3):
            try:
                res = requests.post("https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                if res.status_code == 429:
                    time.sleep(1.0)
                    continue
                if res.status_code != 200:
                    logger.error(f"  Serper API Error {res.status_code} for {email}: {res.text}")
                    return email, False, None
                    
                res_json = res.json()
                organic = res_json.get('organic', [])
                email_lower = email.lower()
                
                if not organic:
                    logger.info(f"  🔍 {email}: Serper returned 0 organic results → DROPPED")
                    return email, False, None
                
                # Strict matching: email must appear as a standalone token
                for org in organic:
                    snippet = org.get('snippet', '').lower()
                    title = org.get('title', '').lower()
                    link = org.get('link', '')
                    
                    if _is_exact_email_match(email_lower, snippet):
                        matched_text = org.get('snippet', '')[:120]
                        logger.info(f"  🔍 {email}: EXACT MATCH in snippet → RECOVERED")
                        logger.info(f"     source: {link}")
                        logger.info(f"     snippet: {matched_text}")
                        return email, True, matched_text
                    
                    if _is_exact_email_match(email_lower, title):
                        matched_text = org.get('title', '')[:120]
                        logger.info(f"  🔍 {email}: EXACT MATCH in title → RECOVERED")
                        logger.info(f"     source: {link}")
                        return email, True, matched_text
                
                # No exact match found in any result
                logger.info(f"  🔍 {email}: {len(organic)} results but NO exact email match → DROPPED")
                return email, False, None
                
            except Exception as ex:
                if attempt == 2:
                    logger.error(f"  Serper API error for {email}: {ex}")
                    return email, False, None
                time.sleep(1.0)
        return email, False, None

    # Results: email → (found, matched_snippet)
    verified_emails = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(test_serper, lead) for lead in emails_to_test]
        for future in concurrent.futures.as_completed(futures):
            email, found, snippet = future.result()
            verified_emails[email] = (found, snippet)

    # Update the Database
    newly_verified = 0
    newly_rejected = 0
    
    for email, contact_list in email_to_contacts.items():
        found, snippet = verified_emails.get(email, (False, None))
        if found:
            newly_verified += 1
            if job_logger:
                job_logger.info(f"  ✅ OSINT PASS: {email}")
        else:
            newly_rejected += 1
            if job_logger:
                job_logger.info(f"  🚫 OSINT FAIL: {email}")
            
        for c in contact_list:
            c_id = c['id']
            ed = c.get('enrichment_data') or {}
            ed['serper_verified'] = found
            if snippet:
                ed['serper_snippet'] = snippet
            supabase_client.table('contacts').update({'enrichment_data': ed}).eq('id', c_id).execute()

    logger.info(f"OSINT FALLBACK COMPLETE: ✅ Recovered {newly_verified} | 🚫 Dropped {newly_rejected}.")
    return newly_verified, newly_rejected


if __name__ == "__main__":
    from supabase import create_client, Client
    import sys
    from dotenv import load_dotenv
    from pathlib import Path
    
    # Load env vars
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(env_path)
    
    # Initialize Supabase
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        logger.error("Missing Supabase credentials in environment.")
        sys.exit(1)
        
    supabase: Client = create_client(supabase_url, supabase_key)
    
    logger.info("Starting standalone OSINT Fallback Verification...")
    
    # Fetch some active contacts to test with
    # E.g. ones that have risky status
    res = supabase.table('contacts').select('*').limit(50).execute()
    contacts = res.data or []
    
    if not contacts:
        logger.info("No active contacts found.")
    else:
        logger.info(f"Found {len(contacts)} contacts. Running Serper bulk verification on any risky ones...")
        verify_risky_contacts_bulk(contacts, supabase)
