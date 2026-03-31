import os
import json
import logging
import requests
import concurrent.futures
from collections import defaultdict

logger = logging.getLogger(__name__)

def verify_risky_contacts_bulk(contacts: list[dict], supabase_client) -> None:
    """
    Extracts all 'risky' emails (including Catch-Alls) from the provided contacts
    that haven't been OSINT-verified yet, runs them through Serper.dev Google Search in bulk,
    and updates the contact's enrichment_data with `serper_verified`: True/False.
    """
    to_verify = []
    
    for c in contacts:
        ed = c.get('enrichment_data') or {}
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except: ed = {}
            c['enrichment_data'] = ed
            
        v_status = ed.get('verification_status')
        v_reason = str(ed.get('verification_reason', ''))
        
        # Identify Risky emails (now includes domain_catch_all and timeouts)
        is_strict_risky = v_status == 'risky' or (v_status == 'valid' and 'domain_catch_all' in v_reason)
        
        if is_strict_risky:
            has_been_checked = 'serper_verified' in ed
            if not has_been_checked:
                email = c.get('email', '').strip()
                company = c.get('company', '').strip()
                if email and '@' in email and company:
                    to_verify.append(c)
                    
    if not to_verify:
        return

    logger.info(f"OSINT FALLBACK: Found {len(to_verify)} unverified risky leads. Preparing bulk Serper check...")

    serper_key = os.getenv('SERPER_API_KEY')
    if not serper_key:
        logger.error("OSINT FALLBACK: No SERPER_API_KEY found in environment or .env. Skipping deep verification.")
        return

    # Use a dict to map the exact email string we send to Serper back to the contact objects
    email_to_contacts = defaultdict(list)
    emails_to_test = []
    
    for c in to_verify:
        email = c['email'].strip()
        email_to_contacts[email].append(c)
        
    for e, contact_list in email_to_contacts.items():
        # Just grab the company text from the first contact mapping
        comp = contact_list[0].get('company', '').strip()
        emails_to_test.append((e, comp))
    
    logger.info(f"OSINT FALLBACK: Initiating lightning-fast Serper API actor for {len(emails_to_test)} unique queries...")
    
    def test_serper(lead_data):
        email, company = lead_data
        # Query precisely the email to maximize recovery yield
        query = f'"{email}"'
        
        payload = json.dumps({
            "q": query,
            "num": 10
        })
        headers = {
            'X-API-KEY': serper_key,
            'Content-Type': 'application/json'
        }
        import time
        for attempt in range(3):
            try:
                res = requests.request("POST", "https://google.serper.dev/search", headers=headers, data=payload, timeout=20)
                if res.status_code == 429:
                    time.sleep(1.0)
                    continue
                if res.status_code != 200:
                    logger.error(f"API Error {res.status_code} for {email}: {res.text}")
                    return email, False
                    
                res_json = res.json()
                organic = res_json.get('organic', [])
                email_lower = email.lower()
                
                found = False
                for org in organic:
                    snippet = org.get('snippet', '').lower()
                    title = org.get('title', '').lower()
                    if email_lower in snippet or email_lower in title:
                        found = True
                        break
                return email, found
            except Exception as ex:
                if attempt == 2:
                    logger.error(f"Serper API error for {email}: {ex}")
                    return email, False
                time.sleep(1.0)
        return email, False

    verified_emails = set()
    
    # Max workers 4 to stay within respectful API concurrency limits
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(test_serper, lead) for lead in emails_to_test]
        for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
            email, found = future.result()
            if found:
                verified_emails.add(email)

    # Update the Database
    newly_verified = 0
    newly_rejected = 0
    
    for email, contact_list in email_to_contacts.items():
        is_verified = email in verified_emails
        if is_verified:
            newly_verified += 1
            logger.info(f"  ✅ [OSINT RECOVERED] Found {email} on Google.")
        else:
            newly_rejected += 1
            logger.info(f"  🚫 [OSINT DROPPED] Could not confidently verify {email} on Google.")
            
        for c in contact_list:
            c_id = c['id']
            ed = c.get('enrichment_data') or {}
            ed['serper_verified'] = is_verified
            supabase_client.table('contacts').update({'enrichment_data': ed}).eq('id', c_id).execute()

    logger.info(f"OSINT FALLBACK COMPLETE: Recovered {newly_verified} | Dropped {newly_rejected}.")


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
