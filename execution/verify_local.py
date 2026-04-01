"""
verify_local.py — Run SMTP verification locally (clean residential IP) and push results to Supabase.

Usage:
    python3 execution/verify_local.py                  # Verify all contacts with no status
    python3 execution/verify_local.py --force          # Re-verify ALL contacts (overwrites existing status)
    python3 execution/verify_local.py --email chris@csp.agency  # Verify a single email
"""

import os
import sys
import json
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / '.env')

sys.path.insert(0, str(Path(__file__).parent.parent))
from execution.verify_email import check_email
from supabase import create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')


def fetch_contacts(force: bool = False, single_email: str = None) -> list[dict]:
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    if single_email:
        res = sb.table('contacts').select('id, email, company, enrichment_data').eq('email', single_email).execute()
        return res.data or []
    
    # Fetch all contacts with an email
    all_contacts = []
    offset = 0
    chunk = 1000
    while True:
        res = sb.table('contacts').select('id, email, company, enrichment_data').not_.is_('email', 'null').range(offset, offset + chunk - 1).execute()
        if not res.data:
            break
        all_contacts.extend(res.data)
        if len(res.data) < chunk:
            break
        offset += chunk
    
    if force:
        return all_contacts
    
    # Only return contacts with no existing verification status
    unverified = []
    for c in all_contacts:
        ed = c.get('enrichment_data') or {}
        if isinstance(ed, str):
            try: ed = json.loads(ed)
            except: ed = {}
        c['enrichment_data'] = ed
        if not ed.get('verification_status'):
            unverified.append(c)
    
    return unverified


def verify_and_update(contact: dict, sb) -> tuple[str, str, str]:
    email = contact['email']
    ed = contact.get('enrichment_data') or {}
    if isinstance(ed, str):
        try: ed = json.loads(ed)
        except: ed = {}
    
    v_status, v_reason = check_email(email)
    ed['verification_status'] = v_status
    ed['verification_reason'] = v_reason
    # Clear old serper data so OSINT re-runs fresh on Railway
    ed.pop('serper_verified', None)
    ed.pop('serper_snippet', None)
    
    sb.table('contacts').update({'enrichment_data': ed}).eq('id', contact['id']).execute()
    return email, v_status, v_reason


def main():
    parser = argparse.ArgumentParser(description='Local SMTP email verifier')
    parser.add_argument('--force', action='store_true', help='Re-verify all contacts, even already-verified ones')
    parser.add_argument('--email', type=str, help='Verify a single email address')
    parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers (default: 10)')
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    logger.info("Fetching contacts from Supabase...")
    contacts = fetch_contacts(force=args.force, single_email=args.email)
    
    if not contacts:
        logger.info("No contacts to verify.")
        return
    
    logger.info(f"Verifying {len(contacts)} contacts locally with {args.workers} workers...")
    
    valid_count = risky_count = invalid_count = 0
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(verify_and_update, c, sb): c for c in contacts}
        for future in as_completed(futures):
            try:
                email, v_status, v_reason = future.result()
                if v_status == 'valid':
                    valid_count += 1
                    logger.info(f"  ✅ {email}: VALID ({v_reason})")
                elif v_status == 'risky':
                    risky_count += 1
                    logger.info(f"  ⚠️  {email}: RISKY ({v_reason})")
                else:
                    invalid_count += 1
                    logger.info(f"  ❌ {email}: INVALID ({v_reason})")
            except Exception as e:
                logger.error(f"  Error verifying {futures[future].get('email', '?')}: {e}")
    
    logger.info(f"\n{'='*50}")
    logger.info(f"DONE: ✅ Valid: {valid_count} | ⚠️  Risky: {risky_count} | ❌ Invalid: {invalid_count}")
    logger.info(f"Results written to Supabase. Railway will pick up the valid ones for sending.")


if __name__ == '__main__':
    main()
