#!/usr/bin/env python3
"""
Send Emails — SMTP Multi-Account Email Sender for Drip Campaigns.

Reads pending email sequences from Supabase where scheduled_at <= now().
Uses SMTPPool to round-robin across multiple Gmail accounts (app passwords).
Enforces hourly/daily limits and random delays to avoid spam filters.

Usage:
    python -m execution.send_emails --dry-run
    python -m execution.send_emails --limit 50
"""

import os
import sys
import json
import argparse
import logging
import time
import random
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path
from execution.smtp_pool import SMTPPool

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DELAY_MIN = int(os.getenv('DELAY_MIN_SECONDS', 45))
DELAY_MAX = int(os.getenv('DELAY_MAX_SECONDS', 90))


def send_pending_emails(limit: int = 50, dry_run: bool = False, project_id: str = None, contact_ids: list[str] = None) -> dict:
    """Send all pending emails where scheduled_at <= now(). Filters by project_id and/or contact_ids if provided."""
    
    # Init Supabase
    from supabase import create_client
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase not configured")
        return {'error': 'No Supabase credentials'}
    
    supabase = create_client(supabase_url, supabase_key)
    
    # Init SMTP Pool
    try:
        pool = SMTPPool()
    except Exception as e:
        logger.error(f"Failed to initialize SMTP pool: {e}")
        return {'error': str(e)}

    # Fetch pending sequences due for sending
    now = datetime.utcnow().isoformat()
    query = supabase.table('email_sequences') \
        .select('*, contacts(name, email)') \
        .eq('status', 'pending')
        
    if contact_ids:
        query = query.in_('contact_id', contact_ids)
    else:
        query = query.lte('scheduled_at', now)
        
    if project_id:
        query = query.eq('project_id', project_id)
        
    result = query.limit(limit).order('scheduled_at').execute()
    
    sequences = result.data or []
    logger.info(f"Found {len(sequences)} emails ready to send")
    
    stats = {'processed': 0, 'sent': 0, 'skipped': 0, 'errors': 0}
    
    for seq in sequences:
        try:
            contact = seq.get('contacts', {})
            to_email = contact.get('email')
            
            if not to_email:
                logger.warning(f"No email for contact, skipping sequence {seq['id']}")
                stats['skipped'] += 1
                continue
            
            # REPLY GUARD: Check if contact has replied — if so, cancel all their pending emails
            contact_status = supabase.table('contacts').select('status').eq('id', seq['contact_id']).execute()
            if contact_status.data and contact_status.data[0].get('status') == 'replied':
                logger.info(f"Contact {to_email} has replied. Cancelling sequence {seq['id']} and all remaining steps.")
                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', seq['contact_id']).eq('status', 'pending').execute()
                stats['skipped'] += 1
                continue
            
            # Get next available email account
            account = pool.get_next_account()
            if not account:
                logger.error("All SMTP accounts exhausted their hourly/daily limits. Stopping.")
                break
                
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Sending step {seq['step_number']} to {to_email} from {account.email}")
            
            # Send Email
            res = pool.send_email(
                account=account,
                to_addr=to_email,
                subject=seq['subject'],
                body_html=seq['body'],
                dry_run=dry_run
            )
            
            if res.get('success'):
                if not dry_run:
                    supabase.table('email_sequences').update({
                        'status': 'sent',
                        'sent_at': datetime.utcnow().isoformat()
                    }).eq('id', seq['id']).execute()
                    
                    # Store sent_from in contact enrichment data optionally, but seq table doesn't have it standard.
                    
                stats['sent'] += 1
            else:
                if not dry_run:
                    supabase.table('email_sequences').update({
                        'status': 'failed'
                    }).eq('id', seq['id']).execute()
                stats['errors'] += 1
            
            stats['processed'] += 1
            
            # Random delay if not dry run and we have more to send
            if not dry_run and stats['processed'] < len(sequences):
                wait_time = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"Waiting {wait_time:.1f}s before next email to avoid spam filters...")
                time.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Error processing sequence {seq.get('id', '?')}: {e}")
            stats['errors'] += 1
    
    logger.info(f"Email send complete: {stats}")
    logger.info(f"Final pool status:\n{json.dumps(pool.get_status(), indent=2)}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send pending drip campaign emails using multi-account SMTP')
    parser.add_argument('--limit', type=int, default=50, help='Max emails to send')
    parser.add_argument('--dry-run', action='store_true', help='Preview without sending')
    parser.add_argument('--project-id', type=str, help='Restrict sending to a specific project ID')
    
    args = parser.parse_args()
    
    # Use DRY_RUN from env if flag is not set manually
    dry_run = args.dry_run or str(os.getenv('DRY_RUN', 'false')).lower() == 'true'
    
    stats = send_pending_emails(limit=args.limit, dry_run=dry_run, project_id=args.project_id)
    print(json.dumps(stats, indent=2))
