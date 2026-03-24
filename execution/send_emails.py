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


def send_pending_emails(limit: int = 600, dry_run: bool = False, project_id: str = None, contact_ids: list[str] = None, skip_reply_check: bool = False) -> dict:
    """Send all pending emails where scheduled_at <= now(). Filters by project_id and/or contact_ids if provided."""
    
    # Init Supabase
    from supabase import create_client
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        logger.error("Supabase not configured")
        return {'error': 'No Supabase credentials'}
    
    supabase = create_client(supabase_url, supabase_key)
    
    # ── AUTO-CHECK REPLIES/BOUNCES ──────────────────────────────────
    # Ensure statuses are fresh before we start sending (unless skipped)
    if not skip_reply_check:
        try:
            from execution.check_replies import check_all_replies
            logger.info("Checking for replies and bounces before sending...")
            check_all_replies(days=7)
        except Exception as e:
            logger.warning(f"Pre-send reply check failed (skipping): {e}")
    else:
        logger.info("Skipping redundant reply check (already performed by daily_run).")
    # ────────────────────────────────────────────────────────────────

    # Init SMTP Pool
    try:
        pool = SMTPPool()
    except Exception as e:
        logger.error(f"Failed to initialize SMTP pool: {e}")
        return {'error': str(e)}

    # Fetch pending sequences due for sending
    now = datetime.utcnow().isoformat()
    query = supabase.table('email_sequences') \
        .select('*, contacts(name, email, enrichment_data)') \
        .eq('status', 'pending') \
        .lte('scheduled_at', now)   # ALWAYS filter by date — never send future steps

    if contact_ids:
        query = query.in_('contact_id', contact_ids)
        
    if project_id:
        query = query.eq('project_id', project_id)
        
    result = query.limit(limit).order('scheduled_at').execute()
    
    sequences = result.data or []
    logger.info(f"Found {len(sequences)} emails ready to send")
    
    stats = {'processed': 0, 'sent': 0, 'skipped': 0, 'errors': 0}
    
    for seq in sequences:
        try:
            contact = seq.get('contacts', {})
            to_email = (contact.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
            
            if not to_email:
                logger.warning(f"No email for contact, skipping sequence {seq['id']}")
                stats['skipped'] += 1
                continue
            
            # BOUNCE PROTECTION: Only send to 'valid' emails
            ed = contact.get('enrichment_data') or {}
            if isinstance(ed, str):
                try: ed = json.loads(ed)
                except: ed = {}
            
            v_status = ed.get('verification_status')
            if v_status == 'invalid':
                logger.warning(f"BOUNCE PROTECTION: Skipping {to_email} (Status: INVALID). Marking as skipped.")
                if not dry_run:
                    supabase.table('email_sequences').update({'status': 'skipped'}).eq('id', seq['id']).execute()
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
                
            usage = pool.get_total_usage()
            limit_total = pool.get_total_limit()
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}[{usage}/{limit_total}] Sending step {seq['step_number']} to {to_email} from {account.email}")
            
            # --- Dynamic Sender Identity ---
            # Templates usually hardcode "Bipul" at the end. We dynamically swap it if Pranav is sending.
            is_pranav = "pranavarora" in account.email.lower()
            sender_name = "Pranav" if is_pranav else "Bipul"
            
            final_body = seq['body']
            if is_pranav:
                final_body = final_body.replace("Bipul", "Pranav").replace("bipul", "pranav")
            # -------------------------------
            
            # Send Email
            res = pool.send_email(
                account=account,
                to_addr=to_email,
                subject=seq['subject'],
                body_html=final_body,
                sender_name=sender_name,
                dry_run=dry_run
            )
            
            if res.get('success'):
                if not dry_run:
                    # ── ATOMIC DUPLICATE GUARD ──────────────────────────────
                    # Re-check the status right before marking as sent to prevent
                    # double sends if daily_send is clicked twice concurrently.
                    recheck = supabase.table('email_sequences') \
                        .select('status') \
                        .eq('id', seq['id']) \
                        .single() \
                        .execute()
                    if not recheck.data or recheck.data.get('status') != 'pending':
                        logger.warning(f"  Skipping update — seq {seq['id']} is no longer pending (was it sent already?). Status: {recheck.data.get('status') if recheck.data else 'missing'}")
                        stats['skipped'] += 1
                        stats['processed'] += 1
                        continue
                    # ────────────────────────────────────────────────────────
                    now_sent = datetime.utcnow()
                    supabase.table('email_sequences').update({
                        'status': 'sent',
                        'sent_at': now_sent.isoformat()
                    }).eq('id', seq['id']).execute()
                    
                    # -------------------------------------------------------
                    # RESCHEDULE: Update subsequent pending steps relative to
                    # when this step was actually sent, not sequence creation.
                    # -------------------------------------------------------
                    try:
                        from datetime import timedelta
                        # Get delay_days for the just-sent step's template
                        sent_template_res = supabase.table('email_templates').select('delay_days').eq('id', seq.get('template_id')).single().execute()
                        sent_delay = sent_template_res.data.get('delay_days', 0) if sent_template_res.data else 0
                        
                        # Get all remaining pending steps for this contact, joined with template delays
                        pending_res = supabase.table('email_sequences') \
                            .select('id, template_id, step_number') \
                            .eq('contact_id', seq['contact_id']) \
                            .eq('status', 'pending') \
                            .eq('project_id', seq['project_id']) \
                            .execute()
                        
                        if pending_res.data:
                            template_ids = [s['template_id'] for s in pending_res.data if s.get('template_id')]
                            templates_res = supabase.table('email_templates').select('id, delay_days').in_('id', template_ids).execute()
                            delay_map = {t['id']: t['delay_days'] for t in (templates_res.data or [])}
                            
                            for pending_step in pending_res.data:
                                tmpl_id = pending_step.get('template_id')
                                if not tmpl_id or tmpl_id not in delay_map:
                                    continue
                                step_delay = delay_map[tmpl_id]
                                delta = step_delay - sent_delay
                                if delta > 0:
                                    new_scheduled = now_sent + timedelta(days=delta)
                                    supabase.table('email_sequences').update({
                                        'scheduled_at': new_scheduled.isoformat()
                                    }).eq('id', pending_step['id']).execute()
                                    logger.info(f"  -> Rescheduled step {pending_step['step_number']} to {new_scheduled.date()} (+{delta}d from now)")
                    except Exception as reschedule_err:
                        logger.warning(f"Reschedule failed (non-critical): {reschedule_err}")
                    
                stats['sent'] += 1
            else:
                if not dry_run:
                    supabase.table('email_sequences').update({
                        'status': 'failed'
                    }).eq('id', seq['id']).execute()
                stats['errors'] += 1
            
            stats['processed'] += 1
            # No per-thread delay here — SMTPPool._send_lock enforces global cadence
            # across all concurrent project threads.

        except Exception as e:
            logger.error(f"Error processing sequence {seq.get('id', '?')}: {e}")
            stats['errors'] += 1
    
    logger.info(f"Email send complete: {stats}")
    logger.info(f"Final pool status:\n{json.dumps(pool.get_status(), indent=2)}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Send pending drip campaign emails using multi-account SMTP')
    parser.add_argument('--limit', type=int, default=600, help='Max emails to send')
    parser.add_argument('--dry-run', action='store_true', help='Preview without sending')
    parser.add_argument('--project-id', type=str, help='Restrict sending to a specific project ID')
    
    args = parser.parse_args()
    
    # Use DRY_RUN from env if flag is not set manually
    dry_run = args.dry_run or str(os.getenv('DRY_RUN', 'false')).lower() == 'true'
    
    stats = send_pending_emails(limit=args.limit, dry_run=dry_run, project_id=args.project_id)
    print(json.dumps(stats, indent=2))
