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


def send_pending_emails(limit: int = 99999, dry_run: bool = False, project_id: str = None, contact_ids: list[str] = None, skip_reply_check: bool = False, logger_callback=None) -> dict:
    """Send all pending emails where scheduled_at <= now(). Filters by project_id and/or contact_ids if provided."""

    def _log(msg, level='info'):
        logger.info(msg)
        if logger_callback:
            logger_callback(msg)
    
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
            _log("Checking for replies and bounces before sending...")
            check_all_replies(days=7, logger_callback=logger_callback)
        except Exception as e:
            logger.warning(f"Pre-send reply check failed (skipping): {e}")
    else:
        _log("Skipping redundant reply check (already performed by daily_run).")
    # ────────────────────────────────────────────────────────────────

    # Init SMTP Pool
    try:
        pool = SMTPPool()
    except Exception as e:
        logger.error(f"Failed to initialize SMTP pool: {e}")
        return {'error': str(e)}

    # Fetch ALL pending sequences due for sending (paginate through 1000-row Supabase chunks)
    now = datetime.utcnow().isoformat()
    
    def build_send_query():
        q = supabase.table('email_sequences') \
            .select('*, contacts(name, email, enrichment_data)') \
            .eq('status', 'pending') \
            .lte('scheduled_at', now)
        if contact_ids:
            q = q.in_('contact_id', contact_ids)
        if project_id:
            q = q.eq('project_id', project_id)
        return q.order('scheduled_at')

    sequences = []
    offset = 0
    while True:
        chunk = build_send_query().range(offset, offset + 999).execute()
        chunk_data = chunk.data or []
        sequences.extend(chunk_data)
        if len(chunk_data) < 1000:
            break
        offset += 1000
    
    _log(f"Found {len(sequences)} emails ready to send")
    
    # Fetch sender_groups for all involved projects
    project_ids = list(set([s.get('project_id') for s in sequences if s.get('project_id')]))
    sender_groups = {}
    if project_ids:
        proj_res = supabase.table('projects').select('id, sender_group').in_('id', project_ids).execute()
        for p in (proj_res.data or []):
            sender_groups[p['id']] = p.get('sender_group', 'all')
            

    stats = {'processed': 0, 'sent': 0, 'skipped': 0, 'errors': 0}
    
    for seq in sequences:
        # Stop once we've SENT enough (skips/errors don't count against limit)
        if stats['sent'] >= limit:
            _log(f"Reached send limit ({limit}). Stopping.")
            break
        try:
            contact = seq.get('contacts', {})
            to_email = (contact.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
            
            if not to_email:
                _log(f"No email for contact, skipping sequence {seq['id']}", level='warning')
                stats['skipped'] += 1
                continue
            
            # BOUNCE PROTECTION: Block invalid AND risky emails from sending
            ed = contact.get('enrichment_data') or {}
            if isinstance(ed, str):
                try: ed = json.loads(ed)
                except: ed = {}
            
            v_status = ed.get('verification_status')
            if v_status == 'invalid':
                _log(f"BOUNCE PROTECTION: Skipping {to_email} (Status: INVALID, Reason: {ed.get('verification_reason', '?')}). marking as skipped.", level='warning')
                if not dry_run:
                    supabase.table('email_sequences').update({'status': 'skipped'}).eq('id', seq['id']).execute()
                stats['skipped'] += 1
                continue
            elif v_status == 'risky':
                # Strict Mode: Only proceed if OSINT fallback verified them.
                serper_passed = ed.get('serper_verified')
                
                # GrowthScout's scraping implies it already did Google searches to find the email
                if ed.get('source_app') == 'growthscout':
                    serper_passed = True
                
                # If OSINT never ran for this contact, do it inline right now
                if serper_passed is None:
                    _log(f"OSINT BOUNCE PROTECTION: {to_email} is RISKY but missing OSINT check. Running real-time verification now...")
                    try:
                        from execution.serper_fallback import verify_risky_contacts_bulk
                        # Reconstruct the expected object structure for the bulk verifier
                        c_obj = {
                            'id': seq['contact_id'], 
                            'email': to_email, 
                            'company': contact.get('company', ''), 
                            'enrichment_data': ed
                        }
                        verify_risky_contacts_bulk([c_obj], supabase)
                        
                        # Fetch the freshly saved result
                        fresh_res = supabase.table('contacts').select('enrichment_data').eq('id', seq['contact_id']).execute()
                        if fresh_res.data:
                            ed = fresh_res.data[0].get('enrichment_data') or {}
                            serper_passed = ed.get('serper_verified')
                    except Exception as e:
                        _log(f"OSINT FALLBACK inline failed for {to_email}: {e}", level='error')
                
                # Evaluate final validation decision
                if serper_passed is True:
                    _log(f"OSINT BOUNCE PROTECTION: Proceeding with {to_email} (Risky, but Google Verified!).")
                else:
                    _log(f"BOUNCE PROTECTION: Skipping {to_email} (Status: RISKY/Catch-All, Not Google Verified). marking as skipped.", level='warning')
                    if not dry_run:
                        supabase.table('email_sequences').update({'status': 'skipped'}).eq('id', seq['id']).execute()
                    stats['skipped'] += 1
                    continue
            # REPLY GUARD: Check if contact has replied — if so, cancel all their pending emails
            contact_status = supabase.table('contacts').select('status').eq('id', seq['contact_id']).execute()
            if contact_status.data and contact_status.data[0].get('status') == 'replied':
                _log(f"Contact {to_email} has replied. Cancelling sequence {seq['id']} and all remaining steps.")
                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', seq['contact_id']).eq('status', 'pending').execute()
                stats['skipped'] += 1
                continue
            
            # Get next available email account
            proj_id = seq.get('project_id')
            sender_group = sender_groups.get(proj_id, 'all')
            
            account = pool.get_next_account(sender_group)
            if not account:
                logger.error(f"All SMTP accounts exhausted their hourly/daily limits for sender group '{sender_group}'. Stopping.")
                break
                
            usage = pool.get_total_usage(sender_group)
            limit_total = pool.get_total_limit(sender_group)
            _log(f"{'[DRY RUN] ' if dry_run else ''}[{usage}/{limit_total}] Sending step {seq['step_number']} to {to_email} from {account.email}")
            
            # --- Dynamic Sender Identity ---
            # Templates usually hardcode "Bipul" at the end. We dynamically swap it if Pranav is sending.
            is_pranav = "pranavarora" in account.email.lower()
            sender_name = "Pranav" if is_pranav else "Bipul"
            
            final_body = seq.get('body') or ""
            final_subject = seq.get('subject') or "Following Up"
            if is_pranav:
                final_body = final_body.replace("Bipul", "Pranav").replace("bipul", "pranav")
            # -------------------------------
            
            # Send Email
            res = pool.send_email(
                account=account,
                to_addr=to_email,
                subject=final_subject,
                body_html=final_body,
                sender_name=sender_name,
                dry_run=dry_run
            )
            
            if res.get('success'):
                if not dry_run:
                    # ── ATOMIC DUPLICATE GUARD ──────────────────────────────
                    # Re-check the status and scheduled_at right before marking as sent
                    # to prevent sending steps that were just rescheduled into the future
                    # by a preceding step in this exact same memory loop.
                    recheck = supabase.table('email_sequences') \
                        .select('status, scheduled_at') \
                        .eq('id', seq['id']) \
                        .single() \
                        .execute()
                    if not recheck.data or recheck.data.get('status') != 'pending':
                        logger.warning(f"  Skipping update — seq {seq['id']} is no longer pending (was it sent already?).")
                        stats['skipped'] += 1
                        stats['processed'] += 1
                        continue
                        
                    # CRITICAL: Prevent sending Step 3 immediately after Step 2 if Step 3 was just rescheduled
                    if recheck.data.get('scheduled_at') and recheck.data.get('scheduled_at') > datetime.utcnow().isoformat():
                        logger.warning(f"  Skipping seq {seq['id']} — it was rescheduled to the future ({recheck.data.get('scheduled_at')}) by a preceding step.")
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
    parser.add_argument('--limit', type=int, default=99999, help='Max emails to send (default: unlimited, SMTP pool limits apply)')
    parser.add_argument('--dry-run', action='store_true', help='Preview without sending')
    parser.add_argument('--project-id', type=str, help='Restrict sending to a specific project ID')
    
    args = parser.parse_args()
    
    # Use DRY_RUN from env if flag is not set manually
    dry_run = args.dry_run or str(os.getenv('DRY_RUN', 'false')).lower() == 'true'
    
    stats = send_pending_emails(limit=args.limit, dry_run=dry_run, project_id=args.project_id)
    print(json.dumps(stats, indent=2))
