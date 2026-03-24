#!/usr/bin/env python3
"""
Check Replies — IMAP Reply Detection for Drip Campaigns.

Connects to the same Gmail accounts used for sending (from .env),
scans their inbox for replies from known prospects, and marks
the prospect as 'replied' in Supabase. This causes send_emails.py
to cancel all remaining pending sequence steps for that prospect.

Usage:
    python -m execution.check_replies
    python -m execution.check_replies --days 7
"""

import os
import sys
import re
import imaplib
import email
import logging
import argparse
import json
import socket
import ssl
from datetime import datetime, timedelta
from email.header import decode_header

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993


def _load_accounts_from_env() -> list[dict]:
    """Load Gmail accounts from .env using GMAIL_N_EMAIL format."""
    accounts = []
    for i in range(1, 20):
        acct_email = os.getenv(f"GMAIL_{i}_EMAIL")
        acct_password = os.getenv(f"GMAIL_{i}_PASSWORD")
        if not acct_email or not acct_password:
            continue
        accounts.append({"email": acct_email.strip(), "app_password": acct_password.strip()})
    return accounts


def _decode_header_value(raw):
    """Safely decode an email header value."""
    if raw is None:
        return ""
    decoded_parts = decode_header(raw)
    result = ""
    for part, charset in decoded_parts:
        if isinstance(part, bytes):
            result += part.decode(charset or "utf-8", errors="replace")
        else:
            result += part
    return result


def _extract_sender_email(from_header: str) -> str:
    """Extract just the email address from a From: header."""
    if "<" in from_header and ">" in from_header:
        return from_header.split("<")[1].split(">")[0].strip().lower()
    return from_header.strip().lower()


def _get_imap_connection(acct_email: str, acct_password: str) -> imaplib.IMAP4_SSL:
    """Helper to establish a fresh IMAP connection with timeout."""
    # Timeout=30 is key for avoiding long hangs
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
    mail.login(acct_email, acct_password)
    mail.select("INBOX")
    return mail


def check_replies_for_account(acct_email: str, acct_password: str, prospect_emails: set, days: int = 7) -> tuple[list[str], list[str]]:
    """
    Connect via IMAP to a single Gmail account and look for:
    1. Direct replies from known prospect emails.
    2. Bounce notifications (mailer-daemon) referring to these prospects.

    Returns (replied_emails, bounced_emails).
    """
    replied = []
    bounced = []
    mail = None
    try:
        mail = _get_imap_connection(acct_email, acct_password)

        # Search for recent emails
        since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
        status, message_ids = mail.search(None, f'(SINCE {since_date})')

        if status != "OK" or not message_ids[0]:
            logger.info(f"[{acct_email}] No recent emails found.")
            mail.logout()
            return [], []

        ids = message_ids[0].split()
        logger.info(f"[{acct_email}] Scanning {len(ids)} emails from last {days} days...")

        for msg_id in ids:
            try:
                # Attempt to fetch. If connection is lost, retry ONCE after reconnecting.
                max_retries = 1
                msg_data = None
                for attempt in range(max_retries + 1):
                    try:
                        status, msg_data = mail.fetch(msg_id, "(RFC822)")
                        if status == "OK":
                            break
                    except (socket.error, imaplib.IMAP4.error, imaplib.IMAP4.abort, ssl.SSLError):
                        if attempt < max_retries:
                            logger.warning(f"  Connection lost on {acct_email} during fetch. Reconnecting...")
                            try: mail.logout()
                            except: pass
                            mail = _get_imap_connection(acct_email, acct_password)
                            continue
                        else: raise

                if not msg_data or status != "OK": continue

                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)
                from_header = _decode_header_value(msg.get("From", ""))
                subject_header = _decode_header_value(msg.get("Subject", ""))
                sender = _extract_sender_email(from_header)

                # A. Direct Reply
                if sender in prospect_emails:
                    logger.info(f"  ✅ Reply: {sender}")
                    replied.append(sender)
                    continue

                # B. Bounce Detection
                is_bounce = False
                if any(x in sender for x in ["mailer-daemon", "postmaster"]):
                    is_bounce = True
                elif any(kw in subject_header.lower() for kw in ["undeliverable", "delivery status notification", "failure", "returned mail"]):
                    is_bounce = True

                if is_bounce:
                    # Get body as string
                    body = ""
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                try:
                                    body += str(part.get_payload(decode=True).decode('utf-8', errors='ignore'))
                                except: pass
                    else:
                        try:
                            body = str(msg.get_payload(decode=True).decode('utf-8', errors='ignore'))
                        except: pass
                    
                    # Regex for Final-Recipient or any known prospect email in the body
                    # 1. Look for structured Final-Recipient field
                    fr_match = re.search(r"Final-Recipient:.*?;\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", body, re.IGNORECASE)
                    if fr_match:
                        email_found = fr_match.group(1).lower()
                        if email_found in prospect_emails:
                            logger.info(f"  ❌ Bounce (Direct): {email_found}")
                            bounced.append(email_found)
                    else:
                        # 2. Heuristic: scan whole body for any monitored email
                        # This works for "User not found: <email>" type plain text bounces
                        for monitoring in prospect_emails:
                            if monitoring in body.lower():
                                logger.info(f"  ❌ Bounce (Heuristic): {monitoring}")
                                bounced.append(monitoring)
                                break
            except Exception as e:
                logger.warning(f"  Error on msg {msg_id}: {e}")
                continue

        if mail: mail.logout()
    except Exception as e:
        logger.error(f"[{acct_email}] IMAP error: {e}")
        try:
            if mail: mail.logout()
        except: pass

    return list(set(replied)), list(set(bounced))


def check_all_replies(days: int = 7) -> dict:
    """Checks all accounts and updates Supabase for replies/bounces."""
    from supabase import create_client
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        logger.error("Supabase not configured")
        return {'error': 'No credentials'}

    supabase = create_client(supabase_url, supabase_key)

    # Fetch prospects in active sequence
    res = supabase.table('contacts').select('id, email').eq('status', 'in_sequence').execute()
    contacts = res.data or []
    if not contacts:
        logger.info("No active prospects.")
        return {'checked': 0, 'replies_found': 0, 'bounces_found': 0}

    email_to_id = {c['email'].strip().lower(): c['id'] for c in contacts if c.get('email')}
    prospect_emails = set(email_to_id.keys())
    
    accounts = _load_accounts_from_env()
    all_replied = set()
    all_bounced = set()

    for acct in accounts:
        replied, bounced = check_replies_for_account(acct['email'], acct['app_password'], prospect_emails, days)
        all_replied.update(replied)
        all_bounced.update(bounced)

    for email in all_replied:
        cid = email_to_id.get(email)
        if cid:
            try:
                supabase.table('contacts').update({'status': 'replied', 'updated_at': datetime.utcnow().isoformat()}).eq('id', cid).execute()
                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', cid).eq('status', 'pending').execute()
                logger.info(f"Marked {email} as REPLIED")
            except Exception as e:
                logger.warning(f"Failed to update status for replied email {email}: {e}")

    for email in all_bounced:
        cid = email_to_id.get(email)
        if cid:
            try:
                # 1. Mark contact as bounced
                supabase.table('contacts').update({'status': 'bounced', 'updated_at': datetime.utcnow().isoformat()}).eq('id', cid).execute()
                
                # 2. Mark the MOST RECENT SENT step as 'bounced' (for dashboard stats)
                recent_sent = supabase.table('email_sequences') \
                    .select('id') \
                    .eq('contact_id', cid) \
                    .eq('status', 'sent') \
                    .order('sent_at', desc=True) \
                    .limit(1) \
                    .execute()
                
                if recent_sent.data:
                    seq_id = recent_sent.data[0]['id']
                    supabase.table('email_sequences') \
                        .update({'status': 'bounced'}) \
                        .eq('id', seq_id) \
                        .execute()

                # 3. Cancel all remaining pending sequences for this contact
                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', cid).eq('status', 'pending').execute()
                logger.info(f"Marked {email} as BOUNCED and cancelled pending steps")
            except Exception as e:
                logger.warning(f"Failed to update status for bounced email {email}: {e}")

    return {
        'monitored': len(prospect_emails),
        'replies': len(all_replied),
        'bounces': len(all_bounced),
        'replied_emails': list(all_replied),
        'bounced_emails': list(all_bounced)
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check for prospect replies via IMAP')
    parser.add_argument('--days', type=int, default=7, help='How many days back to scan (default: 7)')

    args = parser.parse_args()
    stats = check_all_replies(days=args.days)
    print(json.dumps(stats, indent=2))
