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
import imaplib
import email
import logging
import argparse
import json
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


def check_replies_for_account(acct_email: str, acct_password: str, prospect_emails: set, days: int = 7) -> list[str]:
    """
    Connect via IMAP to a single Gmail account and look for replies
    from any of the known prospect emails.

    Returns a list of prospect email addresses that have replied.
    """
    replied = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(acct_email, acct_password)
        mail.select("INBOX")

        # Search for recent emails
        since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
        status, message_ids = mail.search(None, f'(SINCE {since_date})')

        if status != "OK" or not message_ids[0]:
            logger.info(f"[{acct_email}] No recent emails found.")
            mail.logout()
            return replied

        ids = message_ids[0].split()
        logger.info(f"[{acct_email}] Scanning {len(ids)} emails from the last {days} days...")

        for msg_id in ids:
            try:
                status, msg_data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    continue

                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)
                from_header = _decode_header_value(msg.get("From", ""))
                sender = _extract_sender_email(from_header)

                if sender in prospect_emails:
                    logger.info(f"  ✅ Reply detected from prospect: {sender}")
                    replied.append(sender)
            except Exception as e:
                logger.warning(f"  Error parsing email {msg_id}: {e}")
                continue

        mail.logout()

    except imaplib.IMAP4.error as e:
        logger.error(f"[{acct_email}] IMAP auth failed: {e}")
    except Exception as e:
        logger.error(f"[{acct_email}] IMAP error: {e}")

    return replied


def check_all_replies(days: int = 7) -> dict:
    """
    Main entry point. Checks all Gmail accounts for replies from
    known prospects and updates Supabase accordingly.
    """
    from supabase import create_client

    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        logger.error("Supabase not configured")
        return {'error': 'No Supabase credentials'}

    supabase = create_client(supabase_url, supabase_key)

    # Get all prospect emails that are currently in active sequences
    contacts_result = supabase.table('contacts').select('id, email, status').eq('status', 'in_sequence').execute()
    contacts = contacts_result.data or []

    if not contacts:
        logger.info("No contacts currently in active sequences. Nothing to check.")
        return {'checked': 0, 'replies_found': 0}

    # Build a lookup: prospect_email -> contact_id
    email_to_id = {}
    for c in contacts:
        if c.get('email'):
            email_to_id[c['email'].strip().lower()] = c['id']

    prospect_emails = set(email_to_id.keys())
    logger.info(f"Checking replies from {len(prospect_emails)} active prospects...")

    # Load Gmail accounts
    accounts = _load_accounts_from_env()
    if not accounts:
        logger.error("No Gmail accounts found in .env")
        return {'error': 'No Gmail accounts configured'}

    all_replied = set()

    for acct in accounts:
        replied = check_replies_for_account(
            acct_email=acct['email'],
            acct_password=acct['app_password'],
            prospect_emails=prospect_emails,
            days=days
        )
        all_replied.update(replied)

    # Update Supabase for each replied prospect
    updated = 0
    for replied_email in all_replied:
        contact_id = email_to_id.get(replied_email)
        if contact_id:
            # Mark contact as replied
            supabase.table('contacts').update({
                'status': 'replied',
                'updated_at': datetime.utcnow().isoformat()
            }).eq('id', contact_id).execute()

            # Cancel all remaining pending sequences for this contact
            supabase.table('email_sequences').update({
                'status': 'cancelled'
            }).eq('contact_id', contact_id).eq('status', 'pending').execute()

            logger.info(f"Marked contact {replied_email} as replied and cancelled pending sequences.")
            updated += 1

    stats = {
        'accounts_checked': len(accounts),
        'prospects_monitored': len(prospect_emails),
        'replies_found': len(all_replied),
        'contacts_updated': updated,
        'replied_emails': list(all_replied)
    }

    logger.info(f"Reply check complete: {json.dumps(stats, indent=2)}")
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check for prospect replies via IMAP')
    parser.add_argument('--days', type=int, default=7, help='How many days back to scan (default: 7)')

    args = parser.parse_args()
    stats = check_all_replies(days=args.days)
    print(json.dumps(stats, indent=2))
