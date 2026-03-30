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
import time
from datetime import datetime, timedelta
from email.header import decode_header
import google.generativeai as genai

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
from execution.generate_reply import generate_draft_reply
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

# Initialize Gemini Client
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


def analyze_sentiment(text: str) -> tuple[str, float]:
    """Analyze sentiment of an email reply using Gemini."""
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""
        Analyze the sentiment of this cold email reply. 
        Classify it as ONE of: positive, negative, neutral, question, or unsubscribe.
        
        - positive: Interested, wants a call, wants more info, "sounds great"
        - negative: Not interested, "no thanks", "don't contact me"
        - question: Asking for pricing, asking how we found them, asking for a demo
        - unsubscribe: "remove me", "unsubscribe", "stop"
        - neutral: Automatic out-of-office, "ok", "received"
        
        Reply:
        \"\"\"{text[:2000]}\"\"\"
        
        Return ONLY a JSON object: {{"sentiment": "...", "score": 0.0-1.0}}
        """
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        data = json.loads(response.text)
        return data.get("sentiment", "unknown"), data.get("score", 0.0)
    except Exception as e:
        logger.warning(f"Sentiment analysis failed: {e}")
        return "unknown", 0.0


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
    """Extract just the email address from a From: header, handling encoded strings."""
    decoded_from = _decode_header_value(from_header)
    
    # Use regex for better precision if possible
    import re
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', decoded_from)
    if emails:
        return emails[0].strip().lower()
        
    if "<" in decoded_from and ">" in decoded_from:
        return decoded_from.split("<")[1].split(">")[0].strip().lower()
    return decoded_from.strip().lower()


def _get_imap_connection(acct_email: str, acct_password: str) -> imaplib.IMAP4_SSL:
    """Helper to establish a fresh IMAP connection with timeout."""
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
    mail.login(acct_email, acct_password)
    
    # Try [Gmail]/All Mail if available, it's more robust
    try:
        mail.select('"[Gmail]/All Mail"')
    except:
        mail.select("INBOX")
    return mail


def _extract_body(msg) -> str:
    """Recursively extract the plain text body from an email message."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if content_type == "text/plain" and "attachment" not in content_disposition:
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode('utf-8', errors='ignore')
                except: pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode('utf-8', errors='ignore')
        except: pass
    return body.strip()


def check_replies_for_account(acct_email: str, acct_password: str, prospect_emails: set, days: int = 7, logger_callback=None) -> tuple[list[dict], list[str]]:
    """
    Connect via IMAP to a single Gmail account and look for:
    1. Direct replies from known prospect emails.
    2. Bounce notifications (mailer-daemon) referring to these prospects.

    Returns (replied_emails_dicts, bounced_emails).
    """
    replied = []
    bounced = []
    mail = None

    def _log(msg, level='info'):
        if level == 'warning': logger.warning(msg)
        else: logger.info(msg)
        if logger_callback:
            logger_callback(msg)

    try:
        _log(f"Checking {acct_email}...")
        mail = _get_imap_connection(acct_email, acct_password)

        # Search for recent emails
        since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
        status, message_ids = mail.search(None, f'(SINCE {since_date})')

        if status != "OK" or not message_ids[0]:
            _log(f"[{acct_email}] No recent emails found.")
            mail.logout()
            return [], []

        ids = message_ids[0].split()
        _log(f"[{acct_email}] Scanning {len(ids)} emails from last {days} days...")

        for msg_id in ids:
            try:
                # Attempt to fetch. If connection is lost, retry ONCE after reconnecting.
                max_retries = 1
                msg_data = None
                for attempt in range(max_retries + 1):
                    try:
                        # Fetch RFC822 for body AND X-GM-THRID / X-GM-MSGID for threading/dedup
                        status, msg_data = mail.fetch(msg_id, "(RFC822 X-GM-THRID X-GM-MSGID)")
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

                # Extract Gmail metadata from the fetch response
                metadata_raw = msg_data[0][0].decode()
                thread_id = re.search(r"X-GM-THRID (\d+)", metadata_raw)
                thread_id = thread_id.group(1) if thread_id else None
                message_id_gmail = re.search(r"X-GM-MSGID (\d+)", metadata_raw)
                message_id_gmail = message_id_gmail.group(1) if message_id_gmail else None

                # Log every sender to console/debug only to prevent dashboard bloating
                logger.debug(f"  [Scan] From: {sender} | Subject: {subject_header[:30]}...")

                # A. Direct Reply Match
                if sender in prospect_emails:
                    _log(f"  ✅ Reply Match: {sender}")
                    email_body = _extract_body(msg)
                    sentiment, score = analyze_sentiment(email_body)
                    replied.append({
                        'email': sender,
                        'subject': subject_header,
                        'body': email_body,
                        'sentiment': sentiment,
                        'sentiment_score': score,
                        'thread_id': thread_id,
                        'message_id': message_id_gmail,
                        'recipient_email': acct_email
                    })
                    continue

                # B. Bounce Detection
                is_bounce = False
                if any(x in sender for x in ["mailer-daemon", "postmaster"]):
                    is_bounce = True
                elif any(kw in subject_header.lower() for kw in ["undeliverable", "delivery status notification", "failure", "returned mail"]):
                    is_bounce = True

                if is_bounce:
                    # Get body as string - include ALL parts for bounces
                    body_parts = []
                    for part in msg.walk():
                        ctype = part.get_content_type()
                        if ctype in ["text/plain", "text/rfc822-headers", "message/delivery-status"]:
                            try:
                                payload = part.get_payload(decode=True)
                                if payload: body_parts.append(payload.decode('utf-8', errors='ignore'))
                            except: pass
                    
                    full_bounce_body = "\n".join(body_parts).lower()
                    
                    # Search for any known prospect email in the bounce body
                    for p_email in prospect_emails:
                        if p_email in full_bounce_body:
                            _log(f"  ❌ Bounce Detected: {p_email}")
                            bounced.append(p_email)
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

    return replied, list(set(bounced))


def check_all_replies(days: int = 7, logger_callback=None) -> dict:
    """Checks all accounts and updates Supabase for replies/bounces."""
    
    def _log(msg, level='info'):
        if level == 'warning': logger.warning(msg)
        else: logger.info(msg)
        if logger_callback:
            logger_callback(msg)

    from supabase import create_client
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')

    if not supabase_url or not supabase_key:
        logger.error("Supabase not configured")
        return {'error': 'No credentials'}

    supabase = create_client(supabase_url, supabase_key)

    # Fetch ALL prospects that have an email address (very important for catch-all)
    # We include all statuses except maybe 'skipped' or 'new' if they never got mail,
    # but to be safe, let's include EVERYTHING that has been enriched/icebreakers/etc.
    res = supabase.table('contacts') \
        .select('id, email') \
        .not_.is_('email', 'null') \
        .execute()
    
    contacts = res.data or []
    if not contacts:
        _log("No prospects found in database.")
        return {'checked': 0, 'replies_found': 0, 'bounces_found': 0}

    email_to_id = {c['email'].strip().lower(): c['id'] for c in contacts if c.get('email')}
    prospect_emails = set(email_to_id.keys())
    
    _log(f"Monitoring {len(prospect_emails)} prospect emails across all projects...")
    
    accounts = _load_accounts_from_env()
    all_replied = []
    all_bounced = set()

    for acct in accounts:
        replied, bounced = check_replies_for_account(acct['email'], acct['app_password'], prospect_emails, days, logger_callback=logger_callback)
        for r in replied:
            if not any(ar['message_id'] == r['message_id'] for ar in all_replied):
                all_replied.append(r)
        all_bounced.update(bounced)

    for reply_data in all_replied:
        email = reply_data['email']
        cid = email_to_id.get(email)
        if cid:
            try:
                # 1. Look up contact to get project_id
                contact_info = supabase.table('contacts').select('project_id').eq('id', cid).single().execute()
                pid = contact_info.data.get('project_id') if contact_info.data else None

                # 2. Check if reply already exists in DB to prevent duplicates across runs
                existing = supabase.table('replies').select('id').eq('message_id', reply_data['message_id']).execute()
                if not existing.data:
                    # 2.5 Generate draft if positive/question
                    draft = None
                    if reply_data['sentiment'] in ['positive', 'question'] and pid:
                        _log(f"Generating AI draft for {email}...")
                        try:
                            c_res = supabase.table('contacts').select('name').eq('id', cid).single().execute()
                            cname = c_res.data.get('name', 'there') if c_res.data else 'there'
                            draft = generate_draft_reply(pid, reply_data['body'], cname)
                        except Exception as de:
                            _log(f"Drafting failed: {de}", level='warning')

                    # 3. Store in replies table
                    supabase.table('replies').insert({
                        'contact_id': cid,
                        'project_id': pid,
                        'sender_email': email,
                        'recipient_email': reply_data['recipient_email'],
                        'subject': reply_data['subject'],
                        'body': reply_data['body'],
                        'sentiment': reply_data['sentiment'],
                        'sentiment_score': reply_data['sentiment_score'],
                        'thread_id': reply_data['thread_id'],
                        'message_id': reply_data['message_id'],
                        'received_at': datetime.utcnow().isoformat(),
                        'draft_reply': draft,
                        'status': 'needs_review' if draft else 'received'
                    }).execute()
                    _log(f"Stored reply from {email} (Sentiment: {reply_data['sentiment']})")

                # 4. Update contact status
                supabase.table('contacts').update({'status': 'replied', 'updated_at': datetime.utcnow().isoformat()}).eq('id', cid).execute()
                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', cid).eq('status', 'pending').execute()
                _log(f"Marked {email} as REPLIED")
            except Exception as e:
                logger.warning(f"Failed to process reply for {email}: {e}")

    for email in all_bounced:
        cid = email_to_id.get(email)
        if cid:
            try:
                supabase.table('contacts').update({'status': 'bounced', 'updated_at': datetime.utcnow().isoformat()}).eq('id', cid).execute()
                recent_sent = supabase.table('email_sequences') \
                    .select('id') \
                    .eq('contact_id', cid) \
                    .eq('status', 'sent') \
                    .order('sent_at', desc=True) \
                    .limit(1) \
                    .execute()
                
                if recent_sent.data:
                    seq_id = recent_sent.data[0]['id']
                    supabase.table('email_sequences').update({'status': 'bounced'}).eq('id', seq_id).execute()

                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', cid).eq('status', 'pending').execute()
                _log(f"Marked {email} as BOUNCED and cancelled pending steps")
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
