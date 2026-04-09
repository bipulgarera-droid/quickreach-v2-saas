import base64
import email
import os
import time
import logging
from datetime import datetime, timedelta
from email.header import decode_header
from email.utils import parseaddr
from dotenv import load_dotenv
from execution.classify_email import classify_email

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIG ---
IMAP_HOST = "imap.gmail.com"
IMAP_PORT = 993

def _decode_header_value(raw):
    if raw is None: return ""
    try:
        decoded_parts = decode_header(raw)
        result = ""
        for part, charset in decoded_parts:
            if isinstance(part, bytes):
                result += part.decode(charset or "utf-8", errors="replace")
            else:
                result += part
        return result
    except: return str(raw)

def _extract_plain_text_snippet(full_msg) -> str:
    """Extract up to 2000 characters of plain text from the email."""
    body = ""
    if full_msg.is_multipart():
        for part in full_msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                p = part.get_payload(decode=True)
                if p: body += p.decode(errors='ignore') + "\n"
            elif ctype in ["message/delivery-status", "message/rfc822"]:
                body += str(part) + "\n"
    else:
        p = full_msg.get_payload(decode=True)
        if p: body = p.decode(errors='ignore')
    return body.strip()[:2000]

def check_all_replies(days=7, logger_callback=None, skip_db_update=False):
    """
    Deterministic Reply/Bounce Detection Engine.
    
    Scans all Gmail sender accounts, classifies every incoming email using
    pure pattern matching (zero API calls), and updates the database.
    
    Args:
        days: How many days back to scan.
        logger_callback: Optional function to receive log lines.
        skip_db_update: If True, only classify but don't write to DB (for testing).
    """
    def log(m):
        print(m)
        if logger_callback: logger_callback(m)
        
    log(f"--- Starting Deterministic Reply Detection for last {days} days ---")
    
    # Environment Setup
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    env_path = os.path.join(project_root, '.env.tmp')
    if not os.path.exists(env_path):
        env_path = os.path.join(project_root, '.env')
        
    env_vars = {}
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    k = k.strip()
                    v = v.strip().strip("'").strip('"')
                    env_vars[k] = v
                    os.environ[k] = v
                    
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not url or not key:
        log("Error: Supabase credentials missing")
        return
    
    from supabase import create_client
    supabase = create_client(url, key)
    
    log("Building Contact & Subject Maps...")
    
    # Load ALL contacts across ALL projects
    all_contacts = []
    page = 0
    page_size = 1000
    while True:
        res = supabase.table('contacts').select('id, email, company, project_id').range(page * page_size, (page + 1) * page_size - 1).execute()
        if not res.data:
            break
        all_contacts.extend(res.data)
        if len(res.data) < page_size:
            break
        page += 1
    
    prospect_emails = {}  # email -> (contact_id, project_id, company)
    cid_info = {}         # contact_id -> (email, company)
    for c in all_contacts:
        email_val = (c.get('email') or '').lower().strip()
        comp_val = c.get('company') or 'Unknown'
        cid_info[c['id']] = (email_val, comp_val)
        if email_val:
            prospect_emails[email_val] = (c['id'], c['project_id'], comp_val)
            
    # Active subject map (campaign subjects -> contact mappings)
    subject_map = {}  # base_subject -> [(contact_id, project_id)]
    page = 0
    while True:
        seq_page = supabase.table('email_sequences').select('contact_id, project_id, subject').range(page * page_size, (page + 1) * page_size - 1).execute()
        if not seq_page.data:
            break
        for s in seq_page.data:
            subj = (s.get('subject') or '').strip().lower()
            if subj:
                if subj not in subject_map:
                    subject_map[subj] = []
                # Store (contact_id, project_id, email, company)
                cid = s['contact_id']
                em, comp = cid_info.get(cid, ('Unknown', 'Unknown'))
                subject_map[subj].append((cid, s['project_id'], em, comp))
        if len(seq_page.data) < page_size:
            break
        page += 1
                
    log(f"Loaded {len(prospect_emails)} prospect emails and {len(subject_map)} campaign subjects.")

    # ---------------------------------------------------------------------------
    # Get Accounts — multi-tenant: load from Supabase user_email_accounts
    # ---------------------------------------------------------------------------
    log("Loading accounts from Supabase user_email_accounts...")
    
    try:
        imap_res = supabase.table('user_email_accounts') \
            .select('user_id, email_address, refresh_token') \
            .eq('is_active', True) \
            .not_.is_('refresh_token', 'null') \
            .execute()
        imap_rows = imap_res.data or []
    except Exception as e:
        log(f"Warning: Could not load accounts from DB ({e}).")
        imap_rows = []

    # Build accounts list: [(email, refresh_token, user_id)]
    accounts = [(r.get('email_address'), r.get('refresh_token'), r.get('user_id')) for r in imap_rows if r.get('email_address') and r.get('refresh_token')]

    if not accounts:
        log("No Gmail accounts found in DB with active refresh tokens.")
        return

    stats = {'human_replies': 0, 'bounces': 0, 'auto_replies': 0, 'spam_ignored': 0, 'unmatched_replies': 0, 'unmatched_bounces': 0}
    
    # Track already-processed contact IDs this run to avoid duplicate updates
    processed_contacts = set()

    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    client_id = os.getenv("GMAIL_CLIENT_ID")
    client_secret = os.getenv("GMAIL_CLIENT_SECRET")
    if not client_id or not client_secret:
        log("Missing GMAIL_CLIENT_ID or GMAIL_CLIENT_SECRET in environment!")
        return

    # Scan ALL Accounts
    for acct_email, acct_refresh_token, acct_user_id in accounts:
        log(f"\nScanning: {acct_email}...")
        try:
            creds = Credentials(
                token=None,
                refresh_token=acct_refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=client_id,
                client_secret=client_secret
            )
            service = build('gmail', 'v1', credentials=creds, cache_discovery=False)

            timestamp = int(time.time() - (days * 86400))
            query = f"after:{timestamp}"
            
            results = service.users().messages().list(userId='me', q=query, maxResults=200).execute()
            messages = results.get('messages', [])
            
            if not messages:
                continue
                
            log(f"  Messages: {len(messages)}")
            
            # Fetch and classify each email
            for msg_index, msg_info in enumerate(messages):
                try:
                    msg_data = service.users().messages().get(userId='me', id=msg_info['id'], format='raw').execute()
                    raw_encoded = msg_data.get('raw')
                    if not raw_encoded:
                        continue
                        
                    raw_full = base64.urlsafe_b64decode(raw_encoded.encode('ASCII'))
                    msg_obj = email.message_from_bytes(raw_full)
                    from_hdr = _decode_header_value(msg_obj.get("From", ""))
                    subject_hdr = _decode_header_value(msg_obj.get("Subject", ""))
                    
                    _, sender_email = parseaddr(from_hdr.lower())
                    sender = sender_email.strip()
                    
                    if not sender or sender == acct_email.lower():
                        continue
                    
                    body_snippet = _extract_plain_text_snippet(msg_obj)
                    
                    # === DETERMINISTIC CLASSIFICATION ===
                    result = classify_email(
                        sender=sender,
                        subject=subject_hdr,
                        body_snippet=body_snippet,
                        prospect_emails=prospect_emails,
                        subject_map=subject_map
                    )
                    
                    cls = result['classification']
                    contact_id = result['matched_contact_id']
                    project_id = result['matched_project_id']
                    matched_email = result['matched_email']
                    matched_company = result['matched_company']
                    reason = result['reason']
                    
                    # === EXECUTE DB UPDATES ===
                    if cls == 'BOUNCE':
                        stats['bounces'] += 1
                        if contact_id and contact_id not in processed_contacts:
                            processed_contacts.add(contact_id)
                            log(f"  ✅ [BOUNCE] {matched_email} ({matched_company}) | {reason}")
                            if not skip_db_update:
                                supabase.table('contacts').update({'status': 'bounced'}).eq('id', contact_id).execute()
                                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', contact_id).eq('status', 'pending').execute()
                                # Insert into replies table for dashboard visibility
                                try:
                                    supabase.table('replies').insert({
                                        'contact_id': contact_id,
                                        'project_id': project_id,
                                        'sender_email': sender,
                                        'recipient_email': acct_email,
                                        'subject': subject_hdr[:200],
                                        'body': body_snippet[:2000],
                                        'sentiment': 'bounce',
                                        'received_at': datetime.now().isoformat()
                                    }).execute()
                                except Exception as insert_err:
                                    log(f"  ⚠️ Could not insert bounce into replies table: {insert_err}")
                        elif not contact_id:
                            stats['unmatched_bounces'] += 1
                            log(f"  ⚠️ [UNMATCHED BOUNCE] From: {sender} | Subj: {subject_hdr[:60]} | {reason}")
                            
                    elif cls == 'HUMAN_REPLY':
                        stats['human_replies'] += 1
                        if contact_id and contact_id not in processed_contacts:
                            processed_contacts.add(contact_id)
                            log(f"  ✅ [REPLY] {matched_email} ({matched_company}) | {reason}")
                            if not skip_db_update:
                                supabase.table('contacts').update({'status': 'replied'}).eq('id', contact_id).execute()
                                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', contact_id).eq('status', 'pending').execute()
                                # Insert into replies table for dashboard visibility
                                try:
                                    supabase.table('replies').insert({
                                        'contact_id': contact_id,
                                        'project_id': project_id,
                                        'sender_email': sender,
                                        'recipient_email': acct_email,
                                        'subject': subject_hdr[:200],
                                        'body': body_snippet[:2000],
                                        'sentiment': 'neutral',
                                        'received_at': datetime.now().isoformat()
                                    }).execute()
                                except Exception as insert_err:
                                    log(f"  ⚠️ Could not insert reply into replies table: {insert_err}")
                        elif not contact_id:
                            stats['unmatched_replies'] += 1
                            log(f"  ⚠️ [UNMATCHED REPLY] From: {sender} | Subj: {subject_hdr[:60]} | {reason}")
                            
                    elif cls == 'AUTO_REPLY':
                        stats['auto_replies'] += 1
                        if contact_id:
                            log(f"  ⏸️ [AUTO_REPLY] {matched_email} ({matched_company}) | {reason}")
                            if not skip_db_update:
                                try:
                                    supabase.table('replies').insert({
                                        'contact_id': contact_id,
                                        'project_id': project_id,
                                        'sender_email': sender,
                                        'recipient_email': acct_email,
                                        'subject': subject_hdr[:200],
                                        'body': body_snippet[:2000],
                                        'sentiment': 'neutral',
                                        'received_at': datetime.now().isoformat()
                                    }).execute()
                                except Exception as insert_err:
                                    log(f"  ⚠️ Could not insert auto-reply into replies table: {insert_err}")
                        else:
                            log(f"  ⏸️ [AUTO_REPLY] From: {sender} | Subj: {subject_hdr[:60]}")
                    
                    else:  # SPAM
                        stats['spam_ignored'] += 1
                        
                except Exception as msg_err:
                    log(f"  ❌ Error processing message: {msg_err}")
                    continue
                        
        except Exception as e:
            log(f"Error checking {acct_email}: {e}")
            import traceback
            traceback.print_exc()

    log(f"\n--- Detection Complete ---")
    log(f"Human Replies: {stats['human_replies']} | Bounces: {stats['bounces']} | Auto-Replies: {stats['auto_replies']}")
    log(f"Unmatched Replies: {stats['unmatched_replies']} | Unmatched Bounces: {stats['unmatched_bounces']} | Spam Ignored: {stats['spam_ignored']}")
    log(f"[OK] Reply check complete. Found {stats['human_replies']} replies and {stats['bounces']} bounces.")
    return stats

if __name__ == "__main__":
    check_all_replies(days=7)
