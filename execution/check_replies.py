import imaplib
import email
import os
import re
import json
from datetime import datetime, timedelta
from email.header import decode_header
from dotenv import load_dotenv

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

def _extract_sender_email(from_header: str) -> str:
    decoded_from = _decode_header_value(from_header)
    emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', decoded_from)
    if emails: return emails[0].strip().lower()
    return decoded_from.strip().lower()

def is_bounce(from_addr: str, subject: str) -> bool:
    f = (from_addr or "").lower()
    s = (subject or "").lower()
    if any(x in f for x in ['mailer-daemon', 'postmaster', 'no-reply@accounts.google.com']): return True
    if any(x in s for x in ['undeliverable', 'delivery status notification', 'failure', 'returned mail']): return True
    return False

def check_replies():
    """Main synchronizer using Service Role Key to bypass RLS."""
    print("--- Starting Hardened Reply Detection (Zero-Miss) ---")
    
    # Environment Setup
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    env_path = os.path.join(project_root, '.env.tmp')
    if not os.path.exists(env_path):
        env_path = os.path.join(project_root, '.env')
    
    load_dotenv(env_path)
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    
    if not url or not key:
        print("Error: Supabase credentials missing")
        return
    
    from supabase import create_client
    supabase = create_client(url, key)
    
    # 1. Build Comprehensive Domain Map
    print("Building Domain Map for Zero-Miss Detection...")
    res = supabase.table('contacts').select('id, email, company, enrichment_data').execute()
    contacts = res.data or []
    prospect_emails = set()
    domain_map = {} # domain -> contact_id
    
    for c in contacts:
        cid = c['id']
        email_val = (c.get('email') or '').lower().strip()
        if email_val:
            prospect_emails.add(email_val)
            domain_map[email_val.split('@')[-1]] = cid
        
        # Company Name Domain Extraction
        company = (c.get('company') or '').lower().strip()
        if company and len(company) > 3:
            comp_domain = company.replace(' ', '').replace('.com', '') + '.com'
            domain_map[comp_domain] = cid
            
        # Enrichment Data Website Extraction
        enrich = c.get('enrichment_data') or {}
        if isinstance(enrich, str):
            try: enrich = json.loads(enrich)
            except: enrich = {}
        
        website = (enrich.get('website') or enrich.get('company_domain', '') or '').lower().strip()
        if website:
            w_domain = website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
            if len(w_domain) > 3:
                domain_map[w_domain] = cid

    print(f"Loaded {len(prospect_emails)} emails and {len(domain_map)} unique domains.")

    # 2. Get Gmail Accounts
    accounts = []
    for i in range(1, 25):
        e = os.getenv(f'GMAIL_{i}_EMAIL')
        p = os.getenv(f'GMAIL_{i}_PASSWORD')
        if e and p: accounts.append((e, p))

    if not accounts:
        print("No Gmail accounts found in environment")
        return

    # 3. Scan Accounts
    for acct_email, acct_password in accounts:
        print(f"\nScanning: {acct_email}...")
        try:
            mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
            mail.login(acct_email, acct_password)
            
            # Select folder
            folder_status, _ = mail.select('"[Gmail]/All Mail"')
            if folder_status != 'OK':
                mail.select('INBOX')

            # Scan last 10 days
            since_date = (datetime.now() - timedelta(days=10)).strftime("%d-%b-%Y")
            status, message_ids = mail.search(None, f'(SINCE {since_date})')
            
            if status != "OK" or not message_ids[0]:
                mail.logout()
                continue
                
            ids = message_ids[0].split()
            print(f"  Messages: {len(ids)}")
            
            for msg_id in reversed(ids):
                try:
                    # Headers first
                    _, header_data = mail.fetch(msg_id, "(BODY[HEADER.FIELDS (FROM SUBJECT)])")
                    if not header_data or not header_data[0]: continue
                    msg = email.message_from_bytes(header_data[0][1])
                    
                    from_hdr = _decode_header_value(msg.get("From", ""))
                    subject_hdr = _decode_header_value(msg.get("Subject", ""))
                    sender = _extract_sender_email(from_hdr)
                    
                    if sender == acct_email.lower(): continue

                    contact_id = None
                    if sender in prospect_emails:
                        # Find matching contact_id
                        matches = [c['id'] for c in contacts if (c.get('email') or '').lower() == sender]
                        contact_id = matches[0] if matches else None
                    else:
                        # Try domain match
                        domain = sender.split('@')[-1]
                        contact_id = domain_map.get(domain)

                    if contact_id:
                        # Full fetch for deep analysis and bounce detection
                        _, full_data = mail.fetch(msg_id, "(RFC822)")
                        full_msg = email.message_from_bytes(full_data[0][1])
                        
                        body = ""
                        if full_msg.is_multipart():
                            for part in full_msg.walk():
                                if part.get_content_type() == "text/plain":
                                    body += part.get_payload(decode=True).decode(errors='replace')
                        else:
                            body = full_msg.get_payload(decode=True).decode(errors='replace')

                        if is_bounce(from_hdr, subject_hdr):
                            print(f"  [BOUNCE] {sender}")
                            supabase.table('contacts').update({'status': 'bounced'}).eq('id', contact_id).execute()
                        else:
                            print(f"  [REPLY] {sender}")
                            # Save to replies table
                            supabase.table('replies').insert({
                                'contact_id': contact_id,
                                'sender_email': sender,
                                'recipient_email': acct_email,
                                'subject': subject_hdr,
                                'body': body[:5000],
                                'message_id': full_msg.get('Message-ID', ''),
                                'thread_id': full_msg.get('Thread-ID', '')
                            }).execute()
                            # Update contact status
                            supabase.table('contacts').update({'status': 'replied'}).eq('id', contact_id).execute()
                            
                except Exception as e:
                    print(f"  Error on msg {msg_id}: {e}")
            mail.logout()
        except Exception as e:
            print(f"  Connection failed for {acct_email}: {e}")

if __name__ == "__main__":
    check_replies()
