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
    system_emails = ['mailer-daemon', 'postmaster', 'no-reply@accounts.google.com', 'delivery-reports', 'mta-daemon']
    bounce_subjects = ['undeliverable', 'delivery status notification', 'failure', 'returned mail', 'address not found', 'could not be delivered', 'rejected']
    if any(x in f for x in system_emails): return True
    if any(x in s for x in bounce_subjects): return True
    return False

def check_all_replies(days=7, logger_callback=None):
    """Main synchronizer using Service Role Key to bypass RLS."""
    msg = f"--- Starting Hardened Reply Detection (Zero-Miss) for last {days} days ---"
    print(msg)
    if logger_callback: logger_callback(msg)
    
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
    msg = "Building Domain Map and Project ID Map..."
    print(msg)
    if logger_callback: logger_callback(msg)
    
    res = supabase.table('contacts').select('id, email, company, enrichment_data, project_id').execute()
    contacts = res.data or []
    prospect_emails = set()
    domain_map = {} # domain -> (contact_id, project_id)
    contact_project_map = {} # contact_id -> project_id
    
    # Blacklisted domains that should never be mapped to a contact via domain-matching
    BLACKLIST_DOMAINS = {
        'google.com', 'gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'me.com', 
        'linkedin.com', 'facebook.com', 'instagram.com', 'twitter.com', 'x.com', 'youtube.com',
        'googlealerts-noreply@google.com', 'no-reply@linkedin.com'
    }

    for c in contacts:
        cid = c['id']
        pid = c['project_id']
        contact_project_map[cid] = pid
        
        email_val = (c.get('email') or '').lower().strip()
        if email_val:
            prospect_emails.add(email_val)
            e_domain = email_val.split('@')[-1]
            if e_domain not in BLACKLIST_DOMAINS:
                domain_map[e_domain] = (cid, pid)
        
        company = (c.get('company') or '').lower().strip()
        if company and len(company) > 3:
            comp_domain = company.replace(' ', '').replace('.com', '') + '.com'
            if comp_domain not in BLACKLIST_DOMAINS:
                domain_map[comp_domain] = (cid, pid)
            
        enrich = c.get('enrichment_data') or {}
        if isinstance(enrich, str):
            try: enrich = json.loads(enrich)
            except: enrich = {}
        
        website = (enrich.get('website') or enrich.get('company_domain', '') or '').lower().strip()
        if website:
            if any(x in website for x in ['google.com/maps', 'google.com/search', 'linkedin.com', 'facebook.com']):
                continue
            w_domain = website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
            if len(w_domain) > 3 and w_domain not in BLACKLIST_DOMAINS:
                domain_map[w_domain] = (cid, pid)

    msg = f"Loaded {len(prospect_emails)} emails and {len(domain_map)} unique domains."
    print(msg)
    if logger_callback: logger_callback(msg)

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
        msg = f"\nScanning: {acct_email}..."
        print(msg)
        if logger_callback: logger_callback(msg)
        try:
            mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, timeout=30)
            mail.login(acct_email, acct_password)
            
            folder_status, _ = mail.select('"[Gmail]/All Mail"')
            if folder_status != 'OK':
                mail.select('INBOX')

            since_date = (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")
            status, message_ids = mail.search(None, f'(SINCE {since_date})')
            
            if status != "OK" or not message_ids[0]:
                mail.logout()
                continue
                
            ids = message_ids[0].split()
            msg = f"  Messages: {len(ids)}"
            print(msg)
            if logger_callback: logger_callback(msg)
            
            for msg_id in reversed(ids):
                try:
                    res_status, header_data = mail.fetch(msg_id, "(BODY[HEADER.FIELDS (FROM SUBJECT)])")
                    if res_status != 'OK' or not header_data or not isinstance(header_data[0], tuple):
                        continue
                        
                    raw_header = header_data[0][1]
                    if raw_header is None: continue
                    
                    msg_obj = email.message_from_bytes(raw_header)
                    from_hdr = _decode_header_value(msg_obj.get("From", ""))
                    subject_hdr = _decode_header_value(msg_obj.get("Subject", ""))
                    sender = _extract_sender_email(from_hdr)
                    
                    if not sender or sender == acct_email.lower(): continue

                    # 1. Check for Bounce
                    is_b = is_bounce(from_hdr, subject_hdr)
                    
                    # 2. Identify Contact
                    contact_id = None
                    project_id = None
                    full_msg = None
                    body = ""
                    
                    if is_b:
                        # Deep bounce detection
                        res_status, full_data = mail.fetch(msg_id, "(RFC822)")
                        if res_status == 'OK' and full_data and isinstance(full_data[0], tuple):
                            raw_full = full_data[0][1]
                            if raw_full:
                                full_msg = email.message_from_bytes(raw_full)
                                if full_msg.is_multipart():
                                    for part in full_msg.walk():
                                        if part.get_content_type() == "text/plain":
                                            payload = part.get_payload(decode=True)
                                            if payload: body += payload.decode(errors='replace')
                                else:
                                    payload = full_msg.get_payload(decode=True)
                                    if payload: body = payload.decode(errors='replace')
                                
                                for p_email in prospect_emails:
                                    if p_email in body.lower():
                                        matches = [c for c in contacts if (c.get('email') or '').lower() == p_email]
                                        if matches:
                                            contact_id = matches[0]['id']
                                            project_id = matches[0]['project_id']
                                            msg = f"  [BOUNCE] Found recipient: {p_email}"
                                            print(msg)
                                            if logger_callback: logger_callback(msg)
                                            break
                    else:
                        # Normal Reply detection
                        if sender in prospect_emails:
                            matches = [c for c in contacts if (c.get('email') or '').lower() == sender]
                            if matches:
                                contact_id = matches[0]['id']
                                project_id = matches[0]['project_id']
                        else:
                            domain = sender.split('@')[-1]
                            if domain not in BLACKLIST_DOMAINS:
                                match = domain_map.get(domain)
                                if match:
                                    contact_id, project_id = match

                    if contact_id:
                        m_id = None
                        t_id = None
                        
                        if not is_b:
                            res_status, full_data = mail.fetch(msg_id, "(RFC822)")
                            if res_status == 'OK' and full_data and isinstance(full_data[0], tuple):
                                raw_full = full_data[0][1]
                                if raw_full:
                                    full_msg = email.message_from_bytes(raw_full)
                                    if full_msg.is_multipart():
                                        for part in full_msg.walk():
                                            if part.get_content_type() == "text/plain":
                                                payload = part.get_payload(decode=True)
                                                if payload: body += payload.decode(errors='replace')
                                    else:
                                        payload = full_msg.get_payload(decode=True)
                                        if payload: body = payload.decode(errors='replace')

                        if is_b:
                            supabase.table('contacts').update({'status': 'bounced'}).eq('id', contact_id).execute()
                            # Optional: Update email_sequences too
                        else:
                            msg = f"  [REPLY] {sender}"
                            print(msg)
                            if logger_callback: logger_callback(msg)
                            
                            if full_msg:
                                m_id = full_msg.get('Message-ID', '')
                                t_id = full_msg.get('Thread-ID', '')
                            
                            m_id = str(m_id).strip() if m_id else None
                            t_id = str(t_id).strip() if t_id else None
                            
                            # Insert into replies table (skip if duplicate)
                            try:
                                supabase.table('replies').insert({
                                    'contact_id': contact_id,
                                    'project_id': project_id,
                                    'sender_email': sender,
                                    'recipient_email': acct_email,
                                    'subject': subject_hdr,
                                    'body': body[:5000],
                                    'message_id': m_id,
                                    'thread_id': t_id
                                }).execute()
                            except Exception as e:
                                if 'duplicate key' in str(e).lower():
                                    pass # Already logged, no need to error
                                else:
                                    print(f"  Error inserting reply: {e}")
                            
                            # Update contact status (ALWAYS do this if a reply is found)
                            supabase.table('contacts').update({'status': 'replied'}).eq('id', contact_id).execute()
                            
                except Exception as e:
                    msg = f"  Error on msg {msg_id}: {e}"
                    print(msg)
                    if logger_callback: logger_callback(msg)
            mail.logout()
        except Exception as e:
            msg = f"  Connection failed for {acct_email}: {e}"
            print(msg)
            if logger_callback: logger_callback(msg)

if __name__ == "__main__":
    check_all_replies()

# Alias for backward compatibility
check_replies = check_all_replies
