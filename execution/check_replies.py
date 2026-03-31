import imaplib
import email
import os
import re
import json
from datetime import datetime, timedelta
from email.header import decode_header
from email.utils import parseaddr
from dotenv import load_dotenv
from execution.ai_reply_analyzer import analyze_incoming_emails

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
    """Extract up to 1000 characters of plain text from the email for Gemini."""
    body = ""
    if full_msg.is_multipart():
        for part in full_msg.walk():
            if part.get_content_type() == "text/plain":
                p = part.get_payload(decode=True)
                if p: body += p.decode(errors='ignore')
    else:
        p = full_msg.get_payload(decode=True)
        if p: body = p.decode(errors='ignore')
    return body.strip()[:1000]

def check_all_replies(days=7, logger_callback=None):
    """Main synchronizer using Gemini AI Analyzer."""
    def log(m):
        print(m)
        if logger_callback: logger_callback(m)
        
    log(f"--- Starting AI-Powered Reply Detection for last {days} days ---")
    
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
    res = supabase.table('contacts').select('id, email, company, project_id').execute()
    contacts = [c for c in res.data if c is not None] if res.data else []
    
    prospect_emails = {} # email -> (id, pid, company)
    for c in contacts:
        email_val = (c.get('email') or '').lower().strip()
        if email_val:
            prospect_emails[email_val] = (c['id'], c['project_id'], c.get('company', 'Unknown'))
            
    # Active subject map
    subject_map = {} # base_subject -> [(contact_id, project_id)]
    page_size = 1000
    for i in range(0, 50000, page_size):
        seq_page = supabase.table('email_sequences').select('contact_id, project_id, subject').range(i, i + page_size - 1).execute()
        if not seq_page.data: break
        for s in seq_page.data:
            subj = (s.get('subject') or '').strip().lower()
            if subj:
                if subj not in subject_map:
                    subject_map[subj] = []
                subject_map[subj].append((s['contact_id'], s['project_id']))
                
    campaign_subjects = list(subject_map.keys())
    log(f"Loaded {len(prospect_emails)} prospect emails and {len(campaign_subjects)} campaign subjects.")

    # Get Gmail Accounts
    accounts = []
    for i in range(1, 25):
        e = os.getenv(f'GMAIL_{i}_EMAIL')
        p = os.getenv(f'GMAIL_{i}_PASSWORD')
        if e and p: accounts.append((e, p))

    if not accounts:
        log("No Gmail accounts found in environment")
        return

    stats = {'human_replies': 0, 'bounces': 0, 'auto_replies': 0, 'spam_ignored': 0}

    # Scan Accounts
    for acct_email, acct_password in accounts:
        log(f"\nScanning: {acct_email}...")
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
            log(f"  Messages: {len(ids)}")
            
            batch_payload = []
            
            # 1. Fetch all emails into a batch
            for msg_index, msg_id in enumerate(reversed(ids)):
                # Only check up to 50 recent emails per inbox to save time/tokens if inbox is huge
                if msg_index > 50: break 
                
                res_status, fd = mail.fetch(msg_id, "(RFC822)")
                if res_status != 'OK' or not fd or not isinstance(fd[0], tuple):
                    continue
                    
                raw_full = fd[0][1]
                if not raw_full: continue
                
                msg_obj = email.message_from_bytes(raw_full)
                from_hdr = _decode_header_value(msg_obj.get("From", ""))
                subject_hdr = _decode_header_value(msg_obj.get("Subject", ""))
                
                _, sender_email = parseaddr(from_hdr.lower())
                sender = sender_email.strip()
                
                if not sender or sender == acct_email.lower(): continue
                
                # Fast Pre-Filter for obvious irrelevant SPAM (skip Gemini to save tokens/time)
                if sender in ['no-reply@linkedin.com', 'alerts@linkedin.com', 'googlealerts-noreply@google.com']:
                    stats['spam_ignored'] += 1
                    continue
                    
                body_snippet = _extract_plain_text_snippet(msg_obj)
                
                log(f"    [DUMP] From: {sender} | Subj: {subject_hdr[:60]}")
                
                batch_payload.append({
                    "msg_index": msg_index,
                    "sender": sender,
                    "subject": subject_hdr,
                    "body_snippet": body_snippet
                })

            # 2. Analyze the Batch with Gemini!
            if batch_payload:
                log(f"    -> Passing {len(batch_payload)} emails to Gemini AI Analyzer...")
                ai_results = analyze_incoming_emails(batch_payload, campaign_subjects)
                
                # 3. Process the AI Classifications
                for payload in batch_payload:
                    idx = str(payload['msg_index'])
                    if idx not in ai_results: continue
                    
                    res = ai_results[idx]
                    cls = res['classification']
                    reason = res['reason']
                    extracted_email = (res.get('extracted_contact_email') or '').lower().strip()
                    extracted_subject = (res.get('extracted_subject') or '').lower().strip()
                    sender = payload['sender']
                    
                    # Core matching logic
                    contact_id = None
                    project_id = None
                    c_company = "Unknown"
                    c_email = "Unknown"
                    
                    # Tactic 1: Real sender matches our prospect DB
                    if sender in prospect_emails:
                        contact_id, project_id, c_company = prospect_emails[sender]
                        c_email = sender
                    # Tactic 2: Gemini extracted a failed/forwarded email from the body
                    elif extracted_email and extracted_email in prospect_emails:
                        contact_id, project_id, c_company = prospect_emails[extracted_email]
                        c_email = extracted_email
                    # Tactic 3: Gemini perfectly matched the original Subject Line 
                    elif extracted_subject and extracted_subject in subject_map:
                        candidates = subject_map[extracted_subject]
                        # Just pick the first candidate if we can't narrow down by domain
                        # (In reality, subject strings are extremely unique so collisions are rare)
                        contact_id = candidates[0][0]
                        project_id = candidates[0][1]
                        
                        # Just grab the email for logging
                        for c in contacts:
                            if c['id'] == contact_id:
                                c_email = c['email']
                                c_company = c.get('company', 'Unknown')
                                break

                    # Execute DB Updates based on AI Classification
                    if cls == 'BOUNCE':
                        stats['bounces'] += 1
                        if contact_id:
                            log(f"  ✅ [BOUNCE CATCH] Matched Contact: {c_email} | AI Reason: {reason}")
                            supabase.table('contacts').update({'status': 'bounced'}).eq('id', contact_id).execute()
                        else:
                            log(f"  ⚠️ [UNMATCHED BOUNCE] Failed to link bounce to contact. Sender: {sender} | Reason: {reason}")
                            
                    elif cls == 'HUMAN_REPLY':
                        stats['human_replies'] += 1
                        if contact_id:
                            log(f"  ✅ [REPLY CATCH] Matched Contact: {c_email} | AI Reason: {reason}")
                            supabase.table('contacts').update({'status': 'replied'}).eq('id', contact_id).execute()
                            # Optional: Update sequence status too
                            supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', contact_id).eq('status', 'pending').execute()
                        else:
                            log(f"  ⚠️ [UNMATCHED REPLY] AI found a human reply but couldn't link it. Sender: {sender} | Reason: {reason}")
                            
                    elif cls == 'AUTO_REPLY':
                        stats['auto_replies'] += 1
                        log(f"  ⏸️ [AUTO_REPLY IGNORED] {sender} is OOTO. AI Reason: {reason}")
                    
                    else: # SPAM / IGNORE
                        stats['spam_ignored'] += 1
                        # We just ignore these
                        pass
        except Exception as e:
            log(f"Error checking {acct_email}: {e}")
            import traceback
            traceback.print_exc()

    log(f"\n--- AI Detection Complete ---")
    log(f"Human Replies: {stats['human_replies']} | Bounces: {stats['bounces']} | Auto-Replies: {stats['auto_replies']} | Spam Ignored: {stats['spam_ignored']}")
    return stats

if __name__ == "__main__":
    check_all_replies(days=7)
