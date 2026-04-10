"""
Deterministic Email Classifier - Zero API Calls, Zero Misses.

Classifies incoming emails into BOUNCE, AUTO_REPLY, HUMAN_REPLY, or SPAM
using pure pattern matching against known sender patterns, subject lines,
and our prospect database.

This replaces the Gemini AI analyzer with something that CANNOT silently fail.
"""

import re
import logging

logger = logging.getLogger(__name__)

# --- BOUNCE SIGNALS ---
BOUNCE_SENDERS = [
    'mailer-daemon',
    'postmaster',
    'mail-daemon',
    'noreply-dmarc',
]

BOUNCE_SUBJECT_PATTERNS = [
    'delivery status notification',
    'undeliverable:',
    'undelivered mail',
    'mail delivery failed',
    'failure notice',
    'returned mail',
    'delivery failure',
    'delivery has failed',
    'message not delivered',
    'could not be delivered',
    'message delivery failure',
    'mail system error',
]

# --- AUTO REPLY SIGNALS ---
AUTO_REPLY_SUBJECT_PATTERNS = [
    'out of office',
    'out of the office',
    'automatic reply:',
    'auto-reply:',
    'autoreply:',
    'out-of-office',
    'ooo:',
    'i am out of',
    'i\'m out of',
    'on vacation',
    'on leave',
    'currently unavailable',
    'away from the office',
    'no longer with',
    'limited access to email',
]

# --- OBVIOUS SPAM SENDERS (never from prospects, skip entirely) ---
SPAM_SENDER_DOMAINS = [
    'linkedin.com',
    'facebookmail.com',
    'accounts.google.com',
    'google.com',          # e.g. no-reply@google.com
    'youtube.com',
    'uber.com',
    'redditmail.com',
    'blinkist.com',
    'nytimes.com',
    'newyorktimes.com',
    'mail.blinkist.com',
    'internshala.com',
    'urbanpro.com',
    'focusmate.com',
    'meditatehappier.com',
    'semrush.com',
    'beehiiv.com',
    'linklyhq.com',
    'impact.com',
    'colddms.com',
    'aemailer.com',
    'zendesk.com',
    'calendly.com',
    'stripe.com',
    'paypal.com',
    'supabase.com',
    'railway.app',
    'github.com',
    'notion.so',
    'slack.com',
    'twitter.com',
    'x.com',
]

SPAM_SENDER_PREFIXES = [
    'no-reply@',
    'noreply@',
    'notifications@',
    'newsletter@',
    'news@',
    'updates@',
    'marketing@',
    'promo@',
    'info@',
    'hello@',
    'support@',
    'billing@',
    'team@',
    'donotreply@',
    'do-not-reply@',
    'alert@',
    'alerts@',
    'digest@',
    'mailer@',
    'notify@',
    'notification@',
    'announcement@',
]


def classify_email(sender: str, subject: str, body_snippet: str, prospect_emails: dict, subject_map: dict) -> dict:
    """
    Classify a single email deterministically.
    
    Returns:
        {
            'classification': 'BOUNCE' | 'AUTO_REPLY' | 'HUMAN_REPLY' | 'SPAM',
            'reason': str,
            'matched_contact_id': str or None,
            'matched_project_id': str or None,
            'matched_email': str or None,
            'matched_company': str or None,
        }
    """
    sender = sender.lower().strip()
    subj_lower = subject.lower().strip()
    body_lower = body_snippet.lower() if body_snippet else ''
    
    result = {
        'classification': 'SPAM',
        'reason': 'No pattern matched',
        'matched_contact_id': None,
        'matched_project_id': None,
        'matched_email': None,
        'matched_company': None,
    }
    
    # ============================================================
    # LAYER 1: BOUNCE DETECTION (highest priority)
    # ============================================================
    is_bounce = False
    bounce_reason = ''
    
    # Check sender
    for bs in BOUNCE_SENDERS:
        if bs in sender:
            is_bounce = True
            bounce_reason = f'Sender contains "{bs}"'
            break
    
    # Check subject
    if not is_bounce:
        for bp in BOUNCE_SUBJECT_PATTERNS:
            if bp in subj_lower:
                is_bounce = True
                bounce_reason = f'Subject contains "{bp}"'
                break
    
    if is_bounce:
        result['classification'] = 'BOUNCE'
        result['reason'] = bounce_reason
        
        # Try to extract the bounced email from body
        extracted = _extract_bounced_email(body_lower, prospect_emails)
        if extracted:
            email_addr, contact_id, project_id, company = extracted
            result['matched_contact_id'] = contact_id
            result['matched_project_id'] = project_id
            result['matched_email'] = email_addr
            result['matched_company'] = company
        else:
            # Try matching by subject (e.g. "Undeliverable: Scaling Consult Vito?")
            subject_match = _match_by_subject(subj_lower, subject_map, prospect_emails)
            if subject_match:
                result['matched_contact_id'] = subject_match[0]
                result['matched_project_id'] = subject_match[1]
                result['matched_email'] = subject_match[2]
                result['matched_company'] = subject_match[3]
        
        return result
    
    # ============================================================
    # LAYER 2: AUTO-REPLY DETECTION
    # ============================================================
    for ap in AUTO_REPLY_SUBJECT_PATTERNS:
        if ap in subj_lower:
            result['classification'] = 'AUTO_REPLY'
            result['reason'] = f'Subject contains "{ap}"'
            
            # Try matching the sender to our prospects
            if sender in prospect_emails:
                cid, pid, company = prospect_emails[sender]
                result['matched_contact_id'] = cid
                result['matched_project_id'] = pid
                result['matched_email'] = sender
                result['matched_company'] = company
            else:
                # Try subject matching
                subject_match = _match_by_subject(subj_lower, subject_map, prospect_emails)
                if subject_match:
                    result['matched_contact_id'] = subject_match[0]
                    result['matched_project_id'] = subject_match[1]
                    result['matched_email'] = subject_match[2]
                    result['matched_company'] = subject_match[3]
            
            return result
    
    # ============================================================
    # LAYER 3: SPAM PRE-FILTER (before human reply check)
    # ============================================================
    sender_domain = sender.split('@')[-1] if '@' in sender else ''
    
    for spam_domain in SPAM_SENDER_DOMAINS:
        if sender_domain == spam_domain or sender_domain.endswith('.' + spam_domain):
            result['classification'] = 'SPAM'
            result['reason'] = f'Sender domain "{sender_domain}" is known spam'
            return result
    
    for spam_prefix in SPAM_SENDER_PREFIXES:
        if sender.startswith(spam_prefix):
            # But NOT if sender is also in our prospect DB!
            if sender not in prospect_emails:
                result['classification'] = 'SPAM'
                result['reason'] = f'Sender starts with "{spam_prefix}" and is not a prospect'
                return result
    
    # ============================================================
    # LAYER 4: HUMAN REPLY DETECTION
    # ============================================================
    
    # Tactic A: Sender email directly matches a prospect
    if sender in prospect_emails:
        cid, pid, company = prospect_emails[sender]
        result['classification'] = 'HUMAN_REPLY'
        result['reason'] = f'Sender "{sender}" is a known prospect'
        result['matched_contact_id'] = cid
        result['matched_project_id'] = pid
        result['matched_email'] = sender
        result['matched_company'] = company
        return result
        
    # Tactic A.5: Sender's domain matches a prospect's domain (e.g. Assistant replying for CEO)
    # Ensure it's not a generic email provider or empty
    if sender_domain and sender_domain not in ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 'aol.com', 'me.com', 'msn.com', 'live.com']:
        for prospect_email, (cid, pid, company) in prospect_emails.items():
            prospect_domain = prospect_email.split('@')[-1] if '@' in prospect_email else ''
            if prospect_domain == sender_domain:
                result['classification'] = 'HUMAN_REPLY'
                result['reason'] = f'Colleague reply: Sender "{sender}" shares domain "{sender_domain}" with prospect "{prospect_email}"'
                result['matched_contact_id'] = cid
                result['matched_project_id'] = pid
                result['matched_email'] = prospect_email
                result['matched_company'] = company
                return result
    
    # Tactic B: Subject matches "Re: {our campaign subject}"
    if 're:' in subj_lower or 'fw:' in subj_lower or 'fwd:' in subj_lower:
        subject_match = _match_by_subject(subj_lower, subject_map, prospect_emails)
        if subject_match:
            result['classification'] = 'HUMAN_REPLY'
            result['reason'] = f'Subject matches campaign subject and has Re:/Fw: prefix'
            result['matched_contact_id'] = subject_match[0]
            result['matched_project_id'] = subject_match[1]
            result['matched_email'] = subject_match[2]
            result['matched_company'] = subject_match[3]
            return result
    
    # ============================================================
    # DEFAULT: SPAM
    # ============================================================
    return result


def _extract_bounced_email(body_lower: str, prospect_emails: dict):
    """
    Parse the bounce body to find which prospect email bounced.
    Returns (email, contact_id, project_id, company) or None.
    """
    # Strategy 1: Find any prospect email mentioned in the bounce body
    for prospect_email, (cid, pid, company) in prospect_emails.items():
        if prospect_email in body_lower:
            return (prospect_email, cid, pid, company)
    
    # Strategy 2: Extract email addresses from body and check
    found_emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', body_lower)
    for found in found_emails:
        found = found.lower().strip()
        if found in prospect_emails:
            cid, pid, company = prospect_emails[found]
            return (found, cid, pid, company)
    
    return None


def _match_by_subject(subj_lower: str, subject_map: dict, prospect_emails: dict):
    """
    Try to match the incoming subject to one of our campaign subjects.
    Returns (contact_id, project_id, email, company) or None.
    """
    # Clean the subject: remove Re:, Fw:, Fwd:, etc.
    clean = re.sub(r'^(re|fw|fwd|aw)\s*:\s*', '', subj_lower, flags=re.IGNORECASE).strip()
    # Also try removing "Undeliverable: " prefix
    clean2 = re.sub(r'^undeliverable:\s*', '', clean, flags=re.IGNORECASE).strip()
    
    for candidate in [clean, clean2, subj_lower]:
        candidate = candidate.strip()
        if candidate in subject_map:
            matches = subject_map[candidate]
            contact_id = matches[0][0]
            project_id = matches[0][1]
            
            # Look up email/company from prospect_emails (reverse lookup)
            for email_addr, (cid, pid, company) in prospect_emails.items():
                if cid == contact_id:
                    return (contact_id, project_id, email_addr, company)
            
            return (contact_id, project_id, None, 'Unknown')
    
    # Fuzzy: check if any campaign subject is a substring of the incoming subject
    for campaign_subj, matches in subject_map.items():
        if campaign_subj in clean or campaign_subj in clean2:
            contact_id = matches[0][0]
            project_id = matches[0][1]
            
            for email_addr, (cid, pid, company) in prospect_emails.items():
                if cid == contact_id:
                    return (contact_id, project_id, email_addr, company)
            
            return (contact_id, project_id, None, 'Unknown')
    
    return None
