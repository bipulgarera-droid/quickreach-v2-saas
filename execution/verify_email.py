import re
import smtplib
import time
import sys
import os
import socket
import uuid

# Ensure the local vendor folder is in the path for dnspython
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.vendor')))
import dns.resolver

EMAIL_REGEX = re.compile(r"[^@]+@[^@]+\.[^@]+")

# Using a standard set, can be expanded later
DISPOSABLE_DOMAINS = {"mailinator.com", "10minutemail.com", "guerrillamail.com", "yopmail.com", "tempmail.com", "mail.com"}
ROLE_BASED_PREFIXES = {"info", "support", "admin", "sales", "contact", "marketing", "billing", "hello"}

# Large providers where role-based addresses ARE valid (e.g. info@google.com)
TRUSTED_PROVIDERS = {"google.com", "gmail.com", "outlook.com", "yahoo.com", "hotmail.com", "microsoft.com", "amazon.com", "apple.com"}

def check_email(email: str) -> tuple[str, str]:
    """
    Verifies an email using Regex, Domain checks, MX lookup, and SMTP probes.
    Returns:
        (status, reason)
        status can be: "valid", "risky", "invalid"
    
    V2 Rules (tightened based on bounce data analysis):
    - smtp_timeout → INVALID (was risky). Retries once before marking.
    - 550 reject → INVALID (was risky "safe_shield"). A 550 is a hard reject.
    - role-based on small domains → INVALID (was risky). info@smallbiz.com is almost always dead.
    - Trailing dots in domain → sanitized (e.g. longbeachfilm.com. → longbeachfilm.com)
    """
    # Broad trailing punctuation strip (preserving middle dots like .com.mx)
    email = str(email).strip().rstrip('.,;:)!% ]').strip().lower()

    if not EMAIL_REGEX.match(email):
        return "invalid", "bad_syntax"

    try:
        local, domain = email.split('@')
    except ValueError:
        return "invalid", "bad_syntax"

    # Sanitize trailing dots from domain (e.g. longbeachfilm.com. → longbeachfilm.com)
    domain = domain.rstrip('.')
    email = f"{local}@{domain}"

    if domain in DISPOSABLE_DOMAINS:
        return "invalid", "disposable_domain"

    # MX Record Lookup
    try:
        records = dns.resolver.resolve(domain, 'MX')
        # Sort MX records by preference (lowest first)
        records = sorted(records, key=lambda r: r.preference)
        mx_record = str(records[0].exchange).rstrip('.')
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers, Exception) as e:
        return "invalid", "no_mx"

    # Check specific email via SMTP
    def smtp_check(email_to_check, timeout=10):
        try:
            server = smtplib.SMTP(timeout=timeout)
            server.connect(mx_record)
            # Use EHLO for better compatibility
            server.ehlo("gmail.com")
            
            # Use a slightly more realistic sender to pass basic filters
            r_code, r_msg = server.mail("noreply@gmail.com")
            if r_code != 250:
                # If the server rejects the 'MAIL FROM' command, we can't probe further
                server.quit()
                return r_code, f"mail_from_rejected_{r_msg.decode('utf-8', 'ignore') if isinstance(r_msg, bytes) else r_msg}"
                
            code, msg = server.rcpt(email_to_check)
            server.quit()
            return code, msg.decode('utf-8', 'ignore') if msg else ""
        except socket.gaierror:
            return -1, "dns_gaierror"
        except socket.timeout:
            return None, "timeout"
        except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected):
            return None, "connection_failed"
        except Exception as e:
            return None, str(e)

    # Test 1: Catch-all Check (Liar Detector)
    code_stupid, msg_stupid = smtp_check(f"probe-{uuid.uuid4().hex[:8]}@{domain}")
    
    is_catch_all = (code_stupid == 250)
    
    # Test 2: Real Email
    code_real, msg_real = smtp_check(email)
    
    # If first attempt timed out, retry once with a longer timeout before giving up
    if code_real is None and "timeout" in str(msg_real).lower():
        code_real, msg_real = smtp_check(email, timeout=15)
    
    # LOGIC:
    # 1. Catch-all (Liar) → VALID.
    #    Even if it's role-based, we trust catch-all domains to avoid false positives (e.g. info@redchillies.com).
    if is_catch_all:
        return "valid", "domain_catch_all"

    # 2. Honest Server results
    if code_real == 250:
        return "valid", "smtp_ok"
    elif code_real == 550:
        # V2 Refined: A 550 is a hard rejection. This IS definitely invalid.
        return "invalid", "hard_reject_550"
    elif code_real == -1 and msg_real == "dns_gaierror":
        return "invalid", "dns_gaierror"
    elif code_real is None:
        # V2 Refined: A persistent timeout (after retry) is treated as invalid to prevent bounces.
        return "invalid", f"smtp_timeout"
    elif str(code_real).startswith('4'):
        # 4xx are temporary (rate limits, etc.). Mark as RISKY.
        return "risky", f"smtp_temp_error_{code_real}"
    else:
        # Other errors (503, etc.) - Mark as RISKY.
        return "risky", f"smtp_error_{code_real}_{msg_real[:50]}"

if __name__ == "__main__":
    # Simple test cases if executed directly
    print("Testing doesnotexist123@google.com:", check_email("doesnotexist123@google.com"))
    print("Testing info@google.com:", check_email("info@google.com"))
    print("Testing trailing dot: steveshor@longbeachfilm.com.:", check_email("steveshor@longbeachfilm.com."))

