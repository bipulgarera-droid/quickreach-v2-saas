"""
smtp_pool.py — SMTP Connection Pool with Account Rotation

Manages multiple Gmail accounts for round-robin email sending.
Tracks per-account hourly and daily send counts to stay within limits.
Adapted for the Film Festival Outreach App.
"""

import os
import time
import base64
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import json
import threading
from typing import Optional
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning("Supabase credentials missing. Daily stats tracking may fail.")
    supabase: Optional[Client] = None
else:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
MAX_PER_DAY = int(os.getenv("MAX_PER_ACCOUNT_PER_DAY", 30))
MAX_PER_HOUR = int(os.getenv("MAX_PER_ACCOUNT_PER_HOUR", 20))

def get_today_str() -> str:
    return datetime.now().strftime('%Y-%m-%d')



class GmailAccount:
    """Represents a single Gmail account with send tracking."""
    
    def __init__(self, email: str, refresh_token: str, daily_limit: int = 30, hourly_limit: int = 20, group: str = "all"):
        self.email = email
        self.refresh_token = refresh_token
        self.daily_limit = daily_limit
        self.hourly_limit = hourly_limit
        self.group = group
        self.disabled = False
        self._sends_today_cache = 0
        self._sends_hour_cache = 0
        self._last_cache_update = None
        self.credentials = self._build_credentials()
        self.service = build('gmail', 'v1', credentials=self.credentials, cache_discovery=False)
        
    def _build_credentials(self) -> Credentials:
        client_id = os.getenv("GMAIL_CLIENT_ID")
        client_secret = os.getenv("GMAIL_CLIENT_SECRET")
        if not client_id or not client_secret:
            logger.error("Missing GMAIL_CLIENT_ID or GMAIL_CLIENT_SECRET in environment!")
            
        return Credentials(
            token=None,
            refresh_token=self.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret
        )
        
    def _fetch_sends_rolling_24h(self) -> int:
        """Count sends in the last rolling 24-hour window using smtp_send_logs."""
        if not supabase: return 0
        since = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        try:
            res = supabase.table('smtp_send_logs') \
                .select('id', count='exact') \
                .eq('email_address', self.email) \
                .gte('created_at', since) \
                .execute()
            return res.count or 0
        except Exception as e:
            logger.error(f"Error fetching rolling stats for {self.email}: {e}")
            return 0

    def _fetch_sends_rolling_1h(self) -> int:
        """Count sends in the last rolling 1-hour window."""
        if not supabase: return 0
        since = (datetime.utcnow() - timedelta(hours=1)).isoformat()
        try:
            res = supabase.table('smtp_send_logs') \
                .select('id', count='exact') \
                .eq('email_address', self.email) \
                .gte('created_at', since) \
                .execute()
            return res.count or 0
        except Exception as e:
            logger.error(f"Error fetching hourly stats for {self.email}: {e}")
            return 0

    @property
    def sends_today(self) -> int:
        """Current usage in the rolling 24h window."""
        now = datetime.now()
        last_update = self._last_cache_update
        if last_update is None or (now - last_update).total_seconds() > 60:
            self._update_stats_cache()
        return self._sends_today_cache

    @property
    def sends_hour(self) -> int:
        """Current usage in the rolling 1h window."""
        now = datetime.now()
        last_update = self._last_cache_update
        if last_update is None or (now - last_update).total_seconds() > 60:
            self._update_stats_cache()
        return self._sends_hour_cache

    def _update_stats_cache(self):
        """Fetch both hourly and daily stats from Supabase."""
        self._sends_today_cache = self._fetch_sends_rolling_24h()
        self._sends_hour_cache = self._fetch_sends_rolling_1h()
        self._last_cache_update = datetime.now()

    @property
    def can_send(self) -> bool:
        return (not self.disabled and self.sends_today < self.daily_limit and self.sends_hour < self.hourly_limit)

    def record_send(self):
        """Record a new send in both the high-precision log and the daily aggregate."""
        self._sends_today_cache += 1
        self._last_cache_update = datetime.now()
        
        if not supabase: return
        today = get_today_str()
        
        try:
            # 1. Insert high-precision rolling window log
            supabase.table('smtp_send_logs').insert({
                'email_address': self.email
            }).execute()

            # 2. Update daily aggregate for dashboard/history
            res = supabase.table('smtp_daily_stats').select('id, sent_count').eq('email_address', self.email).eq('date', today).execute()
            if res.data:
                new_count = res.data[0]['sent_count'] + 1
                supabase.table('smtp_daily_stats').update({
                    'sent_count': new_count, 
                    'updated_at': datetime.now().isoformat()
                }).eq('id', res.data[0]['id']).execute()
            else:
                supabase.table('smtp_daily_stats').insert({
                    'email_address': self.email,
                    'date': today,
                    'sent_count': 1
                }).execute()
        except Exception as e:
            logger.error(f"Failed to record send for {self.email}: {e}")


class SMTPPool:
    """Round-robin SMTP pool across multiple Gmail accounts."""

    # Global send gate — shared across ALL threads/projects
    _send_lock = threading.Lock()
    _last_send_time: Optional[float] = None

    def __init__(self, accounts_data: list[dict]):
        if not accounts_data:
            logger.warning("[SMTP Pool] Initialized with empty accounts_data.")
            self.accounts = []
        else:
            self.accounts = [
                GmailAccount(
                    a.get("email_address") or a.get("email"), 
                    a["refresh_token"], 
                    a.get("daily_limit", 30), 
                    a.get("hourly_limit", 20), 
                    a.get("group", "all")
                ) for a in accounts_data
            ]
            
        self._index = 0
        logger.info(f"[SMTP Pool] Loaded {len(self.accounts)} accounts.")

    def get_total_usage(self, sender_group: str = "all") -> int:
        """Sum of all sends today across all applicable accounts in the pool."""
        usable_accounts = [
            a for a in self.accounts 
            if sender_group == "all" or a.group == "all" or a.group == sender_group
        ]
        total = 0
        for account in usable_accounts:
            # can_send property triggers the cache update
            _ = account.can_send 
            total += account._sends_today_cache
        return total

    def get_total_limit(self, sender_group: str = "all") -> int:
        """Total daily limit across all applicable accounts."""
        usable_accounts = [
            a for a in self.accounts 
            if sender_group == "all" or a.group == "all" or a.group == sender_group
        ]
        return sum(a.daily_limit for a in usable_accounts)

    def get_account_by_email(self, email: str) -> Optional[GmailAccount]:
        """Find a specific account in the pool by its email address."""
        for a in self.accounts:
            if a.email.lower() == email.lower():
                return a
        return None

    def get_next_account(self, sender_group: str = "all") -> Optional[GmailAccount]:
        """Get the next available account via round-robin, filtered by sender_group."""
        # Find accounts that either belong to the requested group OR if group is 'all', use all accounts.
        # Likewise, if an account's group is 'all', it can send for any project.
        usable_accounts = [
            a for a in self.accounts 
            if sender_group == "all" or a.group == "all" or a.group == sender_group
        ]
        
        if not usable_accounts:
            return None
            
        checked = 0
        while checked < len(usable_accounts):
            # Ensure index wraps within usable_accounts length
            idx = self._index % len(usable_accounts)
            account = usable_accounts[idx]
            self._index += 1
            if account.can_send:
                return account
            checked += 1
        return None  # all usable accounts exhausted

    def send_email(self, account: GmailAccount, to_addr: str, subject: str, body_html: str, dry_run: bool = False, delay_min: Optional[int] = None, delay_max: Optional[int] = None, sender_name: Optional[str] = None, thread_id: Optional[str] = None) -> dict:
        """Send an HTML email via SMTP from the given account.
        
        Enforces a global inter-send delay shared across all concurrent threads,
        so multiple project sends don't race past each other's delays.
        """
        import random
        d_min = delay_min if delay_min is not None else int(os.getenv('DELAY_MIN_SECONDS', 45))
        d_max = delay_max if delay_max is not None else int(os.getenv('DELAY_MAX_SECONDS', 90))

        with SMTPPool._send_lock:
            # Enforce global inter-send delay
            now = time.monotonic()
            last_send = SMTPPool._last_send_time
            if last_send is not None:
                elapsed = now - last_send
                wait = random.uniform(d_min, d_max)
                if elapsed < wait:
                    sleep_for = wait - elapsed
                    logger.info(f"[Global rate] Waiting {sleep_for:.1f}s before next send (global cadence)")
                    time.sleep(sleep_for)


            if dry_run:
                logger.info(f"[DRY RUN] Would send to {to_addr} from {account.email}")
                account.record_send()
                # Update global last send time AFTER processing completes
                SMTPPool._last_send_time = time.monotonic()
                return {"success": True, "error": None}

            try:
                msg = MIMEMultipart("alternative")
                msg["From"] = f"{sender_name} <{account.email}>" if sender_name else account.email
                msg["To"] = to_addr
                msg["Subject"] = subject

                # Plain text version: strip any HTML tags, keep newlines
                body_plain = body_html.replace('<br>', '\n').replace('<br/>', '\n').replace('<p>', '').replace('</p>', '\n')

                # HTML version: convert plain newlines to <br> so they render
                body_formatted = body_html.replace('\n', '<br>\n')

                msg.attach(MIMEText(body_plain, "plain"))
                msg.attach(MIMEText(body_formatted, "html"))

                # Encode for Gmail API
                raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()

                try:
                    send_body = {'raw': raw_msg}
                    if thread_id:
                        send_body['threadId'] = thread_id
                        
                    account.service.users().messages().send(
                        userId='me', 
                        body=send_body
                    ).execute()
                except Exception as e:
                    error_msg = f"Gmail API error for {account.email}: {e}"
                    logger.error(error_msg)
                    if "invalid_grant" in str(e) or "auth" in str(e).lower() or "credentials" in str(e).lower() or "revoked" in str(e).lower():
                        account.disabled = True
                        logger.error(f"[!] Refresh token failed for {account.email}. Account disabled.")
                    return {"success": False, "error": str(e)}

                logger.info(f"Email sent via Gmail API to {to_addr} from {account.email}")
                account.record_send()
                # IMPORTANT: Only update last_send_time AFTER successful transmission.
                # This ensures the inter-send delay (cadence) is measured from 
                # completion to next start, preventing burst behavior.
                SMTPPool._last_send_time = time.monotonic()
                return {"success": True, "error": None}

            except Exception as e:
                error_msg = f"Unexpected error for {account.email}: {e}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}

    def get_status(self) -> dict:
        """Get dict of pool status."""
        stats = {}
        for acc in self.accounts:
            status = "DISABLED" if acc.disabled else ("OK" if acc.can_send else "LIMIT HIT")
            stats[acc.email] = {
                'status': status,
                'sends_today': acc.sends_today,
                'sends_hour': acc.sends_hour,
                'max_per_day': acc.daily_limit,
                'max_per_hour': acc.hourly_limit
            }
        return stats
