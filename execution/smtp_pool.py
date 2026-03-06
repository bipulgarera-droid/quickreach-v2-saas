"""
smtp_pool.py — SMTP Connection Pool with Account Rotation

Manages multiple Gmail accounts for round-robin email sending.
Tracks per-account hourly and daily send counts to stay within limits.
Adapted for the Film Festival Outreach App.
"""

import os
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
MAX_PER_DAY = int(os.getenv("MAX_PER_ACCOUNT_PER_DAY", 150))
MAX_PER_HOUR = int(os.getenv("MAX_PER_ACCOUNT_PER_HOUR", 20))


def _load_accounts_from_env() -> list[dict]:
    """Load Gmail accounts from .env using GMAIL_N_EMAIL format."""
    accounts = []
    for i in range(1, 20):
        email = os.getenv(f"GMAIL_{i}_EMAIL")
        password = os.getenv(f"GMAIL_{i}_PASSWORD")
        if not email or not password:
            continue
        accounts.append({"email": email.strip(), "app_password": password.strip()})
    return accounts


class GmailAccount:
    """Represents a single Gmail account with send tracking."""
    def __init__(self, email: str, app_password: str):
        self.email = email
        self.app_password = app_password
        self.disabled = False
        self.send_log: list[datetime] = []

    @property
    def sends_today(self) -> int:
        cutoff = datetime.now() - timedelta(hours=24)
        return sum(1 for t in self.send_log if t > cutoff)

    @property
    def sends_this_hour(self) -> int:
        cutoff = datetime.now() - timedelta(hours=1)
        return sum(1 for t in self.send_log if t > cutoff)

    @property
    def can_send(self) -> bool:
        return (not self.disabled and 
                self.sends_today < MAX_PER_DAY and 
                self.sends_this_hour < MAX_PER_HOUR)

    def record_send(self):
        self.send_log.append(datetime.now())


class SMTPPool:
    """Round-robin SMTP pool across multiple Gmail accounts."""
    def __init__(self):
        accounts_data = _load_accounts_from_env()
        if not accounts_data:
            raise ValueError("No Gmail accounts found. Add GMAIL_1_EMAIL to .env")
            
        self.accounts = [GmailAccount(a["email"], a["app_password"]) for a in accounts_data]
        self._index = 0
        logger.info(f"[SMTP Pool] Loaded {len(self.accounts)} accounts.")

    def get_next_account(self) -> Optional[GmailAccount]:
        """Get the next available account via round-robin."""
        checked = 0
        while checked < len(self.accounts):
            account = self.accounts[self._index]
            self._index = (self._index + 1) % len(self.accounts)
            if account.can_send:
                return account
            checked += 1
        return None  # all accounts exhausted

    def send_email(self, account: GmailAccount, to_addr: str, subject: str, body_html: str, dry_run: bool = False) -> dict:
        """Send an HTML email via SMTP from the given account."""
        if dry_run:
            logger.info(f"[DRY RUN] Would send to {to_addr} from {account.email}")
            account.record_send()
            return {"success": True, "error": None}

        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = account.email
            msg["To"] = to_addr
            msg["Subject"] = subject
            
            # Create a simple plain text representation from HTML
            body_plain = body_html.replace('<br>', '\n').replace('<p>', '').replace('</p>', '\n')
            
            msg.attach(MIMEText(body_plain, "plain"))
            msg.attach(MIMEText(body_html, "html"))

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(account.email, account.app_password)
                server.sendmail(account.email, to_addr, msg.as_string())

            logger.info(f"Email sent to {to_addr} from {account.email}")
            account.record_send()
            return {"success": True, "error": None}

        except smtplib.SMTPAuthenticationError as e:
            account.disabled = True
            error_msg = f"Auth failed for {account.email} - account disabled: {e}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"SMTP error for {account.email}: {e}"
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
                'sends_this_hour': acc.sends_this_hour
            }
        return stats
