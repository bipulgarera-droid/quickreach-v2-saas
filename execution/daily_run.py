#!/usr/bin/env python3
"""
Daily Run — One-Shot Local Email Sender + Reply Checker.

Run this once a day from your laptop. It will:
1. Check all Gmail inboxes for prospect replies (auto-stops their sequences)
2. Send all pending emails where scheduled_at <= now()
3. Print a summary of what happened

Usage:
    python -m execution.daily_run
    python -m execution.daily_run --limit 250 --delay-min 20 --delay-max 40
    python -m execution.daily_run --dry-run
"""

import os
import sys
import json
import argparse
import logging
import time
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def daily_run(limit: int = 600, dry_run: bool = False, delay_min: int = 20, delay_max: int = 40, project_id: str = None) -> dict:
    """
    Execute the full daily workflow:
    1. Check for replies (IMAP)
    2. Send pending emails (SMTP)
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"  DAILY RUN — {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"  Limit: {limit} emails | Delay: {delay_min}-{delay_max}s")
    logger.info("=" * 60)

    results = {
        'started_at': start_time.isoformat(),
        'reply_check': {},
        'email_send': {},
    }

    # ── Step 1: Check for Replies ──────────────────────────────────
    logger.info("")
    logger.info("📬 STEP 1: Checking for prospect replies...")
    logger.info("-" * 40)
    try:
        from execution.check_replies import check_all_replies
        reply_stats = check_all_replies(days=7)
        results['reply_check'] = reply_stats

        replies_found = reply_stats.get('replies_found', 0)
        bounces_found = reply_stats.get('bounces_found', 0)
        if replies_found > 0 or bounces_found > 0:
            logger.info(f"🛑 Found {replies_found} reply(ies) and {bounces_found} bounce(s). Sequences auto-stopped.")
        else:
            logger.info("✅ No new replies or bounces detected.")
    except Exception as e:
        logger.error(f"❌ Reply check failed: {e}")
        results['reply_check'] = {'error': str(e)}

    # ── Step 2: Send Pending Emails ────────────────────────────────
    logger.info("")
    logger.info("📤 STEP 2: Sending pending emails...")
    logger.info("-" * 40)
    try:
        # Override the delay env vars for this session
        os.environ['DELAY_MIN_SECONDS'] = str(delay_min)
        os.environ['DELAY_MAX_SECONDS'] = str(delay_max)

        from execution.send_emails import send_pending_emails
        send_stats = send_pending_emails(limit=limit, dry_run=dry_run, project_id=project_id, skip_reply_check=True)
        results['email_send'] = send_stats

        sent = send_stats.get('sent', 0)
        errors = send_stats.get('errors', 0)
        skipped = send_stats.get('skipped', 0)
        logger.info(f"📊 Sent: {sent} | Errors: {errors} | Skipped: {skipped}")
    except Exception as e:
        logger.error(f"❌ Email send failed: {e}")
        results['email_send'] = {'error': str(e)}

    # ── Summary ────────────────────────────────────────────────────
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    results['duration_seconds'] = round(duration, 1)
    results['finished_at'] = end_time.isoformat()

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  DAILY RUN COMPLETE — {duration:.0f}s elapsed")
    logger.info(f"  Replies found: {results['reply_check'].get('replies_found', 'N/A')}")
    logger.info(f"  Bounces found: {results['reply_check'].get('bounces_found', 'N/A')}")
    logger.info(f"  Emails sent: {results['email_send'].get('sent', 'N/A')}")
    logger.info("=" * 60)

    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Daily local email sender + reply checker'
    )
    parser.add_argument('--limit', type=int, default=600,
                        help='Max emails to send (default: 600)')
    parser.add_argument('--delay-min', type=int, default=20,
                        help='Min seconds between emails (default: 20)')
    parser.add_argument('--delay-max', type=int, default=40,
                        help='Max seconds between emails (default: 40)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without actually sending')
    parser.add_argument('--project-id', type=str,
                        help='Restrict sending to a specific project ID')

    args = parser.parse_args()

    dry_run = args.dry_run or str(os.getenv('DRY_RUN', 'false')).lower() == 'true'

    stats = daily_run(
        limit=args.limit,
        dry_run=dry_run,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        project_id=args.project_id
    )
    print("\n" + json.dumps(stats, indent=2))
