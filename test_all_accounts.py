import os
import sys
# Set delays to 0 temporarily via env
os.environ['DELAY_MIN_SECONDS'] = '0'
os.environ['DELAY_MAX_SECONDS'] = '0'

from dotenv import load_dotenv
load_dotenv('/Users/bipul/Downloads/ALL WORKSPACES/festivals outreach/.env')
from execution.smtp_pool import SMTPPool

pool = SMTPPool()

print(f"Testing {len(pool.accounts)} accounts...")
success_count = 0
for acc in pool.accounts:
    print(f"Testing {acc.email}...")
    res = pool.send_email(
        account=acc,
        to_addr=acc.email,
        subject="Test SMTP Connection",
        body_html="<p>This is a test to verify the refresh token.</p>",
        dry_run=False,
        delay_min=0,
        delay_max=0,
        sender_name="System Test"
    )
    if res['success']:
        print(f"✅ {acc.email} is working perfectly!")
        success_count += 1
    else:
        print(f"❌ {acc.email} FAILED: {res.get('error')}")

print(f"\nCompleted! {success_count}/{len(pool.accounts)} accounts are working.")
