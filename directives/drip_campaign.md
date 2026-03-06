# Drip Campaign — SOP

## Goal
Run a personalized 12-email drip sequence to film critics and festival programmers.

## Inputs
- Contacts with `status='icebreaker_ready'`
- 12 email templates seeded in `film_email_templates`
- Gmail OAuth credentials configured

## Tools/Scripts
- `execution/generate_icebreakers.py` — Perplexity API icebreaker generation
- `execution/send_emails.py` — Gmail API sender
- Dashboard UI for management

## Steps

### 1. Generate Icebreakers
- Dashboard: **Quick Actions → Gen Icebreakers** or CLI: `python -m execution.generate_icebreakers --limit 50`
- Sets contacts to `status='icebreaker_ready'`

### 2. Seed Templates (one-time)
- Dashboard: **Templates tab → Seed 12-Step Templates**
- Or call `POST /api/seed-templates`

### 3. Create Sequences
- Dashboard: **Sequences tab → Create Sequences** (auto-selects icebreaker_ready contacts)
- Creates 12 email records per contact with scheduled dates

### 4. Send Emails
- Dashboard: **Quick Actions → Send Emails**
- Or CLI: `python -m execution.send_emails --limit 50`
- Sends emails where `scheduled_at <= now()` and `status='pending'`

### 5. Track Performance
- Dashboard: **Dashboard → Email Performance** bars
- Dashboard: **Pipeline** tab for funnel view

## 12-Step Sequence
| Step | Name | Delay |
|------|------|-------|
| 1 | Logline Introduction | Day 0 |
| 2 | Trailer Share | Day 3 |
| 3 | Behind the Scenes | Day 5 |
| 4 | Director's Vision | Day 7 |
| 5 | Press Kit & Stills | Day 10 |
| 6 | Festival Selections | Day 14 |
| 7 | Review Request | Day 18 |
| 8 | Exclusive Clip | Day 22 |
| 9 | Audience Reaction | Day 26 |
| 10 | Screening Invite | Day 30 |
| 11 | Final Nudge | Day 35 |
| 12 | Thank You + Video | Day 40 |

## Gmail Limits
- 500 emails/day for regular accounts
- 2000/day for Google Workspace
- Built-in 2-second delay between sends

## Edge Cases
- Contact has no email → sequence skipped
- Gmail token expired → re-auth required (run script to refresh)
- Bounced emails → status set to `bounced`, excluded from future sends
