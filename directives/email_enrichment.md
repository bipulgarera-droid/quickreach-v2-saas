# Email Enrichment — SOP

## Goal
Find email addresses and Instagram handles for discovered contacts.

## Inputs
- Contacts in Supabase with `status='new'`

## Tools/Scripts
- `execution/enrich_contacts.py` — Finds emails via Hunter.io/Serper, IG via Serper

## Steps
1. Ensure API keys are set in `.env`: `SERPER_API_KEY`, optionally `HUNTER_API_KEY`
2. Run via dashboard: **Dashboard → Quick Actions → Enrich Contacts**
3. Or via CLI: `python -m execution.enrich_contacts --limit 50`
4. Monitor progress in logs
5. Check results in dashboard **Contacts → Enriched** tab

## Email Discovery Priority
1. **Hunter.io** (if key available): Most accurate, finds professional emails
2. **Serper fallback**: Searches `"name" email contact` — less accurate but free(ish)

## Instagram Discovery
- Searches `"name" site:instagram.com` via Serper
- Extracts handle from URL, filters generic pages

## Rate Limits
- Hunter.io: 25 free searches/month, then paid
- Serper: 2 requests per contact (email + IG search)
- Built-in: 1 second delay between contacts

## Output
- Contact updated with `email`, `instagram`, `enrichment_data`, `status='enriched'`
