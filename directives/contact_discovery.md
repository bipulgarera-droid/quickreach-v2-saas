# Contact Discovery — SOP

## Goal
Discover 1000+ film critics, festival programmers, and industry contacts via Google search using Serper API.

## Inputs
- Search queries (e.g., `"programmer film festival site:linkedin.com"`, `"film critic India"`)
- Number of results per query (default: 100)

## Tools/Scripts
- `execution/serper_search.py` — Runs Google searches via Serper API
- `execution/scrape_contacts.py` — Extracts contacts from search results

## Steps
1. Prepare 10-20 targeted search queries covering different angles:
   - `"film festival programmer" site:linkedin.com`
   - `"film critic" India`
   - `"independent film reviewer" email`
   - `"film festival curator" site:linkedin.com`
   - `"movie critic" site:linkedin.com`
2. Run via dashboard: **Search tab → Enter queries → Run Search**
3. Or via CLI: `python -m execution.serper_search --queries "query1" "query2" --output .tmp/results.json`
4. Then: `python -m execution.scrape_contacts --input .tmp/results.json`
5. Verify contacts in dashboard **Contacts tab**

## Edge Cases
- Serper API rate limits: Max ~100 results per query, throttle if you get 429 errors
- LinkedIn profile parsing: Some titles have company names, the parser handles this
- Deduplication: By LinkedIn URL and name (case-insensitive)

## Output
- Contacts stored in Supabase `film_contacts` table with status `new`
- Search runs logged in `film_search_runs`

## Learned
- Serper returns max 100 organic results per query
- LinkedIn URLs ending in `/in/` are personal profiles
- Vary search queries for maximum coverage (different roles, regions, languages)
