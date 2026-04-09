# TASK: Build Email Accounts UI + SMTP Pool Refactor

> **READ EVERY LINE BEFORE DOING ANYTHING. DO NOT SKIP. DO NOT SUGGEST ALTERNATIVES.**

## Context: What Already Exists

This is NOT a new project. This is a fully working application.

**Already done (DO NOT REDO):**
- `public/login.html` — Supabase Auth signup/login with `supabase.auth.signUp()` and `signInWithPassword()`. DONE.
- `public/dashboard.html` — 5400+ line dashboard with auth guard, JWT injection in `apiFetch()`, logout function. DONE.
- `api/index.py` — Flask API with `@app.before_request` JWT verification, `user_id` scoping on all queries, `get_user_project_ids()` helper. DONE.
- Database schema — `users`, `user_email_accounts`, `projects`, `contacts`, `email_sequences`, `replies`, `email_templates`, `search_runs`, `smtp_daily_stats`, `smtp_send_logs`, `job_logs`, `job_events`, `project_knowledge_base` tables all exist with RLS. DONE.

---

## YOUR ONLY TWO TASKS

### TASK 1: Build Google OAuth "Connect Account" Flow

Currently, V1 users manually pasted massive `REFRESH_TOKEN` strings into `.env`. For the SaaS, we need an actual "Sign in with Google" flow that grabs the refresh token and saves it to the DB.

1. **Add an "Email Accounts" tab to `dashboard.html`** (or add it into Settings)
2. **Build a "Connect Gmail" button:**
   - Create a new Flask route in `api/index.py` at `GET /api/auth/google-connect` that redirects the user to the Google OAuth consent screen using `GMAIL_CLIENT_ID` from the environment. Request the `https://www.googleapis.com/auth/gmail.send` score.
   - You MUST pass the `user_id` in the `state` parameter of the OAuth url so the callback knows who to attach it to.
   - Create the callback route at `GET /api/auth/google/callback`. This route exchanges the auth code for a `refresh_token` using `google-auth-oauthlib`.
   - Insert a new row into `public.user_email_accounts` with the `user_id`, `email_address`, and `refresh_token`.
3. **List connected accounts in the dashboard UI** via a GET `/api/email-accounts` endpoint. Include a delete button.

### TASK 2: Refactor `execution/smtp_pool.py` to KEEP the Gmail API

> **CRITICAL: DO NOT REWRITE THIS SCRIPT TO USE `smtplib`. The app MUST run over HTTPS (Port 443) using `googleapiclient.discovery` to bypass cloud port-blockers.**

Currently, `smtp_pool.py` has a function `_load_accounts_from_env()` that reads `GMAIL_1_EMAIL` and `GMAIL_1_REFRESH_TOKEN` from `.env`.

**Change it to:**
1. Leave the `GmailAccount` class completely untouched. It already perfectly uses `Credentials(refresh_token=...)` and `googleapiclient`.
2. Rewrite `__init__` or `_load_accounts_from_env` to query `user_email_accounts` table for all active accounts where `is_active = true`.
3. Build the pool of accounts from those DB rows instead of env vars, passing the `refresh_token` from the DB into the class.
4. Update `get_total_usage()`, `get_next_account()`, etc., to enforce the DB's `daily_limit` and `hourly_limit`.

---

## WHAT NOT TO DO

- DO NOT switch to App Passwords or standard `smtplib`. We MUST use OAuth and the Gmail API over Port 443.
- DO NOT suggest building a new frontend framework (React, Next.js, etc.)
- DO NOT modify the login page, auth guard, or JWT middleware — they are already working.
- DO NOT ask the user what they want — just build it.

## START NOW

Begin with Task 1 by implementing the `/api/auth/google-connect` and callback routes in `api/index.py`. Then build the Connect button in the dashboard.
