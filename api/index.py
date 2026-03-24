#!/usr/bin/env python3
"""
Film Festival Outreach Platform — Main API
Flask application with Supabase backend for contact discovery,
enrichment, icebreaker generation, and email drip campaigns.
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, render_template, redirect, send_file
from flask_cors import CORS, cross_origin
from dotenv import load_dotenv
from pathlib import Path
import requests
import threading
from google import genai
# Setup
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

# Load environment
env_path = BASE_DIR / '.env'
if env_path.exists():
    load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / 'public'),
    static_folder=str(BASE_DIR / 'public'),
    static_url_path=''
)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'quickreach-dev-key')
CORS(app)

# In-memory job tracker for background verification progress
_verify_jobs = {}

# Supabase client
from supabase import create_client, Client

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
SUPABASE_SERVICE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

supabase = None # using type hint as comment since Client might not be imported if unused elsewhere, but we have it above
effective_key = SUPABASE_SERVICE_KEY or SUPABASE_KEY

PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
SENDER_NAME = os.getenv('SENDER_NAME', 'Bipul')

if SUPABASE_URL and effective_key:
    supabase = create_client(SUPABASE_URL, effective_key)
    logger.info("Supabase client initialized")
else:
    logger.warning("Supabase credentials not found")

# =============================================================================
# ROUTES — Pages
# =============================================================================

@app.route('/')
def index():
    return redirect('/dashboard')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/ping')
def ping():
    return jsonify({
        'status': 'ok',
        'app': 'QuickReach',
        'supabase': supabase is not None
    })

# =============================================================================
# ROUTES — Projects
# =============================================================================

@app.route('/api/projects', methods=['GET'])
def list_projects():
    try:
        result = supabase.table('projects').select('*').order('created_at', desc=True).execute()
        return jsonify({'projects': result.data or []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects', methods=['POST'])
def create_project():
    try:
        data = request.json
        name = data.get('name')
        description = data.get('description', '')
        custom_instructions = data.get('custom_instructions', '')
        if not name:
            return jsonify({'error': 'Project name required'}), 400
        
        # 1. Create the project
        result = supabase.table('projects').insert({
            'name': name, 
            'description': description,
            'custom_instructions': custom_instructions
        }).execute()
        
        project = result.data[0]
        project_id = project['id']
        
        # 2. Context-Aware Template Generation using Gemini
        if GEMINI_API_KEY and (description or custom_instructions):
            try:
                system = _get_regen_prompt(name, description, custom_instructions)
                
                client = genai.Client(api_key=GEMINI_API_KEY)
                response = client.models.generate_content(
                    model='gemini-2.5-pro',
                    contents=system,
                )
                
                content = response.text.strip()
                if '```json' in content: content = content.split('```json')[1].split('```')[0].strip()
                elif '```' in content: content = content.split('```')[1].split('```')[0].strip()
                
                import json
                steps = json.loads(content)
                
                # Standard delay pattern for a 4 step campaign
                delays = [0, 3, 7, 14]
                
                templates_to_insert = []
                for i, step in enumerate(steps[:4]):
                    templates_to_insert.append({
                        'project_id': project_id,
                        'name': step.get('name', f'Step {i+1}'),
                        'step_number': i + 1,
                        'subject_template': step.get('subject_template', f'Follow up {i}'),
                        'body_template': step.get('body_template', 'Placeholder body'),
                        'delay_days': delays[i] if i < len(delays) else 14
                    })
                
                if templates_to_insert:
                    supabase.table('email_templates').insert(templates_to_insert).execute()
                    
            except Exception as ai_e:
                logger.error(f"Failed to generate context-templates: {ai_e}")
                # We do not fail the project creation, just log the error and allow empty templates

        return jsonify({'project': project})
    except Exception as e:
        logger.error(f"Project creation error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Dashboard Stats
# =============================================================================

@app.route('/api/dashboard/stats')
def dashboard_stats():
    """Get aggregate statistics for the dashboard."""
    try:
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        # Total contacts
        contacts = supabase.table('contacts').select('id, status', count='exact').eq('project_id', project_id).execute()
        total = contacts.count or 0
        
        # Count by status
        status_counts = {}
        for contact in (contacts.data or []):
            s = contact.get('status', 'new')
            status_counts[s] = status_counts.get(s, 0) + 1
        
        # Email stats
        emails = supabase.table('email_sequences').select('id, status', count='exact').eq('project_id', project_id).execute()
        email_counts = {}
        for seq in (emails.data or []):
            s = seq.get('status', 'pending')
            email_counts[s] = email_counts.get(s, 0) + 1
        
        return jsonify({
            'contacts': {
                'total': total,
                'new': status_counts.get('new', 0),
                'enriched': status_counts.get('enriched', 0),
                'icebreaker_ready': status_counts.get('icebreaker_ready', 0),
                'in_sequence': status_counts.get('in_sequence', 0),
                'replied': status_counts.get('replied', 0),
                'completed': status_counts.get('completed', 0),
            },
            'emails': {
                'total': emails.count or 0,
                'pending': email_counts.get('pending', 0),
                'sent': email_counts.get('sent', 0),
                'opened': email_counts.get('opened', 0),
                'replied': status_counts.get('replied', 0),  # Use contact-based source of truth
                'bounced': status_counts.get('bounced', 0), # Use contact-based source of truth
            }
        })
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Daily Snapshot
# =============================================================================

@app.route('/api/dashboard/daily-snapshot')
def daily_snapshot():
    """Get all pending sequence steps grouped by project for today + overdue. One step per contact."""
    from datetime import timedelta
    import json as json_mod
    try:
        ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
        date_str = ist_now.strftime('%Y-%m-%dT23:59:59')
        today_str = ist_now.strftime('%Y-%m-%d')
        
        result = supabase.table('email_sequences')\
            .select('id, contact_id, subject, body, step_number, project_id, scheduled_at, manual_channel, contacts(name, email, instagram, enrichment_data), projects(name)')\
            .eq('status', 'pending')\
            .lte('scheduled_at', date_str)\
            .order('scheduled_at')\
            .execute()
            
        pending_steps = result.data or []
        
        # Fetch contacts that have already replied (exclude them entirely)
        replied_result = supabase.table('email_sequences')\
            .select('contact_id')\
            .in_('status', ['replied', 'cancelled'])\
            .execute()
        replied_contact_ids = {r['contact_id'] for r in (replied_result.data or []) if r.get('contact_id')}
        
        # Deduplicate: only show the EARLIEST pending step per contact, exclude replied contacts
        seen_contacts = {}
        deduped = []
        for step in pending_steps:
            contact = step.get('contacts')
            if not contact:
                continue
            cid = step.get('contact_id')
            if cid in replied_contact_ids:
                continue  # Skip contacts who have already replied
            if cid not in seen_contacts:
                seen_contacts[cid] = True
                deduped.append(step)
        
        # Group by project
        projects = {}
        
        for step in deduped:
            contact = step.get('contacts')
            if not contact:
                continue
                
            enrichment = contact.get('enrichment_data') or {}
            if isinstance(enrichment, str):
                try:
                    enrichment = json_mod.loads(enrichment)
                except Exception:
                    enrichment = {}
            
            raw_phone = enrichment.get('phone') or enrichment.get('phone_number')
            clean_phone = ''.join(filter(str.isdigit, str(raw_phone))) if raw_phone else None
            ig_handle = contact.get('instagram') or enrichment.get('instagram') or enrichment.get('instagram_handle')
            if ig_handle:
                ig_str = str(ig_handle).strip().rstrip('/')
                # Strip full URL down to just the handle
                for prefix in ['https://www.instagram.com/', 'http://www.instagram.com/', 'https://instagram.com/', 'http://instagram.com/', 'www.instagram.com/', 'instagram.com/']:
                    if ig_str.lower().startswith(prefix):
                        ig_str = ig_str[len(prefix):]
                        break
                # Also strip /p/XXXX post links — extract nothing useful
                clean_ig = ig_str.replace('@', '').strip().rstrip('/') if ig_str and not ig_str.startswith('p/') else None
            else:
                clean_ig = None
            
            scheduled = step.get('scheduled_at', '')
            is_overdue = bool(scheduled) and scheduled < today_str
            
            project_id = step.get('project_id')
            project_name = (step.get('projects') or {}).get('name', 'Unknown Project')
            
            if project_id not in projects:
                projects[project_id] = {'name': project_name, 'steps': []}
            
            projects[project_id]['steps'].append({
                'id': step['id'],
                'step_number': step.get('step_number'),
                'subject': step.get('subject'),
                'body': step.get('body'),
                'scheduled_at': scheduled,
                'is_overdue': is_overdue,
                'contact_name': contact.get('name'),
                'contact_email': contact.get('email'),
                'clean_phone': clean_phone,
                'clean_ig': clean_ig,
                'manual_channel': step.get('manual_channel') or '',
            })
        
        total = sum(len(p['steps']) for p in projects.values())
        return jsonify({'projects': list(projects.values()), 'total_pending': total})
    except Exception as e:
        logger.error(f"Daily snapshot error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Contacts
# =============================================================================

@app.route('/api/contacts')
def list_contacts():
    """List contacts with optional filters."""
    try:
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        status = request.args.get('status')
        search = request.args.get('search', '')
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        query = supabase.table('contacts').select('*', count='exact').eq('project_id', project_id)
        
        if status:
            query = query.eq('status', status)
        
        if search:
            query = query.or_(f"name.ilike.%{search}%,bio.ilike.%{search}%,email.ilike.%{search}%")
        
        result = query.order('created_at', desc=True).range(offset, offset + limit - 1).execute()
        
        return jsonify({
            'contacts': result.data or [],
            'total': result.count or 0,
            'limit': limit,
            'offset': offset
        })
    except Exception as e:
        logger.error(f"List contacts error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/<contact_id>', methods=['GET'])
def get_contact(contact_id):
    """Get single contact."""
    try:
        result = supabase.table('contacts').select('*').eq('id', contact_id).single().execute()
        return jsonify({'contact': result.data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/<contact_id>', methods=['PUT'])
def update_contact(contact_id):
    """Update a contact."""
    try:
        data = request.json
        allowed = ['name', 'bio', 'linkedin_url', 'email', 'instagram', 'icebreaker', 'status']
        update_data = {k: v for k, v in data.items() if k in allowed}
        update_data['updated_at'] = datetime.utcnow().isoformat()
        
        result = supabase.table('contacts').update(update_data).eq('id', contact_id).execute()
        return jsonify({'contact': result.data[0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/<contact_id>', methods=['DELETE'])
def delete_contact(contact_id):
    """Delete a contact."""
    try:
        supabase.table('contacts').delete().eq('id', contact_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/bulk-delete', methods=['POST'])
def bulk_delete_contacts():
    """Delete multiple contacts."""
    try:
        data = request.json
        contact_ids = data.get('contact_ids', [])
        
        if not contact_ids:
            return jsonify({'error': 'No contact IDs provided'}), 400
            
        supabase.table('contacts').delete().in_('id', contact_ids).execute()
        return jsonify({'success': True, 'deleted': len(contact_ids)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Search Pipeline
# =============================================================================

@app.route('/api/contacts/search', methods=['POST'])
def trigger_search():
    """Trigger a Serper search + scrape pipeline."""
    try:
        data = request.json
        project_id = data.get('project_id')
        queries = data.get('queries', [])
        num_results = data.get('num_results', 100)
        
        if not queries or not project_id:
            return jsonify({'error': 'No search queries provided'}), 400
        
        from execution.serper_search import run_search_pipeline
        from execution.scrape_contacts import extract_and_store_contacts
        
        # Run search
        results = run_search_pipeline(queries, num_results, project_id=project_id)
        
        # Extract and store contacts
        all_stats = {'total_results': len(results), 'inserted': 0, 'skipped': 0, 'errors': 0}
        
        for query in queries:
            query_results = [r for r in results if True]  # All results for now
            stats = extract_and_store_contacts(query_results, source_query=query, project_id=project_id)
            all_stats['inserted'] += stats.get('inserted', 0)
            all_stats['skipped'] += stats.get('skipped', 0)
            all_stats['errors'] += stats.get('errors', 0)
        
        return jsonify(all_stats)
    except Exception as e:
        logger.error(f"Search pipeline error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/apify-search', methods=['POST'])
def trigger_apify_search():
    """Find businesses via Apify Google Maps Scraper."""
    try:
        data = request.json or {}
        project_id = data.get('project_id')
        query = data.get('query')
        location = data.get('location')
        num_results = data.get('num_results', 50)

        if not query or not location or not project_id:
            return jsonify({'error': 'query, location, and project_id are required'}), 400

        from execution.apify_search import run_apify_maps_search
        
        # Run search
        stats = run_apify_maps_search(query, location, num_results, project_id)
        
        if stats is None:
            return jsonify({'error': 'Apify search failed'}), 500
            
        return jsonify({
            'total_results': stats.get('inserted', 0) + stats.get('skipped', 0),
            'inserted': stats.get('inserted', 0),
            'skipped': stats.get('skipped', 0),
            'errors': 0
        })
    except Exception as e:
        logger.error(f"Apify search error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/contacts/bulk-search', methods=['POST'])
@cross_origin()
def run_bulk_search_api():
    try:
        data = request.json or {}
        niche = data.get('niche')
        location = data.get('location')
        project_id = data.get('project_id')
        pages = data.get('pages', 10)

        if not niche or not location:
            return jsonify({'error': 'Niche and location are required'}), 400

        from execution.bulk_business_search import run_bulk_search
        
        # We run this synchronously for now because it's a "bulk" operation
        # but the user requested progress updates. 
        # Actually, let's run it in a thread and they can check back in contacts.
        def _run():
            try:
                run_bulk_search(niche, location, project_id=project_id, pages_per_query=pages)
            except Exception as e:
                logger.error(f"Bulk search thread error: {e}")

        import threading
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return jsonify({'message': f'Bulk search started for {niche} in {location}. Results will appear in Contacts soon.'})
    except Exception as e:
        logger.error(f"Bulk search error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/contacts/search', methods=['POST'])
@cross_origin()
def run_biz_search():
    try:
        data = request.json or {}
        queries = data.get('queries', [])
        num_results = data.get('num_results', 100)
        project_id = data.get('project_id')

        if not queries:
            return jsonify({'error': 'No queries provided'}), 400

        def _run():
            try:
                from execution.business_search import run_business_search
                total_stats = run_business_search(queries, num_results=num_results, project_id=project_id)
                logger.info(f"[BizSearch] Done — {total_stats}")
            except Exception as e:
                logger.error(f"Async business search error: {e}")

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        return jsonify({
            'status': 'started',
            'message': f'Business search started for {len(queries)} quer{"y" if len(queries)==1 else "ies"} — contacts will appear shortly'
        })
    except Exception as e:
        logger.error(f"Business search error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Enrichment
# =============================================================================

@app.route('/api/contacts/cleanup', methods=['POST'])
def cleanup_contacts_endpoint():
    """Trigger the contact cleanup script."""
    try:
        data = request.json or {}
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({'error': 'project_id required'}), 400
            
        from execution.cleanup_contacts import cleanup_contacts
        result = cleanup_contacts(project_id)
        
        return jsonify({
            'success': True,
            'message': f"Cleanup complete: {result['updated']} updated, {result['deleted']} deleted/merged.",
            'details': result
        })
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/contacts/enrich', methods=['POST'])
def trigger_enrichment():
    """Trigger email/IG enrichment for pending (or selected) contacts in background."""
    try:
        data = request.json or {}
        limit = data.get('limit', 500)
        contact_ids = data.get('contact_ids', [])
        project_id = data.get('project_id')
        
        import threading
        from execution.enrich_contacts import enrich_contacts
        
        # Run in background thread to avoid Gunicorn timeouts
        def run_enrichment_task():
            logger.info(f"Starting background enrichment task (limit={limit}, project={project_id}, ids_received={len(contact_ids)})")
            enrich_contacts(limit=limit, project_id=project_id, contact_ids=contact_ids)
            logger.info("Background enrichment task complete.")

        thread = threading.Thread(target=run_enrichment_task)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': f'Enrichment started in background for {len(contact_ids) if contact_ids else limit} contacts'
        })
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/camoufox-enrich', methods=['POST'])
def trigger_camoufox_enrichment():
    """Stealth-browser enrichment: scrape website for emails + Instagram via Camoufox."""
    try:
        data = request.json or {}
        contact_ids = data.get('contact_ids', [])

        if not contact_ids:
            return jsonify({'error': 'No contact_ids provided'}), 400

        import threading, json as _json
        from execution.camoufox_scraper import scrape_contact_info
        from execution.verify_email import check_email

        def run_camoufox_batch():
            logger.info(f"[Camoufox] Starting stealth enrichment for {len(contact_ids)} contacts")
            _sb = __import__('supabase', fromlist=['create_client']).create_client(SUPABASE_URL, effective_key)

            for contact_id in contact_ids:
                try:
                    row = _sb.table('contacts').select('*').eq('id', contact_id).single().execute()
                    if not row.data:
                        continue
                    contact = row.data

                    # Skip if already has a verified email
                    enrichment = contact.get('enrichment_data') or {}
                    if isinstance(enrichment, str):
                        try:
                            enrichment = _json.loads(enrichment)
                        except Exception:
                            enrichment = {}

                    if contact.get('email') and enrichment.get('verification_status') in ('valid', 'risky'):
                        logger.info(f"[Camoufox] Skipping {contact.get('name')} — already has verified email")
                        continue

                    result = scrape_contact_info(contact)
                    found_emails = result.get('emails', [])
                    found_ig = result.get('instagram')
                    website = result.get('website')

                    # Verify emails and pick the best
                    best_email = None
                    best_status = None
                    for email in found_emails:
                        v_status, v_reason = check_email(email)
                        logger.info(f"  Email {email} → {v_status} ({v_reason})")
                        enrichment[f'cf_verify_{email}'] = v_status
                        if v_status in ('valid', 'risky') and not best_email:
                            best_email = email
                            best_status = v_status
                            enrichment['verification_status'] = v_status
                            enrichment['verification_reason'] = v_reason

                    # Update enrichment fields
                    if website:
                        enrichment['website'] = website
                    if found_ig and not enrichment.get('instagram'):
                        enrichment['instagram'] = found_ig

                    update_payload = {
                        'enrichment_data': enrichment,
                        'updated_at': datetime.utcnow().isoformat(),
                    }

                    if best_email:
                        update_payload['email'] = best_email
                        update_payload['status'] = 'enriched'
                        logger.info(f"[Camoufox] ✅ {contact.get('name')} → {best_email}")
                    elif found_ig:
                        # No email but IG found — still useful
                        update_payload['instagram'] = found_ig.lstrip('@')
                        logger.info(f"[Camoufox] Partial: {contact.get('name')} → IG {found_ig}")
                    else:
                        logger.warning(f"[Camoufox] ❌ Nothing found for {contact.get('name')}")

                    _sb.table('contacts').update(update_payload).eq('id', contact_id).execute()

                except Exception as contact_err:
                    logger.error(f"[Camoufox] Error processing {contact_id}: {contact_err}")

            logger.info("[Camoufox] Batch complete.")

        thread = threading.Thread(target=run_camoufox_batch, daemon=True)
        thread.start()

        return jsonify({
            'status': 'started',
            'message': f'Camoufox stealth enrichment started for {len(contact_ids)} contacts'
        })

    except Exception as e:
        logger.error(f"Camoufox enrichment trigger error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/verify', methods=['POST'])
def trigger_manual_verification():
    """Manually verify selected contacts and purge invalid ones from sequences."""
    try:
        data = request.json or {}
        contact_ids = data.get('contact_ids', [])
        if not contact_ids:
            return jsonify({'error': 'No contact IDs provided'}), 400

        import threading, uuid as _uuid
        job_id = str(_uuid.uuid4())
        _verify_jobs[job_id] = {
            'status': 'processing',
            'total': len(contact_ids),
            'done': 0,
            'skipped': 0,
            'valid': 0,
            'error': None
        }

        def run_verification_in_background():
            from execution.verify_email import check_email
            from supabase import create_client as _create_client
            import json as _json

            _sb = _create_client(SUPABASE_URL, effective_key)
            job = _verify_jobs[job_id]
            try:
                contacts = _sb.table('contacts').select('id, email, enrichment_data').in_('id', contact_ids).execute()
                if not contacts.data:
                    job['status'] = 'done'
                    return

                for c in contacts.data:
                    email = c.get('email')
                    if not email:
                        job['done'] += 1
                        job['skipped'] += 1
                        continue

                    enrichment_data = c.get('enrichment_data') or {}
                    if isinstance(enrichment_data, str):
                        try: enrichment_data = _json.loads(enrichment_data)
                        except: enrichment_data = {}

                    logger.info(f"Manual Verification: Checking {email} for contact {c['id']}")
                    v_status, v_reason = check_email(email)

                    enrichment_data['verification_status'] = v_status
                    enrichment_data['verification_reason'] = v_reason

                    if v_status == 'invalid':
                        logger.warning(f"  ❌ Manual Verification failed for {email}. Clearing email only — sequences kept for WA/IG outreach.")
                        _sb.table('contacts').update({
                            'email': None,
                            'status': 'skipped',
                            'enrichment_data': enrichment_data
                        }).eq('id', c['id']).execute()
                        job['skipped'] += 1
                    else:
                        logger.info(f"  ✅ Manual Verification passed ({v_status}) for {email}.")
                        _sb.table('contacts').update({
                            'enrichment_data': enrichment_data
                        }).eq('id', c['id']).execute()
                        job['valid'] += 1

                    job['done'] += 1

                job['status'] = 'done'

            except Exception as e:
                logger.error(f"Background verification failed: {e}")
                job['status'] = 'error'
                job['error'] = str(e)

        thread = threading.Thread(target=run_verification_in_background)
        thread.start()

        return jsonify({'job_id': job_id, 'total': len(contact_ids), 'status': 'processing'}), 202

    except Exception as e:
        logger.error(f"Manual verification endpoint error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/verify-jobs/<job_id>', methods=['GET'])
def get_verify_job(job_id):
    """Poll endpoint for verification job progress."""
    job = _verify_jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)

# =============================================================================
# ROUTES — Icebreakers
# =============================================================================

@app.route('/api/contacts/icebreaker', methods=['POST'])
def trigger_icebreakers():
    """Generate icebreakers for enriched contacts (runs asynchronously)."""
    try:
        data = request.json or {}
        limit = data.get('limit', 1000)
        project_id = data.get('project_id')
        contact_ids = data.get('contact_ids')
        
        from execution.generate_icebreakers import generate_icebreakers_batch
        import threading
        
        def run_in_background():
            try:
                generate_icebreakers_batch(
                    limit=limit, 
                    project_id=project_id, 
                    contact_ids=contact_ids
                )
            except Exception as e:
                logger.error(f"Background icebreaker task failed: {e}")
                
        # Start background thread to avoid Gunicorn 30s timeout
        thread = threading.Thread(target=run_in_background)
        thread.start()
        
        return jsonify({
            'message': 'Icebreaker generation started in the background. Give it a few minutes to process.',
            'status': 'processing'
        }), 202
    except Exception as e:
        logger.error(f"Icebreaker error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Email Templates
# =============================================================================

@app.route('/api/templates')
def list_templates():
    """List all email templates and project context."""
    try:
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        
        # Templates
        result = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').execute()
        
        # Project metadata
        proj = supabase.table('projects').select('custom_instructions').eq('id', project_id).single().execute()
        custom_instructions = proj.data.get('custom_instructions') if proj.data else ""
        
        return jsonify({
            'templates': result.data or [],
            'custom_instructions': custom_instructions
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/<int:template_id>', methods=['PUT'])
def update_template(template_id):
    """Update an email template."""
    try:
        data = request.json
        allowed = ['name', 'subject_template', 'body_template', 'delay_days']
        update_data = {k: v for k, v in data.items() if k in allowed}
        
        result = supabase.table('email_templates').update(update_data).eq('id', template_id).execute()
        return jsonify({'template': result.data[0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/generate', methods=['POST'])
def generate_template():
    """Use AI to write an email template subject and body."""
    try:
        if not GEMINI_API_KEY:
            return jsonify({'error': 'Gemini API key missing'}), 400
            
        data = request.json
        prompt = data.get('prompt')
        if not prompt:
            return jsonify({'error': 'Prompt is required'}), 400
            
        system = """You are an expert cold email copywriter. Write a single cold email sequence step.
        Return ONLY valid JSON with 'subject' and 'body' keys.
        Variables you may use: {{first_name}}, {{name}}, {{company}}, {{sender_name}}, {{sender_first_name}}.
        Do NOT use {{icebreaker}} - it is not used.
        Keep the email SHORT (3-5 sentences), concise, and natural. Goal: get a REPLY.
        ALWAYS end the email body with a sign-off using {{sender_name}} or {{sender_first_name}}.
        CRITICAL INSTRUCTIONS:
        1. NEVER include citations, footnotes, or bracketed numbers like [1] or [2].
        2. NO HTML tags. Plain text line breaks only.
        3. Return ONLY a single valid JSON block."""
        
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-2.5-pro',
            contents=system + "\n\nUser Prompt:\n" + prompt,
        )
        
        content = response.text.strip()
        # Extract JSON (Gemini may wrap in markdown blocks)
        if '```json' in content:
            content = content.split('```json')[1].split('```')[0].strip()
        elif '```' in content:
            content = content.split('```')[1].split('```')[0].strip()
            
        import json
        result = json.loads(content)
        
        return jsonify({
            'subject_template': result.get('subject', 'Missing Subject'),
            'body_template': result.get('body', 'Missing Body')
        })
    except Exception as e:
        logger.error(f"Template generation error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/<int:template_id>', methods=['DELETE'])
def delete_template(template_id):
    """Delete an email template."""
    try:
        supabase.table('email_templates').delete().eq('id', template_id).execute()
        return jsonify({'message': 'Template deleted successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/regenerate-all', methods=['POST'])
def regenerate_all_templates():
    """Delete existing templates and regenerate the full 4-step drip from project description."""
    try:
        if not GEMINI_API_KEY:
            return jsonify({'error': 'Gemini API key missing'}), 400
        data = request.json
        project_id = data.get('project_id')
        if not project_id:
            return jsonify({'error': 'project_id required'}), 400

        # Fetch project description
        proj = supabase.table('projects').select('name,description,custom_instructions').eq('id', project_id).single().execute()
        if not proj.data:
            return jsonify({'error': 'Project not found'}), 404
        name = proj.data.get('name', 'Unknown')
        description = proj.data.get('description', '')
        custom_instructions = data.get('custom_instructions') or proj.data.get('custom_instructions', '')
        
        # Save custom_instructions back to project if they were passed in
        if data.get('custom_instructions'):
             supabase.table('projects').update({'custom_instructions': custom_instructions}).eq('id', project_id).execute()

        if not description and not custom_instructions:
            return jsonify({'error': 'Project has no description or custom instructions.'}), 400

        system = _get_regen_prompt(name, description, custom_instructions)

        import json as _json
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-2.5-pro',
            contents=system,
        )
        content = response.text.strip()
        if '```json' in content: content = content.split('```json')[1].split('```')[0].strip()
        elif '```' in content: content = content.split('```')[1].split('```')[0].strip()
        steps = _json.loads(content)

        # Delete old templates
        supabase.table('email_templates').delete().eq('project_id', project_id).execute()

        delays = [0, 3, 7, 14]
        new_templates = []
        for i, step in enumerate(steps[:4]):
            row = {
                'project_id': project_id,
                'name': step.get('name', f'Step {i+1}'),
                'step_number': i + 1,
                'subject_template': step.get('subject_template', f'Follow up {i}'),
                'body_template': step.get('body_template', ''),
                'delay_days': delays[i] if i < len(delays) else 14
            }
            new_templates.append(row)
        supabase.table('email_templates').insert(new_templates).execute()

        # Fetch the newly created templates to return
        result = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').execute()
        return jsonify({'templates': result.data or [], 'message': 'All 4 templates regenerated'})
    except Exception as e:
        logger.error(f"Regenerate all templates error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/templates/reorder', methods=['PUT'])
def reorder_templates():
    """Update the step numbers for a list of template IDs."""
    try:
        data = request.json
        template_ids = data.get('template_ids', [])
        
        # Pass 1: set to negative values to avoid unique constraint collisions
        for index, t_id in enumerate(template_ids):
            supabase.table('email_templates').update({'step_number': -(index + 1)}).eq('id', t_id).execute()
            
        # Pass 2: set to final positive values
        for index, t_id in enumerate(template_ids):
            supabase.table('email_templates').update({'step_number': index + 1}).eq('id', t_id).execute()
            
        return jsonify({'message': 'Templates reordered successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def _get_regen_prompt(name, description, custom_instructions):
    """Generates the system prompt for template generation/regeneration."""
    prompt = f"""You are an elite cold-email copywriter for a project named "{name}"."""
    
    if description:
        prompt += f"\nBase Project Description: \"{description}\""
    
    if custom_instructions:
        prompt += f"\n\nCRITICAL OVERRIDING INSTRUCTIONS:\n\"{custom_instructions}\"\n(Strictly follow these instructions over any default rules or project description.)"
    
    prompt += """
Generate a 4-step cold email drip sequence tailored to this business description and instructions.
Create exactly 4 steps.

CRITICAL COLD EMAIL RULES (UNLESS OVERRIDDEN BY CUSTOM INSTRUCTIONS):
- NEVER include any links, URLs, or attachments in ANY step
- The goal of every email is to get a REPLY, not a click
- The CTA must ALWAYS be a variation of "Want me to send the full report?" or "Can I share the details?"
- Keep emails SHORT (3-5 sentences max for the body)
- Professional but direct tone

VARIABLES — these are substituted per-contact at send time. Use them as {{variable}} in your output:
- {{first_name}} — contact's greeting name (e.g. "Jasmine Spa"). USE THIS in the greeting.
- {{name}} — contact's full display name (e.g. "Jasmine Spa Team"). USE THIS when referencing the business in the body.
- {{company}} — company name. Use sparingly if {{name}} already used.
- {{sender_name}} — the sender's full name (usually for sign-off).
Do NOT use {{icebreaker}}.

MANDATORY TEMPLATE STRUCTURE:
- Step 1: Warm opener about their business → value/findings → CTA
- Steps 2-3: Short follow-ups re-emphasizing value
- Step 4: Polite break-up email.

You MUST return the output as a SINGLE VALID JSON ARRAY of exactly 4 objects.
Each object MUST have three exact keys: 
- "name" (e.g. "Intro", "Follow up 1", "Nudge", "Break up")
- "subject_template" (the email subject line)
- "body_template" (the email body)

Return ONLY the raw JSON array. No markdown block quotes, no explanation."""
    return prompt

# =============================================================================
# ROUTES — Email Sequences
# =============================================================================

@app.route('/api/sequences')
def list_sequences():
    """List email sequences with contact info."""
    try:
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        contact_id = request.args.get('contact_id')
        status = request.args.get('status')
        
        query = supabase.table('email_sequences').select('*, contacts(name, email)').eq('project_id', project_id)
        
        if contact_id:
            query = query.eq('contact_id', contact_id)
        if status:
            query = query.eq('status', status)
        
        result = query.order('created_at', desc=True).limit(10000).execute()
        return jsonify({'sequences': result.data or []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/<sequence_id>', methods=['PUT'])
def update_sequence(sequence_id):
    """Update a specific sequence step (e.g. manual edit, mark sent/replied)."""
    try:
        data = request.json
        allowed = ['subject', 'body', 'status', 'sent_at', 'manual_channel']
        update_data = {k: v for k, v in data.items() if k in allowed}
        
        result = supabase.table('email_sequences').update(update_data).eq('id', sequence_id).execute()
        
        # If marked as replied, cascade: cancel all pending steps for this contact
        if data.get('status') == 'replied' and result.data:
            contact_id = result.data[0].get('contact_id')
            if contact_id:
                supabase.table('email_sequences').update({'status': 'cancelled'}).eq('contact_id', contact_id).eq('status', 'pending').execute()
                supabase.table('contacts').update({'status': 'replied', 'updated_at': datetime.utcnow().isoformat()}).eq('id', contact_id).execute()
        
        # If marked as sent, reschedule subsequent steps (mirrors send_emails.py logic)
        if data.get('status') == 'sent' and result.data:
            seq = result.data[0]
            contact_id = seq.get('contact_id')
            template_id = seq.get('template_id')
            project_id = seq.get('project_id')
            now_sent = datetime.utcnow()
            
            if contact_id and template_id and project_id:
                try:
                    from datetime import timedelta
                    # Get delay_days for the just-sent step's template
                    sent_tmpl = supabase.table('email_templates').select('delay_days').eq('id', template_id).single().execute()
                    sent_delay = sent_tmpl.data.get('delay_days', 0) if sent_tmpl.data else 0
                    
                    # Get all remaining pending steps
                    pending_res = supabase.table('email_sequences') \
                        .select('id, template_id, step_number') \
                        .eq('contact_id', contact_id) \
                        .eq('status', 'pending') \
                        .eq('project_id', project_id) \
                        .execute()
                        
                    if pending_res.data:
                        t_ids = [s['template_id'] for s in pending_res.data if s.get('template_id')]
                        tmpls = supabase.table('email_templates').select('id, delay_days').in_('id', t_ids).execute()
                        delay_map = {t['id']: t['delay_days'] for t in (tmpls.data or [])}
                        
                        for p_step in pending_res.data:
                            tid = p_step.get('template_id')
                            if not tid or tid not in delay_map: continue
                            delta = delay_map[tid] - sent_delay
                            if delta > 0:
                                new_sched = now_sent + timedelta(days=delta)
                                supabase.table('email_sequences').update({
                                    'scheduled_at': new_sched.isoformat()
                                }).eq('id', p_step['id']).execute()
                except Exception as e:
                    print(f"Manual reschedule failed: {e}")
        
        return jsonify({'sequence': result.data[0] if result.data else None})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/<sequence_id>', methods=['DELETE'])
def delete_sequence(sequence_id):
    """Delete a single sequence step by ID."""
    try:
        supabase.table('email_sequences').delete().eq('id', sequence_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/contact/<contact_id>', methods=['DELETE'])
def delete_contact_sequences(contact_id):
    """Delete ALL sequence steps for a contact and reset their status."""
    try:
        project_id = request.args.get('project_id')
        query = supabase.table('email_sequences').delete().eq('contact_id', contact_id)
        if project_id:
            query = query.eq('project_id', project_id)
        query.execute()
        # Reset contact status back to icebreaker_ready so they can be re-sequenced
        supabase.table('contacts').update({
            'status': 'icebreaker_ready',
            'updated_at': datetime.utcnow().isoformat()
        }).eq('id', contact_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def paraphrase_texts_batch(bodies: list, context: dict = None) -> list:
    """Paraphrase multiple email bodies in ONE Gemini Flash call.
    Returns a list of paraphrased strings in the same order as input.
    If anything fails, returns originals as fallback."""
    if not GEMINI_API_KEY or not bodies:
        return bodies
    try:
        contact_info = ""
        if context:
            contact_info = f"\nPROSPECT CONTEXT:\nBusiness: {context.get('name', '')}, Niche: {context.get('niche', '')}, Location: {context.get('location', '')}."
            if context.get('bio'):
                contact_info += f" Bio: {context['bio'][:300]}"

        numbered_input = "\n\n".join(
            f"EMAIL_{i+1}:\n{body}" for i, body in enumerate(bodies)
        )

        system = f"""You are an expert cold email copywriter. You will receive {len(bodies)} email bodies numbered EMAIL_1 through EMAIL_{len(bodies)}.

For EACH email:
- Rewrite so it sounds genuinely fresh (restructure sentences, synonyms, vary rhythm)
- Change ~30% of wording while keeping the same meaning, intent, and length
- CRITICAL: Preserve ALL template variables exactly as written: {{{{name}}}}, {{{{first_name}}}}, {{{{company}}}}, {{{{location}}}}, {{{{niche}}}}, {{{{sender_first_name}}}}, etc.
- Do NOT add new facts or claims not in the original
- No citations, no footnotes, no bracketed numbers like [1]
- Plain text only, no HTML{contact_info}

Return ONLY a JSON array of exactly {len(bodies)} strings, in the same order:
["rewritten EMAIL_1 body", "rewritten EMAIL_2 body", ...]

Return ONLY the raw JSON array. No markdown, no explanation."""

        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=system + "\n\nEmails to paraphrase:\n" + numbered_input,
        )
        content = response.text.strip()
        if '```json' in content: content = content.split('```json')[1].split('```')[0].strip()
        elif '```' in content: content = content.split('```')[1].split('```')[0].strip()

        import json as _json
        result = _json.loads(content)
        if isinstance(result, list) and len(result) == len(bodies):
            return [str(r) for r in result]
        logger.warning(f"Batch paraphrase returned wrong count ({len(result)} vs {len(bodies)}), using originals")
        return bodies
    except Exception as e:
        logger.error(f"Batch paraphrase error: {e}")
        return bodies  # fallback: originals


def paraphrase_text(text: str, context: dict = None) -> str:
    """Single-text wrapper around the batch function (kept for backward compat)."""
    return paraphrase_texts_batch([text], context)[0]


@app.route('/api/sequences/create', methods=['POST'])
def create_sequences():
    """Create email sequences from templates for selected contacts (runs asynchronously)."""
    try:
        data = request.json
        project_id = data.get('project_id')
        contact_ids = data.get('contact_ids', [])
        
        if not contact_ids or not project_id:
            return jsonify({'error': 'No contacts or project_id selected'}), 400
        
        # Get templates synchronously to validate
        templates = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').execute()
        if not templates.data:
            return jsonify({'error': 'No email templates found. Seed them first.'}), 400
        
        # Get contacts synchronously to get the count
        contacts = supabase.table('contacts').select('*').in_('id', contact_ids).execute()
        if not contacts.data:
            return jsonify({'error': 'No valid contacts found.'}), 400
            
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def run_in_background(proj_id, contacts_data, templates_data):
            import re as _re
            import json as _json

            def _clean_biz_name(name):
                if not name: return ""
                name = name.strip()
                
                # 1. CamelCase split
                name = _re.sub(r'([a-z])([A-Z])', r'\1 \2', name)
                
                # 2. Multi-Pass Industry Split
                # We'll run multiple passes to catch things like RedChilliesVfx
                keywords = [
                    'entertainment', 'motionpictures', 'productions', 'production', 
                    'studios', 'studio', 'films', 'film', 'media', 'works', 'creative', 
                    'solutions', 'digital', 'global', 'agency', 'group', 'services', 
                    'official', 'vfx', 'corp', 'company', 'pictures', 'house', 'collective',
                    'mantra', 'wadi', 'power', 'hour', 'baba', 'chillies',
                    'stories', 'maverick', 'jugaad', 'zoom', 'cine', 'power', 'hour', 'that', 'matter'
                ]
                prefixes = ['the', 'wild', 'magic', 'stories', 'red', 'zoom', 'cine', 'jugaad', 'maverick', 'goodfellas', 'magic', 'star', 'grand', 'royal']
                
                for _ in range(3): # Up to 3 splits per word
                    new_name = name
                    words = new_name.split()
                    cleaned_words = []
                    for word in words:
                        if len(word) > 3:
                            # Try prefix split
                            for p in prefixes:
                                if word.lower().startswith(p) and len(word) > len(p) + 2:
                                    word = word[:len(p)] + ' ' + word[len(p):]
                                    break
                            
                            # Try suffix/keyword split
                            for k in keywords:
                                low = word.lower()
                                if k in low:
                                    idx = low.find(k)
                                    if idx > 0 and word[idx-1] != ' ':
                                        word = word[:idx] + ' ' + word[idx:]
                                        break
                                        
                        cleaned_words.append(word)
                    name = ' '.join(cleaned_words)
                    if name == new_name: break # No more changes

                # 3. Final cleanup and casing
                name = _re.sub(r'\.(com|net|org|in|biz|ai|co\.in|io)$', '', name, flags=_re.IGNORECASE)
                junk = ['ltd', 'pvt', 'limited', 'private', 'inc', 'corp', 'corporation', 'llp', 'llc']
                for j in junk:
                    name = _re.sub(rf'\b{j}\b\.?', '', name, flags=_re.IGNORECASE)
                    
                parts = []
                for p in _re.split(r'[-\s_]', name):
                    if p:
                        if p.lower() == 'vfx': parts.append('VFX')
                        elif p.lower() == 'edu': parts.append('Edu')
                        else: parts.append(p.capitalize())
                
                return ' '.join(parts).strip(' -|–—.,;:"\' ')

            def _shorten_company(name):
                name = _clean_biz_name(name)
                if not name: return name
                words = name.split()
                return ' '.join(words[:3]) if len(words) > 4 else name

            def _is_personal_email(email, full_name):
                if not email or not full_name: return False
                local = email.split('@')[0].lower()
                # Split name by spaces/punctuation and only look at parts >= 3 chars
                name_parts = [p.lower() for p in _re.split(r'[\s._-]', full_name) if len(p) >= 3]
                if not name_parts: return False
                # If any significant name part matches the email prefix, it's likely personalized
                return any(part in local for part in name_parts)

            created_total = 0
            errors_total = 0

            def process_contact(contact):
                """Process one contact: batch-paraphrase all steps, insert rows. Returns (created, errors)."""
                created = 0
                errors = 0
                try:
                    # Per-thread supabase client — shared client is NOT thread-safe
                    from supabase import create_client as _create_client
                    _sb = _create_client(SUPABASE_URL, effective_key)
                    base_date = datetime.utcnow()

                    enrichment_data = contact.get('enrichment_data') or {}
                    if isinstance(enrichment_data, str):
                        try: enrichment_data = _json.loads(enrichment_data)
                        except Exception: enrichment_data = {}

                    contact_email = (contact.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
                    if contact_email and enrichment_data.get('verification_status') != 'valid':
                        logger.info(f"Contact {contact['id']} has unverified/risky email '{contact_email}'. Verifying now...")
                        from execution.verify_email import check_email
                        v_status, v_reason = check_email(contact_email)
                        
                        enrichment_data['verification_status'] = v_status
                        enrichment_data['verification_reason'] = v_reason
                        
                        if v_status == 'invalid':
                            logger.warning(f"Verification failed (INVALID) for imported contact {contact['id']}. Clearing email to prevent hard bounces.")
                            # Only clear email; keep sequence rows so WA/IG steps still run
                            _sb.table('contacts').update({
                                'email': None,
                                'status': 'skipped',
                                'enrichment_data': enrichment_data,
                                'updated_at': datetime.utcnow().isoformat()
                            }).eq('id', contact['id']).execute()
                            return created, errors
                        else:
                            logger.info(f"Verification passed or risky ({v_status}) for imported contact {contact['id']}.")
                            # Update DB so we don't verify again next time
                            _sb.table('contacts').update({
                                'enrichment_data': enrichment_data
                            }).eq('id', contact['id']).execute()
                    # --------------------------------------------------------

                    raw_company = contact.get('company') or enrichment_data.get('company') or enrichment_data.get('linkedin_company') or contact.get('name') or 'your company'
                    full_name = contact.get('name', 'there')
                    contact_email = (contact.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
                    
                    is_personal = _is_personal_email(contact_email, full_name)
                    clean_biz = _clean_biz_name(full_name)
                    
                    if is_personal:
                        # Email is personal (e.g. bartosz@...), safe to use name
                        first_name = full_name.split()[0]
                        display_name = full_name
                    else:
                        # Email is generic (e.g. info@...), suppress personal name
                        first_name = "there"
                        # If clean_biz is same as full_name, it's likely a person's name without biz suffix
                        display_name = (clean_biz + " Team") if clean_biz != full_name else "Team"

                    raw_icebreaker = contact.get('icebreaker', '') or ''
                    clean_icebreaker = _re.sub(r'\[\d+\]', '', raw_icebreaker).strip()

                    variables = {
                        'name': display_name,
                        'first_name': first_name,
                        'bio': contact.get('bio', ''),
                        'icebreaker': clean_icebreaker,
                        'company': _shorten_company(raw_company),
                        'sender_name': SENDER_NAME,
                        'sender_first_name': SENDER_NAME.split()[0] if SENDER_NAME else '',
                        'location': enrichment_data.get('location') or enrichment_data.get('search_location') or contact.get('location') or '',
                        'niche': enrichment_data.get('niche') or enrichment_data.get('category') or contact.get('niche') or contact.get('source') or '',
                        'linkedin_headline': enrichment_data.get('linkedin_headline', ''),
                        'linkedin_company': enrichment_data.get('linkedin_company', ''),
                        'linkedin_title': enrichment_data.get('linkedin_title', ''),
                        'linkedin_about': enrichment_data.get('linkedin_about', ''),
                    }

                    # ── BATCH PARAPHRASE: all template bodies in ONE Flash call ──
                    bodies_raw = [t['body_template'] for t in templates_data]
                    bodies_para = paraphrase_texts_batch(bodies_raw, context=variables)

                    # ── SMART REFRESH / DEDUP CHECK ──
                    # Instead of a simple "already exists = skip", we're smarter:
                    # 1. If step is 'pending': Update content (subject/body) but KEEP existing schedule.
                    # 2. If step is 'sent' or 'replied': Leave it alone.
                    # 3. If contact is already 'replied': Skip generating any NEW steps.
                    
                    contact_status = contact.get('status', 'new')

                    for i, template in enumerate(templates_data):
                        try:
                            # Search for an existing sequence row for this specific step number
                            existing_res = _sb.table('email_sequences').select('*') \
                                .eq('contact_id', contact['id']) \
                                .eq('step_number', template['step_number']) \
                                .execute()
                            existing = existing_res.data[0] if existing_res.data else None

                            subject = template['subject_template']
                            body = bodies_para[i]  # already paraphrased, index-safe

                            for key, val in variables.items():
                                val_str = str(val) if val is not None else ''
                                # Robust replacement: handle {{tag}}, {{ tag }}, {{Tag}}, etc.
                                pattern = _re.compile(r'\{\{\s*' + _re.escape(key) + r'\s*\}\}', _re.IGNORECASE)
                                subject = pattern.sub(val_str, subject)
                                body = pattern.sub(val_str, body)

                            if existing:
                                if existing['status'] == 'pending':
                                    # SMART UPDATE: Refresh content but preserve the original date
                                    logger.info(f"  🔄 Updating pending Step {template['step_number']} for contact {contact['id']}")
                                    _sb.table('email_sequences').update({
                                        'template_id': template['id'],
                                        'subject': subject,
                                        'body': body,
                                        'project_id': proj_id
                                    }).eq('id', existing['id']).execute()
                                    created += 1
                                else:
                                    # PROTECTED: Already sent, replied, or cancelled. Skip.
                                    logger.info(f"  🛡️ Preserving {existing['status']} Step {template['step_number']} for contact {contact['id']}")
                                    continue
                            else:
                                # BRAND NEW STEP
                                if contact_status == 'replied':
                                    logger.info(f"  🔕 Skipping new Step {template['step_number']} — contact already replied.")
                                    continue

                                scheduled = base_date + timedelta(days=template.get('delay_days', 0))
                                logger.info(f"  ✨ Creating new Step {template['step_number']} for contact {contact['id']}")
                                _sb.table('email_sequences').insert({
                                    'project_id': proj_id,
                                    'contact_id': contact['id'],
                                    'template_id': template['id'],
                                    'step_number': template['step_number'],
                                    'subject': subject,
                                    'body': body,
                                    'status': 'pending',
                                    'scheduled_at': scheduled.isoformat(),
                                    'created_at': datetime.utcnow().isoformat()
                                }).execute()
                                created += 1
                        except Exception as step_e:
                            logger.error(f"Step {template.get('step_number')} for {contact.get('name')}: {step_e}")
                            errors += 1

                    _sb.table('contacts').update({
                        'status': 'in_sequence',
                        'updated_at': datetime.utcnow().isoformat()
                    }).eq('id', contact['id']).execute()
                    logger.info(f"Sequence created for contact: {contact.get('name')}")

                except Exception as contact_e:
                    errors += 1
                    logger.error(f"Failed for {contact.get('name', contact.get('id'))}: {contact_e}")

                return created, errors

            # ── PARALLEL: process up to 10 contacts concurrently ──
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = {pool.submit(process_contact, c): c for c in contacts_data}
                for fut in as_completed(futures):
                    try:
                        c, e = fut.result()
                        created_total += c
                        errors_total += e
                    except Exception as ex:
                        errors_total += 1
                        logger.error(f"Future error: {ex}")

            logger.info(f"Sequence creation done: {created_total} steps created, {errors_total} errors")


        # Launch background thread (daemon=False so it outlives the request)
        thread = threading.Thread(target=run_in_background, args=(project_id, contacts.data, templates.data), daemon=False)
        thread.start()

        return jsonify({
            'message': f'Started generating sequences for {len(contacts.data)} contacts in the background.',
            'status': 'processing'
        }), 202
    except Exception as e:
        logger.error(f"Create sequences error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/test', methods=['POST'])
def send_test_sequence():
    """Immediately send a test of Step 1 to provided email addresses."""
    try:
        data = request.json
        project_id = data.get('project_id')
        test_emails = data.get('test_emails', [])
        logger.info(f"Test send request: project_id={project_id}, test_emails={test_emails}")
        
        if not project_id:
            return jsonify({'error': 'Project ID required'}), 400
        if not test_emails:
            return jsonify({'error': 'At least one test email required'}), 400
            
        # Get Step 1 Template
        templates = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').limit(1).execute()
        if not templates.data:
            return jsonify({'error': 'No email templates found in this project to test.'}), 400
            
        template = templates.data[0]
        
        # Mock Variables
        variables = {
            'name': 'Test User',
            'first_name': 'Test',
            'company': 'ACME Corp',
            'bio': 'Example Bio: Creating innovative software solutions.',
            'icebreaker': 'I noticed your recent launch and was really impressed by the design.'
        }
        
        subject = template['subject_template']
        body = template['body_template']
        
        for key, val in variables.items():
            subject = subject.replace(f'{{{{{key}}}}}', val)
            body = body.replace(f'{{{{{key}}}}}', val)
            
        # Send via SMTP Pool
        from execution.smtp_pool import SMTPPool
        try:
            pool = SMTPPool()
        except ValueError as e:
            return jsonify({'error': str(e)}), 500
            
        results = []
        for to_email in test_emails:
            to_email = to_email.strip().rstrip('.,;:)!% ]').strip()
            if not to_email: continue
            
            account = pool.get_next_account()
            if not account:
                results.append({'email': to_email, 'success': False, 'error': 'No available SMTP accounts remaining'})
                continue
                
            res = pool.send_email(account, to_email, subject, body, dry_run=False)
            res['email'] = to_email
            results.append(res)
            
        return jsonify({'results': results})
        
    except Exception as e:
        logger.error(f"Test sequence error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/send', methods=['POST'])
def trigger_send():
    """Send pending scheduled emails."""
    try:
        data = request.json or {}
        limit = data.get('limit', 600)
        dry_run = data.get('dry_run', False)
        project_id = data.get('project_id')
        contact_ids = data.get('contact_ids') # For "Send Selected"
        
        def run_send():
            try:
                from execution.send_emails import send_pending_emails
                send_pending_emails(limit=limit, dry_run=dry_run, project_id=project_id, contact_ids=contact_ids)
            except Exception as e:
                logger.error(f"Background send error: {e}")

        thread = threading.Thread(target=run_send)
        thread.daemon = True
        thread.start()
        
        return jsonify({'status': 'started', 'message': 'Email dispatch started in background.'})
    except Exception as e:
        logger.error(f"Send trigger error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/check-replies', methods=['POST'])
def check_replies():
    """Check Gmail inboxes for prospect replies and auto-stop their sequences."""
    try:
        data = request.json or {}
        days = data.get('days', 7)

        from execution.check_replies import check_all_replies
        stats = check_all_replies(days=days)

        return jsonify(stats)
    except Exception as e:
        logger.error(f"Reply check error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/daily-run', methods=['POST'])
def trigger_daily_run():
    """Trigger the full daily workflow: check replies + send pending emails."""
    try:
        data = request.json or {}
        limit = data.get('limit', 600)
        dry_run = data.get('dry_run', False)
        project_id = data.get('project_id')

        def run_daily():
            try:
                from execution.daily_run import daily_run
                daily_run(limit=limit, dry_run=dry_run, project_id=project_id)
            except Exception as e:
                logger.error(f"Background daily run error: {e}")

        thread = threading.Thread(target=run_daily)
        thread.daemon = True
        thread.start()

        return jsonify({'status': 'started', 'message': 'Daily workflow (reply check + send) started in background.'})
    except Exception as e:
        logger.error(f"Daily run trigger error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/smtp-capacity', methods=['GET'])
def get_smtp_capacity():
    """Get today's total SMTP capacity and usage (Rolling 24h)."""
    try:
        from execution.smtp_pool import SMTPPool
        try:
            pool = SMTPPool()
            used = pool.get_total_usage()
            limit = pool.get_total_limit()
            return jsonify({'used': used, 'limit': limit})
        except Exception as e:
            logger.warning(f"Error loading SMTPPool for capacity check: {e}")
            return jsonify({'used': 0, 'limit': 0})
    except Exception as e:
        logger.error(f"Error fetching smtp capacity: {e}")
        return jsonify({'error': str(e)}), 500
@app.route('/api/search-runs')
def list_search_runs():
    """List search run history."""
    try:
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        result = supabase.table('search_runs').select('*').eq('project_id', project_id).order('created_at', desc=True).limit(20).execute()
        return jsonify({'runs': result.data or []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Import Leads from GrowthScout
# =============================================================================

import re as _import_re

def _clean_import_name(name):
    """Strip dash+noise suffixes and trailing punctuation from business names at import time."""
    if not name: return name
    name = name.strip()
    # Strip 'Team', 'Business', etc. with optional dash before them
    name = _import_re.sub(r'\s*[-\u2013\u2014]?\s*(Team|Business|Staff|Group|Page|Hub|Official)\s*$', '', name, flags=_import_re.IGNORECASE).strip()
    # Strip common legal suffixes
    name = _import_re.sub(r'\s*(LLC|Inc\.?|Corp\.?|Ltd\.?|LLP|Co\.?|Limited|Holdings|International|Services|Solutions|Enterprises|Associates|Consulting|Organization|Foundation)\s*$', '', name, flags=_import_re.IGNORECASE).strip()
    # Strip trailing dashes, pipes, commas, underscores
    name = name.strip(' -|,_').strip()
    return name or name

@app.route('/api/import-leads', methods=['POST'])
def import_leads():
    """Import pre-enriched leads from GrowthScout into Outreach contacts.
    
    Expects JSON body:
    {
        "project_id": "uuid",
        "leads": [
            {
                "name": "Business or Founder Name",
                "email": "contact@example.com",
                "company": "Business Name",
                "linkedin": "https://linkedin.com/in/...",
                "instagram": "@handle",
                "phone": "123-456-7890",
                "website": "https://example.com",
                "category": "dentist",
                "bio": "Analysis summary text",
                "enrichment_data": { ... }
            }
        ]
    }
    
    Contacts are inserted with status='enriched' so they skip the 
    enrichment step and go directly to icebreaker generation.
    """
    try:
        data = request.json
        project_id = data.get('project_id')
        leads = data.get('leads', [])
        
        if not project_id:
            return jsonify({'error': 'project_id is required'}), 400
        if not leads:
            return jsonify({'error': 'No leads provided'}), 400
        
        # Verify project exists
        project_check = supabase.table('projects').select('id').eq('id', project_id).execute()
        if not project_check.data:
            return jsonify({'error': f'Project {project_id} not found'}), 404
        
        # Fetch existing contacts in this project for comprehensive deduplication
        existing = supabase.table('contacts').select('name, email, linkedin_url, enrichment_data').eq('project_id', project_id).execute()
        existing_emails = set()
        existing_names = set()
        existing_linkedins = set()
        existing_websites = set()
        
        for row in (existing.data or []):
            if row.get('email'): 
                existing_emails.add(row['email'].lower())
            if row.get('name'): 
                existing_names.add(row['name'].lower().strip())
            if row.get('linkedin_url'): 
                existing_linkedins.add(row['linkedin_url'].lower().rstrip('/'))
            
            # Check website in enrichment_data
            ed = row.get('enrichment_data')
            if ed:
                if isinstance(ed, str):
                    try: ed = json.loads(ed)
                    except: ed = {}
                w = ed.get('website')
                if w:
                    existing_websites.add(w.lower().rstrip('/'))
        
        imported = 0
        skipped_duplicate = 0
        skipped_no_contact = 0
        errors = 0
        
        contacts_to_insert = []
        
        for lead in leads:
            email = (lead.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
            name = (lead.get('name') or '').strip()
            linkedin = (lead.get('linkedin') or '').strip().lower().rstrip('/')
            website = (lead.get('website') or '').strip().lower().rstrip('/')
            
            # Skip if no email AND no phone AND no instagram AND no linkedin — truly no way to reach them
            if not email and not lead.get('phone') and not lead.get('instagram') and not linkedin:
                skipped_no_contact += 1
                continue
            
            # Deduplicate: by email, linkedin, website, or name
            is_dupe = False
            if email and email.lower() in existing_emails:
                is_dupe = True
            elif linkedin and linkedin in existing_linkedins:
                is_dupe = True
            elif website and website in existing_websites:
                is_dupe = True
            elif not email and not linkedin and name and name.lower().strip() in existing_names:
                # Name dedup is a fallback, we only use it if no stronger identifiers are present
                is_dupe = True
                
            if is_dupe:
                skipped_duplicate += 1
                continue

            # Add to sets for internal batch dedup
            if email: existing_emails.add(email.lower())
            if linkedin: existing_linkedins.add(linkedin)
            if website: existing_websites.add(website)
            if name: existing_names.add(name.lower().strip())
            
            # Build enrichment_data JSON with all the extra GrowthScout data
            enrichment = lead.get('enrichment_data', {})
            if not isinstance(enrichment, dict):
                enrichment = {}
            
            # Store GrowthScout-specific data in enrichment_data
            enrichment['source_app'] = 'growthscout'
            if lead.get('company'):
                enrichment['company'] = lead['company']
            if lead.get('website'):
                enrichment['website'] = lead['website']
            if lead.get('phone'):
                enrichment['phone'] = lead['phone']
            if lead.get('pagespeed_mobile') is not None:
                enrichment['pagespeed_mobile'] = lead['pagespeed_mobile']
            if lead.get('pagespeed_desktop') is not None:
                enrichment['pagespeed_desktop'] = lead['pagespeed_desktop']
            if lead.get('audit_data'):
                enrichment['audit_data'] = lead['audit_data']
            if lead.get('analysis_bullets'):
                enrichment['analysis_bullets'] = lead['analysis_bullets']
            # Location: top-level field OR nested in enrichment_data.search_location
            location_val = (lead.get('location') or 
                           enrichment.get('search_location') or 
                           enrichment.get('location') or '').strip()
            if location_val:
                enrichment['location'] = location_val
            # Niche: top-level 'niche' OR 'category'
            niche_val = (lead.get('niche') or lead.get('category') or 
                        enrichment.get('niche') or enrichment.get('category') or '').strip()
            if niche_val and niche_val.lower() not in ('unknown', 'growthscout', ''):
                enrichment['niche'] = niche_val
            
            # Clean the contact name at import time
            clean_name = _clean_import_name(name) or name
            
            contact = {
                'project_id': project_id,
                'name': clean_name if clean_name else lead.get('name', 'Unknown'),
                'email': email,
                'bio': lead.get('bio', ''),
                'linkedin_url': lead.get('linkedin') or None,
                'instagram': lead.get('instagram') or None,
                'source': niche_val or lead.get('category') or 'growthscout',
                'status': 'enriched' if email else 'new',
                'location': location_val or None,
                'niche': niche_val or None,
                'enrichment_data': enrichment,
            }
            
            contacts_to_insert.append(contact)
        
        # Bulk insert in batches
        batch_size = 500
        for i in range(0, len(contacts_to_insert), batch_size):
            batch = contacts_to_insert[i:i + batch_size]
            try:
                supabase.table('contacts').insert(batch).execute()
                imported += len(batch)
            except Exception as e:
                logger.error(f"Batch insert error: {e}")
                errors += len(batch)
        
        return jsonify({
            'imported': imported,
            'skipped': skipped_duplicate + skipped_no_contact,
            'skipped_duplicate': skipped_duplicate,
            'skipped_no_contact': skipped_no_contact,
            'errors': errors,
            'total_received': len(leads)
        })
    except Exception as e:
        logger.error(f"Import leads error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# SEED — Email Templates
# =============================================================================

@app.route('/api/seed-templates', methods=['POST'])
def seed_templates():
    """Seed 12-step drip email templates using Gemini AI based on the project description."""
    try:
        data = request.json or {}
        project_id = data.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        
        # Check if templates already exist
        existing = supabase.table('email_templates').select('id', count='exact').eq('project_id', project_id).execute()
        if existing.count and existing.count > 0:
            return jsonify({'message': f'Templates already seeded ({existing.count} exist)', 'count': existing.count})
        
        # Fetch the project to get its description
        project = supabase.table('projects').select('name, description').eq('id', project_id).execute()
        project_data = project.data[0] if project.data else {}
        project_name = project_data.get('name', 'Outreach Campaign')
        description = project_data.get('description', '')
        
        # Try Gemini AI generation first
        templates_to_insert = []
        if GEMINI_API_KEY and description:
            try:
                system = f"""You are an elite cold-email copywriter for a project named "{project_name}".
                The project is described as: "{description}".
                
                Generate a 4-step cold email drip sequence tailored to this exact business description.
                Create exactly 4 steps.
                
                CRITICAL COLD EMAIL RULES:
                - NEVER include any links, URLs, or attachments in ANY step
                - The goal of every email is to get a REPLY, not a click
                - The CTA must ALWAYS be a variation of "Want me to send the full report?" or "Can I share the details?"
                - Keep emails SHORT (3-5 sentences max for the body)
                - Professional but direct tone
                
                IMPORTANT — UNDERSTANDING THE VARIABLES:
                - {{{{icebreaker}}}} is a WARM PERSONALIZED INTRO about the prospect's business. It is NOT about issues or problems. It contains researched info about what the business does, their recent work, accomplishments, etc. Use it as the opening line to show you've done your homework on THEM.
                - After the icebreaker, the email body should transition into GENERIC issue findings relevant to the project niche (e.g. slow page speed, missing meta tags, low engagement, competitors outperforming them). These findings are STATIC TEXT in the template — do NOT put them in {{{{icebreaker}}}}.
                - {{{{first_name}}}}, {{{{name}}}}, {{{{company}}}} are standard contact variables.
                
                TEMPLATE STRUCTURE FOR STEP 1:
                1. Open with {{{{icebreaker}}}} as a warm, personalized greeting showing you know their business
                2. Transition to generic but scary findings relevant to the niche (page speed, missing tags, competitor gaps, etc.)
                3. Close with a permission-based CTA asking if they want the full report
                
                You MUST return the output as a SINGLE VALID JSON ARRAY of exactly 4 objects.
                Each object MUST have three exact keys: 
                - "name" (a short internal name, e.g. "Intro", "Follow up 1", "Nudge", "Break up")
                - "subject_template" (the email subject line)
                - "body_template" (the email body)
                
                Step 1 MUST include the exact text "{{{{icebreaker}}}}" in its body_template as the opening.
                Steps 2-3 should be short follow-ups that re-emphasize the value of the report.
                Step 4 should be a polite break-up email.
                Return ONLY the raw JSON array. Do not wrap it in markdown block quotes."""
                
                client = genai.Client(api_key=GEMINI_API_KEY)
                response = client.models.generate_content(
                    model='gemini-2.5-pro',
                    contents=system,
                )
                
                content = response.text.strip()
                if '```json' in content: content = content.split('```json')[1].split('```')[0].strip()
                elif '```' in content: content = content.split('```')[1].split('```')[0].strip()
                
                steps = json.loads(content)
                
                delays = [0, 3, 7, 14]
                
                for i, step in enumerate(steps[:4]):
                    templates_to_insert.append({
                        'project_id': project_id,
                        'name': step.get('name', f'Step {i+1}'),
                        'step_number': i + 1,
                        'subject_template': step.get('subject_template', f'Follow up {i}'),
                        'body_template': step.get('body_template', 'Placeholder body'),
                        'delay_days': delays[i] if i < len(delays) else 14
                    })
                    
            except Exception as ai_e:
                logger.error(f"Gemini template generation failed: {ai_e}")
                templates_to_insert = []  # Fall through to fallback
        
        # Fallback: generic 4-step sequence if Gemini fails or no description
        if not templates_to_insert:
            delays = [0, 3, 7, 14]
            fallback_steps = [
                {'name': 'Introduction', 'subject_template': 'Quick question about {{company}}', 'body_template': 'Hi {{first_name}},\n\n{{icebreaker}}\n\nWhile looking into {{company}}, I noticed a few things that might be costing you customers — slow page load times, missing meta tags, and some SEO gaps your competitors are already capitalizing on.\n\nI put together a quick report with the specific findings. Want me to send it over?\n\nBest,\nBipul'},
                {'name': 'Nudge', 'subject_template': 'The report for {{company}} is ready', 'body_template': 'Hi {{first_name}},\n\nJust a quick follow-up — the report I mentioned for {{company}} is ready to go. Happy to share whenever you like.\n\nCheers,\nBipul'},
                {'name': 'Value Reminder', 'subject_template': 'One more thing about {{company}}', 'body_template': 'Hi {{first_name}},\n\nI noticed a couple more things while reviewing {{company}} that are costing you traffic and leads. Worth a quick look.\n\nShall I send the full breakdown?\n\nBest,\nBipul'},
                {'name': 'Break Up', 'subject_template': 'Should I close your file, {{first_name}}?', 'body_template': 'Hi {{first_name}},\n\nHaven\'t heard back so I\'m guessing the timing isn\'t right. Totally understand.\n\nThe report won\'t expire — just reply whenever you\'d like me to send it.\n\nWishing you the best,\nBipul'},
            ]
            for i, step in enumerate(fallback_steps):
                templates_to_insert.append({
                    'project_id': project_id,
                    'name': step['name'],
                    'step_number': i + 1,
                    'subject_template': step['subject_template'],
                    'body_template': step['body_template'],
                    'delay_days': delays[i] if i < len(delays) else 14
                })
        
        # Insert all templates
        supabase.table('email_templates').insert(templates_to_insert).execute()
        
        return jsonify({'message': f'Generated {len(templates_to_insert)} email templates', 'count': len(templates_to_insert)})
    except Exception as e:
        logger.error(f"Seed error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)
