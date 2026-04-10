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
from execution.log_manager import JobLogger
from execution.smtp_pool import SMTPPool

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
    # Check if niche column exists on projects
    try:
        supabase.table('projects').select('niche').limit(1).execute()
        logger.info("Projects.niche column exists")
    except Exception:
        logger.warning("Projects table missing 'niche' column — run: ALTER TABLE projects ADD COLUMN niche TEXT;")
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

@app.route('/api/sender-groups', methods=['GET'])
def list_sender_groups():
    """List distinct sender groups currently configured in .env."""
    try:
        from execution.smtp_pool import SMTPPool
        pool = SMTPPool()
        groups = set(a.group for a in pool.accounts)
        groups.add("all")
        return jsonify({'groups': sorted(list(groups))})
    except Exception as e:
        logger.error(f"Failed to list sender groups: {e}")
        return jsonify({'groups': ["all"]})

@app.route('/api/projects/with-stats', methods=['GET'])
def list_projects_with_stats():
    """List all projects with lead counts."""
    try:
        projects = supabase.table('projects').select('*').order('created_at', desc=True).execute()
        # Single query: fetch contact project_ids, email, instagram and count in Python
        contacts = supabase.table('contacts').select('project_id, email, instagram').execute()
        lead_counts = {}
        email_counts = {}
        ig_counts = {}
        for c in (contacts.data or []):
            pid = c.get('project_id')
            if pid:
                lead_counts[pid] = lead_counts.get(pid, 0) + 1
                if c.get('email'):
                    email_counts[pid] = email_counts.get(pid, 0) + 1
                if c.get('instagram'):
                    ig_counts[pid] = ig_counts.get(pid, 0) + 1
        
        result = []
        for p in (projects.data or []):
            result.append({
                'id': p['id'],
                'name': p.get('name', ''),
                'created_at': p.get('created_at', ''),
                'sender_group': p.get('sender_group', 'all'),
                'lead_count': lead_counts.get(p['id'], 0),
                'email_count': email_counts.get(p['id'], 0),
                'ig_count': ig_counts.get(p['id'], 0)
            })
        return jsonify({'projects': result})
    except Exception as e:
        logger.error(f"Projects with stats error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<project_id>', methods=['PATCH'])
def update_project(project_id):
    """Update a project (name or sender_group)."""
    try:
        data = request.json
        updates = {}
        if 'name' in data:
            name = data.get('name', '').strip()
            if not name:
                return jsonify({'error': 'Name cannot be empty'}), 400
            updates['name'] = name
        if 'sender_group' in data:
            updates['sender_group'] = data.get('sender_group', 'all').strip()
        if 'niche' in data:
            updates['niche'] = (data.get('niche') or '').strip() or None

        if not updates:
            return jsonify({'success': True})

        supabase.table('projects').update(updates).eq('id', project_id).execute()
        return jsonify({'success': True, **updates})
    except Exception as e:
        logger.error(f"Update project error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete a project and all its associated contacts and sequences."""
    try:
        # Delete sequences for contacts in this project
        supabase.table('email_sequences').delete().eq('project_id', project_id).execute()
        # Delete email templates
        supabase.table('email_templates').delete().eq('project_id', project_id).execute()
        # Delete search runs
        supabase.table('search_runs').delete().eq('project_id', project_id).execute()
        # Delete contacts
        supabase.table('contacts').delete().eq('project_id', project_id).execute()
        # Delete the project itself
        supabase.table('projects').delete().eq('id', project_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Delete project error: {e}")
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
        niche = data.get('niche', '').strip()
        insert_data = {
            'name': name, 
            'description': description,
            'custom_instructions': custom_instructions,
            'sender_group': 'all'
        }
        if niche:
            insert_data['niche'] = niche
        result = supabase.table('projects').insert(insert_data).execute()
        
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

        # Helper: count rows matching a status using server-side COUNT (avoids 1k row limit)
        def ccount(table, status=None):
            q = supabase.table(table).select('id', count='exact').eq('project_id', project_id)
            if status:
                q = q.eq('status', status)
            return q.execute().count or 0

        # Contact counts
        total     = ccount('contacts')
        c_new     = ccount('contacts', 'new')
        enriched  = ccount('contacts', 'enriched')
        icebr     = ccount('contacts', 'icebreaker_ready')
        in_seq    = ccount('contacts', 'in_sequence')
        replied_c = ccount('contacts', 'replied')
        completed = ccount('contacts', 'completed')
        bounced_c = ccount('contacts', 'bounced')

        # Email sequence counts
        total_seq = ccount('email_sequences')
        pending   = ccount('email_sequences', 'pending')
        sent      = ccount('email_sequences', 'sent')
        opened    = ccount('email_sequences', 'opened')

        return jsonify({
            'contacts': {
                'total': total,
                'new': c_new,
                'enriched': enriched,
                'icebreaker_ready': icebr,
                'in_sequence': in_seq,
                'replied': replied_c,
                'completed': completed,
            },
            'emails': {
                'total': total_seq,
                'pending': pending,
                'sent': sent,
                'opened': opened,
                'replied': replied_c,   # contact-based source of truth
                'bounced': bounced_c,   # contact-based source of truth
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
                is_full_url = ig_str.lower().startswith('http') or ig_str.lower().startswith('www.')
                if is_full_url:
                    ig_url = ig_str if ig_str.startswith('http') else f'https://{ig_str}'
                    # Extract handle for display
                    for prefix in ['https://www.instagram.com/', 'http://www.instagram.com/', 'https://instagram.com/', 'http://instagram.com/']:
                        if ig_str.lower().startswith(prefix):
                            ig_str = ig_str[len(prefix):]
                            break
                    clean_ig = ig_str.rstrip('/').replace('@', '') if ig_str else None
                else:
                    clean_ig = ig_str.replace('@', '').strip()
                    ig_url = f'https://instagram.com/{clean_ig}'
            else:
                clean_ig = None
                ig_url = None
            
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
                'ig_url': ig_url,
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
        def build_query():
            if status == 'REPLIED':
                q = supabase.table('contacts').select('*, replies(sender_email, body, sentiment, received_at)', count='exact').eq('project_id', project_id)
            else:
                q = supabase.table('contacts').select('*', count='exact').eq('project_id', project_id)
            
            if status:
                q = q.eq('status', status)
            
            if search:
                q = q.or_(f"name.ilike.%{search}%,bio.ilike.%{search}%,email.ilike.%{search}%")
                
            return q.order('created_at', desc=True)

        all_data = []
        total_count = 0
        curr_offset = offset
        
        while len(all_data) < limit:
            chunk_size = min(limit - len(all_data), 1000)
            res = build_query().range(curr_offset, curr_offset + chunk_size - 1).execute()
            
            if len(all_data) == 0:
                total_count = res.count or 0
                
            chunk_data = res.data or []
            if not chunk_data:
                break
                
            all_data.extend(chunk_data)
            curr_offset += len(chunk_data)
            
            if len(chunk_data) < chunk_size:
                break
        
        return jsonify({
            'contacts': all_data,
            'total': total_count,
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
        allowed = ['name', 'company', 'bio', 'linkedin_url', 'email', 'instagram',
                   'website', 'phone', 'icebreaker', 'status', 'notes']
        update_data = {k: v for k, v in data.items() if k in allowed}
        update_data['updated_at'] = datetime.utcnow().isoformat()
        
        result = supabase.table('contacts').update(update_data).eq('id', contact_id).execute()
        return jsonify({'contact': result.data[0]})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/bulk_update', methods=['POST'])
def bulk_update_contacts():
    """Bulk update fields for multiple contacts."""
    try:
        data = request.json
        updates = data.get('updates', [])
        
        if not updates:
            return jsonify({'error': 'No updates provided'}), 400

        allowed = ['name', 'company', 'bio', 'linkedin_url', 'email', 'instagram',
                   'website', 'phone', 'icebreaker', 'status', 'notes']
        
        updated_count = 0
        from concurrent.futures import ThreadPoolExecutor
        
        def process_update(u):
            c_id = u.get('id')
            if not c_id: return False
            
            update_data = {k: v for k, v in u.items() if k in allowed and k != 'id'}
            if not update_data: return False
            
            update_data['updated_at'] = datetime.utcnow().isoformat()
            try:
                supabase.table('contacts').update(update_data).eq('id', c_id).execute()
                return True
            except Exception as ex:
                print(f"Error bulk updating {c_id}: {ex}")
                return False
                
        # Run updates in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(process_update, updates))
            
        updated_count = sum(1 for r in results if r)
        
        return jsonify({'message': 'Bulk update processed', 'updated': updated_count, 'total': len(updates)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/add', methods=['POST'])
def add_contact_manual():
    """Manually add a single contact."""
    try:
        data = request.json
        project_id = request.args.get('project_id') or data.get('project_id')
        if not project_id:
            return jsonify({'error': 'project_id required'}), 400

        name = (data.get('name') or '').strip()
        email = (data.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
        linkedin_url = (data.get('linkedin_url') or '').strip()

        if not name and not email:
            return jsonify({'error': 'At least a name or email is required'}), 400

        # Helper for domain extraction
        from urllib.parse import urlparse
        def _get_domain(url_str):
            if not url_str: return ""
            try:
                nl = urlparse(url_str).netloc.lower()
                if nl.startswith('www.'): nl = nl[4:]
                parts = nl.split('.')
                return ".".join(parts[-2:]) if len(parts) >= 2 else nl
            except: return ""

        new_domain = ""
        if email and '@' in email:
            new_domain = email.split('@')[-1].lower()
        elif data.get('website'):
            new_domain = _get_domain(data.get('website'))

        GENERIC_DOMAINS = {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com', 'aol.com', 'me.com', 'msn.com', 'live.com'}

        # Dedup check
        existing = supabase.table('contacts').select('id, name, email, linkedin_url, website').eq('project_id', project_id).execute()
        for row in (existing.data or []):
            if email and row.get('email') and row['email'].lower() == email.lower():
                return jsonify({'error': f'A contact with email {email} already exists'}), 409
            if linkedin_url and row.get('linkedin_url') and row['linkedin_url'].lower().rstrip('/') == linkedin_url.lower().rstrip('/'):
                return jsonify({'error': f'A contact with that LinkedIn URL already exists'}), 409
                
            # Domain dedupe
            if new_domain and new_domain not in GENERIC_DOMAINS:
                row_domain = ""
                row_email = row.get('email')
                if row_email and '@' in row_email:
                    row_domain = row_email.split('@')[-1].lower()
                elif row.get('website'):
                    row_domain = _get_domain(row.get('website'))
                    
                if row_domain == new_domain:
                    return jsonify({'error': f'A contact from the company domain {new_domain} already exists in this project'}), 409

        # Build enrichment_data
        enrichment = {}
        if data.get('company'):
            enrichment['company'] = data['company']
        if data.get('website'):
            enrichment['website'] = data['website']
        if data.get('phone'):
            enrichment['phone'] = data['phone']
        if data.get('location'):
            enrichment['location'] = data['location']
        if data.get('niche'):
            enrichment['niche'] = data['niche']
        enrichment['source_app'] = 'manual'

        contact = {
            'project_id': project_id,
            'name': name or 'Unknown',
            'email': email or None,
            'bio': (data.get('bio') or '').strip() or None,
            'linkedin_url': linkedin_url or None,
            'instagram': (data.get('instagram') or '').strip() or None,
            'source': data.get('niche') or 'manual',
            'status': 'enriched' if email else 'new',
            'location': (data.get('location') or '').strip() or None,
            'niche': (data.get('niche') or '').strip() or None,
            'company': (data.get('company') or '').strip() or None,
            'enrichment_data': enrichment,
        }

        result = supabase.table('contacts').insert(contact).execute()
        return jsonify({'contact': result.data[0] if result.data else None, 'message': 'Contact added successfully'})
    except Exception as e:
        logger.error(f"Add contact error: {e}")
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
            
        for i in range(0, len(contact_ids), 100):
            chunk = contact_ids[i:i+100]
            supabase.table('contacts').delete().in_('id', chunk).execute()
            
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
@app.route('/api/contacts/apify-people-search', methods=['POST'])
@cross_origin()
def run_apify_people_search():
    try:
        data = request.json or {}
        project_id = data.get('project_id')
        params = {k: v for k, v in data.items() if k != 'project_id'}

        if not project_id:
            return jsonify({'error': 'Project ID is required'}), 400

        job_logger = JobLogger("Apify Leads Search")
        
        def _run():
            try:
                from execution.apify_leads_finder import run_apify_leads_search
                total_stats = run_apify_leads_search(params, project_id=project_id, job_logger=job_logger)
                if total_stats:
                    msg = f"Done — {total_stats.get('inserted', 0)} inserted, {total_stats.get('skipped', 0)} skipped."
                    logger.info(f"[ApifyPeopleSearch] {msg}")
                    job_logger.complete()
                else:
                    job_logger.error("Apify search returned no stats (failed).")
                    job_logger.complete(status='failed')
            except Exception as e:
                logger.error(f"Async apify people search error: {e}")
                job_logger.error(f"Failed: {str(e)}")
                job_logger.complete(status='failed')

        thread = threading.Thread(target=_run)
        thread.start()
        
        return jsonify({
            'success': True, 
            'message': 'Apify Leads search started. Monitoring progress in Live Logs.',
            'job_id': job_logger.job_id
        })

    except Exception as e:
        logger.error(f"Search API error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/contacts/business-search', methods=['POST'])
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


@app.route('/api/contacts/enrich_company', methods=['POST'])
def trigger_company_enrichment():
    """Trigger Jina/Gemini company enrichment for selected contacts in background."""
    try:
        data = request.json or {}
        limit = data.get('limit', 500)
        contact_ids = data.get('contact_ids', [])
        project_id = data.get('project_id')
        custom_prompt = data.get('custom_prompt')
        
        import threading
        from execution.enrich_company import enrich_companies_bulk
        
        def run_company_enrichment_task():
            logger.info(f"Starting background company enrichment task (limit={limit}, project={project_id}, ids_received={len(contact_ids)})")
            enrich_companies_bulk(limit=limit, project_id=project_id, contact_ids=contact_ids, custom_prompt=custom_prompt)
            logger.info("Background company enrichment task complete.")

        thread = threading.Thread(target=run_company_enrichment_task)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': f'Company enrichment started in background for {len(contact_ids) if contact_ids else limit} contacts'
        })
    except Exception as e:
        logger.error(f"Company Enrichment error: {e}")
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
        force = data.get('force', False)
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
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import json as _json

            _sb = _create_client(SUPABASE_URL, effective_key)
            job_in_mem = _verify_jobs[job_id]
            job = JobLogger("Verify Emails")
            try:
                job.info(f"Starting verification task for {len(contact_ids)} contacts...")
                all_contacts_data = []
                chunk_size = 100
                for i in range(0, len(contact_ids), chunk_size):
                    chunk = contact_ids[i:i + chunk_size]
                    chunk_res = _sb.table('contacts').select('id, email, company, enrichment_data').in_('id', chunk).execute()
                    if chunk_res.data:
                        all_contacts_data.extend(chunk_res.data)

                if not all_contacts_data:
                    job_in_mem['status'] = 'done'
                    return

                # Filter out contacts with no email, or already verified
                # Original behavior: preserve existing verification results unless force=True
                to_verify = []
                risky_contacts = []
                already_verified = 0
                for c in all_contacts_data:
                    enrichment_data = c.get('enrichment_data') or {}
                    if isinstance(enrichment_data, str):
                        try: enrichment_data = _json.loads(enrichment_data)
                        except: enrichment_data = {}
                    
                    c['enrichment_data'] = enrichment_data # Save parsed dict for later
                    
                    v_status = enrichment_data.get('verification_status')
                    
                    if not c.get('email'):
                        job_in_mem['done'] += 1
                        job_in_mem['skipped'] += 1
                    elif v_status and not force:
                        # Already has a verification result
                        if v_status == 'risky' and enrichment_data.get('serper_verified') is None:
                            # It's risky but never had OSINT run — push straight to OSINT
                            risky_contacts.append(c)
                        
                        already_verified += 1
                        job_in_mem['done'] += 1
                    else:
                        # No status yet, or force=True → verify
                        to_verify.append(c)
                
                if already_verified > 0:
                    job.info(f"Skipping {already_verified} already-verified contacts (use force=True to re-verify)")

                def verify_one(c):
                    email = c['email']
                    enrichment_data = c.get('enrichment_data') or {}
                    
                    logger.info(f"Manual Verification: Checking {email} for contact {c['id']}")
                    v_status, v_reason = check_email(email)
                    enrichment_data['verification_status'] = v_status
                    enrichment_data['verification_reason'] = v_reason
                    return c['id'], email, v_status, enrichment_data

                # Run up to 20 verifications concurrently (SMTP is I/O-bound, threads help a lot)
                MAX_WORKERS = 20
                
                valid_count = 0
                risky_count = 0
                skipped_count = 0
                done_count = 0
                risky_contacts = []
                
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {executor.submit(verify_one, c): c for c in to_verify}
                    for future in as_completed(futures):
                        try:
                            contact_id, email, v_status, enrichment_data = future.result()
                            if v_status == 'invalid':
                                logger.warning(f"  ❌ Verification failed for {email}. Clearing email.")
                                _sb.table('contacts').update({
                                    'email': None,
                                    'status': 'skipped',
                                    'enrichment_data': enrichment_data
                                }).eq('id', contact_id).execute()
                                skipped_count += 1
                            else:
                                job.info(f"Verified {email}: {v_status.upper()}")
                                # Save the results to Supabase IMMEDIATELY for SMTP probe
                                _sb.table('contacts').update({
                                    'enrichment_data': enrichment_data
                                }).eq('id', contact_id).execute()
                                
                                if v_status == 'risky':
                                    risky_count += 1
                                    # Track risky contacts for OSINT fallback
                                    c_obj = next((c for c in to_verify if c['id'] == contact_id), None)
                                    if c_obj:
                                        c_obj['enrichment_data'] = enrichment_data
                                        risky_contacts.append(c_obj)
                                else:
                                    valid_count += 1
                                        
                        except Exception as e:
                            logger.error(f"Verification worker error: {e}")
                            skipped_count += 1
                        finally:
                            done_count += 1

                # === OSINT FALLBACK (SERPER.DEV) ===
                osint_recovered = 0
                osint_dropped = 0
                if risky_contacts:
                    job.info(f"Running Google OSINT fallback for {len(risky_contacts)} risky leads...")
                    try:
                        from execution.serper_fallback import verify_risky_contacts_bulk
                        osint_recovered, osint_dropped = verify_risky_contacts_bulk(risky_contacts, _sb, job_logger=job)
                        job.info(f"OSINT complete: ✅ Recovered {osint_recovered} | 🚫 Dropped {osint_dropped}")
                    except Exception as e:
                        logger.error(f"Failed to run Serper OSINT fallback: {e}")
                        job.info(f"OSINT fallback failed: {e}")

                # Final status summary
                total_approved = valid_count + osint_recovered
                summary = f"Verification complete. Total Approved: {total_approved}"
                if osint_recovered > 0:
                    summary += f" (✅ {valid_count} SMTP + 🛡️ {osint_recovered} OSINT)"
                if osint_dropped > 0:
                    summary += f" | 🚫 Dropped: {osint_dropped}"
                if skipped_count > 0:
                    summary += f" | ⚠️ Skipped: {skipped_count}"
                
                job.success(summary)
                job.complete('completed')

            except Exception as e:
                logger.error(f"Background verification failed: {e}")
                job.error(f"Verification failed: {e}")
                job.complete('failed')

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


@app.route('/api/templates', methods=['POST'])
def add_template():
    """Add a new blank template to the end of a project's sequence."""
    try:
        data = request.json
        project_id = data.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        
        # Get highest current step number
        res = supabase.table('email_templates').select('step_number').eq('project_id', project_id).order('step_number', desc=True).limit(1).execute()
        next_step = 1
        if res.data:
            next_step = res.data[0].get('step_number', 0) + 1
            
        new_template = {
            'project_id': project_id,
            'step_number': next_step,
            'name': f'Step {next_step} (Manual)',
            'subject_template': '',
            'body_template': '',
            'delay_days': 14
        }
        
        insert_res = supabase.table('email_templates').insert(new_template).execute()
        return jsonify({'template': insert_res.data[0] if insert_res.data else None})
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
        contact_id = request.args.get('contact_id')
        status = request.args.get('status')
        
        def build_seq_query():
            q = supabase.table('email_sequences').select('*, contacts(name, email)').eq('project_id', project_id)
            if contact_id:
                q = q.eq('contact_id', contact_id)
            if status:
                q = q.eq('status', status)
            return q.order('created_at', desc=True)

        # Paginate through 1000-row chunks (Supabase hard caps at 1000 per request)
        all_data = []
        offset = 0
        while True:
            chunk = build_seq_query().range(offset, offset + 999).execute()
            chunk_data = chunk.data or []
            all_data.extend(chunk_data)
            if len(chunk_data) < 1000:
                break
            offset += 1000

        return jsonify({'sequences': all_data})
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

def paraphrase_texts_batch(bodies: list, context: dict = None, company_context: dict = None, personalization_prompt: str = None) -> list:
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

        spam_words = """$$$, €€€, £££, 50% off, A few bob, Accept cash cards, Accept credit cards, Affordable, Affordable deal, Avoid bankruptcy, Bad credit, Bank, Bankruptcy, Bargain, Billing, Billing address, Billion, Billion dollars, Billionaire, Card accepted, Cards accepted, Cash, Cash bonus, Cash out, Cash-out, Cashcashcash, Casino, Cents on the dollar, Check, Check or money order, Claim your discount, Cost, Costs, Credit, Credit bureaus, Credit card, Credit card offers, Credit or Debit, Deal, Debt, Discount, Dollars, Double your, Double your wealth, Earn, Earn $, Earn cash, Earn extra income, Earn from home, Earn monthly, Earn per month, Earn per week, Earn per year, Easy income, Easy terms, F r e e, For free, For just $, Free access, Free consultation, Free gift, Free hosting, Free info, Free investment, Free membership, Free money, Free preview, Free quote, Free trial, Full refund, Get out of debt, Giveaway, Guaranteed deposit, Increase revenue, Increase sales/traffic, Instant earnings, Instant income, Insurance, Investment, Investment advice, Loans, Make $, Money-back guarantee, Mortgage, Mortgage rates, Offer, One hundred percent free, Only $, Price, Price protection, Profits, Quote, Refinance, Save $, Save big money, Subject to credit, US Dollars, Why pay more?, Your income, 100% guaranteed, Access now, Act fast, Amazing deal, Apply now, As seen on, Best deal, Big profit, Can’t miss, Click below, Click here, Deal ending soon, Don’t delete, Double your money, Exclusive deal, Fantastic offer, Free membership, Get it now, Great news, Guaranteed results, Important information, Increase sales, Instant savings, Limited time, Must read, New customers only, No catch, No cost, No credit check, No obligation, No strings attached, Once in a lifetime, Only available here, Order now, Potential earnings, Pure profit, Risk-free, Special invitation, Special offer, This won’t last, Urgent, Will not believe, #1, 100% free, 100% off, 100% satisfied, Additional income, Amazed, Amazing, Amazing deal, Amazing offer, Amazing stuff, Be amazed, Be surprised, Be your own boss, Best bargain, Best deal, Best offer, Best price, Best rates, Big bucks, Bonus, Can’t live without, Consolidate debt, Double your cash, Double your income, Drastically reduced, Earn extra cash, Earn money, Expect to earn, Extra, Extra cash, Extra income, Fantastic, Fantastic deal, Fantastic offer, Fast cash, Financial freedom, Free priority mail, Get paid, Incredible deal, Join millions, Lowest price, Make money, Million dollars, Money-back guarantee, Prize, Promise, Pure profit, Risk-free, Satisfaction guaranteed, Save up to, Special promotion, The best, Thousands, Unbeatable offer, Unbelievable, Unlimited, Wonderful, You will not believe your eyes, Access, Access now, Act, Act immediately, Act now, Act now!, Action, Action required, Apply here, Apply now, Apply now!, Apply online, Become a member, Before it’s too late, Being a member, Buy, Buy direct, Buy now, Buy today, Call, Call free, Call free/now, Call me, Call now, Call now!, Can we have a minute of your time?, Cancel now, Cancellation required, Claim now, Click, Click below, Click here, Click me to download, Click now, Click this link, Click to get, Click to remove, Contact us immediately, Deal ending soon, Do it now, Do it today, Don’t delete, Don’t hesitate, Don’t waste time, Exclusive deal, Expire, Expires today, Final call, For instant access, For Only, For you, Friday before [holiday], Get it away, Get it now, Get now, Get paid, Get started, Get started now, Great offer, Hurry up, Immediately, Info you requested, Information you requested, Instant, Limited time, New customers only, Now, Now only, Offer expires, Once in lifetime, Only, Order now, Order today, Please read, Purchase now, Sign up free, Sign up free today, Supplies are limited, Take action, Take action now, This won’t last, Time limited, Today, Top urgent, Trial, Urgent, What are you waiting for?, While supplies last, You are a winner, 100% natural, All natural, Best price, Certified organic, Clinical trial, Cure for, Diet pill, Doctor recommended, Double blind study, Fat burner, Fast weight loss, Free consultation, Get slim, Guaranteed weight loss, Hair growth, Lose weight fast, Medical breakthrough, Miracle cure, Money-back guarantee, Natural remedy, No prescription needed, Online pharmacy, Over-the-counter, Pain relief, Prescription drugs, Reverse aging, Safe and effective, Scientifically proven, Secret formula, Weight loss, Youthful skin, Access your account, Account update, Action required, Activate now, Antivirus, Change password, Click to verify, Confirm your details, Confidential information, Cyber Monday, Data breach, Download now, Final notice, Free antivirus, Free trial, Important update, Immediate action required, Improve security, Install now, Last warning, Log in now, New login detected, Online account, Password reset, Payment details needed, Phishing alert, Secure payment, Security breach, Security update, Update account, Verify identity, Warning message, Adult content, Bet now, Big win, Blackjack, Casino bonus, Cash out now, Click to win, Double your money, Exclusive access, Free chips, Free spins, Gamble online, Hot deal, Instant winnings, Jackpot, Live dealer, Lottery winner, Lucky chance, Online betting, Online casino, Online gaming, Poker tournament, Risk-free bet, Slots jackpot, Spin to win, Try for free, VIP offer, Winner announced, Winning numbers, XXX"""

        system = f"""You are an expert cold email copywriter. You will receive {len(bodies)} email bodies numbered EMAIL_1 through EMAIL_{len(bodies)}.

For EACH email:
- Rewrite so it sounds genuinely fresh (restructure sentences, synonyms, vary rhythm)
- Change ~30% of wording while keeping the same meaning, intent, and length
- **CRITICAL**: Maintain extremely natural, conversational, human language. Do NOT use forced or awkward synonyms (e.g. do not change "manually" to "by hand", keep natural industry standard words).
- **CRITICAL DELIVERABILITY**: DO NOT use ANY of these exact spam trigger phrases: {spam_words}
- CRITICAL: Preserve ALL template variables EXACTLY as written. Do NOT alter the spelling or format inside the braces.
- Do NOT add new facts or claims not in the original
- No citations, no footnotes, no bracketed numbers like [1]
- Plain text only, no HTML and ABSOLUTELY NO markdown formatting (no asterisks `*` for emphasis, no bolding, no underscores).{contact_info}"""

        if company_context:
            system += f"""\n\n**CRITICAL STEP 1 INSTRUCTIONS**:
Since we have detailed company intelligence, for EMAIL_1 ONLY, you MUST follow this strict 4-line framework perfectly:
COMPANY SCRAPED CONTEXT: 
- Mission: {company_context.get('mission_and_about','')}
- Offerings: {company_context.get('offerings_and_positioning','')}
- Process: {company_context.get('process_and_differentiation','')}
- Proof: {company_context.get('proof_of_success','')}

EMAIL_1 RULES:
Use the company context to craft a personalized opening, but seamlessly integrate it with the original offer. Follow this flow:
"""
            if personalization_prompt:
                system += f"""IMPORTANT: USER PERSONALIZATION OVERRIDE: 
Focus the observation SPECIFICALLY on: "{personalization_prompt}". 
If details matching "{personalization_prompt}" are explicitly present in the COMPANY SCRAPED CONTEXT above, you MUST use them.
If that specific data is absent from the company context, DO NOT MAKE ANY ASSUMPTIONS and do not invent it. Instead, fall back to the closest actual fact present in the context that anchors the offer.
"""
            else:
                system += """IMPORTANT: Work BACKWARDS from the offer. First, identify the core problem that the original email's offer solves (e.g. "repetitive SEO fulfillment eating up time"). Then scan the 4 context points and pick the ONE that most strongly implies the prospect would face that exact problem. Ignore context points that are interesting but unrelated to the problem your offer solves.
"""
            system += """
1. The Observation: Confidently state the chosen context detail so they know you researched them. It MUST be a specific, concrete fact (e.g. "offers 12 marketing campaigns per year", "manages fulfillment for 200+ clients"). BAD EXAMPLES (DO NOT WRITE THESE): "helps operators free up time", "is all about giving people time back", "focuses on helping businesses grow". These are vague summaries, not observations. Do NOT reuse the original opening line. CRITICAL: NEVER use phrases like "I noticed", "I saw", "I was looking at", "Looks like", or "It seems". Just state the observation directly. The names mentioned in the scraped context are likely the founders/recipients, do NOT refer to them in the third person.
2. The Bridge: Write exactly one sentence that connects their specific situation to the problem your offer solves. The reader should think "yeah, that IS my problem" before they even see the offer. DO NOT use em-dashes.
3. The Offer & Proof: Naturally weave in the original core offer and proof statements. Do NOT change the core value proposition, numbers, or factual claims. IMPORTANT: If the original email mentions a specific client or case study name (e.g. "RankJacker"), keep that name in the output.
4. The CTA: Keep the final Call to Action essentially identical to the original.
5. The Sign-off: If the original email ends with a sign-off (e.g. "Best,\nBipul"), you MUST keep it exactly as-is.
6. Keep the total output for EMAIL_1 strictly under 75-100 words. No fluff. No corporate speak.

For EMAIL_2 onwards, just do the standard paraphrasing as normal."""

        system += f"""\n\nReturn ONLY a JSON array of exactly {len(bodies)} strings, in the same order:
["rewritten EMAIL_1 body", "rewritten EMAIL_2 body", ...]

Return ONLY the raw JSON array. No markdown, no explanation."""

        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-2.0-flash',
            contents=system + "\n\nEmails to paraphrase:\n" + numbered_input,
            config={'temperature': 0.2},
        )
        # Strip markdown emphasis symbols at the code level so they physically cannot survive
        content = response.text.strip().replace('*', '').replace('_', '')
        # Fix Gemini dropping the underscore in variables
        import re
        content = re.sub(r'\{\{\s*first_?name\s*\}\}', '{{first_name}}', content, flags=re.IGNORECASE)
        content = re.sub(r'\{\{\s*First_?Name\s*\}\}', '{{first_name}}', content, flags=re.IGNORECASE)
        
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


def paraphrase_text(text: str, context: dict = None, company_context: dict = None) -> str:
    """Single-text wrapper around the batch function (kept for backward compat)."""
    return paraphrase_texts_batch([text], context, company_context)[0]


@app.route('/api/sequences/create', methods=['POST'])
def create_sequences():
    """Create email sequences from templates for selected contacts (runs asynchronously)."""
    try:
        data = request.json
        project_id = data.get('project_id')
        contact_ids = data.get('contact_ids', [])
        personalization_prompt = data.get('personalization_prompt')
        
        if not contact_ids or not project_id:
            return jsonify({'error': 'No contacts or project_id selected'}), 400
        
        # Get templates synchronously to validate
        templates = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').execute()
        if not templates.data:
            return jsonify({'error': 'No email templates found. Seed them first.'}), 400
        
        # Get contacts synchronously to get the count
        all_contacts_data = []
        for i in range(0, len(contact_ids), 100):
            chunk = contact_ids[i:i+100]
            chunk_res = supabase.table('contacts').select('*').in_('id', chunk).execute()
            if chunk_res.data:
                all_contacts_data.extend(chunk_res.data)
                
        if not all_contacts_data:
            return jsonify({'error': 'No valid contacts found.'}), 400
            
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def run_in_background(proj_id, contacts_data, templates_data, custom_prompt=None):
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
                    v_status_existing = enrichment_data.get('verification_status')
                    
                    if contact_email and v_status_existing not in ('valid', 'risky', 'invalid'):
                        logger.info(f"Contact {contact['id']} lacks verification status. Verifying now...")
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
                    contact_email = (contact.get('email') or '').strip().rstrip('.,;:)!% ]').strip()
                    
                    raw_company = contact.get('company') or enrichment_data.get('company') or enrichment_data.get('linkedin_company') or contact.get('name') or 'your company'
                    
                    # Fix for "Unknown" companies to smartly pull from domain fallback
                    if str(raw_company).lower().strip() in ['unknown', 'unknown company', 'unknown business', '', '-', 'n/a', 'none']:
                        if contact_email and '@' in contact_email:
                            domain = contact_email.split('@')[-1]
                            # Simple domains like "remodelboom.com" -> "Remodelboom" 
                            name_part = domain.split('.')[0]
                            # Exclude generic mail domains
                            if name_part.lower() not in ['gmail', 'yahoo', 'outlook', 'hotmail', 'icloud', 'aol']:
                                raw_company = name_part.title()
                            else:
                                raw_company = 'your company'
                        else:
                            raw_company = 'your company'

                    full_name = contact.get('name', 'there')
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
                    company_context = enrichment_data.get('company_context')
                    bodies_para = paraphrase_texts_batch(bodies_raw, context=variables, company_context=company_context, personalization_prompt=custom_prompt)

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
                        created, errors = fut.result()
                        created_total += created
                        errors_total += errors
                    except Exception as ex:
                        errors_total += 1
                        logger.error(f"Future error: {ex}")

            logger.info(f"Sequence creation done: {created_total} steps created, {errors_total} errors")


        # Launch background thread (daemon=False so it outlives the request)
        logger.info(f"Background sequence creation started for {len(all_contacts_data)} contacts.")
        thread = threading.Thread(target=run_in_background, args=(project_id, all_contacts_data, templates.data), daemon=False)
        thread.start()

        return jsonify({
            'message': f'Started generating sequences for {len(all_contacts_data)} contacts in the background.',
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
            
        # Fetch project's sender group
        sender_group = "all"
        proj = supabase.table('projects').select('sender_group').eq('id', project_id).execute()
        if proj.data:
            sender_group = proj.data[0].get('sender_group', 'all')

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
            
            account = pool.get_next_account(sender_group)
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

@app.route('/api/projects/<project_id>/knowledge', methods=['GET'])
def get_project_knowledge(project_id):
    try:
        res = supabase.table('project_knowledge_base').select('*').eq('project_id', project_id).order('created_at', desc=True).execute()
        return jsonify(res.data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/projects/<project_id>/knowledge', methods=['POST'])
def add_project_knowledge(project_id):
    try:
        data = request.json
        res = supabase.table('project_knowledge_base').insert({
            'project_id': project_id,
            'title': data.get('title'),
            'content': data.get('content'),
            'category': data.get('category', 'faq')
        }).execute()
        if not res.data:
            return jsonify({'error': 'Failed to insert knowledge item'}), 500
        return jsonify(res.data[0])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/knowledge/<knowledge_id>', methods=['DELETE'])
def delete_project_knowledge(knowledge_id):
    try:
        supabase.table('project_knowledge_base').delete().eq('id', knowledge_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/replies/<id>/approve', methods=['POST'])
def approve_reply(id):
    """Approves a draft reply and sends it."""
    try:
        data = request.json or {}
        content = data.get('content')
        if not content:
            return jsonify({'error': 'Content is required'}), 400
            
        # 1. Fetch reply details
        res = supabase.table('replies').select('*').eq('id', id).single().execute()
        reply = res.data
        if not reply:
            return jsonify({'error': 'Reply not found'}), 404
            
        # 2. Get sender account from pool
        try:
            pool = SMTPPool()
        except ValueError as e:
            return jsonify({'error': str(e)}), 500
            
        account = pool.get_account_by_email(reply['sender_email'])
        if not account:
            return jsonify({'error': f"Sender account {reply['sender_email']} not found in pool"}), 404
            
        # 3. Send email via Gmail API
        # recipient_email is the prospect's email
        send_res = pool.send_email(
            account=account,
            to_addr=reply['recipient_email'],
            subject=f"Re: {reply.get('subject', 'Outreach')}",
            body_html=content,
            thread_id=reply.get('thread_id')
        )
        
        if not send_res['success']:
            return jsonify({'error': send_res['error']}), 500
            
        # 4. Update reply status to 'sent'
        supabase.table('replies').update({
            'status': 'sent',
            'draft_reply': content,
            'updated_at': datetime.utcnow().isoformat()
        }).eq('id', id).execute()
        
        return jsonify({'success': True, 'message': 'Reply sent successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sequences/send', methods=['POST'])
def trigger_send():
    """Send pending scheduled emails."""
    try:
        data = request.json or {}
        limit = data.get('limit', 99999)
        dry_run = data.get('dry_run', False)
        project_id = data.get('project_id')
        contact_ids = data.get('contact_ids') # For "Send Selected"
        
        def run_send():
            job = JobLogger(f"Email Dispatch{(' (Project: ' + str(project_id) + ')') if project_id else ' (All Active)'}")
            try:
                from execution.send_emails import send_pending_emails
                job.info("Triggering background email dispatch...")
                # We will update send_pending_emails to take a logger_callback
                results = send_pending_emails(limit=limit, dry_run=dry_run, project_id=project_id, contact_ids=contact_ids, logger_callback=job.info)
                job.success(f"Dispatch finished. Results: {results}")
                job.complete('completed')
            except Exception as e:
                logger.error(f"Background send error: {e}")
                job.error(f"Dispatch failed: {e}")
                job.complete('failed')

        thread = threading.Thread(target=run_send)
        thread.daemon = True
        thread.start()
        
        return jsonify({'status': 'started', 'message': 'Email dispatch started in background.'})
    except Exception as e:
        logger.error(f"Send trigger error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/replies', methods=['GET'])
def list_replies():
    """List replies from the new replies table, joined with contact names."""
    try:
        project_id = request.args.get('project_id')
        query = supabase.table('replies').select('*, contacts(name)')
        
        if project_id:
            query = query.eq('project_id', project_id)
            
        res = query.order('received_at', desc=True).execute()
        return jsonify(res.data or [])
    except Exception as e:
        logger.error(f"Error fetching replies: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/replies/<reply_id>/send', methods=['POST'])
def send_manual_reply(reply_id):
    """Send a manual reply to a prospect response."""
    try:
        data = request.json or {}
        reply_body = data.get('body')
        
        if not reply_body:
            return jsonify({'error': 'Reply body is required'}), 400
            
        # 1. Get the original reply details
        reply_res = supabase.table('replies').select('*').eq('id', reply_id).single().execute()
        if not reply_res.data:
            return jsonify({'error': 'Reply not found'}), 404
            
        parent_reply = reply_res.data
        
        # 2. Get the sender account from the pool
        from execution.smtp_pool import SMTPPool
        pool = SMTPPool()
        account = pool.get_account_by_email(parent_reply['recipient_email'])
        
        if not account:
            return jsonify({'error': f"Account {parent_reply['recipient_email']} not found in pool"}), 500
            
        # 3. Send the threaded reply
        subject = parent_reply['subject'] or "Re: Outreach"
        if not subject.lower().startswith('re:'):
            subject = f"Re: {subject}"
            
        result = pool.send_email(
            account=account,
            to_addr=parent_reply['sender_email'],
            subject=subject,
            body_html=reply_body,
            thread_id=parent_reply['thread_id']
        )
        
        if result.get('success'):
            # 4. Update reply status in DB
            supabase.table('replies').update({
                'sent_reply': reply_body,
                'sent_at': datetime.utcnow().isoformat(),
                'reply_status': 'sent'
            }).eq('id', reply_id).execute()
            
            return jsonify({'status': 'sent'})
        else:
            return jsonify({'error': result.get('error')}), 500
            
    except Exception as e:
        logger.error(f"Manual reply send error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/sequences/check-replies', methods=['POST'])
def check_replies():
    """Check Gmail inboxes for prospect replies and auto-stop their sequences (Background)."""
    try:
        data = request.json or {}
        days = data.get('days', 7)

        def run_check():
            job = JobLogger("Reply Check")
            try:
                from execution.check_replies import check_all_replies
                job.info(f"Starting reply check (scanning last {days} days)...")
                stats = check_all_replies(days=days, logger_callback=job.info)
                job.success(f"Reply check complete. Found {stats.get('replies', 0)} replies and {stats.get('bounces', 0)} bounces.")
                job.complete('completed')
            except Exception as e:
                logger.error(f"Background reply check error: {e}")
                job.error(f"Reply check failed: {e}")
                job.complete('failed')

        thread = threading.Thread(target=run_check)
        thread.daemon = True
        thread.start()

        return jsonify({'status': 'started', 'message': 'Reply check started in background.'})
    except Exception as e:
        logger.error(f"Reply check trigger error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/daily-run', methods=['POST'])
def trigger_daily_run():
    """Trigger the full daily workflow: check replies + send pending emails."""
    try:
        data = request.json or {}
        limit = data.get('limit', 99999)
        dry_run = data.get('dry_run', False)
        project_id = data.get('project_id')

        def run_daily():
            job = JobLogger("Daily Sync & Send")
            try:
                # Step 1: Check replies
                from execution.check_replies import check_all_replies
                job.info("Step 1: Synchronizing replies from tracking accounts...")
                reply_stats = check_all_replies(days=7, logger_callback=job.info)
                job.info(f"Reply check complete: {reply_stats}")

                # Step 2: Send pending emails
                job.info("Step 2: Dispatching scheduled emails...")
                from execution.send_emails import send_pending_emails
                results = send_pending_emails(limit=limit, dry_run=dry_run, project_id=project_id, skip_reply_check=True, logger_callback=job.info)
                job.success(f"Daily run finished. Results: {results}")
                job.complete('completed')

            except Exception as e:
                logger.error(f"Background daily run error: {e}")
                job.error(f"Daily run failed: {e}")
                job.complete('failed')

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
        project_id = request.args.get('project_id') # Optional: filter by project's sender_group
        from execution.smtp_pool import SMTPPool
        try:
            pool = SMTPPool()
            
            sender_group = "all"
            if project_id:
                proj = supabase.table('projects').select('sender_group').eq('id', project_id).execute()
                if proj.data:
                    sender_group = proj.data[0].get('sender_group', 'all')
            
            used = pool.get_total_usage(sender_group=sender_group)
            limit = pool.get_total_limit(sender_group=sender_group)
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
        
        # Clean up existing empty-email rows (convert '' to NULL so unique constraint doesn't block)
        try:
            supabase.table('contacts').update({'email': None}).eq('project_id', project_id).eq('email', '').execute()
        except Exception:
            pass  # Non-critical cleanup
        
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
            
            # Skip if no email AND no phone AND no instagram AND no linkedin AND no name — truly no way to reach or identify them
            if not email and not lead.get('phone') and not lead.get('instagram') and not linkedin and not name:
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
                'email': email or None,
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
# ROUTES — Job Logs
# =============================================================================

@app.route('/api/job-logs', methods=['GET'])
def list_job_logs():
    try:
        limit = int(request.args.get('limit', 20))
        res = supabase.table('job_logs').select('*').order('started_at', desc=True).limit(limit).execute()
        return jsonify({'jobs': res.data or []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/job-logs/<job_id>/events', methods=['GET'])
def list_job_events(job_id):
    try:
        # Also fetch the job details
        job_res = supabase.table('job_logs').select('*').eq('id', job_id).single().execute()
        res = supabase.table('job_events').select('*').eq('job_log_id', job_id).order('created_at', desc=False).execute()
        return jsonify({
            'job': job_res.data,
            'events': res.data or []
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)
