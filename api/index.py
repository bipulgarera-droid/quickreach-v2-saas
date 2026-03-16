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
from flask_cors import CORS
from dotenv import load_dotenv
from pathlib import Path
import requests
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
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'filmreach-dev-key')
CORS(app)

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
        'app': 'FilmReach',
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
        if not name:
            return jsonify({'error': 'Project name required'}), 400
        
        # 1. Create the project
        result = supabase.table('projects').insert({
            'name': name, 
            'description': description
        }).execute()
        
        project = result.data[0]
        project_id = project['id']
        
        # 2. Context-Aware Template Generation using Gemini
        if GEMINI_API_KEY and description:
            try:
                system = f"""You are an elite cold-email copywriter for a project named "{name}".
                The project is described as: "{description}".
                
                Generate a 4-step cold email drip sequence tailored to this exact business description.
                Create exactly 4 steps.
                
                CRITICAL COLD EMAIL RULES:
                - NEVER include any links, URLs, or attachments in ANY step
                - The goal of every email is to get a REPLY, not a click
                - The CTA must ALWAYS be a variation of "Want me to send the full report?" or "Can I share the details?"
                - Keep emails SHORT (3-5 sentences max for the body)
                - Professional but direct tone
                
                VARIABLES YOU MAY USE:
                - {{{{first_name}}}} — the contact's first name or business greeting name
                - {{{{name}}}} — the contact's full name
                - {{{{company}}}} — the contact's company name
                Do NOT use {{{{icebreaker}}}} — it has been removed from this system.
                
                TEMPLATE STRUCTURE FOR STEP 1:
                1. Open with a warm but concise observation about the type of business they likely run (generic, no icebreaker needed)
                2. Transition to specific issue findings relevant to the niche (page speed, missing tags, competitor gaps, etc.)
                3. Close with a permission-based CTA asking if they want the full report
                
                You MUST return the output as a SINGLE VALID JSON ARRAY of exactly 4 objects.
                Each object MUST have three exact keys: 
                - "name" (a short internal name, e.g. "Intro", "Follow up 1", "Nudge", "Break up")
                - "subject_template" (the email subject line)
                - "body_template" (the email body)
                
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
                'completed': status_counts.get('completed', 0),
            },
            'emails': {
                'total': emails.count or 0,
                'pending': email_counts.get('pending', 0),
                'sent': email_counts.get('sent', 0),
                'opened': email_counts.get('opened', 0),
                'replied': email_counts.get('replied', 0),
                'bounced': email_counts.get('bounced', 0),
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
            .select('id, contact_id, subject, body, step_number, project_id, scheduled_at, contacts(name, email, enrichment_data), projects(name)')\
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
            ig_handle = enrichment.get('instagram') or enrichment.get('instagram_handle')
            clean_ig = str(ig_handle).replace('@', '').strip() if ig_handle else None
            
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
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        status = request.args.get('status')
        search = request.args.get('search', '')
        limit = int(request.args.get('limit', 50))
        offset = int(request.args.get('offset', 0))
        
        query = supabase.table('contacts').select('*', count='exact').eq('project_id', project_id).eq('project_id', project_id)
        
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

# =============================================================================
# ROUTES — Enrichment
# =============================================================================

@app.route('/api/contacts/enrich', methods=['POST'])
def trigger_enrichment():
    """Trigger email/IG enrichment for pending (or selected) contacts."""
    try:
        data = request.json or {}
        limit = data.get('limit', 50)
        contact_ids = data.get('contact_ids', [])
        
        from execution.enrich_contacts import enrich_contacts
        stats = enrich_contacts(limit=limit, contact_ids=contact_ids)
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        return jsonify({'error': str(e)}), 500

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
    """List all email templates."""
    try:
        project_id = request.args.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        result = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').execute()
        return jsonify({'templates': result.data or []})
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
        proj = supabase.table('projects').select('name,description').eq('id', project_id).single().execute()
        if not proj.data:
            return jsonify({'error': 'Project not found'}), 404
        name = proj.data.get('name', 'Unknown')
        description = proj.data.get('description', '')
        if not description:
            return jsonify({'error': 'Project has no description. Edit the project first.'}), 400

        system = f"""You are an elite cold-email copywriter for a project named "{name}".
        The project is described as: "{description}".

        Generate a 4-step cold email drip sequence. Create exactly 4 steps.

        COLD EMAIL RULES:
        - NEVER include links, URLs, or attachments
        - Goal of every email: get a REPLY, not a click
        - CTA: always a variation of "Want me to send the full report?" or "Can I share the details?"
        - SHORT emails: 3-5 sentences max for the body
        - End EVERY email body with a sign-off line: "Best,\n{{{{sender_name}}}}"

        VARIABLES:
        - {{{{first_name}}}} — contact's greeting name
        - {{{{name}}}} — contact's full name
        - {{{{company}}}} — contact's company
        - {{{{sender_name}}}} — sender's name (always use this in sign-off)
        Do NOT use {{{{icebreaker}}}}.

        Step 1: warm generic opener about their business type → findings → CTA
        Steps 2-3: short follow-ups re-emphasizing value
        Step 4: polite break-up

        Return ONLY a raw JSON array of exactly 4 objects, each with:
        - "name" (short label: "Intro", "Follow up 1", "Nudge", "Break up")
        - "subject_template"
        - "body_template"
        No markdown, no extra text."""

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
        
        query = supabase.table('email_sequences').select('*, contacts(name, email)').eq('project_id', project_id).eq('project_id', project_id)
        
        if contact_id:
            query = query.eq('contact_id', contact_id)
        if status:
            query = query.eq('status', status)
        
        result = query.order('created_at', desc=True).limit(100).execute()
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

def paraphrase_text(text: str, context: dict = None) -> str:
    """Use Gemini to paraphrase a text while preserving variables, using prospect context if provided."""
    if not GEMINI_API_KEY:
        return text
    try:
        contact_info = ""
        if context:
            contact_info = f"\n\nPROSPECT CONTEXT:\nYou are emailing {context.get('name', 'someone')}.\n"
            if context.get('bio'):
                contact_info += f"Their Bio/LinkedIn Summary: {context['bio']}\n"
            if context.get('icebreaker'):
                contact_info += f"Our previous specific Icebreaker for them: {context['icebreaker']}\n"
            # LinkedIn enrichment data for richer personalization
            if context.get('linkedin_headline'):
                contact_info += f"Their LinkedIn Headline: {context['linkedin_headline']}\n"
            if context.get('linkedin_company'):
                contact_info += f"Their Current Company: {context['linkedin_company']}\n"
            if context.get('linkedin_title'):
                contact_info += f"Their Current Title: {context['linkedin_title']}\n"
            if context.get('linkedin_about'):
                about_snippet = context['linkedin_about'][:800]
                contact_info += f"Their LinkedIn About: {about_snippet}\n"
            contact_info += "\nIf appropriate and highly relevant, weave a brief, natural reference to their background or company into the paraphrased text to make the follow-up hyper-personalized. DO NOT hallucinate facts, guess their current challenges, or assume things not explicitly stated in their bio or the icebreaker. Stick strictly to the provided facts."

        system = f"""You are an expert cold email copywriter. Paraphrase the following email body to evade spam filters.
        Rewrite it so it sounds genuinely fresh: restructure sentences, use synonyms, vary the sentence rhythm.
        Aim to change ~30% of the wording while keeping the same meaning, intent, and length.{contact_info}
        
        CRITICAL: Preserve ALL template variables exactly as written: {{{{name}}}}, {{{{first_name}}}}, {{{{company}}}}, {{{{sender_name}}}}, etc. Do NOT modify or remove them.
        CRITICAL: Do NOT add any new facts, claims, or information not present in the original.
        CRITICAL: No citations, no footnotes, no bracketed numbers like [1] or [2].
        DO NOT use HTML tags. Use plain line breaks.
        Return ONLY the rewritten email body text, nothing else."""
        
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model='gemini-2.5-pro',
            contents=system + "\n\nText to Paraphrase:\n" + text,
        )
        
        content = response.text.strip()
        # Clean up any markdown blocks if the AI ignored instructions
        if '```html' in content: content = content.split('```html')[1].split('```')[0].strip()
        elif '```' in content: content = content.split('```')[1].split('```')[0].strip()
        
        return content
    except Exception as e:
        logger.error(f"Paraphrase error (Gemini): {e}")
        return text # fallback to original


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
        
        def run_in_background(proj_id, contacts_data, templates_data):
            import re as _re
            import json as _json
            
            # Shorten company name for clean email personalization
            def _shorten_company(name):
                if not name: return name
                # Strip common legal suffixes
                name = _re.sub(r'\s*(LLC|Inc\.?|Corp\.?|Ltd\.?|LLP|Co\.?|P\.?C\.?|PLLC|Limited|Group|Holdings|International|Services|Solutions|Enterprises|Associates|Consulting|Organization|Foundation)\s*$', '', name, flags=_re.IGNORECASE).strip().rstrip(',').strip()
                # If still too long, take first 3 meaningful words
                words = name.split()
                if len(words) > 4:
                    name = ' '.join(words[:3])
                return name
            
            created = 0
            errors = 0
            for contact in contacts_data:
                try:
                    base_date = datetime.utcnow()
                    
                    # Parse enrichment_data for LinkedIn fields
                    enrichment_data = contact.get('enrichment_data')
                    if isinstance(enrichment_data, str):
                        try:
                            enrichment_data = _json.loads(enrichment_data)
                        except Exception:
                            enrichment_data = {}
                    elif not isinstance(enrichment_data, dict):
                        enrichment_data = {}
                    
                    raw_company = enrichment_data.get('company') or enrichment_data.get('linkedin_company') or contact.get('name', 'your company')
                    
                    full_name = contact.get('name', 'there')
                    # Clean the greeting name: strip trailing " - Team" / "- Business" patterns
                    # so "Jasmine - Team" → "Jasmine" for the Hi {{first_name}} line
                    greeting_name = _re.sub(r'\s*-\s*(Team|Business|Staff|Group|Page|Hub|Official)\s*$', '', full_name, flags=_re.IGNORECASE).strip()
                    # If still a business-style name (has " Team"/" Business" or single word), keep it whole; else first word only
                    first_name = greeting_name if (' Team' in greeting_name or ' Business' in greeting_name or len(greeting_name.split()) == 1) else greeting_name.split()[0]
                    
                    # Strip any leftover [N] citations from stored icebreaker
                    raw_icebreaker = contact.get('icebreaker', '') or ''
                    clean_icebreaker = _re.sub(r'\[\d+\]', '', raw_icebreaker).strip()
                    
                    variables = {
                        'name': full_name,
                        'first_name': first_name,
                        'bio': contact.get('bio', ''),
                        'icebreaker': clean_icebreaker,
                        'company': _shorten_company(raw_company),
                        # Sender variables (from .env SENDER_NAME)
                        'sender_name': SENDER_NAME,
                        'sender_first_name': SENDER_NAME.split()[0] if SENDER_NAME else 'Bipul',
                        # LinkedIn enrichment fields for paraphraser context
                        'linkedin_headline': enrichment_data.get('linkedin_headline', ''),
                        'linkedin_company': enrichment_data.get('linkedin_company', ''),
                        'linkedin_title': enrichment_data.get('linkedin_title', ''),
                        'linkedin_about': enrichment_data.get('linkedin_about', ''),
                    }
                    
                    for template in templates_data:
                        try:
                            subject = template['subject_template']
                            body = template['body_template']
                            
                            # Paraphrase EVERY step per contact for spam avoidance
                            body = paraphrase_text(body, context=variables)
                            
                            for key, val in variables.items():
                                val_str = str(val) if val is not None else ''
                                subject = subject.replace(f'{{{{{key}}}}}', val_str)
                                body = body.replace(f'{{{{{key}}}}}', val_str)
                            
                            scheduled = base_date + timedelta(days=template.get('delay_days', 0))
                            
                            # Dedup check: skip if sequence row already exists for this contact+template
                            existing = supabase.table('email_sequences').select('id').eq('contact_id', contact['id']).eq('template_id', template['id']).execute()
                            if existing.data:
                                logger.info(f"Skipping duplicate sequence for contact {contact['id']} template {template['id']}")
                                continue
                            
                            supabase.table('email_sequences').insert({
                                'project_id': proj_id,
                                'contact_id': contact['id'],
                                'template_id': template['id'],
                                'step_number': template['step_number'],
                                'subject': subject,
                                'body': body,
                                'status': 'pending',
                                'scheduled_at': scheduled.isoformat()
                            }).execute()
                            
                            created += 1
                        except Exception as step_e:
                            logger.error(f"Error on step {template.get('step_number')} for contact {contact.get('name')}: {step_e}")
                    
                    # Update contact status
                    supabase.table('contacts').update({
                        'status': 'in_sequence',
                        'updated_at': datetime.utcnow().isoformat()
                    }).eq('id', contact['id']).execute()
                    logger.info(f"Sequence created for contact: {contact.get('name')}")
                    
                except Exception as contact_e:
                    errors += 1
                    logger.error(f"Failed to create sequence for contact {contact.get('name', contact.get('id'))}: {contact_e}")
                    # Continue to next contact regardless
                    continue
            
            logger.info(f"Background sequence creation done: {created} steps created, {errors} contact errors")

        # Start thread (daemon=False ensures it outlives the request)
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
            to_email = to_email.strip()
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
        limit = data.get('limit', 50)
        dry_run = data.get('dry_run', False)
        
        from execution.send_emails import send_pending_emails
        stats = send_pending_emails(limit=limit, dry_run=dry_run)
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Send error: {e}")
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
        limit = data.get('limit', 250)
        dry_run = data.get('dry_run', False)

        from execution.daily_run import daily_run
        stats = daily_run(limit=limit, dry_run=dry_run)

        return jsonify(stats)
    except Exception as e:
        logger.error(f"Daily run error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Search Runs
# =============================================================================

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
        
        # Fetch existing emails in this project for deduplication
        existing = supabase.table('contacts').select('email').eq('project_id', project_id).execute()
        existing_emails = set()
        for row in (existing.data or []):
            if row.get('email'):
                existing_emails.add(row['email'].lower())
        
        imported = 0
        skipped_duplicate = 0
        skipped_no_contact = 0
        errors = 0
        
        contacts_to_insert = []
        
        # Also fetch existing names for dedup when no email
        existing_names_q = supabase.table('contacts').select('name').eq('project_id', project_id).execute()
        existing_names = set()
        for row in (existing_names_q.data or []):
            if row.get('name'):
                existing_names.add(row['name'].lower().strip())
        
        for lead in leads:
            email = (lead.get('email') or '').strip()
            name = (lead.get('name') or '').strip()
            
            # Skip if no email AND no phone AND no instagram — truly no way to reach them
            if not email and not lead.get('phone') and not lead.get('instagram'):
                skipped_no_contact += 1
                continue
            
            # Deduplicate: by email if available, otherwise by name
            if email and email.lower() in existing_emails:
                skipped_duplicate += 1
                continue
            if email:
                existing_emails.add(email.lower())
            elif name and name.lower() in existing_names:
                skipped_duplicate += 1
                continue
            if name:
                existing_names.add(name.lower())
            
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
            
            contact = {
                'project_id': project_id,
                'name': lead.get('name', 'Unknown'),
                'email': email,
                'bio': lead.get('bio', ''),
                'linkedin_url': lead.get('linkedin') or None,
                'instagram': lead.get('instagram') or None,
                'source': lead.get('category') or 'growthscout',
                'status': 'enriched' if email else 'new',  # enriched if has email, new if manual-only
                'enrichment_data': json.dumps(enrichment),
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
