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
                system = f"""You are an elite B2B and cold-email copywriter for a project named "{name}".
                The project is described as: "{description}".
                
                Generate a full 12-step drip email sequence tailored to this exact business description.
                Create exactly 12 steps. 
                Keep the tone professional yet conversational.
                You MUST return the output as a SINGLE VALID JSON ARRAY of exactly 12 objects.
                Each object MUST have three exact keys: 
                - "name" (a short internal name for the string, e.g. "Intro", "Follow up 1")
                - "subject_template" (the email subject line)
                - "body_template" (the email body)
                
                You may use these placeholder variables in curly braces: {{{{first_name}}}}, {{{{name}}}}, {{{{company}}}}, {{{{icebreaker}}}}.
                The "delay_days" will be calculated automatically by the system, just focus on the content.
                Ensure step 1 is a strong introduction and MUST logically include the exact text "{{{{icebreaker}}}}" somewhere in its body_template to seamlessly inject our pre-researched personalized intro. Steps 2-12 should be polite follow-ups, value adds, case studies, or break-up emails.
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
                
                # Standard delay pattern for a 12 step campaign
                delays = [0, 3, 5, 7, 10, 14, 21, 30, 45, 60, 90, 120]
                
                templates_to_insert = []
                for i, step in enumerate(steps[:12]):
                    templates_to_insert.append({
                        'project_id': project_id,
                        'name': step.get('name', f'Step {i+1}'),
                        'step_number': i + 1,
                        'subject_template': step.get('subject_template', f'Follow up {i}'),
                        'body_template': step.get('body_template', 'Placeholder body'),
                        'delay_days': delays[i] if i < len(delays) else 30
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
    """Trigger email/IG enrichment for pending contacts."""
    try:
        data = request.json or {}
        limit = data.get('limit', 50)
        
        from execution.enrich_contacts import enrich_contacts
        stats = enrich_contacts(limit=limit)
        
        return jsonify(stats)
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# ROUTES — Icebreakers
# =============================================================================

@app.route('/api/contacts/icebreaker', methods=['POST'])
def trigger_icebreakers():
    """Generate icebreakers for enriched contacts."""
    try:
        data = request.json or {}
        limit = data.get('limit', 50)
        
        from execution.generate_icebreakers import generate_icebreakers_batch
        stats = generate_icebreakers_batch(limit=limit)
        
        return jsonify(stats)
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
            
        system = """You are an expert cold email copywriter. Write a single sequence step based on the prompt.
        Return ONLY valid JSON with 'subject' and 'body' keys.
        You may use these variables in curly braces: {{name}}, {{first_name}}, {{icebreaker}}, {{bio}}.
        Keep the email concise and natural.
        CRITICAL INSTRUCTIONS:
        1. NEVER include academic citations, footnotes, or bracketed numbers like [1] or [2] in your response.
        2. DO NOT use HTML tags like <p> or <br>. Use standard text line breaks if needed.
        3. ALWAYS return your entire response as a single, valid JSON block."""
        
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


@app.route('/api/sequences/<int:sequence_id>', methods=['PUT'])
def update_sequence(sequence_id):
    """Update a specific sequence step (e.g. manual edit)."""
    try:
        data = request.json
        allowed = ['subject', 'body']
        update_data = {k: v for k, v in data.items() if k in allowed}
        
        result = supabase.table('email_sequences').update(update_data).eq('id', sequence_id).execute()
        return jsonify({'sequence': result.data[0] if result.data else None})
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
            contact_info += "\nIf appropriate and highly relevant, weave a brief, natural reference to their background or company into the paraphrased text to make the follow-up hyper-personalized. DO NOT hallucinate facts, guess their current challenges, or assume things not explicitly stated in their bio or the icebreaker. Stick strictly to the provided facts."

        system = f"""You are an expert copywriter. Paraphrase the following email body to avoid spam filters.
        Keep the exact same meaning, tone, and roughly the same length, but change about 15-20% of the word choices.{contact_info}
        
        CRITICAL: If you see raw variables like {{{{name}}}}, {{{{first_name}}}}, {{{{icebreaker}}}}, {{{{bio}}}}, or any other bracketed texts, YOU MUST LEAVE THEM EXACTLY AS THEY ARE.
        CRITICAL INSTRUCTIONS:
        1. NEVER include academic citations, footnotes, or bracketed numbers like [1] or [2] in your response.
        2. DO NOT use HTML tags like <p> or <br>. Use standard text line breaks if needed.
        Return ONLY the rewritten text, nothing else."""
        
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
    """Create email sequences from templates for selected contacts."""
    try:
        data = request.json
        project_id = data.get('project_id')
        contact_ids = data.get('contact_ids', [])
        
        if not contact_ids or not project_id:
            return jsonify({'error': 'No contacts or project_id selected'}), 400
        
        # Get templates
        templates = supabase.table('email_templates').select('*').eq('project_id', project_id).order('step_number').execute()
        if not templates.data:
            return jsonify({'error': 'No email templates found. Seed them first.'}), 400
        
        # Get contacts
        contacts = supabase.table('contacts').select('*').in_('id', contact_ids).execute()
        
        created = 0
        for contact in (contacts.data or []):
            base_date = datetime.utcnow()
            
            for template in templates.data:
                # Render template with contact variables
                variables = {
                    'name': contact.get('name', 'there'),
                    'first_name': contact.get('name', 'there').split()[0],
                    'bio': contact.get('bio', ''),
                    'icebreaker': contact.get('icebreaker', ''),
                }
                
                subject = template['subject_template']
                body = template['body_template']
                
                # AI Paraphrase for follow-up steps (Step 2+) to avoid spam filters
                if template['step_number'] > 1:
                    body = paraphrase_text(body, context=variables)
                
                for key, val in variables.items():
                    subject = subject.replace(f'{{{{{key}}}}}', val)
                    body = body.replace(f'{{{{{key}}}}}', val)
                
                scheduled = base_date + timedelta(days=template.get('delay_days', 0))
                
                supabase.table('email_sequences').insert({
                    'project_id': project_id,
                    'contact_id': contact['id'],
                    'template_id': template['id'],
                    'step_number': template['step_number'],
                    'subject': subject,
                    'body': body,
                    'status': 'pending',
                    'scheduled_at': scheduled.isoformat()
                }).execute()
                
                created += 1
            
            # Update contact status
            supabase.table('contacts').update({
                'status': 'in_sequence',
                'updated_at': datetime.utcnow().isoformat()
            }).eq('id', contact['id']).execute()
        
        return jsonify({'created': created, 'contacts': len(contacts.data or [])})
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
# SEED — Email Templates
# =============================================================================

@app.route('/api/seed-templates', methods=['POST'])
def seed_templates():
    """Seed the 12-step drip email templates."""
    try:
        data = request.json or {}
        project_id = data.get('project_id')
        if not project_id: return jsonify({'error': 'project_id required'}), 400
        templates = [
            {
                'step_number': 1,
                'name': 'Logline Introduction',
                'subject_template': 'A story I think you\'d connect with, {{first_name}}',
                'body_template': '<p>Hi {{first_name}},</p><p>{{icebreaker}}</p><p>I\'m reaching out because I\'ve just completed an indie film that I believe aligns with your sensibilities. Here\'s the logline:</p><p><em>[Your logline here]</em></p><p>I\'d love to share more if this piques your interest. Would you be open to a brief conversation?</p><p>Warm regards,<br>[Your Name]</p>',
                'delay_days': 0
            },
            {
                'step_number': 2,
                'name': 'Trailer Share',
                'subject_template': 'The trailer is here — would love your eyes on it',
                'body_template': '<p>Hi {{first_name}},</p><p>Following up on my previous note about our film. We just released the official trailer and I immediately thought of you.</p><p>🎬 <a href="[TRAILER_URL]">Watch the trailer here</a></p><p>The film explores [brief theme] through [unique angle]. I think it speaks to the kind of stories you champion.</p><p>Would love to hear your initial reaction.</p><p>Best,<br>[Your Name]</p>',
                'delay_days': 3
            },
            {
                'step_number': 3,
                'name': 'Behind the Scenes',
                'subject_template': 'The story behind making {{first_name}} — BTS peek',
                'body_template': '<p>Hi {{first_name}},</p><p>I wanted to share something more personal — a behind-the-scenes look at the making of our film.</p><p>We shot over [X days] in [location], with a crew of [X people]. The biggest challenge was [brief challenge], but it\'s exactly what gives the film its authenticity.</p><p>Here are some exclusive BTS photos: [BTS_LINK]</p><p>I think the production story itself is worth telling. Happy to share more details if you\'re interested in covering the filmmaking journey.</p><p>Cheers,<br>[Your Name]</p>',
                'delay_days': 5
            },
            {
                'step_number': 4,
                'name': 'Director\'s Vision',
                'subject_template': 'Why I made this film — a director\'s note',
                'body_template': '<p>Hi {{first_name}},</p><p>I\'ve been thinking about what compelled me to make this film, and I wanted to share that with you directly.</p><p>[2-3 sentences about the director\'s vision, what inspired the story, why it matters now]</p><p>I believe stories like this need voices like yours to help them reach the right audience. Would you be interested in a conversation about the film\'s themes?</p><p>With gratitude,<br>[Your Name]</p>',
                'delay_days': 7
            },
            {
                'step_number': 5,
                'name': 'Press Kit & Stills',
                'subject_template': 'Press kit + exclusive stills for you',
                'body_template': '<p>Hi {{first_name}},</p><p>I\'ve put together a comprehensive press kit for easy reference:</p><ul><li>📋 Press Kit: [PRESS_KIT_LINK]</li><li>📸 Hi-res Production Stills: [STILLS_LINK]</li><li>🎬 Trailer: [TRAILER_LINK]</li><li>📝 Director\'s Statement</li></ul><p>Everything you\'d need if you decide to feature or review the film. No pressure at all — just wanted to make it easy for you.</p><p>Best,<br>[Your Name]</p>',
                'delay_days': 10
            },
            {
                'step_number': 6,
                'name': 'Festival Selections',
                'subject_template': 'Exciting news — festival selections!',
                'body_template': '<p>Hi {{first_name}},</p><p>Wanted to share some exciting news — our film has been selected for [Festival Name(s)]!</p><p>The festival run begins [date/month], and I thought you might want to know ahead of the public announcement.</p><p>If you\'re attending or covering [Festival Name], I\'d love to arrange a screening or interview opportunity.</p><p>More details: [FESTIVAL_LINK]</p><p>Cheers,<br>[Your Name]</p>',
                'delay_days': 14
            },
            {
                'step_number': 7,
                'name': 'Review Request',
                'subject_template': 'Would you consider reviewing our film?',
                'body_template': '<p>Hi {{first_name}},</p><p>I know your time is valuable, so I\'ll be direct — would you be open to watching and reviewing our film?</p><p>I can provide:</p><ul><li>🎥 Private screener link (your eyes only)</li><li>📋 Press notes and director Q&A</li><li>📸 Exclusive stills for your publication</li></ul><p>Your honest perspective would mean the world to our team. No obligation to write positively — we value authentic criticism.</p><p>Just say the word and I\'ll send the screener right over.</p><p>Respectfully yours,<br>[Your Name]</p>',
                'delay_days': 18
            },
        ]
        
        # Check if templates already exist
        existing = supabase.table('email_templates').select('id', count='exact').eq('project_id', project_id).execute()
        if existing.count and existing.count > 0:
            return jsonify({'message': f'Templates already seeded ({existing.count} exist)', 'count': existing.count})
        
        # Insert templates
        for t in templates:
            t['project_id'] = project_id
            supabase.table('email_templates').insert(t).execute()
        
        return jsonify({'message': f'Seeded {len(templates)} email templates', 'count': len(templates)})
    except Exception as e:
        logger.error(f"Seed error: {e}")
        return jsonify({'error': str(e)}), 500

# =============================================================================
# MAIN
# =============================================================================

if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    app.run(host='0.0.0.0', port=port, debug=True)
