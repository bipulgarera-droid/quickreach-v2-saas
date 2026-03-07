import os
import sys
import re
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Setup
env_path = Path(__file__).resolve().parent / '.env'
load_dotenv(env_path)

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)

def clean_text(text):
    if not text:
        return text
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '\n', text)
    # Remove citations like [1], [1][2], etc.
    text = re.sub(r'\[\d+\]', '', text)
    # Clean up excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

print("Cleaning Contacts (Icebreakers)...")
contacts = supabase.table('contacts').select('id, icebreaker').not_.is_('icebreaker', 'null').execute()
for c in (contacts.data or []):
    cleaned = clean_text(c['icebreaker'])
    if cleaned != c['icebreaker']:
        supabase.table('contacts').update({'icebreaker': cleaned}).eq('id', c['id']).execute()

print("Cleaning Templates...")
templates = supabase.table('email_templates').select('id, body_template').execute()
for t in (templates.data or []):
    cleaned = clean_text(t['body_template'])
    if cleaned != t['body_template']:
        supabase.table('email_templates').update({'body_template': cleaned}).eq('id', t['id']).execute()

print("Cleaning Sequences...")
sequences = supabase.table('email_sequences').select('id, body').execute()
for s in (sequences.data or []):
    cleaned = clean_text(s['body'])
    if cleaned != s['body']:
        supabase.table('email_sequences').update({'body': cleaned}).eq('id', s['id']).execute()

print("Database cleanup complete!")
