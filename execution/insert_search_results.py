import os
import sys
import json
import logging
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Load env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def insert_results_to_supabase(results_file):
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
    if not url or not key:
        logger.error("Supabase credentials missing")
        return

    supabase = create_client(url, key)

    with open(results_file, 'r') as f:
        data = json.load(f)
    
    if isinstance(data, dict):
        results = data.get('organic', [])
    else:
        results = data

    # Get a valid project_id from the projects table
    res = supabase.table('projects').select('id').limit(1).execute()
    project_id = res.data[0]['id'] if res.data else None

    logger.info(f"Inserting {len(results)} results into Supabase...")
    
    contacts = []
    for r in results:
        contacts.append({
            'name': r.get('title'),
            'company': r.get('title'),
            'source_url': r.get('link'),
            'status': 'new',
            'project_id': project_id,
            'enrichment_data': {
                'snippet': r.get('snippet'),
                'website': r.get('link')
            }
        })

    # Insert in batches
    batch_size = 50
    for i in range(0, len(contacts), batch_size):
        batch = contacts[i:i + batch_size]
        try:
            supabase.table('contacts').insert(batch).execute()
            logger.info(f"  Inserted batch {i//batch_size + 1}")
        except Exception as e:
            logger.error(f"  Error inserting batch: {e}")

if __name__ == "__main__":
    # We'll save the output of the previous command to search_results.json first
    insert_results_to_supabase('search_results.json')
