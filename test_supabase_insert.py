import json, os, sys
sys.path.insert(0, '/Users/bipul/Downloads/ALL WORKSPACES/festivals outreach')

from dotenv import load_dotenv
load_dotenv('/Users/bipul/Downloads/ALL WORKSPACES/festivals outreach/.env')

from supabase import create_client
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_ROLE_KEY'))

# Get the project id
res = sb.table('projects').select('id, name').execute()
pid = None
for p in res.data:
    if 'seo agencies' in p['name'].lower():
        pid = p['id']
        break
        
print(f"Project ID: {pid}")

from execution.apify_leads_finder import store_apify_results
with open('/tmp/test_bug.json') as f:
    data = json.load(f)

print('Calling store_apify_results...')
try:
    res = store_apify_results(data, 'Test Label from bug', pid)
    print("Success:", res)
except Exception as e:
    import traceback
    traceback.print_exc()
