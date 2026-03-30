import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.getenv('PUBLIC_SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_ROLE_KEY'))
res = sb.table('contacts').select('status').limit(1500).execute()
print(set(r['status'] for r in res.data if r.get('status')))
