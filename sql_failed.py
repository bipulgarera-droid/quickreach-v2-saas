import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_SERVICE_ROLE_KEY'))
try:
    res = sb.table('contacts').update({'status': 'failed'}).eq('status', 'invalid_email').execute()
    print("Test failed ok")
except Exception as e:
    print(e)
    
try:
    res = sb.table('contacts').update({'status': 'invalid'}).eq('status', 'invalid_email').execute()
    print("Test invalid ok")
except Exception as e:
    print(e)
