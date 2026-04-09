import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing Supabase credentials in .env")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

try:
    # Check if the users table exists
    response = supabase.table('users').select('id', count='exact').limit(1).execute()
    print("SUCCESS: SaaS Schema appears to be installed (users table exists).")
except Exception as e:
    print(f"FAILED: SaaS Schema missing or error checking DB: {e}")
