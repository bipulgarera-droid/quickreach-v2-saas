import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'
print(f"Checking env path: {env_path}")
print(f"File exists: {env_path.exists()}")

load_dotenv(env_path)
key = os.getenv('SERPER_API_KEY')
print(f"SERPER_API_KEY found: {'Yes' if key else 'No'}")
if key:
    print(f"Key starts with: {key[:5]}...")
