import os
import sys
from google_auth_oauthlib.flow import InstalledAppFlow

# The scope we need is JUST sending emails. Not reading/deleting them.
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

def main():
    credential_path = 'credentials.json'
    if not os.path.exists(credential_path):
        print(f"ERROR: Could not find {credential_path}. Make sure it is in the root directory.")
        sys.exit(1)

    print("\n--- GMAIL API TOKEN GENERATOR ---")
    print("Your browser will now open to a Google Login screen.")
    print("IMPORTANT: Log in with the account you want to use for SENDING emails (e.g. Account A1).")
    print("If it says 'Google hasn't verified this app', click 'Advanced' -> 'Go to <app name>'.\n")
    
    flow = InstalledAppFlow.from_client_secrets_file(credential_path, SCOPES)
    
    # We must force prompt='consent' so Google returns a refresh token every single time
    try:
        creds = flow.run_local_server(port=0, prompt='consent')
    except Exception as e:
        print(f"Failed to start OAuth flow: {e}")
        sys.exit(1)
    
    if not creds.refresh_token:
        print("\nERROR: No refresh token returned. This usually happens if you didn't force a new consent screen.")
        print("Try running again, or revoke the app's permissions in your Google Account security settings and re-run.")
        sys.exit(1)

    print("\n" + "="*60)
    print("SUCCESS! Copy the line below and paste it into your .env file:")
    print("="*60)
    print(f"GMAIL_1_REFRESH_TOKEN='{creds.refresh_token}'")
    print("="*60)
    print("\nRepeat this script 4 more times for accounts A2-A5, changing the number in the .env variable.")

if __name__ == '__main__':
    main()
