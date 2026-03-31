import os
import json
import logging
from google import genai

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_incoming_emails(emails_batch: list[dict], active_campaign_subjects: list[str]) -> dict:
    """
    Takes a batch of raw incoming emails (sender, subject, body_snippet) and a list of known subjects.
    Passes them to Gemini 2.5 Flash for intelligent, batch classification.
    Returns a dictionary mapping the email message_id -> classification details.
    
    Valid classifications: 'HUMAN_REPLY', 'BOUNCE', 'AUTO_REPLY', 'SPAM', 'IGNORE'
    """
    if not emails_batch:
        return {}

    gemini_key = os.getenv('GEMINI_API_KEY')
    if not gemini_key:
        logger.error("GEMINI_API_KEY is not set. Cannot run AI Reply Analyzer.")
        return {}

    client = genai.Client(api_key=gemini_key)
    
    prompt = f"""
    You are an expert email deliverability and response analyzer for a B2B cold outreach system.
    We are sending cold emails with the following subject lines:
    {json.dumps(active_campaign_subjects, indent=2)}

    Below is a JSON array of incoming emails straight from our inboxes. Some of these are direct replies from humans,
    some are out-of-office automated replies, some are bounce/failure notifications, and some are just random promotional spam.

    Your task is to classify EVERY email perfectly into one of the following categories:
    - "HUMAN_REPLY": A real person wrote back to our cold email (even if it's "unsubscribe", "not interested", or a forward from alias).
    - "BOUNCE": Delivery Status Notifications, 550 errors, undeliverable, postmaster notices.
    - "AUTO_REPLY": "Out of office", "I'm on vacation", "No longer with the company" automated responses.
    - "SPAM": Promotional newsletters, Google Alerts, irrelevant marketing emails.

    IMPORTANT INSTRUCTIONS:
    1. Read the sender, subject, and body snippet carefully.
    2. Bounces can sometimes come from 'mailer-daemon' or from 'postmaster' or regular emails that say 'Delivery failed'.
    3. Human replies often include history or 'Re:' but might come from a different domain if the prospect forwarded it or uses an alias.
    
    You MUST output ONLY a valid JSON array of objects. Do not include markdown blocks like ```json.
    Format:
    [
      {
        "msg_index": "the index provided in the input",
        "classification": "HUMAN_REPLY | BOUNCE | AUTO_REPLY | SPAM",
        "reason": "1 sentence explaining why",
        "extracted_contact_email": "If BOUNCE, try to find the email address that failed in the body. If HUMAN_REPLY, try to find the original recipient email in the quoted text. If not found, leave null.",
        "extracted_subject": "If replying to one of our active campaigns, output the matched subject line exactly as given."
      }
    ]

    INCOMING EMAILS BATCH:
    {json.dumps(emails_batch, indent=2)}
    """

    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json"
            )
        )
        
        raw_text = response.text.strip()
        
        # In case the model still outputs markdown despite response_mime_type
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:]
        if raw_text.startswith("```"):
            raw_text = raw_text[3:]
        if raw_text.endswith("```"):
            raw_text = raw_text[:-3]
            
        classifications = json.loads(raw_text.strip())
        
        # Build mapping: index -> classification
        result_map = {}
        for item in classifications:
            idx = str(item.get('msg_index'))
            result_map[idx] = {
                'classification': item.get('classification', 'SPAM'),
                'reason': item.get('reason', ''),
                'extracted_contact_email': item.get('extracted_contact_email', None),
                'extracted_subject': item.get('extracted_subject', None)
            }
            
        return result_map

    except Exception as e:
        logger.error(f"Gemini Analyzer crash: {e}")
        return {}
