"""
Test Script: Validates the deterministic email classifier against
the EXACT emails from the user's live production logs.

Every single one of these MUST be classified correctly.
"""
from execution.classify_email import classify_email

# Simulate our prospect database
MOCK_PROSPECT_EMAILS = {
    'mandy@mandykaymarketing.com': ('c1', 'p1', 'Mandy Kay Marketing'),
    'immensityit@gmail.com': ('c2', 'p1', 'Immensity It'),
    'rhea@peppershock.com': ('c3', 'p1', 'Peppershock'),
    'oscar.fullmer@fasthippomedia.com': ('c4', 'p1', 'Fast Hippo Media'),
    'action@colagecommunication.in': ('c5', 'p2', 'Colage Communication'),
    'vince@meltatl.com': ('c6', 'p2', 'Melt Sports'),
    'anthony@mkswebdesign.com': ('c7', 'p2', 'MKS Web Design'),
    'rs@aqaba.digital': ('c8', 'p3', 'Aqaba Digital'),
    'marcelo@m4worldwide.com': ('c9', 'p3', 'M4 Worldwide'),
    'ray@smb360.io': ('c10', 'p3', 'SMB 360'),
    'colan@schureconsulting.com': ('c11', 'p3', 'Schure Consulting'),
    'ashley@sandpiperagency.com': ('c12', 'p3', 'Sandpiper Agency'),
    'bob@bobgoldpr.com': ('c13', 'p3', 'Bob Gold & Associates'),
    'peter@mailpix.com': ('c14', 'p1', 'Mailpix'),
    'julianano@standoutconsultingservices.com': ('c15', 'p3', 'Stand Out Consulting'),
    'jcrouch@relentless-digital.com': ('c16', 'p3', 'Relentless Digital'),
    'education@cineworks.ca': ('c17', 'p1', 'Cineworks'),
}

MOCK_SUBJECT_MAP = {
    'scaling mandy kay marketing?': [('c1', 'p1')],
    'scaling fast hippo media?': [('c4', 'p1')],
    'quick one for immensity it': [('c2', 'p1')],
    'scaling peppershock?': [('c3', 'p1')],
    'quick question about "colage communication': [('c5', 'p2')],
    'scaling melt sports/culinary/ entertainment?': [('c6', 'p2')],
    'scaling mks web design?': [('c7', 'p2')],
    'scaling aqaba digital?': [('c8', 'p3')],
    'scaling m4 worldwide?': [('c9', 'p3')],
    'scaling smb 360?': [('c10', 'p3')],
    'scaling elevation': [('c11', 'p3')],
    'scaling sandpiper agency?': [('c12', 'p3')],
    'scaling bob gold & associates?': [('c13', 'p3')],
    'scaling mailpix?': [('c14', 'p1')],
    'scaling stand out consulting services?': [('c15', 'p3')],
    'scaling relentless digital marketing solutions?': [('c16', 'p3')],
    'scaling consult vito?': [('c18', 'p2')],
    'scaling internet marketing pros?': [('c19', 'p2')],
    'scaling revline marketing?': [('c20', 'p2')],
    'for your consideration: "embers of us" (short film)': [('c17', 'p1')],
}

# These are the EXACT emails from the user's production logs
TEST_CASES = [
    # === HUMAN REPLIES (MUST be HUMAN_REPLY) ===
    {
        'sender': 'mandy@mandykaymarketing.com',
        'subject': 'Re: Scaling Mandy Kay Marketing?',
        'body': 'Hi, thanks for reaching out...',
        'expected': 'HUMAN_REPLY',
        'label': 'Mandy Kay direct reply',
    },
    {
        'sender': 'immensityit@gmail.com',
        'subject': 'Re: quick one for Immensity It',
        'body': 'Hey, we are interested...',
        'expected': 'HUMAN_REPLY',
        'label': 'Immensity IT direct reply',
    },
    {
        'sender': 'rhea@peppershock.com',
        'subject': "Here's VIP entrance to my crowded inbox! 🫶 Re: Scaling Peppershock?",
        'body': 'Thanks for reaching out!',
        'expected': 'HUMAN_REPLY',
        'label': 'Peppershock creative reply',
    },
    {
        'sender': 'oscar.fullmer@fasthippomedia.com',
        'subject': 'Re: Scaling Fast Hippo Media?',
        'body': 'Not interested at this time.',
        'expected': 'HUMAN_REPLY',
        'label': 'Fast Hippo Media reply',
    },
    {
        'sender': 'action@colagecommunication.in',
        'subject': 'Re: Quick question about "colage Communication',
        'body': 'Please tell me more.',
        'expected': 'HUMAN_REPLY',
        'label': 'Colage Communication reply',
    },
    {
        'sender': 'vince@meltatl.com',
        'subject': 'Re: Scaling Melt Sports/culinary/ Entertainment?',
        'body': 'What services do you offer?',
        'expected': 'HUMAN_REPLY',
        'label': 'Melt Sports reply',
    },
    {
        'sender': 'rs@aqaba.digital',
        'subject': 'Re: Scaling Aqaba Digital?',
        'body': 'Interesting, let me know more.',
        'expected': 'HUMAN_REPLY',
        'label': 'Aqaba Digital reply',
    },
    {
        'sender': 'marcelo@m4worldwide.com',
        'subject': 'Re: Scaling M4 Worldwide?',
        'body': 'Remove me from your list.',
        'expected': 'HUMAN_REPLY',
        'label': 'M4 Worldwide reply',
    },
    {
        'sender': 'ray@smb360.io',
        'subject': 'Re: Scaling Smb 360?',
        'body': 'Thanks, can we schedule a call?',
        'expected': 'HUMAN_REPLY',
        'label': 'SMB 360 reply',
    },
    {
        'sender': 'colan@schureconsulting.com',
        'subject': '**Attention** New Email - please read. Re: Scaling Elevation',
        'body': 'I forwarded this to our team.',
        'expected': 'HUMAN_REPLY',
        'label': 'Schure Consulting forwarded reply',
    },
    {
        'sender': 'education@cineworks.ca',
        'subject': 'Re: For your consideration: "Embers of Us" (Short Film)',
        'body': 'Thank you for sending this.',
        'expected': 'HUMAN_REPLY',
        'label': 'Cineworks reply to film pitch',
    },
    
    # === BOUNCES (MUST be BOUNCE) ===
    {
        'sender': 'mailer-daemon@googlemail.com',
        'subject': 'Delivery Status Notification (Failure)',
        'body': 'Your message to john@deadcompany.com could not be delivered.',
        'expected': 'BOUNCE',
        'label': 'Google mailer-daemon bounce',
    },
    {
        'sender': 'postmaster@netorgft3473216.onmicrosoft.com',
        'subject': 'Undeliverable: Scaling Consult Vito?',
        'body': 'Delivery has failed to these recipients.',
        'expected': 'BOUNCE',
        'label': 'Microsoft Undeliverable bounce',
    },
    {
        'sender': 'postmaster@netorgft5598755.onmicrosoft.com',
        'subject': 'Undeliverable: Scaling Internet Marketing Pros?',
        'body': 'This message could not be delivered.',
        'expected': 'BOUNCE',
        'label': 'Microsoft Undeliverable bounce 2',
    },
    {
        'sender': 'postmaster@revlinemarketing.com',
        'subject': 'Undeliverable: Scaling Revline Marketing?',
        'body': 'The email account does not exist.',
        'expected': 'BOUNCE',
        'label': 'Revline postmaster bounce',
    },
    
    # === AUTO REPLIES (MUST be AUTO_REPLY) ===
    {
        'sender': 'ashley@sandpiperagency.com',
        'subject': 'Out of Office - Limited Access to Email Re: Scaling Sandpiper Agency?',
        'body': 'I am currently out of the office with limited access.',
        'expected': 'AUTO_REPLY',
        'label': 'Sandpiper OOO',
    },
    {
        'sender': 'anthony@mkswebdesign.com',
        'subject': 'Out of the office Re: Scaling Mks Web Design?',
        'body': 'I will be out of the office until April 5th.',
        'expected': 'AUTO_REPLY',
        'label': 'MKS Web Design OOO',
    },
    {
        'sender': 'bob@bobgoldpr.com',
        'subject': 'Automatic reply: Scaling Bob Gold & Associates?',
        'body': 'Thank you for your email. I am out of the office.',
        'expected': 'AUTO_REPLY',
        'label': 'Bob Gold auto reply',
    },
    {
        'sender': 'peter@mailpix.com',
        'subject': 'Automatic reply: Scaling Mailpix?',
        'body': 'I am currently out of the office.',
        'expected': 'AUTO_REPLY',
        'label': 'Mailpix auto reply',
    },
    {
        'sender': 'julianano@standoutconsultingservices.com',
        'subject': 'Out of office - Spring Break Re:Scaling Stand Out Consulting Services?',
        'body': 'I am on Spring Break and will return April 7th.',
        'expected': 'AUTO_REPLY',
        'label': 'Stand Out Consulting OOO',
    },
    {
        'sender': 'jcrouch@relentless-digital.com',
        'subject': 'Out of Office Re: Scaling Relentless Digital Marketing Solutions?',
        'body': 'I am out of the office.',
        'expected': 'AUTO_REPLY',
        'label': 'Relentless Digital OOO',
    },
    
    # === SPAM (MUST be SPAM) ===
    {
        'sender': 'ceo@focusmate.com',
        'subject': 'Will your Focusmate be a weirdo?',
        'body': 'Join Focusmate and be productive.',
        'expected': 'SPAM',
        'label': 'Focusmate newsletter',
    },
    {
        'sender': 'noreply@uber.com',
        'subject': 'Your Tuesday afternoon trip with Uber',
        'body': 'Trip receipt.',
        'expected': 'SPAM',
        'label': 'Uber receipt',
    },
    {
        'sender': 'hello@mail.blinkist.com',
        'subject': '🚀 Become sharper through expert guidance',
        'body': 'Read more non-fiction in 15 minutes.',
        'expected': 'SPAM',
        'label': 'Blinkist newsletter',
    },
    {
        'sender': 'noreply@redditmail.com',
        'subject': '"I\'m giving this away for free. Does anyone want it?"',
        'body': 'Trending on Reddit.',
        'expected': 'SPAM',
        'label': 'Reddit notification',
    },
    {
        'sender': 'news@team.semrush.com',
        'subject': 'How to write content AI will cite',
        'body': 'SEMrush newsletter.',
        'expected': 'SPAM',
        'label': 'Semrush newsletter',
    },
    {
        'sender': 'no-reply@accounts.google.com',
        'subject': 'Security alert',
        'body': 'A new device signed in.',
        'expected': 'SPAM',
        'label': 'Google security alert',
    },
    {
        'sender': 'nytimes@e.newyorktimes.com',
        'subject': 'Ends tomorrow. Our best offer: ₹25 a week.',
        'body': 'Subscribe to NYT.',
        'expected': 'SPAM',
        'label': 'NYT marketing',
    },
    {
        'sender': 'ant.wilson@supabase.com',
        'subject': 'Your Supabase Project Linkedin poster has been paused.',
        'body': 'Your project was paused due to inactivity.',
        'expected': 'SPAM',
        'label': 'Supabase notification',
    },
    {
        'sender': 'contact@colddms.com',
        'subject': 'your unfair advantage ends tomorrow',
        'body': 'Buy our course.',
        'expected': 'SPAM',
        'label': 'Cold DMs marketing',
    },
]

def run_tests():
    passed = 0
    failed = 0
    total = len(TEST_CASES)
    
    print(f"{'='*70}")
    print(f"  DETERMINISTIC EMAIL CLASSIFIER TEST")
    print(f"  Testing {total} emails from live production logs")
    print(f"{'='*70}\n")
    
    for i, tc in enumerate(TEST_CASES, 1):
        result = classify_email(
            sender=tc['sender'],
            subject=tc['subject'],
            body_snippet=tc['body'],
            prospect_emails=MOCK_PROSPECT_EMAILS,
            subject_map=MOCK_SUBJECT_MAP,
        )
        
        actual = result['classification']
        expected = tc['expected']
        match = actual == expected
        
        icon = "✅" if match else "❌"
        print(f"  {icon} [{i:02d}/{total}] {tc['label']}")
        print(f"       Sender:   {tc['sender']}")
        print(f"       Subject:  {tc['subject'][:60]}")
        print(f"       Expected: {expected} | Got: {actual} | Reason: {result['reason']}")
        
        if result['matched_email']:
            print(f"       Matched:  {result['matched_email']} ({result['matched_company']})")
        
        if match:
            passed += 1
        else:
            failed += 1
            print(f"       *** FAILED ***")
        
        print()
    
    print(f"{'='*70}")
    print(f"  RESULTS: {passed}/{total} passed, {failed}/{total} failed")
    if failed == 0:
        print(f"  🎯 PERFECT SCORE — Every email classified correctly!")
    else:
        print(f"  ⚠️  {failed} FAILURES — Must be fixed before deployment!")
    print(f"{'='*70}")
    
    return failed == 0

if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)
