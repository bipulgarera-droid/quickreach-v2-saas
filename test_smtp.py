from dotenv import load_dotenv
load_dotenv()
try:
    from execution.smtp_pool import SMTPPool
    print("Loading pool...")
    pool = SMTPPool()
    print("Usage:", pool.get_total_usage())
    print("Limit:", pool.get_total_limit())
except Exception as e:
    import traceback
    traceback.print_exc()
