import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def update_store_172():
    url = os.getenv("DIRECT_URL")
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    
    # Check current status with integer ID
    cur.execute('SELECT "facilityId", status, "modelName" FROM "CopyDraft" WHERE "facilityId" = 172;')
    row = cur.fetchone()
    print(f"Current State for Store 172: {row}")
    
    # Force update to 'Pending' (which shows as Needs Review in UI)
    if row:
        cur.execute('UPDATE "CopyDraft" SET status = \'Pending\' WHERE "facilityId" = 172;')
        conn.commit()
        print("Status updated to 'Pending' (Needs Review)")
    else:
        print("Store 172 not found in CopyDraft yet. It might still be generating.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    update_store_172()
