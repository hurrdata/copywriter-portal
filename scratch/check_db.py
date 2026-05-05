import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_db():
    url = os.getenv("DIRECT_URL")
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    
    cur.execute('SELECT "facilityId", status, "modelName" FROM "CopyDraft" LIMIT 5;')
    rows = cur.fetchall()
    print("Sample Rows:")
    for row in rows:
        print(row)
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_db()
