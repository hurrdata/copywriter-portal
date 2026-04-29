import os
import json
import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types as genai_types
from bs4 import BeautifulSoup
import backoff
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import pool

# Load environment variables from the Next.js portal
# Look in current dir, then parent dir
if os.path.exists('.env'):
    load_dotenv('.env')
elif os.path.exists('../.env'):
    load_dotenv('../.env')
elif os.path.exists('copywriter-portal/.env'):
    load_dotenv('copywriter-portal/.env')

DATABASE_URL = os.environ.get("DIRECT_URL")
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")

# --- BATCH SETTINGS ---
MODEL_NAME = 'gemini-2.5-flash'
BATCH_LIMIT = 5       # Small sample test as requested
MAX_WORKERS = 5       # How many stores to process in parallel
OVERWRITE_EXISTING = True # Forcing refresh to test the new rules
# ----------------------

def strip_html(html_str):
    if not html_str: return ""
    return BeautifulSoup(html_str, "html.parser").get_text(separator="\n", strip=True)

def load_context_files():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, 'EXR_Content_Bullet_Library_v2.html'), 'r') as f:
        rules = strip_html(f.read())
    with open(os.path.join(script_dir, 'human_gold_standards.html'), 'r') as f:
        gold_standards = strip_html(f.read())
    return rules, gold_standards

def insert_facility(cur, row_dict):
    """Inserts a facility into the Prisma Facility table if it doesn't exist, returning its ID."""
    try:
        cur.execute("""
            INSERT INTO "Facility" ("storeNumber", "address", "city", "state", "zip", "geoData", "demographicData", "updatedAt")
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT ("storeNumber") DO UPDATE SET "updatedAt" = NOW()
            RETURNING id;
        """, (
            str(row_dict.get('Store Number', '')),
            str(row_dict.get('Address', '')),
            str(row_dict.get('City', '')),
            str(row_dict.get('State', '')),
            str(row_dict.get('Zip', '')),
            json.dumps(row_dict, default=str), # Use default=str for non-serializable types
            json.dumps({'persona': row_dict.get('Demographic Persona', '')}) 
        ))
        res = cur.fetchone()
        return res[0] if res else None
    except Exception as e:
        print(f"Error inserting Facility: {e}")
        return None

@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def generate_copy_for_facility(client, rules, gold_standards, row_dict):
    """Calls Gemini to generate the copy based on rules and provided data. Returns (json, prompt)."""
    prompt = f"""
You are an Extra Space Storage SEO copywriter. Your task is to write the
'About Our Location' section for a specific facility page.

You must strictly follow the rules in the EXR Content Bullet Library AND the
Strategic Market Direction below. Strategic rules take full precedence.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## RULE HIERARCHY (highest priority first)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### RULE 1 — SEGMENT STRATEGY (determines your overall angle)
Read the 'Segment' field carefully. It controls the entire editorial direction.

**SEGMENT A — HOME DOMINANT**
This store's customers are overwhelmingly local (one ZIP code dominates).
- Bullet #1 MUST be "Home Community" — describe the immediate neighborhood using
  names from `POIs — Neighborhoods`. Be specific. Do not use generic references.
- Bullet #2 MUST be "Nearby Neighborhoods" — list 2-3 specific neighborhood names
  pulled directly from `POIs — Neighborhoods`. Distance cues (e.g. "just half a mile
  away") are encouraged.
- DO NOT use "Second City Draw" as a bullet for Segment A stores.
- Intro tone: warm, hyperlocal, community-focused.

**SEGMENT B — MIXED CATCHMENT**
This store draws from multiple ZIPs with no single dominant source.
- Bullet #1 should be "Home Community" or "Nearby Neighborhoods".
- If a second city appears in `Second City Draw`, acknowledge it naturally in the
  intro or a bullet (e.g. "convenient for residents coming from [City]").
- Intro tone: balanced, access-forward, broad appeal.

**SEGMENT C — HIGHLY DISTRIBUTED**
Customers arrive from many different ZIP codes. No single community dominates.
- Lead with convenience and access — prioritize "Interstate/Highway Exit" and
  "Airport Proximity" bullets if distances qualify.
- Avoid hyper-neighborhood language since no single community is dominant.
- Intro tone: logistics-focused, location-advantage driven.

**MISALIGNED**
The existing content angle does not match the actual customer data.
- Ignore the 'Content Angle (existing)' and 'Primary Signal (existing)' fields.
- Build the copy from scratch using ONLY the demographic data and POI data.
- If `Misaligned Sub-Type` is "Attractor Driven" (e.g., near an airport), the
  existing copy over-indexed on that attractor. Redirect to the actual community.
- Intro tone: reset to what the data supports, not the legacy angle.

---

### RULE 2 — CROSSROADS (mandatory geography anchor)
[PRIMARY_CROSSROADS]: {row_dict.get('nearest_major_intersection', 'Not Available')}

If [PRIMARY_CROSSROADS] is provided (not "Not Available"), you MUST include it
in the introductory paragraph as the geographic anchor. Example:
"Conveniently located near the intersection of [Crossroads], our [City] storage
facility serves the surrounding neighborhoods of..."

---

### RULE 3 — CONTENT HOOK OVERRIDE (Bullet #2 only)
If 'Content Hook (Bullet 2)' is provided in the data, use that strategic angle
for your second bullet — UNLESS the Segment is "Misaligned" (Rule 1 wins).

---

### RULE 4 — DEMOGRAPHIC MODIFIERS (apply after Segment rules)
These adjust tone and secondary bullets, not the primary bullet selection.

- MILITARY: If 'military_base_distance_mi' < 15 → include a military/deployment
  angle in a bullet or the intro.
- SENIORS: If 'Wtd. Median Age' > 55 → shift tone toward downsizing, convenience,
  and peace of mind.
- UNIVERSITY: If `POIs — Universities/Colleges` has a result within 5 miles →
  include a student storage mention for summer/dorm transitions.
- PREMIUM: If 'Wtd. Median Income' > 100000 → use a professional, secure, and
  elevated tone (business inventory, climate control, high-value items).
- RENTERS: If 'Wtd. Renter Rate %' > 40 → reference apartment decluttering and
  flexible month-to-month storage for frequent movers.

---

### RULE 5 — ZIP COMMUNITY ACKNOWLEDGMENT
Read `zip_customer_mix` from the data. It is a list of ZIP codes and their
customer share percentages.
- If any Non-Home ZIP has a share > 10%, you MUST acknowledge that community
  somewhere in the copy (intro or a bullet) by referencing its city or
  neighborhood name. Example: "We also welcome neighbors from [nearby city]."
- Use the city name associated with the ZIP, not the ZIP code number itself.

---

### RULE 6 — POI MINING (use real names, not generic references)
You have access to pipe-delimited lists of verified nearby POIs. Always pull
real, specific names from these lists rather than writing generic copy.

- `POIs — Neighborhoods` → Use actual neighborhood names in Home Community /
  Nearby Neighborhoods bullets (e.g. "Sabal Palms Estates" not "local neighborhoods").
- `POIs — Schools` → If referencing local schools, use real names (e.g.
  "Silver Lakes Middle School" not "nearby schools").
- `POIs — Residential Areas` → Use real apartment/condo complex names when
  targeting renters (e.g. "residents of Courtyards of Broward...").
### RULE 7 — PRIORITY MATRIX & COMBINATORIAL BULLETS
When building bullets, strictly adhere to these priorities:
- **Top Priorities (Must Haves if applicable):** Home community, nearby neighborhoods, second city draw. Interstate access (ONLY if very close; otherwise deprioritize). College & Military (very strong signal, feature every time if close). Urban residential communities (high priority in large/downtown cities).
- **Secondary Priorities (Backups):** Nearby schools, landmarks. Marina/RV (small % of stores, use only if very prominent).
- **The "Santa Cruz" Mix & Match Rule:** Being narrowly focused is great, but flexibility is allowed to get "an extra bullet in" (fitting 5 concepts into 4 slots). For example, if a store is highly urban, you can combine Interstate Access + Specific Neighborhoods into a single bullet to save room for College or Military.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## REFERENCE MATERIALS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Master Rulebook:
{rules}

Gold Standard Examples (Mimic this cadence, structure, and mixing strategy):
{gold_standards}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## LOCATION DATA FOR THIS STORE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{json.dumps(row_dict, indent=2, default=str)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
## OUTPUT FORMAT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Return ONLY a raw JSON object. No markdown. No backticks. No explanation.
Every bullet tag MUST be selected from the 12 official EXR Library categories.
Do not invent new categories.

{{
  "introParagraph": "2-3 sentence intro grounding the reader in local geography and community. Must include the crossroads if provided.",
  "bullet1": "Full text of bullet 1.",
  "bullet1Tag": "Exact category name from the EXR Library",
  "bullet2": "Full text of bullet 2.",
  "bullet2Tag": "Exact category name from the EXR Library",
  "bullet3": "Full text of bullet 3.",
  "bullet3Tag": "Exact category name from the EXR Library",
  "bullet4": "Full text of bullet 4.",
  "bullet4Tag": "Exact category name from the EXR Library"
}}
"""

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=genai_types.GenerateContentConfig(response_mime_type="application/json")
    )
    data = json.loads(response.text)
    if isinstance(data, list) and len(data) > 0:
        return data[0], prompt
    return data, prompt

def process_single_store(store_index, total_stores, row, db_pool, client, rules, gold_standards):
    """Processes a single store: Check DB -> Generate -> Save."""
    store_num = row['Store Number']
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        
        # 1. Ensure Facility exists and get its ID
        row_dict = row.fillna('').to_dict()
        facility_id = insert_facility(cur, row_dict)
        if not facility_id:
            print(f"[{store_index}/{total_stores}] Store {store_num}: Failed to insert/fetch facility.", flush=True)
            return

        # 2. Resume Logic Check
        if not OVERWRITE_EXISTING:
            cur.execute('SELECT 1 FROM "CopyDraft" WHERE "facilityId" = %s', (facility_id,))
            if cur.fetchone():
                print(f"[{store_index}/{total_stores}] Store {store_num}: Skipping (Draft already exists).", flush=True)
                return

        # 3. Fetch enriched data from DB
        cur.execute('SELECT "geoData" FROM "Facility" WHERE id = %s', (facility_id,))
        res = cur.fetchone()
        db_geo = res[0] if res else None
        if db_geo:
            row_dict.update(db_geo)

        print(f"[{store_index}/{total_stores}] Store {store_num}: Requesting Copy from {MODEL_NAME}...", flush=True)
        copy_json, final_prompt = generate_copy_for_facility(client, rules, gold_standards, row_dict)
        
        print(f"[{store_index}/{total_stores}] Store {store_num}: Saving Draft to Postgres (with prompt audit)...", flush=True)
        cur.execute("""
            INSERT INTO "CopyDraft" ("facilityId", "introParagraph", "bullet1", "bullet1Tag", "bullet2", "bullet2Tag", "bullet3", "bullet3Tag", "bullet4", "bullet4Tag", "prompt", "status", "updatedAt")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Pending', NOW())
            ON CONFLICT ("facilityId") DO UPDATE SET
            "introParagraph" = EXCLUDED."introParagraph",
            "bullet1" = EXCLUDED."bullet1",
            "bullet1Tag" = EXCLUDED."bullet1Tag",
            "bullet2" = EXCLUDED."bullet2",
            "bullet2Tag" = EXCLUDED."bullet2Tag",
            "bullet3" = EXCLUDED."bullet3",
            "bullet3Tag" = EXCLUDED."bullet3Tag",
            "bullet4" = EXCLUDED."bullet4",
            "bullet4Tag" = EXCLUDED."bullet4Tag",
            "prompt" = EXCLUDED."prompt",
            "updatedAt" = NOW();
        """, (
            facility_id,
            copy_json.get('introParagraph', ''),
            copy_json.get('bullet1', ''),
            copy_json.get('bullet1Tag', ''),
            copy_json.get('bullet2', ''),
            copy_json.get('bullet2Tag', ''),
            copy_json.get('bullet3', ''),
            copy_json.get('bullet3Tag', ''),
            copy_json.get('bullet4', ''),
            copy_json.get('bullet4Tag', ''),
            final_prompt
        ))
        conn.commit()
        print(f"[{store_index}/{total_stores}] Store {store_num}: Generated and Saved!", flush=True)
        
    except Exception as e:
        print(f"[{store_index}/{total_stores}] Store {store_num}: Error - {e}", flush=True)
    finally:
        db_pool.putconn(conn)

def main():
    if not GOOGLE_API_KEY:
        print("ERROR: Missing GOOGLE_API_KEY in copywriter-portal/.env")
        return
        
    print("Connecting to Supabase (Pool)...", flush=True)
    db_pool = pool.ThreadedConnectionPool(1, MAX_WORKERS + 2, DATABASE_URL)
    print(" -> Pool initialized.", flush=True)
    
    print("Loading Rulebook & Examples...", flush=True)
    # Helper to find files in script dir or parent
    def find_file(name):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        paths = [name, os.path.join(script_dir, name), os.path.join(script_dir, '..', name)]
        for p in paths:
            if os.path.exists(p): return p
        return name

    rules, gold_standards = load_context_files()
    
    print("Reading expansion files...", flush=True)
    master_file = find_file('output_final.xlsx')
    expansion_file = find_file('SEO_Zipcode_Target_Expansion 1.xlsx')
    
    df = pd.read_excel(master_file)
    expansion_df = pd.read_excel(expansion_file)
    test_stores = expansion_df[expansion_df['Groups'] == 'test']['Store Number'].astype(str).tolist()
    
    df['Store Number'] = df['Store Number'].astype(str)
    test_df = df[df['Store Number'].isin(test_stores)].copy()
    test_df['StoreNumInt'] = test_df['Store Number'].astype(int)
    test_df = test_df.sort_values('StoreNumInt')
    
    if BATCH_LIMIT:
        test_df = test_df.head(BATCH_LIMIT)
    
    total_stores = len(test_df)
    print(f"Targeting {total_stores} facilities for generation.", flush=True)
    
    client = genai.Client(api_key=GOOGLE_API_KEY)
    
    print(f"\nStarting parallel execution with {MAX_WORKERS} workers...\n", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i, (_, row) in enumerate(test_df.iterrows()):
            executor.submit(
                process_single_store, 
                i + 1, total_stores, row, db_pool, client, rules, gold_standards
            )

    db_pool.closeall()
    print("\nBatch generation complete!")

if __name__ == "__main__":
    main()
