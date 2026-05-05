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
from pydantic import BaseModel, Field
import anthropic

class CopyDraftSchema(BaseModel):
    introParagraph: str = Field(description="2-3 sentence intro grounding the reader in local geography and community. Must include the crossroads if provided.")
    bullet1: str = Field(description="Full text of bullet 1.")
    bullet1Tag: str = Field(description="Exact category name from the EXR Library (e.g. 'Home Community', 'Nearby Neighborhoods', etc.)")
    bullet2: str = Field(description="Full text of bullet 2.")
    bullet2Tag: str = Field(description="Exact category name from the EXR Library")
    bullet3: str = Field(description="Full text of bullet 3.")
    bullet3Tag: str = Field(description="Exact category name from the EXR Library")
    bullet4: str = Field(description="Full text of bullet 4.")
    bullet4Tag: str = Field(description="Exact category name from the EXR Library")


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
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# --- BATCH SETTINGS ---
MODEL_NAME = 'claude-sonnet-4-6'              # Claude Sonnet 4.6
BATCH_LIMIT = 100                          # Production test batch
MAX_WORKERS = 5                            # How many stores to process in parallel
OVERWRITE_EXISTING = True                  # Forcing refresh to test the new model

# Market rates per 1M tokens (USD)
MODEL_COSTS = {
    'gemini-2.5-flash': {'input': 0.10 / 1_000_000, 'output': 0.40 / 1_000_000},
    'gemini-2.0-flash': {'input': 0.10 / 1_000_000, 'output': 0.40 / 1_000_000},
    'gemini-1.5-pro':   {'input': 3.50 / 1_000_000, 'output': 10.50 / 1_000_000},
    'claude-sonnet-4-6': {'input': 3.00 / 1_000_000, 'output': 15.00 / 1_000_000},
    'claude-3-5-sonnet-20241022': {'input': 3.00 / 1_000_000, 'output': 15.00 / 1_000_000}
}
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


def _build_prompt(rules, gold_standards, row_dict):
    """Builds the shared prompt text used by both Gemini and Claude."""
    return f"""
You are an Extra Space Storage SEO copywriter. Your task is to write the
'About Our Location' section for a specific facility page.

You must strictly follow the rules in the EXR Content Bullet Library AND the
Strategic Market Direction below. Strategic rules take full precedence.

------------------------------------------------------------
## RULE HIERARCHY (highest priority first)
------------------------------------------------------------

### RULE 1 - SEGMENT STRATEGY (determines your overall angle)
Read the 'Segment' field carefully. It controls the entire editorial direction.

**SEGMENT A - HOME DOMINANT**
This store's customers are overwhelmingly local (one ZIP code dominates).
- Bullet #1 MUST be "Home Community" - describe the immediate neighborhood using
  names from `POIs - Neighborhoods`. Be specific. Do not use generic references.
- Bullet #2 MUST be "Nearby Neighborhoods" - list 2-3 specific neighborhood names
  pulled directly from `POIs - Neighborhoods`. Distance cues (e.g. "just half a mile
  away") are encouraged.
- DO NOT use "Second City Draw" as a bullet for Segment A stores.
- Intro tone: warm, hyperlocal, community-focused.

**SEGMENT B - MIXED CATCHMENT**
This store draws from multiple ZIPs with no single dominant source.
- Bullet #1 should be "Home Community" or "Nearby Neighborhoods".
- If a second city appears in `Second City Draw`, acknowledge it naturally in the
  intro or a bullet (e.g. "convenient for residents coming from [City]").
- Intro tone: balanced, access-forward, broad appeal.

**SEGMENT C - HIGHLY DISTRIBUTED**
Customers arrive from many different ZIP codes. No single community dominates.
- Lead with convenience and access - prioritize "Interstate/Highway Exit" and
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

### RULE 2 - CROSSROADS (mandatory geography anchor)
[PRIMARY_CROSSROADS]: {row_dict.get('nearest_major_intersection', 'Not Available')}

If [PRIMARY_CROSSROADS] is provided (not "Not Available"), you MUST include it
in the introductory paragraph as the geographic anchor. Example:
"Conveniently located near the intersection of [Crossroads], our [City] storage
facility serves the surrounding neighborhoods of..."

---

### RULE 3 - CONTENT HOOK OVERRIDE (Bullet #2 only)
If 'Content Hook (Bullet 2)' is provided in the data, use that strategic angle
for your second bullet - UNLESS the Segment is "Misaligned" (Rule 1 wins).

---

### RULE 4 - DEMOGRAPHIC MODIFIERS (apply after Segment rules)
These adjust tone and secondary bullets, not the primary bullet selection.

- MILITARY: If 'military_base_distance_mi' < 15 - include a military/deployment
  angle in a bullet or the intro.
- SENIORS: If 'Wtd. Median Age' > 55 - shift tone toward downsizing, convenience,
  and peace of mind.
- UNIVERSITY: If `POIs - Universities/Colleges` has a result within 5 miles -
  include a student storage mention for summer/dorm transitions.
- PREMIUM: If 'Wtd. Median Income' > 100000 - use a professional, secure, and
  elevated tone (business inventory, climate control, high-value items).
- RENTERS: If 'Wtd. Renter Rate %' > 40 - reference apartment decluttering and
  flexible month-to-month storage for frequent movers.

---

### RULE 5 - ZIP COMMUNITY ACKNOWLEDGMENT
Read `zip_customer_mix` from the data. It is a list of ZIP codes and their
customer share percentages.
- If any Non-Home ZIP has a share > 10%, you MUST acknowledge that community
  somewhere in the copy (intro or a bullet) by referencing its city or
  neighborhood name. Example: "We also welcome neighbors from [nearby city]."
- Use the city name associated with the ZIP, not the ZIP code number itself.

---

### RULE 6 - POI MINING (use real names, not generic references)
You have access to pipe-delimited lists of verified nearby POIs. Always pull
real, specific names from these lists rather than writing generic copy.

- `POIs - Neighborhoods` - Use actual neighborhood names in Home Community /
  Nearby Neighborhoods bullets (e.g. "Sabal Palms Estates" not "local neighborhoods").
- `POIs - Schools` - If referencing local schools, use real names (e.g.
  "Silver Lakes Middle School" not "nearby schools").
- `POIs - Residential Areas` - Use real apartment/condo complex names when
  targeting renters (e.g. "residents of Courtyards of Broward...").

### RULE 7 - TIERED BULLET SELECTION (fill from Tier 1 down)
When building bullets, strictly follow this 4-tier priority system. Fill from
Tier 1 down. Stop when you have 4 bullets.

**TIER 1 - ALWAYS INCLUDE (core story)**
- C-01 Home Community: Always present. Every location, no exceptions.
- C-02 Nearby Neighborhoods: When neighborhood data exists. Readers in those
  areas recognize the names immediately.
- U-01 Urban Residential Communities: The urban equivalent of C-02. In downtown
  and dense urban markets, treat this as Tier 1. Urban audiences recognize their
  building names the same way suburban audiences recognize subdivisions.
- C-03 Second City Draw: Include whenever cross-municipal draw is confirmed
  (>=10% of customers from a different city).

**TIER 2 - STRONG SIGNALS (almost always earn a bullet when confirmed)**
- G-01 Interstate/Highway Exit: Very strong signal. Include whenever a nearby
  junction is confirmed. Rarely optional.
- G-02 Airport Proximity: Less common, but earns a bullet whenever confirmed.
- A-01 University/College: Must-include in any market where a college or
  university is within the search radius.
- A-02 Military Base: Must-include near active installations. Name the base and
  speak to PCS moves and deployment directly.

**TIER 3 - FILL-IN (use only when Tier 1 + 2 don't fill all 4 slots)**
- A-03 Local Schools: Less compelling than institutional anchors. Best for
  Growing Families persona only. Not a reflexive add-on.
- A-04 Notable Landmark: Earns its own bullet only when the landmark is broadly
  recognizable without explanation (stadium, regional hospital, major attraction).
  If you'd have to explain what it is, weave it into intro copy instead.

**TIER 4 - INTRO MENTION ONLY (rarely justify their own bullet)**
- L-01 Marina/Waterfront: Own bullet only when boat storage is confirmed AND no
  stronger option is available. Otherwise, reference in intro as lifestyle texture.
- L-02 RV Park/Outdoor: Same logic as L-01. Own bullet only when RV storage is
  confirmed and no better option exists.

**Key Principle:** The goal is 4 bullets that tell a coherent, specific story
about why THIS location is the right choice for THIS audience. A strong intro
that mentions a nearby landmark is almost always better than a weak landmark bullet.

------------------------------------------------------------
## REFERENCE MATERIALS
------------------------------------------------------------

Master Rulebook:
{rules}

Gold Standard Examples (Mimic this cadence, structure, and mixing strategy):
{gold_standards}

------------------------------------------------------------
## LOCATION DATA FOR THIS STORE
------------------------------------------------------------

{json.dumps(row_dict, indent=2, default=str)}
"""


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def generate_copy_for_facility(client, rules, gold_standards, row_dict):
    """Calls Gemini to generate the copy based on rules and provided data. Returns (json, prompt, usage)."""
    prompt = _build_prompt(rules, gold_standards, row_dict) + """
------------------------------------------------------------
## OUTPUT INSTRUCTIONS
------------------------------------------------------------

Your output must be a single JSON object. 
Every bullet tag MUST be selected from the 12 official EXR Library categories.
Do not invent new categories.
"""

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            response_mime_type="application/json",
            response_json_schema=CopyDraftSchema.model_json_schema()
        )
    )
    usage = response.usage_metadata
    
    # Use the Pydantic model to validate and parse the response
    try:
        validated_data = CopyDraftSchema.model_validate_json(response.text)
        return validated_data.model_dump(), prompt, usage
    except Exception as e:
        print(f"Schema validation failed: {e}")
        # Fallback to standard json loads if validation fails
        data = json.loads(response.text)
        return data, prompt, usage


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def generate_with_claude(client, rules, gold_standards, row_dict):
    """Calls Claude 3.5 Sonnet to generate copy using Tool Use for structured JSON."""
    prompt = _build_prompt(rules, gold_standards, row_dict)

    # Anthropic "Tool Use" to force JSON structure equivalent to Pydantic schema
    response = client.messages.create(
        model=MODEL_NAME,
        max_tokens=2048,
        tools=[{
            "name": "generate_copy_draft",
            "description": "Generate a structured copy draft for an Extra Space Storage facility.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "introParagraph": {"type": "string", "description": "2-3 sentence intro grounding the reader in local geography and community. Must include the crossroads if provided."},
                    "bullet1": {"type": "string", "description": "Full text of bullet 1."},
                    "bullet1Tag": {"type": "string", "description": "Exact category name from the EXR Library (e.g. 'Home Community', 'Nearby Neighborhoods', etc.)"},
                    "bullet2": {"type": "string", "description": "Full text of bullet 2."},
                    "bullet2Tag": {"type": "string", "description": "Exact category name from the EXR Library"},
                    "bullet3": {"type": "string", "description": "Full text of bullet 3."},
                    "bullet3Tag": {"type": "string", "description": "Exact category name from the EXR Library"},
                    "bullet4": {"type": "string", "description": "Full text of bullet 4."},
                    "bullet4Tag": {"type": "string", "description": "Exact category name from the EXR Library"},
                },
                "required": ["introParagraph", "bullet1", "bullet1Tag", "bullet2", "bullet2Tag", "bullet3", "bullet3Tag", "bullet4", "bullet4Tag"]
            }
        }],
        tool_choice={"type": "tool", "name": "generate_copy_draft"},
        messages=[{"role": "user", "content": prompt}]
    )
    
    # Extract data from tool use response
    tool_use_block = next(block for block in response.content if block.type == "tool_use")
    data = tool_use_block.input
    
    # Map usage metadata to match our tracker's format
    usage = type('obj', (object,), {
        'prompt_token_count': response.usage.input_tokens,
        'candidates_token_count': response.usage.output_tokens
    })
    
    return data, prompt, usage


def process_single_store(store_index, total_stores, row, db_pool, gemini_client, anthropic_client, rules, gold_standards):
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
        
        if 'claude' in MODEL_NAME:
            copy_json, final_prompt, usage = generate_with_claude(anthropic_client, rules, gold_standards, row_dict)
        else:
            copy_json, final_prompt, usage = generate_copy_for_facility(gemini_client, rules, gold_standards, row_dict)
        
        # Calculate Cost
        input_tokens = usage.prompt_token_count or 0
        output_tokens = usage.candidates_token_count or 0
        rates = MODEL_COSTS.get(MODEL_NAME, {'input': 0, 'output': 0})
        estimated_cost = (input_tokens * rates['input']) + (output_tokens * rates['output'])

        print(f"[{store_index}/{total_stores}] Store {store_num}: Saving Draft to Postgres (Cost: ${estimated_cost:,.4f})...", flush=True)
        cur.execute("""
            INSERT INTO "CopyDraft" (
                "facilityId", "introParagraph", "bullet1", "bullet1Tag", "bullet2", "bullet2Tag", 
                "bullet3", "bullet3Tag", "bullet4", "bullet4Tag", "prompt", 
                "inputTokens", "outputTokens", "cost", "modelName", "status", "updatedAt"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Pending', NOW())
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
            "inputTokens" = EXCLUDED."inputTokens",
            "outputTokens" = EXCLUDED."outputTokens",
            "cost" = EXCLUDED."cost",
            "modelName" = EXCLUDED."modelName",
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
            final_prompt,
            input_tokens,
            output_tokens,
            estimated_cost,
            MODEL_NAME
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
    
    gemini_client = genai.Client(api_key=GOOGLE_API_KEY)
    anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    print(f"\nStarting parallel execution with {MAX_WORKERS} workers...\n", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i, (_, row) in enumerate(test_df.iterrows()):
            executor.submit(
                process_single_store, 
                i + 1, total_stores, row, db_pool, gemini_client, anthropic_client, rules, gold_standards
            )

    db_pool.closeall()
    print("\nBatch generation complete!")

if __name__ == "__main__":
    main()
