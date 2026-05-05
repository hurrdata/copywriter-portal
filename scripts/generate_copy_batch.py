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
MAX_WORKERS = 1                            # Lowered to 1 to stay under 10k/min rate limit
OVERWRITE_EXISTING = True                  # Forcing refresh to test the new model
RATE_LIMIT_SLEEP = 30                      # Seconds to wait between stores to avoid 429s
TARGET_STORES = []                         # If not empty, only process these store numbers (e.g. ['172'])
# ----------------------

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


def _build_static_context(rules, gold_standards):
    """Builds the static part of the prompt that can be cached."""
    return f"""
You are an Extra Space Storage SEO copywriter. Your task is to write the
'About Our Location' section for a specific facility page.

You must strictly follow the rules in the EXR Content Bullet Library AND the
Strategic Market Direction below. Strategic rules take full precedence.

------------------------------------------------------------
## EDITORIAL GUIDELINES (CRITICAL)
------------------------------------------------------------

- **CONCISENESS**: Bullet points must be concise, punchy, and strictly focused on their specific topic. Avoid fluff.
- **UNIQUE CATEGORIES**: Every bullet in a store's draft MUST have a unique category tag. Never repeat a category (e.g., do not have two "Home Community" bullets).
- **AGE HANDLING**: Resident age data (e.g. "Wtd. Median Age") is provided for context only. NEVER explicitly mention ages, age brackets, or "seniors" in the copy. Use this data only to shift the tone (e.g., toward downsizing or convenience).
- **HUMAN VOICE**: Avoid common AI indicators. DO NOT use em dashes (—). Use simple, direct language. Avoid "perfect" marketing transitions. Write like a local.

------------------------------------------------------------
## RULE HIERARCHY (highest priority first)
------------------------------------------------------------

### RULE 1 - SEGMENT STRATEGY (determines your overall angle)
Read the 'Segment' field carefully. It controls the entire editorial direction.

**SEGMENT A - HOME DOMINANT**
This store's customers are overwhelmingly local (one ZIP code dominates).
- Bullet #1 MUST be "Home Community" - describe the immediate neighborhood using
  names from `POIs - Neighborhoods`. Be specific.
- Bullet #2 MUST be "Nearby Neighborhoods" - list 2-3 specific neighborhood names.
- Intro tone: warm, hyperlocal, community-focused.

**SEGMENT B - MIXED CATCHMENT**
This store draws from multiple ZIPs with no single dominant source.
- Bullet #1 should be "Home Community" or "Nearby Neighborhoods".
- Intro tone: balanced, access-forward, broad appeal.

**SEGMENT C - HIGHLY DISTRIBUTED**
Customers arrive from many different ZIP codes. No single community dominates.
- Lead with convenience and access - prioritize "Interstate/Highway Exit" and
  "Airport Proximity" bullets if distances qualify.
- Intro tone: logistics-focused, location-advantage driven.

**MISALIGNED**
- Ignore existing content angle. Build from scratch using demographic and POI data.
- Intro tone: reset to what the data supports.

---

### RULE 2 - CROSSROADS (mandatory geography anchor)
Include the crossroads if provided in the location data as the geographic anchor.

---

### RULE 3 - CONTENT HOOK OVERRIDE (Bullet #2 only)
If 'Content Hook (Bullet 2)' is provided, use that angle unless the Segment is "Misaligned".

---

### RULE 4 - DEMOGRAPHIC MODIFIERS (apply after Segment rules)
These adjust tone and secondary bullets.
- MILITARY: < 15 mi -> include military angle.
- SENIORS: Age > 55 -> shift tone toward downsizing/convenience (WITHOUT mentioning age).
- UNIVERSITY: < 5 mi -> include student storage.
- PREMIUM: Income > 100k -> use professional, secure tone.
- RENTERS: Renter Rate > 40% -> reference apartment decluttering.

---

### RULE 5 - ZIP COMMUNITY ACKNOWLEDGMENT
If any Non-Home ZIP has a share > 10%, acknowledge that community by name.

---

### RULE 6 - POI MINING
Always pull real, specific names from `POIs - Neighborhoods`, `POIs - Schools`, and `POIs - Residential Areas`.

---

### RULE 7 - TIERED BULLET SELECTION (fill from Tier 1 down)
1. Tier 1: Home Community, Nearby Neighborhoods, Urban Residential, Second City Draw.
2. Tier 2: Interstate/Highway Exit, Airport Proximity, University/College, Military Base.
3. Tier 3: Local Schools, Notable Landmark.
4. Tier 4: Marina/Waterfront, RV Park/Outdoor (intro mention preferred).

**REMINDER**: Use 4 DIFFERENT categories.

------------------------------------------------------------
## REFERENCE MATERIALS
------------------------------------------------------------

Master Rulebook:
{rules}

Gold Standard Examples:
{gold_standards}
"""

def _build_dynamic_data(row_dict):
    """Builds the dynamic part of the prompt for a specific store."""
    return f"""
------------------------------------------------------------
## LOCATION DATA FOR THIS STORE
------------------------------------------------------------

[PRIMARY_CROSSROADS]: {row_dict.get('nearest_major_intersection', 'Not Available')}

{json.dumps(row_dict, indent=2, default=str)}
"""

def _build_prompt(rules, gold_standards, row_dict):
    """Legacy helper for Gemini (no split)."""
    return _build_static_context(rules, gold_standards) + _build_dynamic_data(row_dict)


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
    """Calls Claude 3.5 Sonnet to generate copy using Tool Use and Prompt Caching."""
    static_context = _build_static_context(rules, gold_standards)
    dynamic_data = _build_dynamic_data(row_dict)

    # Anthropic "Tool Use" to force JSON structure equivalent to Pydantic schema
    response = client.messages.create(
        model=MODEL_NAME,
        max_tokens=2048,
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"}, # Ensure caching is enabled
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
        messages=[
            {
                "role": "user", 
                "content": [
                    {
                        "type": "text",
                        "text": static_context,
                        "cache_control": {"type": "ephemeral"} # Cache the heavy rules and examples
                    },
                    {
                        "type": "text",
                        "text": dynamic_data
                    }
                ]
            }
        ]
    )
    
    # Extract data from tool use response
    tool_use_block = next(block for block in response.content if block.type == "tool_use")
    data = tool_use_block.input
    
    # Map usage metadata to match our tracker's format, including cache stats for transparency
    usage = type('obj', (object,), {
        'prompt_token_count': response.usage.input_tokens,
        'candidates_token_count': response.usage.output_tokens,
        'cache_creation_input_tokens': getattr(response.usage, 'cache_creation_input_tokens', 0),
        'cache_read_input_tokens': getattr(response.usage, 'cache_read_input_tokens', 0)
    })
    
    return data, static_context + dynamic_data, usage


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

        # Rate limit protection for low-tier Anthropic accounts
        if store_index > 1:
            time.sleep(RATE_LIMIT_SLEEP)

        print(f"[{store_index}/{total_stores}] Store {store_num}: Requesting Copy from {MODEL_NAME}...", flush=True)
        
        if 'claude' in MODEL_NAME:
            copy_json, final_prompt, usage = generate_with_claude(anthropic_client, rules, gold_standards, row_dict)
        else:
            copy_json, final_prompt, usage = generate_copy_for_facility(gemini_client, rules, gold_standards, row_dict)
        
        # Calculate Cost
        input_tokens = usage.prompt_token_count or 0
        output_tokens = usage.candidates_token_count or 0
        rates = MODEL_COSTS.get(MODEL_NAME, {'input': 0, 'output': 0})
        
        # Adjust input tokens based on cache hits (which are cheaper)
        cache_creation_tokens = getattr(usage, 'cache_creation_input_tokens', 0)
        cache_read_tokens = getattr(usage, 'cache_read_input_tokens', 0)
        
        # Anthropic Pricing for Caching: 
        # Creation: +25% premium (1.25x)
        # Read: 10% of base price (0.1x)
        base_input_tokens = input_tokens - cache_creation_tokens - cache_read_tokens
        estimated_cost = (
            (base_input_tokens * rates['input']) + 
            (cache_creation_tokens * rates['input'] * 1.25) + 
            (cache_read_tokens * rates['input'] * 0.10) + 
            (output_tokens * rates['output'])
        )

        print(f"[{store_index}/{total_stores}] Store {store_num}: Saving Draft to Postgres (Cost: ${estimated_cost:,.4f})...", flush=True)
        cur.execute("""
            INSERT INTO \"CopyDraft\" (
                \"facilityId\", \"introParagraph\", \"bullet1\", \"bullet1Tag\", \"bullet2\", \"bullet2Tag\", 
                \"bullet3\", \"bullet3Tag\", \"bullet4\", \"bullet4Tag\", \"prompt\", 
                \"inputTokens\", \"outputTokens\", \"cost\", \"modelName\", \"status\", \"updatedAt\"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Pending', NOW())
            ON CONFLICT (\"facilityId\") DO UPDATE SET
            \"introParagraph\" = EXCLUDED.\"introParagraph\",
            \"bullet1\" = EXCLUDED.\"bullet1\",
            \"bullet1Tag\" = EXCLUDED.\"bullet1Tag\",
            \"bullet2\" = EXCLUDED.\"bullet2\",
            \"bullet2Tag\" = EXCLUDED.\"bullet2Tag\",
            \"bullet3\" = EXCLUDED.\"bullet3\",
            \"bullet3Tag\" = EXCLUDED.\"bullet3Tag\",
            \"bullet4\" = EXCLUDED.\"bullet4\",
            \"bullet4Tag\" = EXCLUDED.\"bullet4Tag\",
            \"prompt\" = EXCLUDED.\"prompt\",
            \"inputTokens\" = EXCLUDED.\"inputTokens\",
            \"outputTokens\" = EXCLUDED.\"outputTokens\",
            \"cost\" = EXCLUDED.\"cost\",
            \"modelName\" = EXCLUDED.\"modelName\",
            \"status\" = 'Pending',
            \"updatedAt\" = NOW();
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
    
    # Apply targeted store filter if provided
    if TARGET_STORES:
        print(f"Filtering for targeted stores: {TARGET_STORES}")
        test_df = test_df[test_df['Store Number'].isin([str(s) for s in TARGET_STORES])].copy()

    test_df['StoreNumInt'] = test_df['Store Number'].astype(int)
    test_df = test_df.sort_values('StoreNumInt')
    
    if BATCH_LIMIT and not TARGET_STORES:
        test_df = test_df.head(BATCH_LIMIT)
    
    total_stores = len(test_df)
    if total_stores == 0:
        print("No stores found to process.")
        return

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
