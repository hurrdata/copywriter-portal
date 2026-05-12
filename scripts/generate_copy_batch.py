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
import time
import anthropic

class CopyDraftSchema(BaseModel):
    introParagraph: str = Field(description="EXACTLY 1-2 sentences. Must open with 'Our [Address] storage facility' or 'Our [City] storage facility'. Include the crossroads intersection. Mention one use case. NO zip codes. NO neighborhood names.")
    bullet1: str = Field(description="Full text of bullet 1. No period at end.")
    bullet1Tag: str = Field(description="Category name. Must be EXACTLY one of: 'Home Community', 'Nearby Neighborhoods', 'Second City Draw', 'Interstate/Highway Exit', 'Airport Proximity', 'University/College Proximity', 'Military Base/Community', 'Local Schools', 'Notable Nearby Landmark', 'Marina/Waterfront', 'RV Park/Outdoor Recreation', 'Urban Residential Communities'")
    bullet2: str = Field(description="Full text of bullet 2. No period at end.")
    bullet2Tag: str = Field(description="Category name (must be one of the 12 listed above).")
    bullet3: str = Field(description="Full text of bullet 3. No period at end.")
    bullet3Tag: str = Field(description="Category name (must be one of the 12 listed above).")
    bullet4: str = Field(description="Full text of bullet 4. No period at end.")
    bullet4Tag: str = Field(description="Category name (must be one of the 12 listed above).")


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
MODEL_NAME = 'claude-sonnet-4-6'              # Step 2: Claude Sonnet 4.6 (copy writing)
STRATEGY_MODEL = MODEL_NAME                   # Step 1: Claude Sonnet (editorial strategy)
BATCH_LIMIT = 100                             # Stores per run (next 100 of the 779 remaining)
MAX_WORKERS = 1                               # Sequential to stay under rate limits
OVERWRITE_EXISTING = False                    # Preserve editor-approved copy
RATE_LIMIT_SLEEP = 30                         # Seconds between stores (Sonnet rate limit)
TARGET_STORES = []                            # If not empty, only process these store numbers
# ----------------------

# Market rates per 1M tokens (USD)
MODEL_COSTS = {
    'gemini-2.5-flash': {'input': 0.10 / 1_000_000, 'output': 0.40 / 1_000_000},
    'gemini-2.0-flash': {'input': 0.10 / 1_000_000, 'output': 0.40 / 1_000_000},
    'gemini-1.5-pro':   {'input': 3.50 / 1_000_000, 'output': 10.50 / 1_000_000},
    'claude-sonnet-4-6': {'input': 3.00 / 1_000_000, 'output': 15.00 / 1_000_000},
    'claude-haiku-4-5':  {'input': 0.80 / 1_000_000, 'output': 4.00 / 1_000_000},
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

You will receive a pre-built EDITORIAL STRATEGY from Step 1. Follow it precisely.
The strategy locks in which bullet categories to use, which zip codes to feature,
and what use case angle to take. Your job is execution — write the prose.

------------------------------------------------------------
## CRITICAL STYLE RULES (non-negotiable)
------------------------------------------------------------

### INTRO PARAGRAPH
- **EXACTLY 1-2 sentences. No more.**
- Open with: "Our [Street Address] storage facility..." or "Our [City] storage facility..."
- Include the crossroads/intersection.
- Mention one relevant use case (downsizing, local moves, seasonal gear, student storage, apartment decluttering, etc.) based on the demographic tone in the strategy.
- **NO zip codes in the intro.**
- **NO neighborhood or apartment names in the intro.** Save those for bullets.

### ZIP CODES
- Zip codes appear in **bullets ONLY**, never in the intro.
- Format: **City Name (ZIP)** — e.g., "North Lauderdale (33068)"
- If a second zip has >10% share, give it its own bullet (Second City Draw or Nearby Neighborhoods).
- Example: "A convenient option for residents throughout Tamarac (33321) and Lauderdale Lakes (33319)"

### NEIGHBORHOODS & COMMUNITIES
- Only use neighborhood names confirmed in `POIs — Neighborhoods` or `POIs — Residential Areas`.
- Do NOT invent or assume neighborhood names.
- If both POI fields are empty, make the bullet more general — do not fabricate.
- Urban Residential Communities: use **exact apartment complex names** from `POIs — Residential Areas`.
  Format: "Minutes from established apartment communities like [Name] and [Name], perfect for renters who need additional space at home"
- Avoid trailer parks and vague 'downtown' or 'urban core' descriptions.

### DEMOGRAPHICS — TONE ONLY, NO RAW NUMBERS
- **Never** mention specific income figures (e.g., "$64K median income", "middle-income community").
- **Never** mention renter percentages (e.g., "40% of residents are renters").
- Use demographic data for tone and use case selection only:
  - High renter rate → reference apartment decluttering or "extra space at home"
  - Lower income → lead with value, flexibility, practical unit sizes
  - High % families → seasonal gear, making room for the family
  - High % seniors → downsizing, convenience (never mention 'seniors' or ages)

### SEO KEYWORDS
- Include city name or address naturally in the intro (once is enough).
- Pattern: "Our [City] storage facility" or "Our [Address] storage facility is located in the heart of [City]"

### DISTANCES — ALWAYS SPECIFIC
- Always use an exact distance: "1.4 miles from I-95" not "near I-95" or "close to the freeway"
- Pull exact distances from the provided location data.

### STREET ABBREVIATIONS
- Always abbreviate: Rd, Ave, Blvd, St, Dr, Ln, Hwy, Pkwy
- Never spell out: Road, Avenue, Boulevard, Street, Drive, Lane, Highway, Parkway

### BULLET FORMATTING
- No period at the end of any bullet point.
- No em dashes (—). Use a comma or rewrite the sentence.
- Every bullet must have a unique category. No repeated categories.
- Be concise and specific — no filler phrases.

------------------------------------------------------------
## SEGMENT STRATEGY REFERENCE
------------------------------------------------------------

**SEGMENT A - HOME DOMINANT**: Bullet #1 = Home Community. Bullet #2 = Nearby Neighborhoods.
**SEGMENT B - MIXED CATCHMENT**: Lead with Home Community or Nearby Neighborhoods.
**SEGMENT C - HIGHLY DISTRIBUTED**: Lead with Interstate/Highway Exit or Airport Proximity.
**MISALIGNED**: Ignore existing content angle. Build from data only.

------------------------------------------------------------
## TIERED BULLET SELECTION
------------------------------------------------------------

Fill from Tier 1 down. Stop when you have 4 unique categories.
1. Tier 1: Home Community, Nearby Neighborhoods, Urban Residential Communities, Second City Draw
2. Tier 2: Interstate/Highway Exit, Airport Proximity, University/College Proximity, Military Base/Community
3. Tier 3: Local Schools, Notable Nearby Landmark
4. Tier 4: Marina/Waterfront, RV Park/Outdoor Recreation

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

[FACILITY ADDRESS]: {row_dict.get('Address', 'Not Available')}
[CITY]: {row_dict.get('City', 'Not Available')}
[PRIMARY ZIP]: {row_dict.get('Zip', 'Not Available')}
[CROSSROADS]: {row_dict.get('nearest_major_intersection', 'Not Available')}

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



@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def generate_strategy_with_haiku(client, row_dict):
    """
    Step 1: Uses Claude Haiku to produce a structured editorial strategy for a store.
    Plans which bullets, zip codes, neighborhoods, and use case angle to use.
    Returns (strategy_dict, has_empty_poi_flag).
    """
    neighborhoods = row_dict.get('POIs \u2014 Neighborhoods', '') or ''
    residential = row_dict.get('POIs \u2014 Residential Areas', '') or ''
    has_empty_poi = not neighborhoods.strip() and not residential.strip()
    zip_mix = row_dict.get('zip_customer_mix', []) or []
    secondary_zips = [z for z in zip_mix if z.get('type') != 'Home' and z.get('share', 0) >= 10]

    prompt = f"""You are an editorial strategist for Extra Space Storage SEO copy.
Given the store data below, produce a concise editorial strategy as a JSON object.

STORE DATA:
- Address: {row_dict.get('Address', 'N/A')}
- City: {row_dict.get('City', 'N/A')}
- Primary ZIP: {row_dict.get('Zip', 'N/A')}
- Segment: {row_dict.get('Segment', 'N/A')}
- Crossroads: {row_dict.get('nearest_major_intersection', 'N/A')}
- POIs Neighborhoods: {neighborhoods or 'EMPTY'}
- POIs Residential Areas: {residential or 'EMPTY'}
- ZIP Customer Mix: {json.dumps(zip_mix, default=str)}
- Secondary ZIPs (>10% share): {json.dumps(secondary_zips, default=str)}
- Wtd. Median Age: {row_dict.get('Wtd. Median Age', 'N/A')}
- Wtd. Renter Rate %: {row_dict.get('Wtd. Renter Rate %', 'N/A')}
- Wtd. Median Income: {row_dict.get('Wtd. Median Income', 'N/A')}
- Wtd. % Under 18: {row_dict.get('Wtd. % Under 18', 'N/A')}
- Content Hook (Bullet 2): {row_dict.get('Content Hook (Bullet 2)', 'N/A')}
- Nearest Interstate: {row_dict.get('nearest_interstate', 'N/A')} ({row_dict.get('interstate_distance_mi', 'N/A')} mi)
- POIs Interstate Exits: {row_dict.get('POIs \u2014 Interstate Exits', 'N/A')}
- Nearest Airport: {row_dict.get('nearest_airport', 'N/A')} ({row_dict.get('airport_distance_mi', 'N/A')} mi)
- Nearest University: {row_dict.get('nearest_university_verified', 'N/A')} ({row_dict.get('university_distance_mi', 'N/A')} mi)
- Military Base Distance: {row_dict.get('military_base_distance_mi', 'N/A')} mi
- Second City Draw: {row_dict.get('Second City Draw', 'N/A')}

Produce a JSON object with these exact fields:
{{
  "use_case_angle": "one of: downsizing | apartment_decluttering | local_moves | seasonal_gear | student_storage | business_storage | general",
  "demographic_tone": "brief tone note based on demographics. No raw numbers or percentages.",
  "bullet_plan": [
    {{"category": "Home Community", "zip": "City (ZIP)", "notes": "what to highlight"}},
    {{"category": "Nearby Neighborhoods", "zip": "City (ZIP) if relevant", "notes": "specific neighborhood or apartment names from POI data only"}},
    {{"category": "Interstate/Highway Exit", "zip": null, "notes": "exact exit number and distance in miles"}},
    {{"category": "Local Schools", "zip": null, "notes": "school names if in POI data"}}
  ],
  "poi_data_available": true,
  "flags": []
}}

Rules:
- Exactly 4 bullets with UNIQUE categories from the 12 official EXR categories.
- If secondary ZIPs exist (>10% share), use Second City Draw as a bullet.
- If POIs Neighborhoods AND Residential Areas are EMPTY, set poi_data_available to false and add "EMPTY_POI" to flags.
- Only reference neighborhoods/apartments that appear in the provided POI data.
- Use exact distances (e.g., "1.4 miles") for all access bullets.
- Do not mention specific income figures or renter percentages in demographic_tone.

Respond with only the JSON object, no explanation.
"""
    response = client.messages.create(
        model=STRATEGY_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}]
    )

    try:
        raw = response.content[0].text.strip()
        # Strip markdown code fences if Haiku wraps in ```json ... ```
        if raw.startswith("```"):
            raw = raw.split("```")[-2] if "```" in raw else raw
            raw = raw.lstrip("json").strip()
        strategy = json.loads(raw)
    except Exception:
        strategy = {
            "use_case_angle": "general",
            "demographic_tone": "general community",
            "bullet_plan": [],
            "poi_data_available": not has_empty_poi,
            "flags": ["STRATEGY_PARSE_ERROR"]
        }

    if has_empty_poi and "EMPTY_POI" not in strategy.get("flags", []):
        strategy.setdefault("flags", []).append("EMPTY_POI")

    return strategy, has_empty_poi


@backoff.on_exception(backoff.expo, Exception, max_tries=5)
def generate_with_claude(client, rules, gold_standards, row_dict, strategy):
    """
    Step 2: Calls Claude Sonnet to write the final copy, guided by the Step 1 strategy.
    Uses Prompt Caching on the static rules/examples context.
    """
    static_context = _build_static_context(rules, gold_standards)
    dynamic_data = _build_dynamic_data(row_dict)

    strategy_block = f"""
------------------------------------------------------------
## EDITORIAL STRATEGY (from Step 1 — follow this precisely)
------------------------------------------------------------

Use Case Angle: {strategy.get('use_case_angle', 'general')}
Demographic Tone: {strategy.get('demographic_tone', '')}
POI Data Available: {strategy.get('poi_data_available', True)}

Bullet Plan:
{json.dumps(strategy.get('bullet_plan', []), indent=2)}

IMPORTANT: If POI Data Available is false, make neighborhood bullets more general.
Do NOT invent neighborhood or apartment names not confirmed in the location data.
"""

    response = client.messages.create(
        model=MODEL_NAME,
        max_tokens=2048,
        extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        tools=[{
            "name": "generate_copy_draft",
            "description": "Generate a structured copy draft for an Extra Space Storage facility.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "introParagraph": {"type": "string", "description": "EXACTLY 1-2 sentences. Open with 'Our [Address] storage facility' or 'Our [City] storage facility'. Include crossroads. Mention one use case from the strategy. NO zip codes. NO neighborhood names."},
                    "bullet1": {"type": "string", "description": "Full text of bullet 1. No period at end."},
                    "bullet1Tag": {"type": "string", "description": "Must be EXACTLY one of: 'Home Community', 'Nearby Neighborhoods', 'Second City Draw', 'Interstate/Highway Exit', 'Airport Proximity', 'University/College Proximity', 'Military Base/Community', 'Local Schools', 'Notable Nearby Landmark', 'Marina/Waterfront', 'RV Park/Outdoor Recreation', 'Urban Residential Communities'"},
                    "bullet2": {"type": "string", "description": "Full text of bullet 2. No period at end."},
                    "bullet2Tag": {"type": "string", "description": "Must be EXACTLY one of the 12 categories listed above."},
                    "bullet3": {"type": "string", "description": "Full text of bullet 3. No period at end."},
                    "bullet3Tag": {"type": "string", "description": "Must be EXACTLY one of the 12 categories listed above."},
                    "bullet4": {"type": "string", "description": "Full text of bullet 4. No period at end."},
                    "bullet4Tag": {"type": "string", "description": "Must be EXACTLY one of the 12 categories listed above."},
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
                        "cache_control": {"type": "ephemeral"}
                    },
                    {
                        "type": "text",
                        "text": strategy_block + dynamic_data
                    }
                ]
            }
        ]
    )

    tool_use_block = next(block for block in response.content if block.type == "tool_use")
    data = tool_use_block.input

    usage = type('obj', (object,), {
        'prompt_token_count': response.usage.input_tokens,
        'candidates_token_count': response.usage.output_tokens,
        'cache_creation_input_tokens': getattr(response.usage, 'cache_creation_input_tokens', 0),
        'cache_read_input_tokens': getattr(response.usage, 'cache_read_input_tokens', 0)
    })

    return data, static_context + strategy_block + dynamic_data, usage



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
            # Step 1: Generate editorial strategy with Haiku
            print(f"[{store_index}/{total_stores}] Store {store_num}: Step 1 - Building strategy with {STRATEGY_MODEL}...", flush=True)
            strategy, has_empty_poi = generate_strategy_with_haiku(anthropic_client, row_dict)
            if has_empty_poi:
                print(f"[{store_index}/{total_stores}] Store {store_num}: ⚠️  EMPTY_POI FLAG - no neighborhood/residential POI data found. Bullet will be general.", flush=True)
            flags = strategy.get("flags", [])
            if flags:
                print(f"[{store_index}/{total_stores}] Store {store_num}: Strategy flags: {flags}", flush=True)
            # Step 2: Write copy with Sonnet using the strategy
            copy_json, final_prompt, usage = generate_with_claude(anthropic_client, rules, gold_standards, row_dict, strategy)
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
        estimated_cost = (
            (input_tokens * rates['input']) + 
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
    
    # Filter out stores that already have drafts if we are not overwriting
    if not OVERWRITE_EXISTING and not TARGET_STORES:
        print("Filtering out already-completed stores...", flush=True)
        conn = db_pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT "storeNumber" FROM "Facility" f JOIN "CopyDraft" c ON f.id = c."facilityId"')
            existing_stores = {str(r[0]) for r in cur.fetchall()}
            test_df = test_df[~test_df['Store Number'].isin(existing_stores)].copy()
        finally:
            db_pool.putconn(conn)
    
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
