"""
sync_zip_customer_mix.py
Reads the Top 3 customer ZIP data from SEO_Zipcode_Tiered_Segments.xlsx
and writes a structured `zip_customer_mix` array into geoData for each facility.

Run: venv/bin/python sync_zip_customer_mix.py
"""
import os
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv('copywriter-portal/.env')
DATABASE_URL = os.environ.get("DIRECT_URL")

ZIP_TABS = [
    'Segment A — Home Dominant',
    'Segment B — Mixed Catchment',
    'Segment C — Highly Distributed',
    'Misaligned Stores ⚠',
]

def load_zip_data():
    """Load and combine all segment tabs into a single DataFrame, deduplicated by store."""
    xl = pd.ExcelFile('SEO_Zipcode_Tiered_Segments.xlsx')
    all_dfs = []

    for tab in ZIP_TABS:
        df = xl.parse(tab, header=1)
        df.columns = [str(c).strip() for c in df.columns]
        first_col = df.columns[0]
        df = df.rename(columns={first_col: 'Store Number'})
        df['_source_tab'] = tab
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    combined['Store Number'] = combined['Store Number'].astype(str).str.zfill(4)
    combined['Home Zip Share'] = pd.to_numeric(combined['Home Zip Share'], errors='coerce')

    # Deduplicate: keep the row with the highest Home Zip Share per store
    combined = combined.sort_values('Home Zip Share', ascending=False)
    combined = combined.drop_duplicates(subset='Store Number', keep='first')

    print(f"Loaded {len(combined)} unique stores from Tiered Segments file.")
    return combined


def build_zip_mix(row):
    """Build a structured list of ZIP share objects for a store row."""
    zips = []

    # Home ZIP
    home_zip = str(row.get('Store Postal Code', '')).strip()
    home_share = float(row.get('Home Zip Share', 0) or 0)
    if home_zip and home_zip not in ('nan', '0000x', ''):
        zips.append({'zip': home_zip, 'share': round(home_share * 100, 1), 'type': 'Home'})

    # Non-home ZIPs 1-3
    for i in range(1, 4):
        z = str(row.get(f'Non-Home Zip #{i}', '')).strip()
        s = row.get(f'Zip #{i} Share', None)
        if z and z not in ('nan', '0000x', '') and s and float(s) > 0:
            zips.append({'zip': z, 'share': round(float(s) * 100, 1), 'type': 'Non-Home'})

    return zips


def main():
    print("Loading ZIP customer mix data...")
    zip_df = load_zip_data()

    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    updated = 0
    skipped = 0

    for _, row in zip_df.iterrows():
        store_num = row['Store Number'].lstrip('0') or '0'  # DB stores without leading zeros

        zip_mix = build_zip_mix(row)
        if not zip_mix:
            skipped += 1
            continue

        # Fetch existing geoData
        cur.execute('SELECT id, "geoData" FROM "Facility" WHERE "storeNumber" = %s', (store_num,))
        result = cur.fetchone()
        if not result:
            skipped += 1
            continue

        facility_id, geo = result
        geo = geo or {}
        geo['zip_customer_mix'] = zip_mix

        cur.execute(
            'UPDATE "Facility" SET "geoData" = %s, "updatedAt" = NOW() WHERE id = %s',
            (json.dumps(geo), facility_id)
        )
        updated += 1
        if updated % 50 == 0:
            print(f"  Updated {updated} stores...")

    cur.close()
    conn.close()
    print(f"\nDone. Updated: {updated} | Skipped: {skipped}")


if __name__ == "__main__":
    main()
