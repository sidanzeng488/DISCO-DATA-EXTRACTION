"""
Add Quaternary treatment links for 'Likely' plants
Plants with requires_quaternary_treatment = 'Likely' should be linked to Quaternary requirement
with appropriate notes explaining the reason (sensitive water OR dilution < 10)
"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

print("=" * 60)
print("Add Quaternary Treatment Links for 'Likely' Plants")
print("=" * 60)

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# Step 1: Check current Quaternary links
print("\n1. Current Quaternary links...")
cur.execute("""
    SELECT prl.notes, COUNT(*) 
    FROM plant_requirement_link prl
    JOIN uwwtd_requirement r ON prl.requirement_id = r.id
    WHERE r.treatment_tier = 'Quaternary'
    GROUP BY prl.notes
    ORDER BY prl.notes
""")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

# Step 2: Add links for 'Likely' plants with sensitive protected areas
print("\n2. Adding links for 'Likely' plants with sensitive protected areas...")

# First, get plants with sensitive areas (pick one protected area type per plant)
cur.execute("""
    WITH sensitive_plants AS (
        SELECT DISTINCT ON (p.plant_id) 
            p.plant_id, 
            wpa.protected_area_type
        FROM plants p
        JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
        JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
        WHERE p.requires_quaternary_treatment = 'Likely'
        AND p.plant_capacity >= 10000
        AND p.plant_capacity < 150000
        AND wpa.protected_area_type IN ('Bathing waters', 'Drinking water protection area', 'Shellfish designated water')
        ORDER BY p.plant_id, wpa.protected_area_type
    )
    INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
    SELECT sp.plant_id, r.id, 
           'Likely: >= 10,000 PE + sensitive protected area (' || sp.protected_area_type || ')'
    FROM sensitive_plants sp
    CROSS JOIN uwwtd_requirement r
    WHERE r.treatment_tier = 'Quaternary'
    ON CONFLICT (plant_id, requirement_id) DO UPDATE SET notes = EXCLUDED.notes
""")
sensitive_count = cur.rowcount
print(f"   Added/Updated {sensitive_count} links for sensitive protected areas")

# Step 3: Add links for 'Likely' plants with low dilution (that don't already have a link)
print("\n3. Adding links for 'Likely' plants with dilution < 10...")
cur.execute("""
    INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
    SELECT p.plant_id, r.id, 
           'Likely: >= 10,000 PE + dilution ratio < 10 (' || ROUND(p.dilution::numeric, 2) || ')'
    FROM plants p
    CROSS JOIN uwwtd_requirement r
    WHERE p.requires_quaternary_treatment = 'Likely'
    AND r.treatment_tier = 'Quaternary'
    AND p.plant_capacity >= 10000
    AND p.plant_capacity < 150000
    AND p.dilution IS NOT NULL
    AND p.dilution < 10
    AND NOT EXISTS (
        SELECT 1 FROM plant_requirement_link prl 
        WHERE prl.plant_id = p.plant_id 
        AND prl.requirement_id = r.id
    )
    ON CONFLICT (plant_id, requirement_id) DO NOTHING
""")
dilution_count = cur.rowcount
print(f"   Added {dilution_count} links for low dilution")

conn.commit()

# Step 4: Verify results
print("\n4. Updated Quaternary links distribution...")
cur.execute("""
    SELECT 
        CASE 
            WHEN notes LIKE '%Yes%' OR notes LIKE '%150000%' THEN 'Yes (>= 150k PE)'
            WHEN notes LIKE '%sensitive%' OR notes LIKE '%protected area%' THEN 'Likely (sensitive water)'
            WHEN notes LIKE '%dilution%' THEN 'Likely (dilution < 10)'
            ELSE notes
        END as category,
        COUNT(*)
    FROM plant_requirement_link prl
    JOIN uwwtd_requirement r ON prl.requirement_id = r.id
    WHERE r.treatment_tier = 'Quaternary'
    GROUP BY category
    ORDER BY category
""")
print("   Distribution:")
for row in cur.fetchall():
    print(f"     {row[0]}: {row[1]}")

# Total count
cur.execute("""
    SELECT COUNT(*) 
    FROM plant_requirement_link prl
    JOIN uwwtd_requirement r ON prl.requirement_id = r.id
    WHERE r.treatment_tier = 'Quaternary'
""")
total = cur.fetchone()[0]
print(f"\n   Total Quaternary links: {total}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
