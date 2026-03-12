"""
Fill requires_quaternary_treatment field
Based on revised UWWTD thresholds and receiving water sensitivity

Logic:
- >= 150,000 PE -> "Yes"
- >= 10,000 PE + sensitive water (bathing/drinking/shellfish) -> "Likely"
- >= 10,000 PE + dilution ratio < 10 -> "Likely"
- Others -> "No"
- No capacity data -> NULL
"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

print("=" * 60)
print("Quaternary Treatment Field - Implementation")
print("=" * 60)

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# Step 1: 检查并添加列
print("\n1. Checking/Adding column...")
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'plants' AND column_name = 'requires_quaternary_treatment'
""")
if cur.fetchone():
    print("   Column already exists")
else:
    print("   Adding column...")
    cur.execute("ALTER TABLE plants ADD COLUMN requires_quaternary_treatment VARCHAR(10)")
    conn.commit()
    print("   Column added")

# Step 2a: Find plants >= 10,000 PE with sensitive receiving waters
print("\n2. Finding plants with sensitive conditions...")
print("   2a. Plants with sensitive protected areas...")
cur.execute("""
    SELECT DISTINCT p.uwwtp_code
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
    WHERE p.plant_capacity >= 10000
    AND p.plant_capacity < 150000
    AND wpa.protected_area_type IN ('Bathing waters', 'Drinking water protection area', 'Shellfish designated water')
""")
sensitive_plants = set(r[0] for r in cur.fetchall())
print(f"       Found {len(sensitive_plants)} plants with sensitive protected areas")

# Step 2b: Find plants >= 10,000 PE with dilution ratio < 10
print("   2b. Plants with low dilution ratio (< 10)...")
cur.execute("""
    SELECT DISTINCT uwwtp_code
    FROM plants
    WHERE plant_capacity >= 10000
    AND plant_capacity < 150000
    AND dilution IS NOT NULL
    AND dilution < 10
""")
low_dilution_plants = set(r[0] for r in cur.fetchall())
print(f"       Found {len(low_dilution_plants)} plants with dilution < 10")

# Combine both conditions
sensitive_plants = sensitive_plants.union(low_dilution_plants)
print(f"   Total plants meeting 'Likely' criteria: {len(sensitive_plants)}")

# Step 3: 填充数据
print("\n3. Populating values...")

# 3a: Set "Yes" for >= 150,000 PE
print("   Setting 'Yes' for >= 150,000 PE...")
cur.execute("""
    UPDATE plants 
    SET requires_quaternary_treatment = 'Yes'
    WHERE plant_capacity >= 150000
""")
yes_count = cur.rowcount
print(f"   Updated {yes_count} plants to 'Yes'")

# 3b: Set "Likely" for 10k-150k PE with sensitive waters OR low dilution
print("   Setting 'Likely' for 10k-150k PE + sensitive waters or dilution < 10...")
if sensitive_plants:
    # 使用 IN 子句批量更新
    placeholders = ','.join(['%s'] * len(sensitive_plants))
    cur.execute(f"""
        UPDATE plants 
        SET requires_quaternary_treatment = 'Likely'
        WHERE uwwtp_code IN ({placeholders})
        AND requires_quaternary_treatment IS DISTINCT FROM 'Yes'
    """, list(sensitive_plants))
    likely_count = cur.rowcount
else:
    likely_count = 0
print(f"   Updated {likely_count} plants to 'Likely'")

# 3c: Set "No" for all others with capacity
print("   Setting 'No' for remaining plants with capacity...")
cur.execute("""
    UPDATE plants 
    SET requires_quaternary_treatment = 'No'
    WHERE plant_capacity IS NOT NULL
    AND requires_quaternary_treatment IS NULL
""")
no_count = cur.rowcount
print(f"   Updated {no_count} plants to 'No'")

conn.commit()

# Step 4: 验证结果
print("\n4. Verifying results...")
cur.execute("""
    SELECT requires_quaternary_treatment, COUNT(*) 
    FROM plants 
    GROUP BY requires_quaternary_treatment
    ORDER BY requires_quaternary_treatment
""")
print("   Distribution:")
for row in cur.fetchall():
    val = row[0] if row[0] else 'NULL'
    print(f"     {val}: {row[1]}")

# Step 5: Add column comment
print("\n5. Adding column comment...")
cur.execute("""
    COMMENT ON COLUMN plants.requires_quaternary_treatment IS 
    'Quaternary treatment requirement under revised UWWTD: Yes (>=150k PE), Likely (>=10k PE + sensitive receiving water OR dilution < 10), No (others)'
""")
conn.commit()
print("   Comment added")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
