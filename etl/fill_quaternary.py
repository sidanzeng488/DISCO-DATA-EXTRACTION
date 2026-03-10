"""
填充 requires_quaternary_treatment 字段
基于修订后的 UWWTD 阈值和接收水体敏感性

逻辑：
- >= 150,000 PE → "Yes"
- >= 10,000 PE + 敏感水体(bathing/drinking/shellfish) → "Likely"  
- 其他 → "No"
- 无 capacity 数据 → NULL
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

# Step 2: 获取 >= 10,000 PE 且排放到敏感水体的 plants
print("\n2. Finding plants with sensitive receiving waters...")
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
print(f"   Found {len(sensitive_plants)} plants (10k-150k PE) with sensitive waters")

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

# 3b: Set "Likely" for 10k-150k PE with sensitive waters
print("   Setting 'Likely' for 10k-150k PE + sensitive waters...")
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

# Step 5: 添加注释
print("\n5. Adding column comment...")
cur.execute("""
    COMMENT ON COLUMN plants.requires_quaternary_treatment IS 
    'Quaternary treatment requirement under revised UWWTD: Yes (>=150k PE), Likely (>=10k PE + sensitive receiving water), No (others)'
""")
conn.commit()
print("   Comment added")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
