"""
修正 PE 字段使用：改为 plant_waste_load_pe
1. 重新计算 requires_quaternary_treatment
2. 添加 Secondary requirement 关联 (1000-2000 PE)
"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

print("=" * 60)
print("Fix PE Field Usage (plant_waste_load_pe)")
print("=" * 60)

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# ============================================
# 1. 修正 requires_quaternary_treatment
# ============================================
print("\n1. Recalculating requires_quaternary_treatment using plant_waste_load_pe...")

# 清空现有值
cur.execute("UPDATE plants SET requires_quaternary_treatment = NULL")
print("   Cleared existing values")

# 获取 >= 10,000 PE 且排放到敏感水体的 plants
cur.execute("""
    SELECT DISTINCT p.uwwtp_code
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
    WHERE p.plant_waste_load_pe >= 10000
    AND p.plant_waste_load_pe < 150000
    AND wpa.protected_area_type IN ('Bathing waters', 'Drinking water protection area', 'Shellfish designated water')
""")
sensitive_plants = set(r[0] for r in cur.fetchall())
print(f"   Found {len(sensitive_plants)} plants (10k-150k PE) with sensitive waters")

# Set "Yes" for >= 150,000 PE
cur.execute("""
    UPDATE plants 
    SET requires_quaternary_treatment = 'Yes'
    WHERE plant_waste_load_pe >= 150000
""")
yes_count = cur.rowcount
print(f"   Set 'Yes': {yes_count} plants (>= 150,000 PE)")

# Set "Likely" for 10k-150k PE with sensitive waters
if sensitive_plants:
    placeholders = ','.join(['%s'] * len(sensitive_plants))
    cur.execute(f"""
        UPDATE plants 
        SET requires_quaternary_treatment = 'Likely'
        WHERE uwwtp_code IN ({placeholders})
        AND requires_quaternary_treatment IS NULL
    """, list(sensitive_plants))
    likely_count = cur.rowcount
else:
    likely_count = 0
print(f"   Set 'Likely': {likely_count} plants (10k-150k PE + sensitive)")

# Set "No" for all others with PE data
cur.execute("""
    UPDATE plants 
    SET requires_quaternary_treatment = 'No'
    WHERE plant_waste_load_pe IS NOT NULL
    AND requires_quaternary_treatment IS NULL
""")
no_count = cur.rowcount
print(f"   Set 'No': {no_count} plants")

conn.commit()

# 验证
cur.execute("""
    SELECT requires_quaternary_treatment, COUNT(*) 
    FROM plants 
    GROUP BY requires_quaternary_treatment
    ORDER BY requires_quaternary_treatment
""")
print("   New distribution:")
for row in cur.fetchall():
    val = row[0] if row[0] else 'NULL'
    print(f"     {val}: {row[1]}")

# ============================================
# 2. 添加 Secondary requirement 关联
# ============================================
print("\n2. Adding Secondary requirement links (1000-2000 PE)...")

cur.execute("""
    INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
    SELECT p.plant_id, r.id, '1000-2000 PE (plant_waste_load_pe)'
    FROM plants p
    CROSS JOIN uwwtd_requirement r
    WHERE p.plant_waste_load_pe >= 1000
    AND p.plant_waste_load_pe < 2000
    AND r.treatment_tier = 'Secondary'
    ON CONFLICT (plant_id, requirement_id) DO NOTHING
""")
secondary_count = cur.rowcount
print(f"   Inserted {secondary_count} Secondary links")

conn.commit()

# ============================================
# 3. 更新 Quaternary 关联 (基于新的 requires_quaternary_treatment)
# ============================================
print("\n3. Updating Quaternary requirement links...")

# 先删除旧的 Quaternary 关联
cur.execute("""
    DELETE FROM plant_requirement_link 
    WHERE requirement_id = (SELECT id FROM uwwtd_requirement WHERE treatment_tier = 'Quaternary')
""")
deleted = cur.rowcount
print(f"   Deleted {deleted} old Quaternary links")

# 重新插入基于 requires_quaternary_treatment = 'Yes'
cur.execute("""
    INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
    SELECT p.plant_id, r.id, 'requires_quaternary_treatment = Yes (plant_waste_load_pe >= 150000)'
    FROM plants p
    CROSS JOIN uwwtd_requirement r
    WHERE p.requires_quaternary_treatment = 'Yes'
    AND r.treatment_tier = 'Quaternary'
    ON CONFLICT (plant_id, requirement_id) DO NOTHING
""")
quat_count = cur.rowcount
print(f"   Inserted {quat_count} Quaternary links")

conn.commit()

# ============================================
# 4. 汇总
# ============================================
print("\n4. Summary of plant_requirement_link...")
cur.execute("""
    SELECT r.treatment_tier, COUNT(prl.id)
    FROM uwwtd_requirement r
    LEFT JOIN plant_requirement_link prl ON r.id = prl.requirement_id
    GROUP BY r.id, r.treatment_tier
    ORDER BY r.id
""")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]} plants")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
