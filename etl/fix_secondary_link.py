"""修正 Secondary treatment 关联"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

# 1. 删除旧的 Secondary 关联
print("1. Deleting old Secondary links...")
cur.execute("""
    DELETE FROM plant_requirement_link 
    WHERE requirement_id = (SELECT id FROM uwwtd_requirement WHERE treatment_tier = 'Secondary')
""")
deleted = cur.rowcount
print(f"   Deleted: {deleted}")

# 2. 插入新的 Secondary 关联 (requires_secondary_treatment = Yes)
print("2. Inserting new Secondary links (requires_secondary_treatment = Yes)...")
cur.execute("""
    INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
    SELECT p.plant_id, r.id, 'requires_secondary_treatment = Yes'
    FROM plants p
    CROSS JOIN uwwtd_requirement r
    WHERE p.requires_secondary_treatment = 'Yes'
    AND r.treatment_tier = 'Secondary'
    ON CONFLICT (plant_id, requirement_id) DO NOTHING
""")
inserted = cur.rowcount
print(f"   Inserted: {inserted}")

conn.commit()

# 3. 验证
print("\n3. Summary of plant_requirement_link:")
cur.execute("""
    SELECT r.treatment_tier, COUNT(prl.id)
    FROM uwwtd_requirement r
    LEFT JOIN plant_requirement_link prl ON r.id = prl.requirement_id
    GROUP BY r.id, r.treatment_tier
    ORDER BY r.id
""")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

conn.close()
print("\nDone!")
