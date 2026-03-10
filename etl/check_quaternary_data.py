"""检查计算 quaternary treatment 所需的数据"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

# 1. 检查 plants 的 PE 相关列
print("=== plants PE columns ===")
cur.execute("""SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'plants' AND (column_name LIKE '%%capacity%%' OR column_name LIKE '%%load%%' OR column_name LIKE '%%pe%%')""")
print([r[0] for r in cur.fetchall()])

# PE 分布统计
print("\n=== plants capacity distribution ===")
cur.execute("""
    SELECT 
        COUNT(*) FILTER (WHERE plant_capacity >= 150000) as over_150k,
        COUNT(*) FILTER (WHERE plant_capacity >= 10000 AND plant_capacity < 150000) as between_10k_150k,
        COUNT(*) FILTER (WHERE plant_capacity < 10000) as under_10k,
        COUNT(*) FILTER (WHERE plant_capacity IS NULL) as null_capacity
    FROM plants
""")
row = cur.fetchone()
print(f"  >= 150,000 PE: {row[0]}")
print(f"  10,000-150,000 PE: {row[1]}")
print(f"  < 10,000 PE: {row[2]}")
print(f"  NULL: {row[3]}")

# 2. 检查 water_body_protected_areas 的 protected_area_type
print("\n=== water_body_protected_areas.protected_area_type values ===")
cur.execute("SELECT protected_area_type, COUNT(*) FROM water_body_protected_areas GROUP BY protected_area_type ORDER BY COUNT(*) DESC")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

# 3. 检查 discharge_points 结构
print("\n=== discharge_points columns ===")
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name = 'discharge_points' ORDER BY ordinal_position""")
print([r[0] for r in cur.fetchall()])

# 4. 检查是否有 dilution ratio 数据
print("\n=== Any dilution ratio data in any table? ===")
cur.execute("""SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public' AND column_name LIKE '%%dilut%%' """)
results = cur.fetchall()
if results:
    print(results)
else:
    print("  No dilution ratio columns found")

# 5. 检查 plants 与 discharge_points 的关联
print("\n=== plants -> discharge_points -> water_bodies -> protected_areas link ===")
cur.execute("""
    SELECT COUNT(DISTINCT p.uwwtp_code) 
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    WHERE dp.water_body_code IS NOT NULL
""")
print(f"  Plants with linked water body: {cur.fetchone()[0]}")

cur.execute("""
    SELECT COUNT(DISTINCT p.uwwtp_code) 
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
    WHERE wpa.protected_area_type IN ('Bathing waters', 'Drinking water protection area', 'Shellfish designated water')
""")
print(f"  Plants discharging to bathing/drinking/shellfish areas: {cur.fetchone()[0]}")

# 检查是用 plant_capacity 还是 plant_waste_load_pe
print("\n=== 10,000+ PE plants with sensitive receiving waters ===")
cur.execute("""
    SELECT COUNT(DISTINCT p.uwwtp_code) 
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
    WHERE p.plant_capacity >= 10000
    AND wpa.protected_area_type IN ('Bathing waters', 'Drinking water protection area', 'Shellfish designated water')
""")
print(f"  Count: {cur.fetchone()[0]}")

cur.close()
conn.close()
