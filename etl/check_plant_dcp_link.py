"""检查 plants 和 discharge_points 的关联问题"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

print("=== Why low plant-discharge_point match? ===\n")

# 检查 plant_code 格式
cur.execute("SELECT plant_code FROM discharge_points LIMIT 5")
print("Sample discharge_points.plant_code:", [r[0] for r in cur.fetchall()])

cur.execute("SELECT uwwtp_code FROM plants LIMIT 5")
print("Sample plants.uwwtp_code:", [r[0] for r in cur.fetchall()])

# 检查 discharge_points 有多少 plant_code 能匹配到 plants
cur.execute("""SELECT COUNT(DISTINCT dp.plant_code) FROM discharge_points dp 
    WHERE EXISTS (SELECT 1 FROM plants p WHERE p.uwwtp_code = dp.plant_code)""")
matched = cur.fetchone()[0]
cur.execute("SELECT COUNT(DISTINCT plant_code) FROM discharge_points")
total = cur.fetchone()[0]
print(f"\ndischarge_points.plant_code matching plants: {matched}/{total} ({matched/total*100:.1f}%)")

# 检查不匹配的例子
cur.execute("""SELECT DISTINCT dp.plant_code FROM discharge_points dp 
    WHERE NOT EXISTS (SELECT 1 FROM plants p WHERE p.uwwtp_code = dp.plant_code)
    LIMIT 5""")
print("Unmatched plant_codes:", [r[0] for r in cur.fetchall()])

# 检查匹配的例子
cur.execute("""SELECT DISTINCT dp.plant_code FROM discharge_points dp 
    WHERE EXISTS (SELECT 1 FROM plants p WHERE p.uwwtp_code = dp.plant_code)
    LIMIT 5""")
print("Matched plant_codes:", [r[0] for r in cur.fetchall()])

conn.close()
