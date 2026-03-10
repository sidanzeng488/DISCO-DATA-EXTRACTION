"""检查 requires_secondary_treatment 逻辑"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

# 检查 requires_secondary_treatment = Yes 的 plants 的 provides_secondary_treatment 值
print("requires_secondary_treatment = 'Yes' plants:")
cur.execute("""
    SELECT provides_secondary_treatment, COUNT(*) 
    FROM plants 
    WHERE requires_secondary_treatment = 'Yes'
    GROUP BY provides_secondary_treatment
""")
print("provides_secondary_treatment distribution:")
for row in cur.fetchall():
    val = 'NULL' if row[0] is None else str(row[0])
    print(f"  {val}: {row[1]}")

# 样本检查
print("\nSample data:")
cur.execute("""
    SELECT uwwtp_code, plant_waste_load_pe, provides_secondary_treatment 
    FROM plants 
    WHERE requires_secondary_treatment = 'Yes'
    LIMIT 10
""")
for row in cur.fetchall():
    print(f"  {row[0]}: PE={row[1]}, provides_secondary={row[2]}")

conn.close()
