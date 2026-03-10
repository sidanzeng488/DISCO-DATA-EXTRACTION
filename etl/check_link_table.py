import psycopg2
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM plant_requirement_link")
print(f"plant_requirement_link rows: {cur.fetchone()[0]}")

cur.execute("""
    SELECT r.treatment_tier, COUNT(prl.id)
    FROM uwwtd_requirement r
    LEFT JOIN plant_requirement_link prl ON r.id = prl.requirement_id
    GROUP BY r.id, r.treatment_tier
    ORDER BY r.id
""")
print("By requirement:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
