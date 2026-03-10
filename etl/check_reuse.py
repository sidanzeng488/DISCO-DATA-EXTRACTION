"""检查 pct_wastewater_reused 填充情况"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

# 检查列是否存在
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name = 'plants' AND column_name LIKE '%wastewater%' """)
cols = cur.fetchall()
print("Columns with 'wastewater':", [c[0] for c in cols])

# 检查 pct_wastewater_reused 填充率
cur.execute('SELECT COUNT(*) FROM plants WHERE pct_wastewater_reused IS NOT NULL')
filled = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM plants')
total = cur.fetchone()[0]
pct = filled/total*100 if total > 0 else 0
print(f"pct_wastewater_reused: {filled}/{total} ({pct:.1f}%)")

# 样本
cur.execute('SELECT uwwtp_code, pct_wastewater_reused FROM plants WHERE pct_wastewater_reused IS NOT NULL LIMIT 5')
print("Sample:", cur.fetchall())

cur.close()
conn.close()
