"""检查 plants 表的经纬度列"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg2
from supabase.config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

print("Plants 表的所有列：")
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'plants' 
    ORDER BY ordinal_position
""")
for name, dtype in cur.fetchall():
    # 标记可能是经纬度的列
    marker = " ← 经纬度?" if any(x in name.lower() for x in ['lat', 'lon', 'long']) else ""
    print(f"  {name}: {dtype}{marker}")

cur.close()
conn.close()
