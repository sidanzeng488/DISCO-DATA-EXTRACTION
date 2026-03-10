"""查看 Supabase 数据库现有数据"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

conn = psycopg2.connect(
    host=SUPABASE_HOST,
    port=SUPABASE_PORT,
    database=SUPABASE_DATABASE,
    user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# 查看所有表
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name
""")
tables = cur.fetchall()

print('=' * 50)
print('Supabase Database Status')
print('=' * 50)

for t in tables:
    table_name = t[0]
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        count = cur.fetchone()[0]
        print(f'  {table_name}: {count} rows')
    except Exception as e:
        print(f'  {table_name}: (error: {e})')

cur.close()
conn.close()
