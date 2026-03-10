"""Check country table for NULL names"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

# 查看 country_name 为 NULL 的记录
cur.execute('SELECT country_code, country_name FROM country WHERE country_name IS NULL ORDER BY country_code')
nulls = cur.fetchall()
print(f'Countries with NULL name: {len(nulls)}')
for code, name in nulls:
    print(f'  {code}')

print()

# 查看所有记录
cur.execute('SELECT country_code, country_name FROM country ORDER BY country_code')
all_rows = cur.fetchall()
print(f'Total countries: {len(all_rows)}')
print(f'Sample (first 30):')
for code, name in all_rows[:30]:
    status = '✓' if name else '✗'
    print(f'  {status} {code}: {name}')

cur.close()
conn.close()
