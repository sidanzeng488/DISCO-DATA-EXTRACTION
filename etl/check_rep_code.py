"""检查 repCode 数据和外键约束"""
import sys
import os
import csv
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'DATA', 'current')

# 1. 检查 CSV 中的 repCode
print("=" * 60)
print("1. CSV 中的 repCode 数据")
print("=" * 60)

with open(os.path.join(DATA_DIR, 'plants.csv'), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    
    rep_codes = set()
    total = 0
    has_rep_code = 0
    
    for row in reader:
        total += 1
        code = row.get('repCode', '').strip()
        if code:
            has_rep_code += 1
            rep_codes.add(code)
    
    print(f"Total records: {total}")
    print(f"Records with repCode: {has_rep_code}")
    print(f"Unique repCodes: {len(rep_codes)}")
    print(f"\nSample repCodes:")
    for code in list(rep_codes)[:10]:
        print(f"  '{code}'")

# 2. 检查数据库中的 report_code 表
print("\n" + "=" * 60)
print("2. 数据库 report_code 表")
print("=" * 60)

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM report_code")
count = cur.fetchone()[0]
print(f"Total report_code records: {count}")

cur.execute("SELECT rep_code FROM report_code LIMIT 10")
db_codes = [r[0] for r in cur.fetchall()]
print(f"\nSample DB rep_codes:")
for code in db_codes:
    print(f"  '{code}'")

# 3. 检查外键约束
print("\n" + "=" * 60)
print("3. 外键约束检查")
print("=" * 60)

cur.execute("""
    SELECT constraint_name, constraint_type 
    FROM information_schema.table_constraints 
    WHERE table_name = 'plants' AND constraint_name LIKE '%rep%'
""")
constraints = cur.fetchall()
print(f"Rep_code related constraints on plants:")
for name, ctype in constraints:
    print(f"  {name}: {ctype}")

# 4. 检查是否匹配
print("\n" + "=" * 60)
print("4. CSV repCode 是否存在于 report_code 表")
print("=" * 60)

cur.execute("SELECT rep_code FROM report_code")
db_rep_codes = {r[0].strip() if r[0] else '' for r in cur.fetchall()}

matched = 0
not_matched = []
for code in rep_codes:
    clean_code = code.strip()
    if clean_code in db_rep_codes:
        matched += 1
    else:
        if len(not_matched) < 5:
            not_matched.append(clean_code)

print(f"CSV repCodes that exist in DB: {matched}/{len(rep_codes)}")
if not_matched:
    print(f"Sample NOT in DB:")
    for code in not_matched:
        print(f"  '{code}'")

cur.close()
conn.close()
