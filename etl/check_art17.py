"""检查 art17 数据匹配情况"""
import csv
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

# 读取 art17 CSV 中的 uwwCode
csv_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current', 'art17_investments.csv')
with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    art17_codes = set(r.get('uwwCode', '') for r in reader if r.get('uwwCode'))

print(f"art17_investments.csv uwwCodes: {len(art17_codes)}")

# 读取 plants 中的 uwwtp_code
conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()
cur.execute("SELECT uwwtp_code FROM plants")
plant_codes = set(r[0] for r in cur.fetchall())
print(f"plants.uwwtp_code: {len(plant_codes)}")

# 找出不在 plants 中的 art17 codes
missing = art17_codes - plant_codes
print(f"art17 codes NOT in plants: {len(missing)}")
if missing:
    print(f"Sample missing codes: {list(missing)[:5]}")

# 找出在 plants 中但没有 art17 数据的
cur.execute("SELECT COUNT(*) FROM plants WHERE article17_compliance_status IS NOT NULL")
has_art17 = cur.fetchone()[0]
print(f"plants with art17 data: {has_art17}")

matched = art17_codes.intersection(plant_codes)
print(f"art17 codes matching plants: {len(matched)}")

# 差异分析
diff = len(matched) - has_art17
print(f"Difference (matched - has_art17): {diff}")

conn.close()
