"""Check TSS compliance data in Supabase"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT, 
    database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD
)
cur = conn.cursor()

print('=== Supabase: TSS Compliance by Country (Top 20) ===')
cur.execute("""
    SELECT 
        country_code,
        COUNT(*) as total,
        SUM(CASE WHEN tss_compliance = 'P' THEN 1 ELSE 0 END) as pass,
        SUM(CASE WHEN tss_compliance = 'F' THEN 1 ELSE 0 END) as fail,
        SUM(CASE WHEN tss_compliance = 'NR' THEN 1 ELSE 0 END) as nr,
        SUM(CASE WHEN tss_compliance = 'NA' THEN 1 ELSE 0 END) as na,
        SUM(CASE WHEN tss_compliance IS NULL THEN 1 ELSE 0 END) as empty
    FROM plants 
    GROUP BY country_code
    ORDER BY COUNT(*) DESC
    LIMIT 20
""")

print('{:<6} {:<7} {:<6} {:<5} {:<6} {:<5} {:<7} {:<8}'.format(
    'Code', 'Total', 'P', 'F', 'NR', 'NA', 'Empty', 'Complete'))
print('-' * 60)

for row in cur.fetchall():
    cc, total, p, f, nr, na, empty = row
    has_data = total - empty
    pct = has_data / total * 100 if total > 0 else 0
    print('{:<6} {:<7} {:<6} {:<5} {:<6} {:<5} {:<7} {:.1f}%'.format(
        cc, total, p, f, nr, na, empty, pct))

cur.close()
conn.close()
