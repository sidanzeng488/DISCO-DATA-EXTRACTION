"""检查数据库各表的完整性"""
import sys
import os
import csv
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

CURRENT_DIR = r'C:\Users\Sidan.Zeng\OneDrive - Media Analytics Limited\Desktop\DISCO link\DATA\current'
TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')


def count_csv(filepath):
    if not os.path.exists(filepath):
        return 0
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return sum(1 for _ in f) - 1


conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

print('=' * 75)
print('Database Completeness Check')
print('=' * 75)
print(f'{"Table":<40} {"DB Rows":>10} {"CSV Rows":>10} {"Status":>10}')
print('-' * 75)

# 检查各表
checks = [
    ('country', None),
    ('sensitivity', None),
    ('report_code', os.path.join(TRANSFORMED_DIR, 'report_periods.csv')),
    ('water_bodies', os.path.join(TRANSFORMED_DIR, 'water_bodies.csv')),
    ('water_body_protected_areas', os.path.join(TRANSFORMED_DIR, 'water_body_protected_areas.csv')),
    ('agglomeration', os.path.join(TRANSFORMED_DIR, 'agglomerations.csv')),
    ('plants', os.path.join(TRANSFORMED_DIR, 'plants.csv')),
    ('discharge_points', os.path.join(TRANSFORMED_DIR, 'discharge_points.csv')),
]

total_db = 0
total_csv = 0

for table, csv_path in checks:
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        db_count = cur.fetchone()[0]
    except Exception as e:
        db_count = 0
    
    total_db += db_count
    
    if csv_path:
        csv_count = count_csv(csv_path)
        total_csv += csv_count
        
        if db_count >= csv_count:
            status = '✓ OK'
        elif db_count >= csv_count * 0.7:
            status = '~ Partial'
        else:
            status = '✗ Missing'
    else:
        csv_count = '-'
        status = '✓ OK' if db_count > 0 else '-'
    
    print(f'{table:<40} {db_count:>10} {str(csv_count):>10} {status:>10}')

print('-' * 75)
print(f'{"TOTAL":<40} {total_db:>10}')
print('=' * 75)

# 额外检查：discharge_points 的外键完整性
print('\nForeign Key Analysis (discharge_points):')
cur.execute('SELECT COUNT(*) FROM discharge_points WHERE water_body_code IS NOT NULL')
with_wb = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM discharge_points WHERE water_body_code IS NULL')
without_wb = cur.fetchone()[0]
print(f'  With water_body_code: {with_wb}')
print(f'  Without water_body_code: {without_wb}')

cur.close()
conn.close()
