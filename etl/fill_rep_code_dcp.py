"""
填充 discharge_points.rep_code 字段
"""
import sys
import os
import csv
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_batch
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'DATA', 'current')


def read_csv(filepath):
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def clean(value):
    if value is None or value == '' or value == 'NULL':
        return None
    return value.strip()


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def main():
    print('=' * 60)
    print('Fill rep_code in discharge_points')
    print('=' * 60)
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 获取有效 rep_codes
    cur.execute("SELECT rep_code FROM report_code")
    valid_rep_codes = {r[0].strip() if r[0] else '' for r in cur.fetchall()}
    print(f'Valid rep_codes: {len(valid_rep_codes)}')
    
    # 读取 discharge_points CSV
    data = read_csv(os.path.join(DATA_DIR, 'discharge_points.csv'))
    print(f'CSV records: {len(data)}')
    
    # 构建更新
    updates = []
    for row in data:
        dcp_code = clean(row.get('dcpCode'))
        rep_code = clean(row.get('repCode'))
        
        if dcp_code and rep_code and rep_code in valid_rep_codes:
            updates.append((rep_code, dcp_code))
    
    print(f'Records to update: {len(updates)}')
    
    if updates:
        sql = "UPDATE discharge_points SET rep_code = %s WHERE dcp_code = %s"
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'✓ Updated rep_code')
    
    # 验证
    cur.execute('SELECT COUNT(*) FROM discharge_points WHERE rep_code IS NOT NULL')
    filled = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM discharge_points')
    total = cur.fetchone()[0]
    print(f'\nResult: {filled}/{total} ({filled*100/total:.1f}%)')
    
    cur.close()
    conn.close()
    print('✅ Done!')


if __name__ == '__main__':
    main()
