"""
从 discharge_points.csv 获取 uwwCode，然后关联 plants 获取 rep_code
"""
import sys
import os
import csv
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
    print('Fill discharge_points rep_code from CSV uwwCode')
    print('=' * 60)
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 获取 plants 的 uwwtp_code -> rep_code 映射
    cur.execute("SELECT uwwtp_code, rep_code FROM plants WHERE rep_code IS NOT NULL")
    plant_rep_codes = {r[0]: r[1] for r in cur.fetchall()}
    print(f'Plants with rep_code: {len(plant_rep_codes)}')
    
    # 读取 discharge_points CSV
    data = read_csv(os.path.join(DATA_DIR, 'discharge_points.csv'))
    print(f'CSV records: {len(data)}')
    
    # 检查 uwwCode 列
    if data:
        print(f'Columns: {list(data[0].keys())[:10]}...')
    
    # 构建更新
    updates = []
    for row in data:
        dcp_code = clean(row.get('dcpCode'))
        uww_code = clean(row.get('uwwCode'))
        
        if dcp_code and uww_code and uww_code in plant_rep_codes:
            rep_code = plant_rep_codes[uww_code]
            updates.append((rep_code, dcp_code))
    
    print(f'Records to update: {len(updates)}')
    
    if updates:
        sql = "UPDATE discharge_points SET rep_code = %s WHERE dcp_code = %s AND rep_code IS NULL"
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'✓ Updated')
    
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
