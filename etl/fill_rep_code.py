"""
填充 plants.rep_code 字段
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
    print('Fill rep_code in plants')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 获取 report_code 表中的有效代码
    cur.execute("SELECT rep_code FROM report_code")
    valid_rep_codes = {r[0].strip() if r[0] else '' for r in cur.fetchall()}
    print(f'\nValid rep_codes in DB: {len(valid_rep_codes)}')
    
    # 读取 CSV
    data = read_csv(os.path.join(DATA_DIR, 'plants.csv'))
    print(f'CSV records: {len(data)}')
    
    # 构建更新
    updates = []
    not_found = set()
    
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        rep_code = clean(row.get('repCode'))
        
        if not uwwtp_code or not rep_code:
            continue
        
        # 检查 rep_code 是否在有效列表中
        if rep_code in valid_rep_codes:
            updates.append((rep_code, uwwtp_code))
        else:
            not_found.add(rep_code)
    
    print(f'Records to update: {len(updates)}')
    if not_found:
        print(f'rep_codes not found in DB: {not_found}')
    
    if updates:
        sql = """
            UPDATE plants 
            SET rep_code = %s 
            WHERE uwwtp_code = %s
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'✓ Updated rep_code')
    
    # 验证
    cur.execute('SELECT COUNT(*) FROM plants WHERE rep_code IS NOT NULL')
    filled_count = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM plants')
    total_count = cur.fetchone()[0]
    print(f'\nResult: {filled_count}/{total_count} plants have rep_code ({filled_count*100/total_count:.1f}%)')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Completed!')


if __name__ == '__main__':
    main()
