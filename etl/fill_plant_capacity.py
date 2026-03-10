"""
填充 plants.plant_capacity 字段
源数据: plants.csv -> uwwCapacity
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
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def clean(value):
    if value is None or value == '' or value == 'NULL':
        return None
    return value.strip() if isinstance(value, str) else value


def to_int(value):
    if value is None or value == '':
        return None
    try:
        return int(float(value))
    except:
        return None


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def main():
    print('=' * 60)
    print('Fill Plant Capacity')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # 读取数据
    data = read_csv(os.path.join(DATA_DIR, 'plants.csv'))
    print(f'\nCSV records: {len(data)}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 构建更新
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        capacity = to_int(row.get('uwwCapacity'))
        
        if uwwtp_code and capacity:
            updates.append((capacity, uwwtp_code))
    
    print(f'Records to update: {len(updates)}')
    
    if updates:
        sql = """
            UPDATE plants 
            SET plant_capacity = %s 
            WHERE uwwtp_code = %s
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'✓ Updated plant_capacity')
    
    # 验证
    cur.execute('SELECT COUNT(*) FROM plants WHERE plant_capacity IS NOT NULL')
    filled_count = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM plants')
    total_count = cur.fetchone()[0]
    print(f'\nResult: {filled_count}/{total_count} plants have plant_capacity ({filled_count*100/total_count:.1f}%)')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Completed!')


if __name__ == '__main__':
    main()
