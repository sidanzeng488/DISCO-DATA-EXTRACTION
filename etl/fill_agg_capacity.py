"""
填充 agglomeration.agg_capacity 字段
源数据: agglomerations.csv -> aggCapacity
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
    print('Fill Agglomeration Capacity')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # 读取数据
    data = read_csv(os.path.join(DATA_DIR, 'agglomerations.csv'))
    print(f'\nCSV records: {len(data)}')
    
    # 检查有多少有 capacity 数据
    has_capacity = sum(1 for r in data if r.get('aggCapacity'))
    print(f'Records with aggCapacity: {has_capacity}')
    
    # 显示一些样本
    samples = [(r.get('aggCode'), r.get('aggCapacity')) for r in data[:5]]
    print(f'Samples: {samples}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 构建更新
    updates = []
    for row in data:
        agg_code = clean(row.get('aggCode'))
        capacity = to_int(row.get('aggCapacity'))
        
        if agg_code and capacity:
            updates.append((capacity, agg_code))
    
    print(f'\nRecords to update: {len(updates)}')
    
    if updates:
        sql = """
            UPDATE agglomeration 
            SET agg_capacity = %s 
            WHERE agg_code = %s AND agg_capacity IS NULL
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'✓ Updated agg_capacity')
    
    # 验证
    cur.execute('SELECT COUNT(*) FROM agglomeration WHERE agg_capacity IS NOT NULL')
    filled_count = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM agglomeration')
    total_count = cur.fetchone()[0]
    print(f'\nResult: {filled_count}/{total_count} agglomerations have agg_capacity ({filled_count*100/total_count:.1f}%)')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Completed!')


if __name__ == '__main__':
    main()
