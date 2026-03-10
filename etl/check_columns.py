"""检查数据库表的列填充情况"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

tables = ['plants', 'water_bodies', 'discharge_points', 'agglomeration']

for table in tables:
    print(f'\n{"=" * 60}')
    print(f'Table: {table}')
    print('=' * 60)
    
    # 获取列信息
    cur.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table}' 
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    
    # 检查每列的非空值数量
    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    total = cur.fetchone()[0]
    
    print(f'Total rows: {total}')
    print(f'\n{"Column":<40} {"Type":<15} {"Non-NULL":>10} {"Fill %":>8}')
    print('-' * 75)
    
    for col_name, col_type in columns:
        cur.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{col_name}" IS NOT NULL')
        non_null = cur.fetchone()[0]
        fill_pct = (non_null / total * 100) if total > 0 else 0
        
        status = '✓' if fill_pct > 50 else '○' if fill_pct > 0 else '✗'
        print(f'{status} {col_name:<38} {col_type:<15} {non_null:>10} {fill_pct:>7.1f}%')

cur.close()
conn.close()
