"""
通过 plants 表填充 discharge_points.rep_code
使用 uwwCode -> plants.uwwtp_code -> plants.rep_code
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def main():
    print('=' * 60)
    print('Fill discharge_points.rep_code via plants table')
    print('=' * 60)
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 通过 SQL 直接更新，使用 plants 表的 rep_code
    sql = """
        UPDATE discharge_points dp
        SET rep_code = p.rep_code
        FROM plants p
        WHERE dp.plant_code = p.uwwtp_code
        AND dp.rep_code IS NULL
        AND p.rep_code IS NOT NULL
    """
    
    cur.execute(sql)
    updated = cur.rowcount
    print(f'Updated via plant_code match: {updated}')
    
    # 验证
    cur.execute('SELECT COUNT(*) FROM discharge_points WHERE rep_code IS NOT NULL')
    filled = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM discharge_points')
    total = cur.fetchone()[0]
    print(f'\nResult: {filled}/{total} ({filled*100/total:.1f}%)')
    
    # 检查 discharge_points 中有多少有 plant_code
    cur.execute('SELECT COUNT(*) FROM discharge_points WHERE plant_code IS NOT NULL')
    has_plant_code = cur.fetchone()[0]
    print(f'Discharge points with plant_code: {has_plant_code}')
    
    cur.close()
    conn.close()
    print('✅ Done!')


if __name__ == '__main__':
    main()
