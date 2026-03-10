"""
检查并修复外键问题
只生成有对应 water_body 的 discharge_points
"""
import sys
import os
import csv
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
from etl.config import CURRENT_DIR

TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')
SQL_DIR = os.path.join(os.path.dirname(CURRENT_DIR), 'sql')


def read_csv(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def escape_sql(value):
    if value is None or value == '':
        return 'NULL'
    value = str(value).replace("'", "''")
    return f"'{value}'"


def get_existing_water_bodies():
    """获取数据库中已存在的 water_body_code"""
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    cur.execute('SELECT eu_water_body_code FROM water_bodies')
    codes = set(row[0] for row in cur.fetchall() if row[0])
    cur.close()
    conn.close()
    return codes


def main():
    print('=' * 50)
    print('Fix Foreign Key Issues')
    print('=' * 50)
    
    # 获取数据库中的 water_body_codes
    print('\n[1] Getting existing water_body_codes from DB...')
    existing_wb = get_existing_water_bodies()
    print(f'  Found: {len(existing_wb)} water bodies in DB')
    
    # 读取 discharge_points
    print('\n[2] Reading discharge_points...')
    dcp_data = read_csv(os.path.join(TRANSFORMED_DIR, 'discharge_points.csv'))
    print(f'  Total: {len(dcp_data)} records')
    
    # 检查有多少没有对应的 water_body
    missing_wb = set()
    valid_dcp = []
    null_wb_dcp = []
    
    for row in dcp_data:
        wb_code = row.get('water_body_code')
        if not wb_code or wb_code == '':
            null_wb_dcp.append(row)
        elif wb_code in existing_wb:
            valid_dcp.append(row)
        else:
            missing_wb.add(wb_code)
    
    print(f'\n[3] Analysis:')
    print(f'  Valid (has water_body): {len(valid_dcp)}')
    print(f'  NULL water_body_code: {len(null_wb_dcp)}')
    print(f'  Missing water_body: {len(dcp_data) - len(valid_dcp) - len(null_wb_dcp)}')
    print(f'  Unique missing codes: {len(missing_wb)}')
    
    if missing_wb:
        print(f'\n  Sample missing codes:')
        for code in list(missing_wb)[:5]:
            print(f'    - {code}')
    
    # 生成只包含有效数据的 SQL
    print('\n[4] Generating fixed SQL...')
    
    # 合并有效和 NULL 的（NULL 可以插入）
    all_valid = valid_dcp + null_wb_dcp
    print(f'  Records to insert: {len(all_valid)}')
    
    # 获取已存在的 dcp_code
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    cur.execute('SELECT dcp_code FROM discharge_points')
    existing_dcp = set(row[0] for row in cur.fetchall() if row[0])
    cur.close()
    conn.close()
    
    # 过滤掉已存在的
    new_dcp = [row for row in all_valid if row.get('dcp_code') not in existing_dcp]
    print(f'  New records: {len(new_dcp)}')
    
    # 生成 SQL 文件
    batch_size = 1000
    file_count = 0
    
    for i in range(0, len(new_dcp), 10000):  # 每文件 10000 条
        file_count += 1
        chunk = new_dcp[i:i+10000]
        
        sql_file = os.path.join(SQL_DIR, f'discharge_points_fixed_part{file_count:02d}.sql')
        with open(sql_file, 'w', encoding='utf-8') as f:
            f.write(f'-- discharge_points (fixed) Part {file_count}\n')
            f.write(f'-- Records: {len(chunk)}\n\n')
            
            for j in range(0, len(chunk), batch_size):
                batch = chunk[j:j+batch_size]
                f.write(f'-- Batch {j//batch_size + 1}\n')
                
                # 使用简单的 INSERT，water_body_code 可以为 NULL
                f.write('INSERT INTO discharge_points (dcp_code, is_surface_water, water_body_code) VALUES\n')
                
                values = []
                for row in batch:
                    wb_code = row.get('water_body_code')
                    if wb_code and wb_code in existing_wb:
                        wb_sql = escape_sql(wb_code)
                    else:
                        wb_sql = 'NULL'
                    
                    is_surface = 'true' if str(row.get('is_surface_water', '')).lower() == 'true' else 'false'
                    v = f"({escape_sql(row.get('dcp_code'))}, {is_surface}, {wb_sql})"
                    values.append(v)
                
                f.write(',\n'.join(values))
                f.write('\nON CONFLICT (dcp_code) DO NOTHING;\n\n')
        
        print(f'  Created: discharge_points_fixed_part{file_count:02d}.sql')
    
    print(f'\n✅ Done! Created {file_count} files')
    print('   Execute: discharge_points_fixed_part01.sql, part02.sql, ...')


if __name__ == '__main__':
    main()
