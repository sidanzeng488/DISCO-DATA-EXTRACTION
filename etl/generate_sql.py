"""
生成 SQL INSERT 语句文件
用于在 Supabase SQL Editor 中快速导入数据
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
    """转义 SQL 字符串"""
    if value is None or value == '':
        return 'NULL'
    # 转义单引号
    value = str(value).replace("'", "''")
    return f"'{value}'"


def get_existing_keys(table_name, key_column):
    """获取数据库中已存在的主键"""
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    cur.execute(f'SELECT "{key_column}" FROM "{table_name}"')
    keys = set(row[0] for row in cur.fetchall())
    cur.close()
    conn.close()
    return keys


def generate_water_bodies_sql():
    """生成 water_bodies 的 SQL（跳过已存在的）"""
    print('\n[Generate] water_bodies.sql')
    
    # 获取已存在的 water_body_code
    existing = get_existing_keys('water_bodies', 'eu_water_body_code')
    print(f'  Existing in DB: {len(existing)}')
    
    # 读取数据
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'water_bodies.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    # 过滤出新数据
    new_data = [row for row in data if row.get('eu_water_body_code') not in existing]
    print(f'  New to insert: {len(new_data)}')
    
    if not new_data:
        print('  No new data to insert')
        return
    
    # 生成 SQL
    sql_file = os.path.join(SQL_DIR, 'water_bodies.sql')
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write('-- Water Bodies INSERT\n')
        f.write(f'-- New records: {len(new_data)}\n\n')
        
        # 分批插入，每批 1000 条
        batch_size = 1000
        for i in range(0, len(new_data), batch_size):
            batch = new_data[i:i+batch_size]
            f.write(f'-- Batch {i//batch_size + 1}\n')
            f.write('INSERT INTO water_bodies (eu_water_body_code, water_type, water_body_name, country_code) VALUES\n')
            
            values = []
            for row in batch:
                v = f"({escape_sql(row.get('eu_water_body_code'))}, {escape_sql(row.get('water_type'))}, {escape_sql(row.get('water_body_name'))}, {escape_sql(row.get('country_code'))})"
                values.append(v)
            
            f.write(',\n'.join(values))
            f.write('\nON CONFLICT (eu_water_body_code) DO NOTHING;\n\n')
    
    print(f'  Saved: {sql_file}')


def generate_simple_sql(table_name, csv_name, columns, key_column):
    """生成简单表的 SQL"""
    print(f'\n[Generate] {table_name}.sql')
    
    # 检查是否有数据
    existing = get_existing_keys(table_name, key_column)
    print(f'  Existing in DB: {len(existing)}')
    
    # 读取数据
    data = read_csv(os.path.join(TRANSFORMED_DIR, csv_name))
    print(f'  Total in CSV: {len(data)}')
    
    # 过滤出新数据
    new_data = [row for row in data if row.get(key_column) not in existing]
    print(f'  New to insert: {len(new_data)}')
    
    if not new_data:
        print('  No new data to insert')
        return
    
    # 生成 SQL
    sql_file = os.path.join(SQL_DIR, f'{table_name}.sql')
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write(f'-- {table_name} INSERT\n')
        f.write(f'-- New records: {len(new_data)}\n\n')
        
        # 分批插入
        batch_size = 1000
        for i in range(0, len(new_data), batch_size):
            batch = new_data[i:i+batch_size]
            f.write(f'-- Batch {i//batch_size + 1}\n')
            f.write(f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES\n')
            
            values = []
            for row in batch:
                vals = [escape_sql(row.get(col)) for col in columns]
                values.append(f"({', '.join(vals)})")
            
            f.write(',\n'.join(values))
            f.write(f'\nON CONFLICT ({key_column}) DO NOTHING;\n\n')
    
    print(f'  Saved: {sql_file}')


def generate_protected_areas_sql():
    """生成 protected areas 的 SQL（无主键，直接插入）"""
    print('\n[Generate] water_body_protected_areas.sql')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'water_body_protected_areas.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    sql_file = os.path.join(SQL_DIR, 'water_body_protected_areas.sql')
    with open(sql_file, 'w', encoding='utf-8') as f:
        f.write('-- Water Body Protected Areas INSERT\n')
        f.write(f'-- Total records: {len(data)}\n\n')
        
        # 分批插入
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            f.write(f'-- Batch {i//batch_size + 1}\n')
            f.write('INSERT INTO water_body_protected_areas (water_body_code, eu_protected_area_code, protected_area_type) VALUES\n')
            
            values = []
            for row in batch:
                v = f"({escape_sql(row.get('water_body_code'))}, {escape_sql(row.get('eu_protected_area_code'))}, {escape_sql(row.get('protected_area_type'))})"
                values.append(v)
            
            f.write(',\n'.join(values))
            f.write(';\n\n')
    
    print(f'  Saved: {sql_file}')


def main():
    print('=' * 50)
    print('Generate SQL Files')
    print('=' * 50)
    
    os.makedirs(SQL_DIR, exist_ok=True)
    
    # 生成各表的 SQL
    generate_water_bodies_sql()
    
    generate_simple_sql(
        'agglomeration', 'agglomerations.csv',
        ['agg_code', 'agglomeration_name'],
        'agg_code'
    )
    
    generate_simple_sql(
        'plants', 'plants.csv',
        ['uwwtp_code', 'plant_name', 'country_code', 'lat', 'longitude'],
        'uwwtp_code'
    )
    
    generate_simple_sql(
        'discharge_points', 'discharge_points.csv',
        ['dcp_code', 'is_surface_water', 'water_body_code'],
        'dcp_code'
    )
    
    generate_protected_areas_sql()
    
    print('\n' + '=' * 50)
    print(f'SQL files saved to: {SQL_DIR}')
    print('Import order:')
    print('  1. water_bodies.sql')
    print('  2. agglomeration.sql')
    print('  3. plants.sql')
    print('  4. discharge_points.sql')
    print('  5. water_body_protected_areas.sql')


if __name__ == '__main__':
    main()
