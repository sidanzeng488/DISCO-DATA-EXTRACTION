"""
快速批量导入数据到 Supabase
使用 execute_values 批量插入，速度快 100 倍
"""
import sys
import os
import csv
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_values
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
from etl.config import CURRENT_DIR

TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')


def read_csv(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def clean(value):
    """清理值"""
    if value is None or value == '' or value == 'NULL':
        return None
    return value


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def batch_insert(cur, table, columns, data, conflict_column=None, batch_size=5000):
    """批量插入数据"""
    if not data:
        return 0
    
    # 构建 SQL
    cols = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    
    if conflict_column:
        sql = f"""
            INSERT INTO {table} ({cols}) VALUES %s
            ON CONFLICT ({conflict_column}) DO NOTHING
        """
    else:
        sql = f"INSERT INTO {table} ({cols}) VALUES %s"
    
    # 分批插入
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        try:
            execute_values(cur, sql, batch, page_size=batch_size)
            total += len(batch)
            print(f'    Inserted {total}/{len(data)}...', end='\r')
        except Exception as e:
            print(f'\n    Error at batch {i//batch_size}: {e}')
    
    print(f'    Inserted {total}/{len(data)} rows    ')
    return total


def import_water_bodies(cur):
    """导入 water_bodies"""
    print('\n[1/5] Importing water_bodies...')
    
    # 获取已存在的
    cur.execute('SELECT eu_water_body_code FROM water_bodies')
    existing = set(row[0] for row in cur.fetchall() if row[0])
    print(f'  Existing: {len(existing)}')
    
    # 读取数据
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'water_bodies.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    # 过滤新数据
    new_data = [row for row in data if row.get('eu_water_body_code') not in existing]
    print(f'  New to insert: {len(new_data)}')
    
    if not new_data:
        return 0
    
    # 准备数据
    columns = ['eu_water_body_code', 'water_type', 'water_body_name', 'country_code']
    values = [
        (clean(row.get('eu_water_body_code')), clean(row.get('water_type')), 
         clean(row.get('water_body_name')), clean(row.get('country_code')))
        for row in new_data
    ]
    
    return batch_insert(cur, 'water_bodies', columns, values, 'eu_water_body_code')


def import_agglomeration(cur):
    """导入 agglomeration"""
    print('\n[2/5] Importing agglomeration...')
    
    cur.execute('SELECT agg_code FROM agglomeration')
    existing = set(row[0] for row in cur.fetchall() if row[0])
    print(f'  Existing: {len(existing)}')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'agglomerations.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    new_data = [row for row in data if row.get('agg_code') not in existing]
    print(f'  New to insert: {len(new_data)}')
    
    if not new_data:
        return 0
    
    columns = ['agg_code', 'agglomeration_name']
    values = [(clean(row.get('agg_code')), clean(row.get('agglomeration_name'))) for row in new_data]
    
    return batch_insert(cur, 'agglomeration', columns, values, 'agg_code')


def import_plants(cur):
    """导入 plants"""
    print('\n[3/5] Importing plants...')
    
    cur.execute('SELECT uwwtp_code FROM plants')
    existing = set(row[0] for row in cur.fetchall() if row[0])
    print(f'  Existing: {len(existing)}')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'plants.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    new_data = [row for row in data if row.get('uwwtp_code') not in existing]
    print(f'  New to insert: {len(new_data)}')
    
    if not new_data:
        return 0
    
    columns = ['uwwtp_code', 'plant_name', 'country_id', 'lat', 'longitude']
    values = [
        (clean(row.get('uwwtp_code')), clean(row.get('plant_name')), 
         clean(row.get('country_code')), clean(row.get('lat')), clean(row.get('longitude')))
        for row in new_data
    ]
    
    return batch_insert(cur, 'plants', columns, values, 'uwwtp_code')


def import_discharge_points(cur):
    """导入 discharge_points"""
    print('\n[4/5] Importing discharge_points...')
    
    # 获取已存在的 dcp_code
    cur.execute('SELECT dcp_code FROM discharge_points')
    existing_dcp = set(row[0] for row in cur.fetchall() if row[0])
    print(f'  Existing: {len(existing_dcp)}')
    
    # 获取数据库中的 water_body_codes
    cur.execute('SELECT eu_water_body_code FROM water_bodies')
    existing_wb = set(row[0] for row in cur.fetchall() if row[0])
    print(f'  Water bodies in DB: {len(existing_wb)}')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'discharge_points.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    # 过滤：新的 + (有对应 water_body 或 NULL)
    new_data = []
    skipped = 0
    for row in data:
        if row.get('dcp_code') in existing_dcp:
            continue
        wb_code = row.get('water_body_code')
        if wb_code and wb_code not in existing_wb:
            skipped += 1
            continue
        new_data.append(row)
    
    print(f'  New to insert: {len(new_data)}')
    print(f'  Skipped (missing FK): {skipped}')
    
    if not new_data:
        return 0
    
    columns = ['dcp_code', 'is_surface_water', 'water_body_code']
    values = []
    for row in new_data:
        wb_code = row.get('water_body_code')
        if wb_code and wb_code not in existing_wb:
            wb_code = None
        is_surface = str(row.get('is_surface_water', '')).lower() == 'true'
        values.append((clean(row.get('dcp_code')), is_surface, clean(wb_code)))
    
    return batch_insert(cur, 'discharge_points', columns, values, 'dcp_code')


def import_protected_areas(cur):
    """导入 water_body_protected_areas"""
    print('\n[5/5] Importing water_body_protected_areas...')
    
    # 检查是否已有数据
    cur.execute('SELECT COUNT(*) FROM water_body_protected_areas')
    existing_count = cur.fetchone()[0]
    print(f'  Existing: {existing_count}')
    
    if existing_count > 0:
        print('  Skipping (already has data)')
        return 0
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'water_body_protected_areas.csv'))
    print(f'  Total in CSV: {len(data)}')
    
    columns = ['water_body_code', 'eu_protected_area_code', 'protected_area_type']
    values = [
        (clean(row.get('water_body_code')), clean(row.get('eu_protected_area_code')), 
         clean(row.get('protected_area_type')))
        for row in data
    ]
    
    return batch_insert(cur, 'water_body_protected_areas', columns, values, batch_size=10000)


def main():
    print('=' * 60)
    print('Fast Batch Import to Supabase')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Target: {SUPABASE_HOST}')
    
    # 连接数据库
    print('\n[Connect] Connecting to Supabase...')
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('  ✓ Connected')
    
    # 导入数据
    stats = {}
    
    stats['water_bodies'] = import_water_bodies(cur)
    stats['agglomeration'] = import_agglomeration(cur)
    stats['plants'] = import_plants(cur)
    stats['discharge_points'] = import_discharge_points(cur)
    stats['protected_areas'] = import_protected_areas(cur)
    
    # 关闭连接
    cur.close()
    conn.close()
    
    # 打印总结
    print('\n' + '=' * 60)
    print('Import Summary')
    print('=' * 60)
    for table, count in stats.items():
        status = '✓' if count > 0 else '-'
        print(f'  {status} {table}: {count} rows')
    print(f'\n  Total: {sum(stats.values())} rows')
    print('=' * 60)
    print('✅ Import completed!')


if __name__ == '__main__':
    main()
