"""
部署数据到 Supabase
从 DATA/current/transformed/ 读取转换后的数据
按外键依赖顺序导入
"""
import os
import sys
import csv
import json
from datetime import datetime

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from etl.config import CURRENT_DIR, MANIFEST_FILE

# Supabase 配置
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'supabase'))
from config import (
    SUPABASE_HOST,
    SUPABASE_PORT,
    SUPABASE_DATABASE,
    SUPABASE_USER,
    SUPABASE_PASSWORD
)

TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')

# 导入顺序（按外键依赖）
IMPORT_ORDER = [
    'countries',
    'sensitivity',
    'report_periods',
    'water_bodies',
    'agglomerations',
    'plants',
    'discharge_points',
    'water_body_protected_areas',
]


def get_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def read_csv(filepath):
    """读取 CSV 文件"""
    if not os.path.exists(filepath):
        return []
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return list(reader)


def clean_value(value):
    """清理值，处理空字符串"""
    if value == '' or value is None:
        return None
    return value


def import_countries(cur, data):
    """导入国家数据"""
    for row in data:
        cur.execute("""
            INSERT INTO country (country_code, country_name)
            VALUES (%s, %s)
            ON CONFLICT (country_code) DO NOTHING
        """, (
            clean_value(row.get('country_code')),
            clean_value(row.get('country_name')),
        ))
    return len(data)


def import_sensitivity(cur, data):
    """导入敏感度查找表"""
    for row in data:
        cur.execute("""
            INSERT INTO sensitivity (code, definition, nutrient_sensitivity)
            VALUES (%s, %s, %s)
            ON CONFLICT (code) DO NOTHING
        """, (
            clean_value(row.get('code')),
            clean_value(row.get('definition')),
            row.get('nutrient_sensitivity', '').lower() == 'true',
        ))
    return len(data)


def import_report_periods(cur, data):
    """导入报告期数据"""
    for row in data:
        cur.execute("""
            INSERT INTO report_code (rep_code, country_code, year, version)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (rep_code) DO NOTHING
        """, (
            clean_value(row.get('rep_code')),
            clean_value(row.get('country_code')),
            clean_value(row.get('reported_period')),
            clean_value(row.get('version')),
        ))
    return len(data)


def import_water_bodies(cur, data):
    """导入水体数据"""
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO water_bodies (
                    eu_water_body_code, water_type, water_body_name, country_code,
                    eu_rbd_code, rbd_name, c_year,
                    surface_water_category, c_area, c_length,
                    sw_ecological_status, sw_chemical_status,
                    gw_quantitative_status, gw_chemical_status, file_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (eu_water_body_code) DO NOTHING
            """, (
                clean_value(row.get('eu_water_body_code')),
                clean_value(row.get('water_type')),
                clean_value(row.get('water_body_name')),
                clean_value(row.get('country_code')),
                clean_value(row.get('eu_rbd_code')),
                clean_value(row.get('rbd_name')),
                clean_value(row.get('c_year')),
                clean_value(row.get('surface_water_category')),
                clean_value(row.get('c_area')),
                clean_value(row.get('c_length')),
                clean_value(row.get('sw_ecological_status')),
                clean_value(row.get('sw_chemical_status')),
                clean_value(row.get('gw_quantitative_status')),
                clean_value(row.get('gw_chemical_status')),
                clean_value(row.get('file_url')),
            ))
            count += 1
        except Exception as e:
            pass
    return count


def import_agglomerations(cur, data):
    """导入聚居区数据"""
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO agglomeration (
                    agg_code, agglomeration_name, country_code,
                    agg_capacity, agg_generated, latitude, longitude
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (agg_code) DO NOTHING
            """, (
                clean_value(row.get('agg_code')),
                clean_value(row.get('agglomeration_name')),
                clean_value(row.get('country_code')),
                clean_value(row.get('agg_capacity')),
                clean_value(row.get('agg_generated')),
                clean_value(row.get('latitude')),
                clean_value(row.get('longitude')),
            ))
            count += 1
        except Exception as e:
            pass
    return count


def import_plants(cur, data):
    """导入处理厂数据"""
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO plants (
                    uwwtp_code, plant_name, country_code, lat, longitude,
                    provides_primary_treatment, provides_secondary_treatment,
                    other_treatment_provided, provides_nitrogen_removal, provides_phosphorus_removal,
                    includes_uv_treatment, includes_chlorination, includes_ozonation,
                    includes_sand_filtration, includes_microfiltration,
                    bod_removal_pct, cod_removal_pct, nitrogen_removal_pct, phosphorus_removal_pct,
                    rep_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (uwwtp_code) DO NOTHING
            """, (
                clean_value(row.get('uwwtp_code')),
                clean_value(row.get('plant_name')),
                clean_value(row.get('country_code')),
                clean_value(row.get('lat')),
                clean_value(row.get('longitude')),
                row.get('provides_primary_treatment', '').lower() == 'true',
                row.get('provides_secondary_treatment', '').lower() == 'true',
                row.get('other_treatment_provided', '').lower() == 'true',
                row.get('provides_nitrogen_removal', '').lower() == 'true',
                row.get('provides_phosphorus_removal', '').lower() == 'true',
                row.get('includes_uv_treatment', '').lower() == 'true',
                row.get('includes_chlorination', '').lower() == 'true',
                row.get('includes_ozonation', '').lower() == 'true',
                row.get('includes_sand_filtration', '').lower() == 'true',
                row.get('includes_microfiltration', '').lower() == 'true',
                clean_value(row.get('bod_removal_pct')),
                clean_value(row.get('cod_removal_pct')),
                clean_value(row.get('nitrogen_removal_pct')),
                clean_value(row.get('phosphorus_removal_pct')),
                clean_value(row.get('rep_code')),
            ))
            count += 1
        except Exception as e:
            pass
    return count


def import_discharge_points(cur, data):
    """导入排放点数据"""
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO discharge_points (
                    dcp_code, plant_code, country_code,
                    latitude, longitude,
                    is_surface_water, water_body_type, water_body_code,
                    sensitivity_code, rca_code,
                    receiving_water, wfd_rbd, wfd_sub_unit, rep_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dcp_code) DO NOTHING
            """, (
                clean_value(row.get('dcp_code')),
                clean_value(row.get('plant_code')),
                clean_value(row.get('country_code')),
                clean_value(row.get('latitude')),
                clean_value(row.get('longitude')),
                row.get('is_surface_water', '').lower() == 'true',
                clean_value(row.get('water_body_type')),
                clean_value(row.get('water_body_code')),
                clean_value(row.get('sensitivity_code')),
                clean_value(row.get('rca_code')),
                clean_value(row.get('receiving_water')),
                clean_value(row.get('wfd_rbd')),
                clean_value(row.get('wfd_sub_unit')),
                clean_value(row.get('rep_code')),
            ))
            count += 1
        except Exception as e:
            pass
    return count


def import_water_body_protected_areas(cur, data):
    """导入水体保护区数据"""
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO water_body_protected_areas (
                    water_body_code, eu_protected_area_code,
                    protected_area_type, objectives_set, objectives_met
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                clean_value(row.get('water_body_code')),
                clean_value(row.get('eu_protected_area_code')),
                clean_value(row.get('protected_area_type')),
                row.get('objectives_set', '').lower() == 'true',
                row.get('objectives_met', '').lower() == 'true',
            ))
            count += 1
        except Exception as e:
            pass
    return count


# 导入函数映射
IMPORT_FUNCTIONS = {
    'countries': import_countries,
    'sensitivity': import_sensitivity,
    'report_periods': import_report_periods,
    'water_bodies': import_water_bodies,
    'agglomerations': import_agglomerations,
    'plants': import_plants,
    'discharge_points': import_discharge_points,
    'water_body_protected_areas': import_water_body_protected_areas,
}


def main():
    """主部署流程"""
    print('=' * 60)
    print('DISCO Data Deploy to Supabase')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Source: {TRANSFORMED_DIR}')
    print(f'Target: {SUPABASE_HOST}')
    
    # 检查转换后的数据是否存在
    if not os.path.exists(TRANSFORMED_DIR):
        print('\n❌ Error: Transformed data not found!')
        print('   Please run: python etl/extract.py && python etl/transform.py')
        return
    
    # 连接数据库
    print('\n[Connect] Connecting to Supabase...')
    try:
        conn = get_connection()
        conn.autocommit = True
        cur = conn.cursor()
        print('  ✓ Connected')
    except Exception as e:
        print(f'  ❌ Connection failed: {e}')
        print('\n  Please check supabase/config.py settings.')
        return
    
    # 按顺序导入
    stats = {}
    for table_name in IMPORT_ORDER:
        csv_file = os.path.join(TRANSFORMED_DIR, f'{table_name}.csv')
        
        if not os.path.exists(csv_file):
            print(f'\n[Skip] {table_name} - file not found')
            continue
        
        print(f'\n[Import] {table_name}')
        
        # 读取数据
        data = read_csv(csv_file)
        print(f'  Source rows: {len(data)}')
        
        # 导入数据
        import_func = IMPORT_FUNCTIONS.get(table_name)
        if import_func:
            try:
                count = import_func(cur, data)
                stats[table_name] = count
                print(f'  ✓ Imported: {count} rows')
            except Exception as e:
                print(f'  ❌ Error: {e}')
                stats[table_name] = 0
        else:
            print(f'  ⚠ No import function defined')
    
    # 关闭连接
    cur.close()
    conn.close()
    
    # 打印总结
    print('\n' + '=' * 60)
    print('Deploy Summary')
    print('=' * 60)
    for table_name, count in stats.items():
        status = '✓' if count > 0 else '✗'
        print(f'  {status} {table_name}: {count} rows')
    print(f'\n  Total: {sum(stats.values())} rows')
    print('=' * 60)
    print('✅ Deploy completed!')


if __name__ == '__main__':
    main()
