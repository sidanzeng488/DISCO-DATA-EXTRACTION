"""
UPDATE 补充缺失的字段
保留现有数据，只更新空字段
"""
import sys
import os
import csv
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_batch
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
from etl.config import CURRENT_DIR

TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')


def read_csv(filepath):
    if not os.path.exists(filepath):
        return []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def clean(value):
    if value is None or value == '' or value == 'NULL':
        return None
    return value


def to_bool(value):
    if value is None or value == '':
        return None
    return str(value).lower() == 'true'


def to_float(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except:
        return None


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


def update_plants(cur):
    """更新 plants 表的缺失字段"""
    print('\n[1/3] Updating plants...')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'plants.csv'))
    print(f'  CSV records: {len(data)}')
    
    # 构建更新数据 (不包括 rep_code，因为外键约束)
    updates = []
    for row in data:
        updates.append((
            to_bool(row.get('provides_primary_treatment')),
            to_bool(row.get('provides_secondary_treatment')),
            to_bool(row.get('other_treatment_provided')),
            to_bool(row.get('provides_nitrogen_removal')),
            to_bool(row.get('provides_phosphorus_removal')),
            to_bool(row.get('includes_uv_treatment')),
            to_bool(row.get('includes_chlorination')),
            to_bool(row.get('includes_ozonation')),
            to_bool(row.get('includes_sand_filtration')),
            to_bool(row.get('includes_microfiltration')),
            to_float(row.get('bod_removal_pct')),
            to_float(row.get('cod_removal_pct')),
            to_float(row.get('nitrogen_removal_pct')),
            to_float(row.get('phosphorus_removal_pct')),
            to_float(row.get('bod_incoming_measured')),
            to_float(row.get('bod_incoming_calculated')),
            to_float(row.get('bod_incoming_estimated')),
            to_float(row.get('bod_outgoing_measured')),
            to_float(row.get('bod_outgoing_calculated')),
            to_float(row.get('bod_outgoing_estimated')),
            to_float(row.get('cod_incoming_measured')),
            to_float(row.get('cod_incoming_calculated')),
            to_float(row.get('cod_incoming_estimated')),
            to_float(row.get('nitrogen_incoming_measured')),
            to_float(row.get('nitrogen_incoming_calculated')),
            to_float(row.get('nitrogen_incoming_estimated')),
            to_float(row.get('nitrogen_outgoing_measured')),
            to_float(row.get('nitrogen_outgoing_calculated')),
            to_float(row.get('nitrogen_outgoing_estimated')),
            to_float(row.get('phosphorus_incoming_measured')),
            to_float(row.get('phosphorus_incoming_calculated')),
            to_float(row.get('phosphorus_incoming_estimated')),
            to_float(row.get('phosphorus_outgoing_measured')),
            to_float(row.get('phosphorus_outgoing_calculated')),
            to_float(row.get('phosphorus_outgoing_estimated')),
            clean(row.get('failure_notes')),
            clean(row.get('plant_notes')),
            to_float(row.get('pct_wastewater_reused')),
            # WHERE 条件
            clean(row.get('uwwtp_code')),
        ))
    
    sql = """
        UPDATE plants SET
            provides_primary_treatment = COALESCE(provides_primary_treatment, %s),
            provides_secondary_treatment = COALESCE(provides_secondary_treatment, %s),
            other_treatment_provided = COALESCE(other_treatment_provided, %s),
            provides_nitrogen_removal = COALESCE(provides_nitrogen_removal, %s),
            provides_phosphorus_removal = COALESCE(provides_phosphorus_removal, %s),
            includes_uv_treatment = COALESCE(includes_uv_treatment, %s),
            includes_chlorination = COALESCE(includes_chlorination, %s),
            includes_ozonation = COALESCE(includes_ozonation, %s),
            includes_sand_filtration = COALESCE(includes_sand_filtration, %s),
            includes_microfiltration = COALESCE(includes_microfiltration, %s),
            bod_removal_pct = COALESCE(bod_removal_pct, %s),
            cod_removal_pct = COALESCE(cod_removal_pct, %s),
            nitrogen_removal_pct = COALESCE(nitrogen_removal_pct, %s),
            phosphorus_removal_pct = COALESCE(phosphorus_removal_pct, %s),
            bod_incoming_measured = COALESCE(bod_incoming_measured, %s),
            bod_incoming_calculated = COALESCE(bod_incoming_calculated, %s),
            bod_incoming_estimated = COALESCE(bod_incoming_estimated, %s),
            bod_outgoing_measured = COALESCE(bod_outgoing_measured, %s),
            bod_outgoing_calculated = COALESCE(bod_outgoing_calculated, %s),
            bod_outgoing_estimated = COALESCE(bod_outgoing_estimated, %s),
            cod_incoming_measured = COALESCE(cod_incoming_measured, %s),
            cod_incoming_calculated = COALESCE(cod_incoming_calculated, %s),
            cod_incoming_estimated = COALESCE(cod_incoming_estimated, %s),
            nitrogen_incoming_measured = COALESCE(nitrogen_incoming_measured, %s),
            nitrogen_incoming_calculated = COALESCE(nitrogen_incoming_calculated, %s),
            nitrogen_incoming_estimated = COALESCE(nitrogen_incoming_estimated, %s),
            nitrogen_outgoing_measured = COALESCE(nitrogen_outgoing_measured, %s),
            nitrogen_outgoing_calculated = COALESCE(nitrogen_outgoing_calculated, %s),
            nitrogen_outgoing_estimated = COALESCE(nitrogen_outgoing_estimated, %s),
            phosphorus_incoming_measured = COALESCE(phosphorus_incoming_measured, %s),
            phosphorus_incoming_calculated = COALESCE(phosphorus_incoming_calculated, %s),
            phosphorus_incoming_estimated = COALESCE(phosphorus_incoming_estimated, %s),
            phosphorus_outgoing_measured = COALESCE(phosphorus_outgoing_measured, %s),
            phosphorus_outgoing_calculated = COALESCE(phosphorus_outgoing_calculated, %s),
            phosphorus_outgoing_estimated = COALESCE(phosphorus_outgoing_estimated, %s),
            failure_notes = COALESCE(failure_notes, %s),
            plant_notes = COALESCE(plant_notes, %s),
            pct_wastewater_reused = COALESCE(pct_wastewater_reused, %s)
        WHERE uwwtp_code = %s
    """
    
    execute_batch(cur, sql, updates, page_size=1000)
    print(f'  Updated: {len(updates)} rows')


def update_discharge_points(cur):
    """更新 discharge_points 表的缺失字段"""
    print('\n[2/3] Updating discharge_points...')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'discharge_points.csv'))
    print(f'  CSV records: {len(data)}')
    
    # 不包括 rep_code 和 plant_code，因为外键约束
    updates = []
    for row in data:
        updates.append((
            clean(row.get('country_code')),
            to_float(row.get('latitude')),
            to_float(row.get('longitude')),
            clean(row.get('water_body_type')),
            clean(row.get('sensitivity_code')),
            clean(row.get('rca_code')),
            clean(row.get('receiving_water')),
            clean(row.get('wfd_rbd')),
            clean(row.get('wfd_sub_unit')),
            # WHERE
            clean(row.get('dcp_code')),
        ))
    
    sql = """
        UPDATE discharge_points SET
            country_code = COALESCE(country_code, %s),
            latitude = COALESCE(latitude, %s),
            longitude = COALESCE(longitude, %s),
            water_body_type = COALESCE(water_body_type, %s),
            sensitivity_code = COALESCE(sensitivity_code, %s),
            rca_code = COALESCE(rca_code, %s),
            receiving_water = COALESCE(receiving_water, %s),
            wfd_rbd = COALESCE(wfd_rbd, %s),
            wfd_sub_unit = COALESCE(wfd_sub_unit, %s)
        WHERE dcp_code = %s
    """
    
    execute_batch(cur, sql, updates, page_size=1000)
    print(f'  Updated: {len(updates)} rows')


def update_agglomeration(cur):
    """更新 agglomeration 表的缺失字段"""
    print('\n[3/3] Updating agglomeration...')
    
    data = read_csv(os.path.join(TRANSFORMED_DIR, 'agglomerations.csv'))
    print(f'  CSV records: {len(data)}')
    
    updates = []
    for row in data:
        updates.append((
            clean(row.get('country_code')),
            to_int(row.get('agg_capacity')),
            to_float(row.get('agg_generated')),
            to_float(row.get('latitude')),
            to_float(row.get('longitude')),
            # WHERE
            clean(row.get('agg_code')),
        ))
    
    sql = """
        UPDATE agglomeration SET
            country_code = COALESCE(country_code, %s),
            agg_capacity = COALESCE(agg_capacity, %s),
            agg_generated = COALESCE(agg_generated, %s),
            latitude = COALESCE(latitude, %s),
            longitude = COALESCE(longitude, %s)
        WHERE agg_code = %s
    """
    
    execute_batch(cur, sql, updates, page_size=1000)
    print(f'  Updated: {len(updates)} rows')


def main():
    print('=' * 60)
    print('Update Missing Fields')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    update_plants(cur)
    update_discharge_points(cur)
    update_agglomeration(cur)
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Update completed!')
    print('   Run check_columns.py to verify')


if __name__ == '__main__':
    main()
