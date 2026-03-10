"""
修复缺失字段：
1. COD outgoing 数据
2. Article 17 investment_type
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


def to_float(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except:
        return None


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def fix_cod_outgoing(cur):
    """修复 COD outgoing 字段"""
    print('\n[1/2] Fixing COD outgoing fields...')
    
    data = read_csv(os.path.join(DATA_DIR, 'plants.csv'))
    print(f'  CSV records: {len(data)}')
    
    # 检查源数据
    has_data = 0
    for row in data:
        if row.get('uwwCODDischargeMeasured'):
            has_data += 1
    print(f'  Records with COD discharge data: {has_data}')
    
    # 构建更新
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        if not uwwtp_code:
            continue
        
        cod_measured = to_float(row.get('uwwCODDischargeMeasured'))
        cod_calculated = to_float(row.get('uwwCODDischargeCalculated'))
        cod_estimated = to_float(row.get('uwwCODDischargeEstimated'))
        
        # 只有有数据的才更新
        if cod_measured or cod_calculated or cod_estimated:
            updates.append((cod_measured, cod_calculated, cod_estimated, uwwtp_code))
    
    print(f'  Records to update: {len(updates)}')
    
    if updates:
        sql = """
            UPDATE plants SET
                cod_outgoing_measured = COALESCE(cod_outgoing_measured, %s),
                cod_outgoing_calculated = COALESCE(cod_outgoing_calculated, %s),
                cod_outgoing_estimated = COALESCE(cod_outgoing_estimated, %s)
            WHERE uwwtp_code = %s
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'  ✓ Updated COD outgoing for {len(updates)} rows')
    else:
        print('  No updates needed')


def fix_art17_type(cur):
    """修复 Article 17 investment_type 字段"""
    print('\n[2/2] Fixing Article 17 investment_type...')
    
    data = read_csv(os.path.join(DATA_DIR, 'art17_investments.csv'))
    print(f'  Art17 CSV records: {len(data)}')
    
    # 检查源数据
    has_data = 0
    for row in data:
        if row.get('flatpExpecTreatment'):
            has_data += 1
    print(f'  Records with treatment type: {has_data}')
    
    # 构建更新
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        if not uwwtp_code:
            continue
        
        treatment_type = clean(row.get('flatpExpecTreatment'))
        if treatment_type:
            updates.append((treatment_type, uwwtp_code))
    
    print(f'  Records to update: {len(updates)}')
    
    if updates:
        sql = """
            UPDATE plants SET
                article17_investment_type = COALESCE(article17_investment_type, %s)
            WHERE uwwtp_code = %s
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'  ✓ Updated investment_type for {len(updates)} rows')
    else:
        print('  No updates needed')


def main():
    print('=' * 60)
    print('Fix Missing Fields')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    fix_cod_outgoing(cur)
    fix_art17_type(cur)
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Fix completed!')
    print('\nNote about Article 17 fields:')
    print('  - article17_investment_planned (boolean) ← flatpMeasures is TEXT, not boolean')
    print('  - article17_investment_need (numeric) ← flatpReasons is TEXT, not numeric')
    print('  These fields have data type mismatch with source data.')


if __name__ == '__main__':
    main()
