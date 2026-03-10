"""
根据 fields mapping with DISCO.xlsx 更新数据库
只更新不需要计算的字段
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

# 原始数据目录
RAW_DIR = CURRENT_DIR


def read_csv(filepath):
    if not os.path.exists(filepath):
        print(f'  File not found: {filepath}')
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
    v = str(value).lower().strip()
    if v in ('true', '1', 'yes'):
        return True
    if v in ('false', '0', 'no'):
        return False
    return None


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


def to_date(value):
    """转换日期字段"""
    if value is None or value == '':
        return None
    try:
        # 尝试多种日期格式
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%Y-%m-%dT%H:%M:%S']:
            try:
                return datetime.strptime(value.split('T')[0], fmt.split('T')[0]).date()
            except:
                continue
        return None
    except:
        return None


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def update_plants(cur):
    """
    更新 plants 表
    根据 Excel 映射：
    - 合规性字段：uwwBOD5Perf, uwwCODPerf, uwwTSSPerf, uwwNTotPerf, uwwPTotPerf, uwwOtherPerf
    - 日期：uwwBeginLife
    - 容量：uwwWasteWaterTreated
    - 不更新 removal_pct 字段（需要计算）
    """
    print('\n[1/2] Updating plants from raw DISCO data...')
    
    # 读取原始数据
    data = read_csv(os.path.join(RAW_DIR, 'plants.csv'))
    if not data:
        print('  No data found')
        return
    
    print(f'  CSV records: {len(data)}')
    print(f'  Sample columns: {list(data[0].keys())[:10]}...')
    
    # 检查需要的列是否存在
    sample = data[0]
    needed_cols = ['uwwCode', 'uwwBOD5Perf', 'uwwCODPerf', 'uwwBeginLife', 'uwwWasteWaterTreated']
    for col in needed_cols:
        if col in sample:
            print(f'  ✓ Found column: {col}')
        else:
            print(f'  ✗ Missing column: {col}')
    
    # 构建更新数据
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        if not uwwtp_code:
            continue
            
        updates.append((
            # 合规性字段 (不是 removal %)
            clean(row.get('uwwBOD5Perf')),      # bod_compliance
            clean(row.get('uwwCODPerf')),       # cod_compliance  
            clean(row.get('uwwTSSPerf')),       # tss_compliance
            clean(row.get('uwwNTotPerf')),      # nitrogen_compliance
            clean(row.get('uwwPTotPerf')),      # phosphorus_compliance
            clean(row.get('uwwOtherPerf')),     # other_compliance
            # 日期
            to_date(row.get('uwwBeginLife')),   # commissioning_date
            # 容量
            to_float(row.get('uwwWasteWaterTreated')),  # volume_wastewater_reused_m3_per_year
            # WHERE
            uwwtp_code,
        ))
    
    print(f'  Records to update: {len(updates)}')
    
    # 先检查数据库是否有这些列
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'plants' AND column_name IN (
            'bod_compliance', 'cod_compliance', 'tss_compliance',
            'nitrogen_compliance', 'phosphorus_compliance', 'other_compliance',
            'commissioning_date', 'volume_wastewater_reused_m3_per_year'
        )
    """)
    existing_cols = [r[0] for r in cur.fetchall()]
    print(f'  Existing DB columns for compliance: {existing_cols}')
    
    # 只更新存在的列
    print(f'  Compliance columns in DB: {existing_cols}')
    print('  (compliance columns not in current schema)')
    print('  Updating existing columns only...')
    
    # 获取所有plants列
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'plants'")
    all_cols = [r[0] for r in cur.fetchall()]
    
    updated_count = 0
    
    if 'commissioning_date' in all_cols:
        print('  Updating commissioning_date...')
        updates_date = [(to_date(row.get('uwwBeginLife')), clean(row.get('uwwCode'))) 
                       for row in data if clean(row.get('uwwCode'))]
        sql = "UPDATE plants SET commissioning_date = COALESCE(commissioning_date, %s) WHERE uwwtp_code = %s"
        execute_batch(cur, sql, updates_date, page_size=1000)
        print(f'    ✓ Updated commissioning_date')
        updated_count += 1
    
    if 'volume_wastewater_reused_m3_per_year' in all_cols:
        print('  Updating volume_wastewater_reused_m3_per_year...')
        updates_vol = [(to_float(row.get('uwwWasteWaterTreated')), clean(row.get('uwwCode'))) 
                      for row in data if clean(row.get('uwwCode'))]
        sql = "UPDATE plants SET volume_wastewater_reused_m3_per_year = COALESCE(volume_wastewater_reused_m3_per_year, %s) WHERE uwwtp_code = %s"
        execute_batch(cur, sql, updates_vol, page_size=1000)
        print(f'    ✓ Updated volume_wastewater_reused_m3_per_year')
        updated_count += 1
    
    print(f'  Total fields updated: {updated_count}')


def update_art17(cur):
    """
    更新 plants 表的 Article 17 相关字段
    从 T_Art17_FLAUWWTP 表获取数据
    """
    print('\n[2/2] Updating Article 17 fields...')
    
    # 读取 Art17 数据
    data = read_csv(os.path.join(RAW_DIR, 'art17_investments.csv'))
    if not data:
        print('  No Art17 data found')
        return
    
    print(f'  Art17 CSV records: {len(data)}')
    print(f'  Sample columns: {list(data[0].keys())[:15]}...')
    
    # 检查数据库中 plants 表是否有 Article 17 列
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'plants' AND column_name LIKE 'article17%'
    """)
    art17_cols = [r[0] for r in cur.fetchall()]
    print(f'  Article 17 columns in DB: {art17_cols}')
    
    if not art17_cols:
        print('  ⚠️ No Article 17 columns in plants table, skipping...')
        return
    
    # 构建更新数据
    # Art17 字段映射：
    # flatpStatus -> article17_compliance_status
    # flatpMeasures -> article17_investment_planned
    # flatpInv -> article17_investment_cost
    # flatpEUFundName -> eu_funding_scheme
    # flatpEUFund -> eu_funding_amount
    # flatpOtherFundName -> other_funding_scheme
    # flatpOtherFund -> other_funding_amount
    # flatpExpecDateStart -> planning_start_date
    # flatpExpecDateStartWork -> construction_start_date
    # flatpExpecDateCompletion -> construction_completion_date
    # flatpExpecDatePerformance -> expected_commissioning_date
    # flatpExpCapacity -> capacity_expansion
    
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        if not uwwtp_code:
            continue
        
        updates.append((
            clean(row.get('flatpStatus')),
            to_bool(row.get('flatpMeasures')),
            to_float(row.get('flatpInv')),
            clean(row.get('flatpEUFundName')),
            to_float(row.get('flatpEUFund')),
            clean(row.get('flatpOtherFundName')),
            to_float(row.get('flatpOtherFund')),
            to_date(row.get('flatpExpecDateStart')),
            to_date(row.get('flatpExpecDateStartWork')),
            to_date(row.get('flatpExpecDateCompletion')),
            to_date(row.get('flatpExpecDatePerformance')),
            to_float(row.get('flatpExpCapacity')),
            # WHERE
            uwwtp_code,
        ))
    
    print(f'  Records to update: {len(updates)}')
    
    sql = """
        UPDATE plants SET
            article17_compliance_status = COALESCE(article17_compliance_status, %s),
            article17_investment_planned = COALESCE(article17_investment_planned, %s),
            article17_investment_cost = COALESCE(article17_investment_cost, %s),
            eu_funding_scheme = COALESCE(eu_funding_scheme, %s),
            eu_funding_amount = COALESCE(eu_funding_amount, %s),
            other_funding_scheme = COALESCE(other_funding_scheme, %s),
            other_funding_amount = COALESCE(other_funding_amount, %s),
            planning_start_date = COALESCE(planning_start_date, %s),
            construction_start_date = COALESCE(construction_start_date, %s),
            construction_completion_date = COALESCE(construction_completion_date, %s),
            expected_commissioning_date = COALESCE(expected_commissioning_date, %s),
            capacity_expansion = COALESCE(capacity_expansion, %s)
        WHERE uwwtp_code = %s
    """
    
    try:
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'  ✓ Updated: {len(updates)} rows')
    except Exception as e:
        print(f'  ✗ Error: {e}')
        print('  Some Article 17 columns may not exist in the schema')


def main():
    print('=' * 60)
    print('Update Database from DISCO Mapping')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Data directory: {RAW_DIR}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    update_plants(cur)
    update_art17(cur)
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Update completed!')
    print('Note: removal_pct fields need to be calculated separately')


if __name__ == '__main__':
    main()
