"""
使用 art17_investments.csv 和 art17_contacts.csv 填充 plants 表的 Article 17 字段
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
        print(f'  File not found: {filepath}')
        return []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def clean(value):
    if value is None or value == '' or value == 'NULL':
        return None
    return value.strip()


def to_float(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except:
        return None


def to_date(value):
    if value is None or value == '':
        return None
    try:
        # 处理 YYYY-MM-DD 格式
        return value.split('T')[0] if 'T' in value else value
    except:
        return None


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def check_empty_columns(cur):
    """检查 plants 表中哪些列是空的"""
    print('\n' + '=' * 60)
    print('检查 plants 表中的空列')
    print('=' * 60)
    
    cur.execute("SELECT COUNT(*) FROM plants")
    total = cur.fetchone()[0]
    
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'plants' ORDER BY ordinal_position
    """)
    columns = [r[0] for r in cur.fetchall()]
    
    empty_cols = []
    low_fill_cols = []
    
    for col in columns:
        cur.execute(f'SELECT COUNT(*) FROM plants WHERE "{col}" IS NOT NULL')
        filled = cur.fetchone()[0]
        pct = filled * 100 / total if total > 0 else 0
        
        if pct == 0:
            empty_cols.append(col)
        elif pct < 20:
            low_fill_cols.append((col, pct))
    
    print(f'\n完全为空的列 ({len(empty_cols)}):')
    for col in empty_cols:
        print(f'  ✗ {col}')
    
    print(f'\n填充率 < 20% 的列 ({len(low_fill_cols)}):')
    for col, pct in low_fill_cols:
        print(f'  ○ {col}: {pct:.1f}%')
    
    return empty_cols


def fill_from_art17_investments(cur):
    """从 art17_investments.csv 填充"""
    print('\n' + '=' * 60)
    print('从 art17_investments.csv 填充')
    print('=' * 60)
    
    data = read_csv(os.path.join(DATA_DIR, 'art17_investments.csv'))
    print(f'CSV records: {len(data)}')
    
    if not data:
        return
    
    # 显示可用的列
    print(f'Available columns: {list(data[0].keys())}')
    
    # 构建更新 - 映射关系
    # flatpStatus -> article17_compliance_status
    # flatpMeasures -> article17_investment_planned (text)
    # flatpReasons -> article17_investment_need (text)
    # flatpExpecTreatment -> article17_investment_type
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
            clean(row.get('flatpMeasures')),
            clean(row.get('flatpReasons')),
            clean(row.get('flatpExpecTreatment')),
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
            uwwtp_code,
        ))
    
    print(f'Records to update: {len(updates)}')
    
    sql = """
        UPDATE plants SET
            article17_compliance_status = COALESCE(article17_compliance_status, %s),
            article17_investment_planned = COALESCE(article17_investment_planned, %s),
            article17_investment_need = COALESCE(article17_investment_need, %s),
            article17_investment_type = COALESCE(article17_investment_type, %s),
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
    
    execute_batch(cur, sql, updates, page_size=1000)
    print('✓ Updated from art17_investments')


def fill_report_date_from_contacts(cur):
    """从 art17_contacts.csv 获取报告日期"""
    print('\n' + '=' * 60)
    print('从 art17_contacts.csv 填充 article17_report_date')
    print('=' * 60)
    
    contacts = read_csv(os.path.join(DATA_DIR, 'art17_contacts.csv'))
    investments = read_csv(os.path.join(DATA_DIR, 'art17_investments.csv'))
    
    print(f'Contacts CSV: {len(contacts)} records')
    print(f'Investments CSV: {len(investments)} records')
    
    if not contacts or not investments:
        return
    
    # 构建 flarepCode -> flaconSituationAt 映射
    flarep_to_date = {}
    for row in contacts:
        flarep_code = clean(row.get('flarepCode'))
        situation_at = to_date(row.get('flaconSituationAt'))
        if flarep_code and situation_at:
            flarep_to_date[flarep_code] = situation_at
    
    print(f'flarepCode -> date mappings: {len(flarep_to_date)}')
    
    # 构建 uwwCode -> report_date 映射
    updates = []
    for row in investments:
        uwwtp_code = clean(row.get('uwwCode'))
        flarep_code = clean(row.get('flarepCode'))
        
        if uwwtp_code and flarep_code:
            report_date = flarep_to_date.get(flarep_code)
            if report_date:
                updates.append((report_date, uwwtp_code))
    
    print(f'Records to update: {len(updates)}')
    
    if updates:
        sql = """
            UPDATE plants 
            SET article17_report_date = %s 
            WHERE uwwtp_code = %s AND article17_report_date IS NULL
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print('✓ Updated article17_report_date')


def main():
    print('=' * 60)
    print('Complete Art17 Data Fill')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 先检查空列
    check_empty_columns(cur)
    
    # 填充 Art17 数据
    fill_from_art17_investments(cur)
    fill_report_date_from_contacts(cur)
    
    # 再次检查
    print('\n' + '=' * 60)
    print('填充后的状态')
    print('=' * 60)
    check_empty_columns(cur)
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Completed!')


if __name__ == '__main__':
    main()
