"""
填充新添加的字段：
1. Compliance 字段 (bod_compliance, cod_compliance 等)
2. Article 17 文本字段 (investment_planned, investment_need)
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


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def fill_compliance_fields(cur):
    """填充合规性字段"""
    print('\n[1/2] Filling compliance fields...')
    
    data = read_csv(os.path.join(DATA_DIR, 'plants.csv'))
    print(f'  CSV records: {len(data)}')
    
    # 检查有多少数据
    for field in ['uwwBOD5Perf', 'uwwCODPerf', 'uwwTSSPerf', 'uwwNTotPerf', 'uwwPTotPerf', 'uwwOtherPerf']:
        count = sum(1 for r in data if r.get(field))
        print(f'    {field}: {count} records')
    
    # 构建更新
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        if not uwwtp_code:
            continue
        
        updates.append((
            clean(row.get('uwwBOD5Perf')),     # bod_compliance
            clean(row.get('uwwCODPerf')),      # cod_compliance
            clean(row.get('uwwTSSPerf')),      # tss_compliance
            clean(row.get('uwwNTotPerf')),     # nitrogen_compliance
            clean(row.get('uwwPTotPerf')),     # phosphorus_compliance
            clean(row.get('uwwOtherPerf')),    # other_compliance
            uwwtp_code,
        ))
    
    print(f'  Records to update: {len(updates)}')
    
    sql = """
        UPDATE plants SET
            bod_compliance = COALESCE(bod_compliance, %s),
            cod_compliance = COALESCE(cod_compliance, %s),
            tss_compliance = COALESCE(tss_compliance, %s),
            nitrogen_compliance = COALESCE(nitrogen_compliance, %s),
            phosphorus_compliance = COALESCE(phosphorus_compliance, %s),
            other_compliance = COALESCE(other_compliance, %s)
        WHERE uwwtp_code = %s
    """
    
    execute_batch(cur, sql, updates, page_size=1000)
    print(f'  ✓ Updated compliance fields')


def fill_art17_text_fields(cur):
    """填充 Article 17 文本字段"""
    print('\n[2/2] Filling Article 17 text fields...')
    
    data = read_csv(os.path.join(DATA_DIR, 'art17_investments.csv'))
    print(f'  Art17 CSV records: {len(data)}')
    
    # 检查有多少数据
    for field in ['flatpMeasures', 'flatpReasons']:
        count = sum(1 for r in data if r.get(field))
        print(f'    {field}: {count} records')
    
    # 构建更新
    updates = []
    for row in data:
        uwwtp_code = clean(row.get('uwwCode'))
        if not uwwtp_code:
            continue
        
        measures = clean(row.get('flatpMeasures'))  # investment_planned (now text)
        reasons = clean(row.get('flatpReasons'))    # investment_need (now text)
        
        if measures or reasons:
            updates.append((measures, reasons, uwwtp_code))
    
    print(f'  Records to update: {len(updates)}')
    
    if updates:
        sql = """
            UPDATE plants SET
                article17_investment_planned = COALESCE(article17_investment_planned, %s),
                article17_investment_need = COALESCE(article17_investment_need, %s)
            WHERE uwwtp_code = %s
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'  ✓ Updated Article 17 text fields')


def main():
    print('=' * 60)
    print('Fill New Schema Fields')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    fill_compliance_fields(cur)
    fill_art17_text_fields(cur)
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Fill completed!')


if __name__ == '__main__':
    main()
