"""
根据 plant_agglo_links.csv 链接 plants 和 agglomeration
更新 plants.agglomeration_id
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


def main():
    print('=' * 60)
    print('Link Plants to Agglomerations')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # 读取链接数据
    links = read_csv(os.path.join(DATA_DIR, 'plant_agglo_links.csv'))
    print(f'\nLink records: {len(links)}')
    
    if not links:
        print('No link data found!')
        return
    
    # 显示样本
    print(f'Sample: aucUwwCode={links[0].get("aucUwwCode")}, aucAggCode={links[0].get("aucAggCode")}')
    
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # 获取 agglomeration 的 agg_code -> agglomeration_id 映射
    print('\nBuilding agglomeration lookup...')
    cur.execute('SELECT agg_code, agglomeration_id FROM agglomeration WHERE agg_code IS NOT NULL')
    agg_lookup = {row[0]: row[1] for row in cur.fetchall()}
    print(f'  Agglomerations in DB: {len(agg_lookup)}')
    
    # 构建更新数据
    updates = []
    not_found_agg = set()
    
    for link in links:
        uwwtp_code = clean(link.get('aucUwwCode'))
        agg_code = clean(link.get('aucAggCode'))
        
        if not uwwtp_code or not agg_code:
            continue
        
        agg_id = agg_lookup.get(agg_code)
        if agg_id:
            updates.append((agg_id, uwwtp_code))
        else:
            not_found_agg.add(agg_code)
    
    print(f'\nLinks to update: {len(updates)}')
    print(f'Agglomeration codes not found in DB: {len(not_found_agg)}')
    if not_found_agg and len(not_found_agg) <= 10:
        print(f'  Missing codes: {list(not_found_agg)[:10]}')
    
    if updates:
        sql = """
            UPDATE plants 
            SET agglomeration_id = %s 
            WHERE uwwtp_code = %s AND agglomeration_id IS NULL
        """
        execute_batch(cur, sql, updates, page_size=1000)
        print(f'\n✓ Updated agglomeration_id for plants')
    
    # 验证
    cur.execute('SELECT COUNT(*) FROM plants WHERE agglomeration_id IS NOT NULL')
    linked_count = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM plants')
    total_count = cur.fetchone()[0]
    print(f'\nResult: {linked_count}/{total_count} plants now have agglomeration_id ({linked_count*100/total_count:.1f}%)')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('✅ Linking completed!')


if __name__ == '__main__':
    main()
