"""检查 funding 字段的填充情况"""
import psycopg2
import csv
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

CURRENT_DIR = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')

def main():
    # 1. 检查数据库
    print("=" * 70)
    print("Database funding fields")
    print("=" * 70)
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE,
        user=SUPABASE_USER, password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    cur.execute('SELECT COUNT(*) FROM plants')
    total = cur.fetchone()[0]
    print(f'Total plants: {total}\n')
    
    funding_fields = ['eu_funding_scheme', 'eu_funding_amount', 'other_funding_scheme', 'other_funding_amount']
    
    for field in funding_fields:
        cur.execute(f'SELECT COUNT(*) FROM plants WHERE {field} IS NOT NULL')
        filled = cur.fetchone()[0]
        print(f'{field}: {filled}/{total} ({filled/total*100:.1f}%)')
    
    print('\nSample EU funding data from DB:')
    cur.execute('''
        SELECT uwwtp_code, eu_funding_scheme, eu_funding_amount 
        FROM plants 
        WHERE eu_funding_scheme IS NOT NULL OR eu_funding_amount IS NOT NULL
        LIMIT 10
    ''')
    for row in cur.fetchall():
        print(f'  {row[0]}: scheme={row[1]}, amount={row[2]}')
    
    # 2. 检查 CSV 源数据
    print("\n" + "=" * 70)
    print("CSV source data (art17_investments.csv)")
    print("=" * 70)
    
    csv_path = os.path.join(CURRENT_DIR, 'art17_investments.csv')
    with open(csv_path, 'r', encoding='utf-8') as f:
        data = list(csv.DictReader(f))
    
    print(f'Total records: {len(data)}\n')
    
    csv_funding_fields = [
        ('flatpEUFundName', 'eu_funding_scheme'),
        ('flatpEUFund', 'eu_funding_amount'),
        ('flatpOtherFundName', 'other_funding_scheme'),
        ('flatpOtherFund', 'other_funding_amount'),
    ]
    
    for csv_field, db_field in csv_funding_fields:
        non_empty = sum(1 for row in data if row.get(csv_field) and row.get(csv_field).strip())
        print(f'{csv_field} -> {db_field}: {non_empty}/{len(data)} ({non_empty/len(data)*100:.1f}%)')
    
    # 3. 检查映射是否正确
    print("\n" + "=" * 70)
    print("Sample raw values from CSV")
    print("=" * 70)
    
    print('\nflatpEUFund (should be numeric):')
    count = 0
    for row in data:
        val = row.get('flatpEUFund', '')
        if val and val.strip():
            print(f'  "{val}"')
            count += 1
            if count >= 5:
                break
    
    print('\nflatpEUFundName (should be text):')
    count = 0
    for row in data:
        val = row.get('flatpEUFundName', '')
        if val and val.strip():
            print(f'  "{val[:80]}..."' if len(val) > 80 else f'  "{val}"')
            count += 1
            if count >= 5:
                break
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
