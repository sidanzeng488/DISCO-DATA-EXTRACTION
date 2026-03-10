"""
应用 feilds mapping with DISCO.xlsx 的完整修复
1. 添加缺失的日期字段
2. 填充 date_published 和 date_situation_at
3. 计算 removal %
"""
import psycopg2
import csv
import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
from psycopg2.extras import execute_batch

CURRENT_DIR = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')

def read_csv(filepath):
    with open(filepath, mode='r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def clean_value(val):
    if val is None or val == '' or val == 'NULL':
        return None
    return str(val).strip()

def parse_date(val):
    if not val or val == '' or val == 'NULL':
        return None
    try:
        if 'T' in str(val):
            return datetime.fromisoformat(val.replace('T00:00:00', '')).date()
        return datetime.strptime(val, '%Y-%m-%d').date()
    except:
        return None

def get_row_count(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]

def main():
    print("=" * 70)
    print("Apply Mapping Fix - Based on feilds mapping with DISCO.xlsx")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    print("Connected to Supabase\n")
    
    total = get_row_count(cur, 'plants')
    
    # ================================================================
    # Step 1: Add missing columns
    # ================================================================
    print("=" * 70)
    print("Step 1: Add missing date columns")
    print("=" * 70)
    
    try:
        cur.execute("ALTER TABLE plants ADD COLUMN IF NOT EXISTS date_published DATE")
        cur.execute("ALTER TABLE plants ADD COLUMN IF NOT EXISTS date_situation_at DATE")
        conn.commit()
        print("✓ Columns added (or already exist)")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()
    
    # ================================================================
    # Step 2: Fill date_published and date_situation_at
    # ================================================================
    print("\n" + "=" * 70)
    print("Step 2: Fill date_published and date_situation_at via rep_code")
    print("=" * 70)
    
    # Read report_periods.csv
    report_periods_path = os.path.join(CURRENT_DIR, 'report_periods.csv')
    report_data = read_csv(report_periods_path)
    print(f"report_periods.csv: {len(report_data)} records")
    
    # Build mapping: rep_code -> (date_published, date_situation_at)
    rep_code_dates = {}
    for row in report_data:
        rep_code = clean_value(row.get('repCode'))
        rep_version = parse_date(row.get('repVersion'))
        rep_situation_at = parse_date(row.get('repSituationAt'))
        if rep_code:
            rep_code_dates[rep_code] = (rep_version, rep_situation_at)
    
    print(f"rep_code mappings: {len(rep_code_dates)}")
    
    # Get distinct rep_codes from plants
    cur.execute("SELECT DISTINCT rep_code FROM plants WHERE rep_code IS NOT NULL")
    plant_rep_codes = [r[0] for r in cur.fetchall()]
    print(f"plants with rep_code: {len(plant_rep_codes)} unique codes")
    
    # Update plants
    updates = []
    for rep_code in plant_rep_codes:
        if rep_code in rep_code_dates:
            date_pub, date_sit = rep_code_dates[rep_code]
            if date_pub or date_sit:
                updates.append((date_pub, date_sit, rep_code))
    
    print(f"Updates to apply: {len(updates)}")
    
    if updates:
        query = """
            UPDATE plants 
            SET date_published = %s, date_situation_at = %s
            WHERE rep_code = %s
        """
        execute_batch(cur, query, updates, page_size=100)
        conn.commit()
        print("✓ Date fields updated")
    
    # ================================================================
    # Step 3: Calculate removal percentages
    # ================================================================
    print("\n" + "=" * 70)
    print("Step 3: Calculate removal percentages")
    print("=" * 70)
    
    calculations = [
        ('bod_removal_pct', 'bod_incoming_measured', 'bod_outgoing_measured'),
        ('cod_removal_pct', 'cod_incoming_measured', 'cod_outgoing_measured'),
        ('nitrogen_removal_pct', 'nitrogen_incoming_measured', 'nitrogen_outgoing_measured'),
        ('phosphorus_removal_pct', 'phosphorus_incoming_measured', 'phosphorus_outgoing_measured'),
    ]
    
    for target, incoming, outgoing in calculations:
        sql = f"""
            UPDATE plants 
            SET {target} = 
                CASE 
                    WHEN {incoming} > 0 AND {outgoing} IS NOT NULL 
                    THEN ROUND((({incoming} - {outgoing}) / {incoming}) * 100, 2)
                    ELSE NULL 
                END
            WHERE {incoming} IS NOT NULL AND {outgoing} IS NOT NULL
        """
        cur.execute(sql)
        updated = cur.rowcount
        print(f"  {target}: {updated} rows calculated")
    
    conn.commit()
    print("✓ Removal percentages calculated")
    
    # ================================================================
    # Step 4: Verify results
    # ================================================================
    print("\n" + "=" * 70)
    print("Step 4: Verify results")
    print("=" * 70)
    
    fields_to_check = [
        'date_published',
        'date_situation_at',
        'bod_removal_pct',
        'cod_removal_pct',
        'nitrogen_removal_pct',
        'phosphorus_removal_pct',
    ]
    
    print(f"\nTotal plants: {total}")
    print(f"\n{'Field':<30} {'Filled':<10} {'Percentage':<10}")
    print("-" * 55)
    
    for field in fields_to_check:
        cur.execute(f"SELECT COUNT(*) FROM plants WHERE {field} IS NOT NULL")
        filled = cur.fetchone()[0]
        pct = filled / total * 100
        status = "✓" if pct > 20 else "○"
        print(f"{status} {field:<28} {filled:<10} {pct:5.1f}%")
    
    print("\n" + "=" * 70)
    print("✅ All fixes applied!")
    print("=" * 70)
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
