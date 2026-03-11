"""
Add dilution field to plants table from GWI Excel file
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

EXCEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'GWI', 'micropollutant costings EU data - Lara.xlsx')


def main():
    print('=' * 60)
    print('Add dilution to plants table')
    print('Source: GWI micropollutant costings EU data')
    print('=' * 60)
    
    # 1. Read Excel
    print('\n[1/5] Reading Excel file...')
    df = pd.read_excel(EXCEL_PATH)
    print(f'  Rows: {len(df)}')
    
    # Get dilution data
    dilution_col = 'Dilution '
    dilution_data = df[['uwwCode', dilution_col]].dropna()
    print(f'  Records with dilution: {len(dilution_data)}')
    
    # Connect to database
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    # 2. Check if column exists
    print('\n[2/5] Checking if dilution column exists...')
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'plants' AND column_name = 'dilution'
    """)
    exists = cur.fetchone()
    
    if exists:
        print('  [OK] Column already exists')
    else:
        print('  Adding column...')
        cur.execute("ALTER TABLE plants ADD COLUMN dilution NUMERIC")
        conn.commit()
        print('  [OK] Column added')
    
    # 3. Add comment
    print('\n[3/5] Adding column comment...')
    cur.execute("""
        COMMENT ON COLUMN plants.dilution IS 
        'Dilution ratio at discharge point - ratio of receiving water flow to effluent flow. Source: GWI micropollutant costings EU data (Lara). Higher values indicate better dilution capacity.'
    """)
    conn.commit()
    print('  [OK] Comment added')
    
    # 4. Build updates
    print('\n[4/5] Preparing updates...')
    updates = []
    for _, row in dilution_data.iterrows():
        uwwtp_code = row['uwwCode']
        dilution = row[dilution_col]
        if pd.notna(dilution):
            updates.append((float(dilution), uwwtp_code))
    
    print(f'  Updates to apply: {len(updates)}')
    
    # 5. Update database
    print('\n[5/5] Updating database...')
    query = """
        UPDATE plants 
        SET dilution = %s 
        WHERE uwwtp_code = %s
    """
    execute_batch(cur, query, updates, page_size=1000)
    conn.commit()
    
    # Count actual updates
    cur.execute("SELECT COUNT(*) FROM plants WHERE dilution IS NOT NULL")
    updated_count = cur.fetchone()[0]
    print(f'  [OK] Plants with dilution: {updated_count}')
    
    # Verification
    print('\n' + '=' * 60)
    print('Verification:')
    print('=' * 60)
    
    cur.execute("""
        SELECT 
            MIN(dilution) as min_val,
            MAX(dilution) as max_val,
            AVG(dilution) as avg_val,
            COUNT(*) as count
        FROM plants 
        WHERE dilution IS NOT NULL
    """)
    row = cur.fetchone()
    print(f'  Min: {row[0]:.2f}')
    print(f'  Max: {row[1]:.2f}')
    print(f'  Avg: {row[2]:.2f}')
    print(f'  Count: {row[3]}')
    
    # Sample
    print('\nSample plants with dilution:')
    cur.execute("""
        SELECT uwwtp_code, plant_name, dilution
        FROM plants 
        WHERE dilution IS NOT NULL
        ORDER BY dilution DESC
        LIMIT 10
    """)
    for row in cur.fetchall():
        name = row[1][:30] if row[1] else ''
        print(f'  {row[0]}: {name} - dilution: {row[2]:.2f}')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('[DONE] Completed!')
    print('=' * 60)


if __name__ == '__main__':
    main()
