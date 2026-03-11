"""
Add uwwSpecification field to plants table
"""
import psycopg2
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')


def clean_value(value):
    if value is None or value in ['', 'NULL', 'null']:
        return None
    return value.strip()


def main():
    print('=' * 60)
    print('Add uwwSpecification to plants table')
    print('=' * 60)
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    # 1. Check if column exists
    print('\n[1/4] Checking if specification column exists...')
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'plants' AND column_name = 'specification'
    """)
    exists = cur.fetchone()
    
    if exists:
        print('  [OK] Column already exists')
    else:
        print('  Adding column...')
        cur.execute("ALTER TABLE plants ADD COLUMN specification TEXT")
        conn.commit()
        print('  [OK] Column added')
    
    # 2. Add comment
    print('\n[2/4] Adding column comment...')
    cur.execute("""
        COMMENT ON COLUMN plants.specification IS 
        'Additional specification of other treatment types (uwwSpecification from T_UWWTPS) - e.g. Nitrification, MBR, Peracetic acid disinfection'
    """)
    conn.commit()
    print('  [OK] Comment added')
    
    # 3. Read CSV
    print('\n[3/4] Reading plants.csv...')
    csv_path = os.path.join(DATA_DIR, 'plants.csv')
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f'  Records: {len(rows)}')
    
    # Build updates
    updates = []
    for row in rows:
        uwwtp_code = clean_value(row.get('uwwCode'))
        specification = clean_value(row.get('uwwSpecification'))
        if uwwtp_code and specification:
            updates.append((specification, uwwtp_code))
    
    print(f'  Records with specification: {len(updates)}')
    
    # 4. Update database
    print('\n[4/4] Updating database...')
    from psycopg2.extras import execute_batch
    
    query = """
        UPDATE plants 
        SET specification = %s 
        WHERE uwwtp_code = %s
    """
    execute_batch(cur, query, updates, page_size=1000)
    conn.commit()
    print(f'  [OK] Updated {len(updates)} records')
    
    # Verify
    print('\n' + '=' * 60)
    print('Verification - Top specifications:')
    print('=' * 60)
    cur.execute("""
        SELECT specification, COUNT(*) as cnt
        FROM plants 
        WHERE specification IS NOT NULL
        GROUP BY specification
        ORDER BY cnt DESC
        LIMIT 15
    """)
    
    print('{:<50} {:<8}'.format('Specification', 'Count'))
    print('-' * 60)
    for row in cur.fetchall():
        spec = row[0][:48] + '..' if len(row[0]) > 48 else row[0]
        print('{:<50} {:<8}'.format(spec, row[1]))
    
    # Total count
    cur.execute("SELECT COUNT(*) FROM plants WHERE specification IS NOT NULL")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM plants")
    all_plants = cur.fetchone()[0]
    print(f'\nTotal with specification: {total}/{all_plants} ({total/all_plants*100:.1f}%)')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('[DONE] Completed!')
    print('=' * 60)


if __name__ == '__main__':
    main()
