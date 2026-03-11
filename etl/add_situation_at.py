"""
Add situation_at field to report_code table
"""
import psycopg2
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')


def parse_date(value):
    """Parse date string to date object"""
    if not value or value in ['', 'NULL']:
        return None
    # Handle ISO format: 2022-12-31T00:00:00
    if 'T' in value:
        value = value.split('T')[0]
    return value


def main():
    print('=' * 60)
    print('Add situation_at to report_code table')
    print('=' * 60)
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    # 1. Check if column exists
    print('\n[1/4] Checking if situation_at column exists...')
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'report_code' AND column_name = 'situation_at'
    """)
    exists = cur.fetchone()
    
    if exists:
        print('  [OK] Column already exists')
    else:
        print('  Adding column...')
        cur.execute("ALTER TABLE report_code ADD COLUMN situation_at DATE")
        conn.commit()
        print('  [OK] Column added')
    
    # 2. Add comment
    print('\n[2/4] Adding column comment...')
    cur.execute("""
        COMMENT ON COLUMN report_code.situation_at IS 
        'Data situation date - the reference date for the reported data (repSituationAt from T_ReportPeriod)'
    """)
    conn.commit()
    print('  [OK] Comment added')
    
    # 3. Read CSV and update
    print('\n[3/4] Reading report_periods.csv...')
    csv_path = os.path.join(DATA_DIR, 'report_periods.csv')
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f'  Records: {len(rows)}')
    
    # Build updates
    updates = []
    for row in rows:
        rep_code = row.get('repCode', '').strip()
        situation_at = parse_date(row.get('repSituationAt'))
        if rep_code and situation_at:
            updates.append((situation_at, rep_code))
    
    print(f'  Updates to apply: {len(updates)}')
    
    # 4. Update database
    print('\n[4/4] Updating database...')
    updated = 0
    for situation_at, rep_code in updates:
        cur.execute("""
            UPDATE report_code 
            SET situation_at = %s 
            WHERE rep_code = %s
        """, (situation_at, rep_code))
        updated += cur.rowcount
    
    conn.commit()
    print(f'  [OK] Updated {updated} records')
    
    # Verify
    print('\n' + '=' * 60)
    print('Verification:')
    print('=' * 60)
    cur.execute("""
        SELECT rep_code, country_code, year, version, situation_at 
        FROM report_code 
        ORDER BY country_code
        LIMIT 10
    """)
    
    print('{:<25} {:<6} {:<6} {:<12} {:<12}'.format('rep_code', 'cc', 'year', 'version', 'situation_at'))
    print('-' * 65)
    for row in cur.fetchall():
        version_short = str(row[3])[:10] if row[3] else ''
        sit_at = str(row[4]) if row[4] else ''
        print('{:<25} {:<6} {:<6} {:<12} {:<12}'.format(row[0][:24], row[1], row[2] or '', version_short, sit_at))
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('[DONE] Completed!')
    print('=' * 60)


if __name__ == '__main__':
    main()
