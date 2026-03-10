"""
Import country data from ISO country code CSV
"""
import sys
import os
import csv
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_values
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD


def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST, port=SUPABASE_PORT,
        database=SUPABASE_DATABASE, user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )


def clean(value):
    if value is None or value == '' or value == '""':
        return None
    # Remove surrounding quotes if present
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    return value


def main():
    print('=' * 60)
    print('Import Country Data')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # Read CSV file
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'country code.csv')
    print(f'\nReading: {csv_path}')
    
    countries = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            countries.append({
                'country_name': clean(row.get('name')),
                'country_code': clean(row.get('alpha-2')),
            })
    
    print(f'Total countries in CSV: {len(countries)}')
    
    # Connect to database
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    print('Connected to Supabase')
    
    # Check table structure
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'country' 
        ORDER BY ordinal_position
    """)
    columns = cur.fetchall()
    print(f'\nCountry table structure:')
    for col_name, col_type in columns:
        print(f'  {col_name}: {col_type}')
    
    # Check existing data
    cur.execute('SELECT COUNT(*) FROM country')
    existing_count = cur.fetchone()[0]
    print(f'\nExisting rows: {existing_count}')
    
    # Get existing country codes
    cur.execute('SELECT country_code FROM country')
    existing_codes = {row[0] for row in cur.fetchall()}
    
    # Filter out duplicates
    new_countries = [c for c in countries if c['country_code'] not in existing_codes]
    print(f'New countries to insert: {len(new_countries)}')
    
    if not new_countries:
        print('No new countries to insert.')
        cur.close()
        conn.close()
        return
    
    # Prepare values for batch insert
    values = [
        (
            c['country_name'],
            c['country_code'],
        )
        for c in new_countries
    ]
    
    # Insert data
    query = """
        INSERT INTO country (country_name, country_code)
        VALUES %s
        ON CONFLICT (country_code) DO UPDATE SET
            country_name = EXCLUDED.country_name
    """
    
    execute_values(cur, query, values, page_size=100)
    print(f'\n✅ Inserted {len(new_countries)} countries')
    
    # Verify
    cur.execute('SELECT COUNT(*) FROM country')
    final_count = cur.fetchone()[0]
    print(f'Final row count: {final_count}')
    
    cur.close()
    conn.close()
    print('\nDone!')


if __name__ == '__main__':
    main()
