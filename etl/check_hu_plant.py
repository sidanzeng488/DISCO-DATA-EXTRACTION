"""Check HU-WWTP-AHZ861"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD
)
cur = conn.cursor()

code = 'HU-WWTP-AHZ861'

# Check as uwwtp_code
cur.execute("""
    SELECT uwwtp_code, plant_name, country_code, rep_code, date_published, date_situation_at
    FROM plants 
    WHERE uwwtp_code = %s
""", (code,))
row = cur.fetchone()

if row:
    print(f'Found in plants table as uwwtp_code:')
    print(f'  uwwtp_code: {row[0]}')
    print(f'  plant_name: {row[1]}')
    print(f'  country_code: {row[2]}')
    print(f'  rep_code: {row[3]}')
    print(f'  date_published: {row[4]}')
    print(f'  date_situation_at: {row[5]}')
else:
    print(f'{code} not found as uwwtp_code')
    
    # Check as rep_code
    cur.execute("SELECT COUNT(*) FROM plants WHERE rep_code = %s", (code,))
    count = cur.fetchone()[0]
    if count > 0:
        print(f'Found as rep_code: {count} plants use this rep_code')
    else:
        print('Not found as rep_code either')

# Show all HU rep_codes
print('\n--- All HU rep_codes in report_periods ---')
cur.execute("SELECT DISTINCT rep_code FROM plants WHERE country_code = 'HU' LIMIT 10")
for r in cur.fetchall():
    print(f'  {r[0]}')

cur.close()
conn.close()
