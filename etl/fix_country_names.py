"""Fix NULL country names for EU countries"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from supabase.config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

# EU country codes mapping (including EL for Greece which is different from ISO GR)
EU_COUNTRIES = {
    'AT': 'Austria',
    'BE': 'Belgium',
    'BG': 'Bulgaria',
    'CH': 'Switzerland',
    'CY': 'Cyprus',
    'CZ': 'Czechia',
    'DE': 'Germany',
    'DK': 'Denmark',
    'EE': 'Estonia',
    'EL': 'Greece',  # EL is EU code, GR is ISO code
    'ES': 'Spain',
    'FI': 'Finland',
    'FR': 'France',
    'HR': 'Croatia',
    'HU': 'Hungary',
    'IE': 'Ireland',
    'IS': 'Iceland',
    'IT': 'Italy',
    'LT': 'Lithuania',
    'LU': 'Luxembourg',
    'LV': 'Latvia',
    'MT': 'Malta',
    'NL': 'Netherlands',
    'NO': 'Norway',
    'PL': 'Poland',
    'PT': 'Portugal',
    'RO': 'Romania',
    'SE': 'Sweden',
    'SI': 'Slovenia',
    'SK': 'Slovakia',
}

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
conn.autocommit = True
cur = conn.cursor()

print('Updating EU country names...\n')

updated = 0
for code, name in EU_COUNTRIES.items():
    cur.execute('UPDATE country SET country_name = %s WHERE country_code = %s AND country_name IS NULL', (name, code))
    if cur.rowcount > 0:
        print(f'  ✓ {code}: {name}')
        updated += 1

print(f'\nUpdated: {updated} countries')

# Verify
cur.execute('SELECT COUNT(*) FROM country WHERE country_name IS NULL')
remaining_nulls = cur.fetchone()[0]
print(f'Remaining NULL names: {remaining_nulls}')

cur.close()
conn.close()
