"""
Export plants by agglomeration statistics to CSV files
"""
import sys
import os
import csv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
import psycopg2

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')

print("=" * 60)
print("Export Plants by Agglomeration Statistics")
print("=" * 60)

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# Check what tables exist
print("\n1. Checking database tables...")
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name
""")
tables = [row[0] for row in cur.fetchall()]
print(f"   Found tables: {', '.join(tables)}")

# Check if plants has agg_code
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'plants' 
    AND column_name LIKE '%agg%'
""")
agg_cols = [row[0] for row in cur.fetchall()]
print(f"   Plants agg columns: {agg_cols}")

# 1. Detail: each plant with its agglomeration
print("\n2. Generating plant-agglomeration detail...")
cur.execute("""
    SELECT 
        p.uwwtp_code,
        p.plant_name,
        p.country_code,
        p.plant_capacity,
        p.plant_waste_load_pe,
        a.agg_code,
        a.agglomeration_name,
        a.agg_generated,
        p.rep_code
    FROM plants p
    LEFT JOIN agglomeration a ON p.agglomeration_id = a.agglomeration_id
    ORDER BY p.country_code, a.agg_code, p.uwwtp_code
""")
detail_rows = cur.fetchall()
detail_headers = ['uwwtp_code', 'plant_name', 'country_code', 'plant_capacity', 'plant_waste_load_pe', 'agg_code', 'agglomeration_name', 'agg_generated', 'rep_code']

detail_file = os.path.join(OUTPUT_DIR, 'plants_by_agglomeration_detail.csv')
with open(detail_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(detail_headers)
    writer.writerows(detail_rows)
print(f"   Written {len(detail_rows)} rows to plants_by_agglomeration_detail.csv")

# 2. Summary: count of plants per agglomeration
print("\n3. Generating agglomeration summary...")
cur.execute("""
    SELECT 
        COALESCE(a.agg_code, 'NO_AGG') as agg_code,
        a.agglomeration_name,
        p.country_code,
        a.agg_generated,
        COUNT(*) as plant_count,
        SUM(p.plant_capacity) as total_plant_capacity,
        SUM(p.plant_waste_load_pe) as total_waste_load_pe,
        AVG(p.plant_capacity) as avg_plant_capacity
    FROM plants p
    LEFT JOIN agglomeration a ON p.agglomeration_id = a.agglomeration_id
    GROUP BY a.agg_code, a.agglomeration_name, p.country_code, a.agg_generated
    ORDER BY plant_count DESC, p.country_code, a.agg_code
""")
summary_rows = cur.fetchall()
summary_headers = ['agg_code', 'agglomeration_name', 'country_code', 'agg_generated', 'plant_count', 'total_plant_capacity', 'total_waste_load_pe', 'avg_plant_capacity']

summary_file = os.path.join(OUTPUT_DIR, 'plants_by_agglomeration_summary.csv')
with open(summary_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(summary_headers)
    writer.writerows(summary_rows)
print(f"   Written {len(summary_rows)} rows to plants_by_agglomeration_summary.csv")

# 3. Country summary
print("\n4. Generating country summary...")
cur.execute("""
    SELECT 
        p.country_code,
        COUNT(DISTINCT a.agg_code) as agglomeration_count,
        COUNT(*) as plant_count,
        SUM(p.plant_capacity) as total_plant_capacity,
        SUM(p.plant_waste_load_pe) as total_waste_load_pe
    FROM plants p
    LEFT JOIN agglomeration a ON p.agglomeration_id = a.agglomeration_id
    GROUP BY p.country_code
    ORDER BY plant_count DESC
""")
country_rows = cur.fetchall()
country_headers = ['country_code', 'agglomeration_count', 'plant_count', 'total_plant_capacity', 'total_waste_load_pe']

country_file = os.path.join(OUTPUT_DIR, 'plants_by_country_summary.csv')
with open(country_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(country_headers)
    writer.writerows(country_rows)
print(f"   Written {len(country_rows)} rows to plants_by_country_summary.csv")

# 4. Show sample data
print("\n5. Sample data (top 10 agglomerations by plant count)...")
cur.execute("""
    SELECT 
        COALESCE(a.agg_code, 'NO_AGG') as agg_code,
        p.country_code,
        COUNT(*) as plant_count
    FROM plants p
    LEFT JOIN agglomeration a ON p.agglomeration_id = a.agglomeration_id
    GROUP BY a.agg_code, p.country_code
    ORDER BY plant_count DESC
    LIMIT 10
""")
print("   Agg Code | Country | Plant Count")
print("   " + "-" * 40)
for row in cur.fetchall():
    print(f"   {row[0][:30]:30} | {row[1]:7} | {row[2]}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done! Files saved to DATA/current/")
print("=" * 60)
