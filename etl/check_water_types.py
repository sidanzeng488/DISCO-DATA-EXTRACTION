"""检查地表水和地下水的分布"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

print("=== discharge_points: Surface Water vs Groundwater ===")
cur.execute("""
    SELECT 
        is_surface_water,
        COUNT(*) as total,
        COUNT(water_body_code) as with_wb
    FROM discharge_points 
    GROUP BY is_surface_water
""")
for row in cur.fetchall():
    if row[0] == True:
        surface = "Surface Water (SWB)"
    elif row[0] == False:
        surface = "Groundwater (GWB)"
    else:
        surface = "NULL/Unknown"
    print(f"  {surface}: {row[1]} records, {row[2]} with water_body_code")

print("\n=== water_bodies table by type ===")
cur.execute("SELECT water_type, COUNT(*) FROM water_bodies GROUP BY water_type")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

print("\n=== discharge_points linked to which water_body type ===")
cur.execute("""
    SELECT wb.water_type, COUNT(DISTINCT dp.dcp_id)
    FROM discharge_points dp
    JOIN water_bodies wb ON dp.water_body_code = wb.eu_water_body_code
    GROUP BY wb.water_type
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} discharge points")

print("\n=== Plants by receiving water type ===")
cur.execute("""
    SELECT 
        CASE 
            WHEN dp.is_surface_water = true THEN 'Surface Water'
            WHEN dp.is_surface_water = false THEN 'Groundwater'
            ELSE 'Unknown'
        END as water_type,
        COUNT(DISTINCT p.uwwtp_code) as plant_count
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    GROUP BY dp.is_surface_water
""")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} plants")

conn.close()
