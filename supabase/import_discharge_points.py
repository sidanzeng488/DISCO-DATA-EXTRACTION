"""
从 DISCODATA 导入排放点数据到 Supabase
自动判断 SWB/GWB 关联
"""
import sys
sys.path.append('..')

import psycopg2
from config import (
    SUPABASE_HOST, 
    SUPABASE_PORT, 
    SUPABASE_DATABASE, 
    SUPABASE_USER, 
    SUPABASE_PASSWORD
)
from discodata_client import DiscoDataClient

def get_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )

def import_discharge_points(client, cur, limit=None):
    """导入排放点数据"""
    print('\n[DCP] Fetching Discharge Points data...')
    
    query = """
        SELECT dcpCode, dcpName, uwwCode, CountryCode,
               dcpLatitude, dcpLongitude,
               dcpSurfaceWaters, dcpWaterBodyType,
               dcpWaterbodyID, dcpGroundWater,
               dcpReceivingWater, dcpWFDRBD, dcpWFDSubUnit, repCode
        FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    """
    
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    data = client.query(query, max_records=limit)
    print(f'  Fetched {len(data)} discharge point records')
    
    inserted = 0
    skipped_no_water_body = 0
    
    for row in data:
        # 判断是地表水还是地下水
        is_surface_water = row.get('dcpSurfaceWaters') == True or row.get('dcpSurfaceWaters') == 'True'
        
        # 确定水体代码
        if is_surface_water:
            water_body_code = row.get('dcpWaterbodyID')
        else:
            water_body_code = row.get('dcpGroundWater')
        
        try:
            cur.execute("""
                INSERT INTO discharge_points (
                    dcp_code, dcp_name, plant_code, country_code,
                    latitude, longitude,
                    is_surface_water, water_body_type, water_body_code,
                    receiving_water, wfd_rbd, wfd_sub_unit, rep_code
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dcp_code) DO NOTHING
            """, (
                row.get('dcpCode'),
                row.get('dcpName'),
                row.get('uwwCode'),
                row.get('CountryCode'),
                row.get('dcpLatitude'),
                row.get('dcpLongitude'),
                is_surface_water,
                row.get('dcpWaterBodyType'),
                water_body_code,
                row.get('dcpReceivingWater'),
                row.get('dcpWFDRBD'),
                row.get('dcpWFDSubUnit'),
                row.get('repCode')
            ))
            inserted += 1
        except Exception as e:
            print(f'  Error inserting DCP {row.get("dcpCode")}: {e}')
    
    print(f'  ✓ Inserted {inserted} discharge point records')
    return inserted

if __name__ == '__main__':
    print('=' * 50)
    print('Import Discharge Points from DISCODATA to Supabase')
    print('=' * 50)
    
    # 可选：设置导入限制（用于测试）
    LIMIT = 100  # 设为 None 导入全部数据
    
    disco_client = DiscoDataClient()
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        count = import_discharge_points(disco_client, cur, limit=LIMIT)
        
        print('\n' + '=' * 50)
        print(f'✅ Import completed!')
        print(f'   Discharge Points: {count} records')
        
    finally:
        cur.close()
        conn.close()
