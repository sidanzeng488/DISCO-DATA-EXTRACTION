"""
从 DISCODATA 导入水体数据到 Supabase
合并 SWB 和 GWB 为统一表
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

def import_swb_data(client, cur, limit=None):
    """导入地表水体数据"""
    print('\n[SWB] Fetching Surface Water Body data...')
    
    query = """
        SELECT euSurfaceWaterBodyCode, surfaceWaterBodyName, countryCode,
               euRBDCode, rbdName, euSubUnitCode, subUnitName, cYear,
               surfaceWaterBodyCategory, naturalAWBHMWB, cArea, cLength,
               swEcologicalStatusOrPotentialValue, swEcologicalAssessmentYear,
               swChemicalStatusValue, swChemicalAssessmentYear, fileUrl
        FROM [WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]
    """
    
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    data = client.query(query, max_records=limit)
    print(f'  Fetched {len(data)} SWB records')
    
    inserted = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO water_bodies (
                    eu_water_body_code, water_type, water_body_name, country_code,
                    eu_rbd_code, rbd_name, eu_sub_unit_code, sub_unit_name, c_year,
                    surface_water_category, natural_awb_hmwb, c_area, c_length,
                    sw_ecological_status, sw_ecological_assessment_year,
                    sw_chemical_status, sw_chemical_assessment_year, file_url
                ) VALUES (%s, 'SWB', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (eu_water_body_code) DO NOTHING
            """, (
                row.get('euSurfaceWaterBodyCode'),
                row.get('surfaceWaterBodyName'),
                row.get('countryCode'),
                row.get('euRBDCode'),
                row.get('rbdName'),
                row.get('euSubUnitCode'),
                row.get('subUnitName'),
                row.get('cYear'),
                row.get('surfaceWaterBodyCategory'),
                row.get('naturalAWBHMWB'),
                row.get('cArea'),
                row.get('cLength'),
                row.get('swEcologicalStatusOrPotentialValue'),
                row.get('swEcologicalAssessmentYear'),
                row.get('swChemicalStatusValue'),
                row.get('swChemicalAssessmentYear'),
                row.get('fileUrl')
            ))
            inserted += 1
        except Exception as e:
            print(f'  Error inserting SWB: {e}')
    
    print(f'  ✓ Inserted {inserted} SWB records')
    return inserted

def import_gwb_data(client, cur, limit=None):
    """导入地下水体数据"""
    print('\n[GWB] Fetching Groundwater Body data...')
    
    query = """
        SELECT euGroundWaterBodyCode, countryCode,
               euRBDCode, rbdName, cYear,
               gwQuantitativeStatusValue, gwQuantitativeAssessmentYear,
               gwChemicalStatusValue, gwChemicalAssessmentYear, fileUrl
        FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody]
    """
    
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    data = client.query(query, max_records=limit)
    print(f'  Fetched {len(data)} GWB records')
    
    inserted = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO water_bodies (
                    eu_water_body_code, water_type, country_code,
                    eu_rbd_code, rbd_name, c_year,
                    gw_quantitative_status, gw_quantitative_assessment_year,
                    gw_chemical_status, gw_chemical_assessment_year, file_url
                ) VALUES (%s, 'GWB', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (eu_water_body_code) DO NOTHING
            """, (
                row.get('euGroundWaterBodyCode'),
                row.get('countryCode'),
                row.get('euRBDCode'),
                row.get('rbdName'),
                row.get('cYear'),
                row.get('gwQuantitativeStatusValue'),
                row.get('gwQuantitativeAssessmentYear'),
                row.get('gwChemicalStatusValue'),
                row.get('gwChemicalAssessmentYear'),
                row.get('fileUrl')
            ))
            inserted += 1
        except Exception as e:
            print(f'  Error inserting GWB: {e}')
    
    print(f'  ✓ Inserted {inserted} GWB records')
    return inserted

if __name__ == '__main__':
    print('=' * 50)
    print('Import Water Bodies from DISCODATA to Supabase')
    print('=' * 50)
    
    # 可选：设置导入限制（用于测试）
    LIMIT = 100  # 设为 None 导入全部数据
    
    disco_client = DiscoDataClient()
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        swb_count = import_swb_data(disco_client, cur, limit=LIMIT)
        gwb_count = import_gwb_data(disco_client, cur, limit=LIMIT)
        
        print('\n' + '=' * 50)
        print(f'✅ Import completed!')
        print(f'   SWB: {swb_count} records')
        print(f'   GWB: {gwb_count} records')
        print(f'   Total: {swb_count + gwb_count} records')
        
    finally:
        cur.close()
        conn.close()
