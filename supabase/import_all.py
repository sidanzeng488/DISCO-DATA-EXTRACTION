"""
完整 ETL: 从 DISCODATA 导入所有数据到 Supabase
执行顺序很重要（外键依赖）
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

# ============================================
# 1. 导入国家数据
# ============================================
def import_countries(client, cur):
    print('\n[1/6] Importing Countries...')
    
    # 从 UWWTPS 表获取所有国家
    data = client.query("""
        SELECT DISTINCT CountryCode
        FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
        WHERE CountryCode IS NOT NULL
    """)
    
    for row in data:
        cur.execute("""
            INSERT INTO countries (country_code)
            VALUES (%s)
            ON CONFLICT (country_code) DO NOTHING
        """, (row.get('CountryCode'),))
    
    print(f'  ✓ {len(data)} countries')

# ============================================
# 2. 导入水体数据 (SWB + GWB)
# ============================================
def import_water_bodies(client, cur, limit=None):
    print('\n[2/6] Importing Water Bodies (SWB + GWB)...')
    
    # SWB
    query = """
        SELECT euSurfaceWaterBodyCode, surfaceWaterBodyName, countryCode,
               euRBDCode, rbdName, cYear,
               swEcologicalStatusOrPotentialValue, swChemicalStatusValue, fileUrl
        FROM [WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]
    """
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    swb_data = client.query(query, max_records=limit)
    swb_count = 0
    for row in swb_data:
        try:
            cur.execute("""
                INSERT INTO water_bodies (eu_water_body_code, water_type, water_body_name, 
                    country_code, eu_rbd_code, rbd_name, c_year,
                    sw_ecological_status, sw_chemical_status, file_url)
                VALUES (%s, 'SWB', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (eu_water_body_code) DO NOTHING
            """, (
                row.get('euSurfaceWaterBodyCode'),
                row.get('surfaceWaterBodyName'),
                row.get('countryCode'),
                row.get('euRBDCode'),
                row.get('rbdName'),
                row.get('cYear'),
                row.get('swEcologicalStatusOrPotentialValue'),
                row.get('swChemicalStatusValue'),
                row.get('fileUrl')
            ))
            swb_count += 1
        except Exception as e:
            pass
    
    # GWB
    query = """
        SELECT euGroundWaterBodyCode, countryCode,
               euRBDCode, rbdName, cYear,
               gwQuantitativeStatusValue, gwChemicalStatusValue, fileUrl
        FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody]
    """
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    gwb_data = client.query(query, max_records=limit)
    gwb_count = 0
    for row in gwb_data:
        try:
            cur.execute("""
                INSERT INTO water_bodies (eu_water_body_code, water_type,
                    country_code, eu_rbd_code, rbd_name, c_year,
                    gw_quantitative_status, gw_chemical_status, file_url)
                VALUES (%s, 'GWB', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (eu_water_body_code) DO NOTHING
            """, (
                row.get('euGroundWaterBodyCode'),
                row.get('countryCode'),
                row.get('euRBDCode'),
                row.get('rbdName'),
                row.get('cYear'),
                row.get('gwQuantitativeStatusValue'),
                row.get('gwChemicalStatusValue'),
                row.get('fileUrl')
            ))
            gwb_count += 1
        except Exception as e:
            pass
    
    print(f'  ✓ SWB: {swb_count}, GWB: {gwb_count}')

# ============================================
# 3. 导入聚居区数据
# ============================================
def import_agglomerations(client, cur, limit=None):
    print('\n[3/6] Importing Agglomerations...')
    
    query = """
        SELECT aggCode, aggName, CountryCode, aggCapacity, aggGenerated,
               aggLatitude, aggLongitude, repCode
        FROM [WISE_UWWTD].[v1r1].[T_Agglomerations]
    """
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    data = client.query(query, max_records=limit)
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO agglomerations (agg_code, agg_name, country_code, 
                    agg_capacity, agg_generated, latitude, longitude, rep_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (agg_code) DO NOTHING
            """, (
                row.get('aggCode'),
                row.get('aggName'),
                row.get('CountryCode'),
                row.get('aggCapacity'),
                row.get('aggGenerated'),
                row.get('aggLatitude'),
                row.get('aggLongitude'),
                row.get('repCode')
            ))
            count += 1
        except Exception as e:
            pass
    
    print(f'  ✓ {count} agglomerations')

# ============================================
# 4. 导入处理厂数据
# ============================================
def import_plants(client, cur, limit=None):
    print('\n[4/6] Importing Treatment Plants...')
    
    query = """
        SELECT uwwCode, uwwName, CountryCode, uwwLatitude, uwwLongitude,
               uwwCapacity, uwwLoadEnteringUWWTP,
               uwwPrimaryTreatment, uwwSecondaryTreatment, uwwOtherTreatment,
               uwwNRemoval, uwwPRemoval, uwwUV, uwwChlorination, uwwOzonation,
               uwwSandFiltration, uwwMicroFiltration,
               uwwBOD5Perf, uwwCODPerf, uwwTSSPerf, uwwNTotPerf, uwwPTotPerf,
               repCode
        FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
    """
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    data = client.query(query, max_records=limit)
    count = 0
    for row in data:
        try:
            cur.execute("""
                INSERT INTO plants (uwwtp_code, plant_name, country_code, 
                    latitude, longitude, capacity_pe, load_entering,
                    has_primary_treatment, has_secondary_treatment, has_other_treatment,
                    has_n_removal, has_p_removal, has_uv, has_chlorination, has_ozonation,
                    has_sand_filtration, has_micro_filtration,
                    bod5_perf, cod_perf, tss_perf, n_tot_perf, p_tot_perf, rep_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (uwwtp_code) DO NOTHING
            """, (
                row.get('uwwCode'),
                row.get('uwwName'),
                row.get('CountryCode'),
                row.get('uwwLatitude'),
                row.get('uwwLongitude'),
                row.get('uwwCapacity'),
                row.get('uwwLoadEnteringUWWTP'),
                row.get('uwwPrimaryTreatment'),
                row.get('uwwSecondaryTreatment'),
                row.get('uwwOtherTreatment'),
                row.get('uwwNRemoval'),
                row.get('uwwPRemoval'),
                row.get('uwwUV'),
                row.get('uwwChlorination'),
                row.get('uwwOzonation'),
                row.get('uwwSandFiltration'),
                row.get('uwwMicroFiltration'),
                row.get('uwwBOD5Perf'),
                row.get('uwwCODPerf'),
                row.get('uwwTSSPerf'),
                row.get('uwwNTotPerf'),
                row.get('uwwPTotPerf'),
                row.get('repCode')
            ))
            count += 1
        except Exception as e:
            pass
    
    print(f'  ✓ {count} plants')

# ============================================
# 5. 导入排放点数据
# ============================================
def import_discharge_points(client, cur, limit=None):
    print('\n[5/6] Importing Discharge Points...')
    
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
    count = 0
    for row in data:
        is_surface_water = row.get('dcpSurfaceWaters') == True or row.get('dcpSurfaceWaters') == 'True'
        water_body_code = row.get('dcpWaterbodyID') if is_surface_water else row.get('dcpGroundWater')
        
        try:
            cur.execute("""
                INSERT INTO discharge_points (dcp_code, dcp_name, plant_code, country_code,
                    latitude, longitude, is_surface_water, water_body_type, water_body_code,
                    receiving_water, wfd_rbd, wfd_sub_unit, rep_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            count += 1
        except Exception as e:
            pass
    
    print(f'  ✓ {count} discharge points')

# ============================================
# 6. 导入保护区关联
# ============================================
def import_protected_areas(client, cur, limit=None):
    print('\n[6/6] Importing Protected Areas...')
    
    # SWB Protected Areas
    query = """
        SELECT euSurfaceWaterBodyCode, euProtectedAreaCode, 
               protectedAreaType, protectedAreaObjectivesSet, protectedAreaObjectivesMet
        FROM [WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]
    """
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    swb_pa = client.query(query, max_records=limit)
    swb_count = 0
    for row in swb_pa:
        try:
            cur.execute("""
                INSERT INTO water_body_protected_areas (water_body_code, eu_protected_area_code,
                    protected_area_type, objectives_set, objectives_met)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row.get('euSurfaceWaterBodyCode'),
                row.get('euProtectedAreaCode'),
                row.get('protectedAreaType'),
                row.get('protectedAreaObjectivesSet'),
                row.get('protectedAreaObjectivesMet')
            ))
            swb_count += 1
        except Exception as e:
            pass
    
    # GWB Protected Areas
    query = """
        SELECT euGroundWaterBodyCode, euProtectedAreaCode,
               protectedAreaType, protectedAreaObjectivesSet, protectedAreaObjectivesMet
        FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]
    """
    if limit:
        query = query.replace('SELECT', f'SELECT TOP {limit}')
    
    gwb_pa = client.query(query, max_records=limit)
    gwb_count = 0
    for row in gwb_pa:
        try:
            cur.execute("""
                INSERT INTO water_body_protected_areas (water_body_code, eu_protected_area_code,
                    protected_area_type, objectives_set, objectives_met)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row.get('euGroundWaterBodyCode'),
                row.get('euProtectedAreaCode'),
                row.get('protectedAreaType'),
                row.get('protectedAreaObjectivesSet'),
                row.get('protectedAreaObjectivesMet')
            ))
            gwb_count += 1
        except Exception as e:
            pass
    
    print(f'  ✓ SWB PA: {swb_count}, GWB PA: {gwb_count}')

# ============================================
# MAIN
# ============================================
if __name__ == '__main__':
    print('=' * 60)
    print('DISCO DATA → Supabase Full ETL')
    print('=' * 60)
    
    # 设置导入限制（用于测试，设为 None 导入全部）
    LIMIT = 100
    
    print(f'\nLimit: {LIMIT if LIMIT else "ALL DATA"}')
    print('=' * 60)
    
    disco_client = DiscoDataClient()
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        import_countries(disco_client, cur)
        import_water_bodies(disco_client, cur, limit=LIMIT)
        import_agglomerations(disco_client, cur, limit=LIMIT)
        import_plants(disco_client, cur, limit=LIMIT)
        import_discharge_points(disco_client, cur, limit=LIMIT)
        import_protected_areas(disco_client, cur, limit=LIMIT)
        
        print('\n' + '=' * 60)
        print('✅ Full ETL completed!')
        print('=' * 60)
        
    finally:
        cur.close()
        conn.close()
