"""
创建 DISCO 数据的 Supabase Schema
基于统一水体模型设计
"""
import psycopg2
from config import (
    SUPABASE_HOST, 
    SUPABASE_PORT, 
    SUPABASE_DATABASE, 
    SUPABASE_USER, 
    SUPABASE_PASSWORD
)

conn = psycopg2.connect(
    host=SUPABASE_HOST,
    port=SUPABASE_PORT,
    database=SUPABASE_DATABASE,
    user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
conn.autocommit = True
cur = conn.cursor()

print('Creating DISCO Data Schema...')
print('=' * 50)

# === 1. LOOKUP TABLES ===
print('\n[1/5] Creating lookup tables...')

cur.execute("""
-- 国家表
CREATE TABLE IF NOT EXISTS countries (
    country_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(100)
);

-- 报告期表
CREATE TABLE IF NOT EXISTS report_periods (
    rep_code VARCHAR(50) PRIMARY KEY,
    country_code VARCHAR(10) REFERENCES countries(country_code),
    reported_period VARCHAR(50),
    situation_at DATE,
    version VARCHAR(20)
);
""")
print('  ✓ countries, report_periods')

# === 2. WATER BODIES (统一水体表) ===
print('\n[2/5] Creating unified water_bodies table...')

cur.execute("""
-- 统一水体表 (合并 SWB + GWB)
CREATE TABLE IF NOT EXISTS water_bodies (
    water_body_id SERIAL PRIMARY KEY,
    eu_water_body_code VARCHAR(100) UNIQUE NOT NULL,
    water_type VARCHAR(10) NOT NULL CHECK (water_type IN ('SWB', 'GWB')),
    water_body_name VARCHAR(500),
    country_code VARCHAR(10) REFERENCES countries(country_code),
    eu_rbd_code VARCHAR(50),
    rbd_name VARCHAR(200),
    eu_sub_unit_code VARCHAR(50),
    sub_unit_name VARCHAR(200),
    c_year INT,
    
    -- SWB 特有字段 (GWB 时为 NULL)
    surface_water_category VARCHAR(50),
    natural_awb_hmwb VARCHAR(50),
    c_area NUMERIC,
    c_length NUMERIC,
    sw_ecological_status VARCHAR(50),
    sw_ecological_assessment_year INT,
    sw_chemical_status VARCHAR(50),
    sw_chemical_assessment_year INT,
    
    -- GWB 特有字段 (SWB 时为 NULL)  
    gw_quantitative_status VARCHAR(50),
    gw_quantitative_assessment_year INT,
    gw_chemical_status VARCHAR(50),
    gw_chemical_assessment_year INT,
    
    file_url TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_water_bodies_type ON water_bodies(water_type);
CREATE INDEX IF NOT EXISTS idx_water_bodies_country ON water_bodies(country_code);
""")
print('  ✓ water_bodies')

# === 3. WATER BODY PROTECTED AREAS ===
print('\n[3/5] Creating water_body_protected_areas table...')

cur.execute("""
-- 水体保护区关联表
CREATE TABLE IF NOT EXISTS water_body_protected_areas (
    protected_area_id SERIAL PRIMARY KEY,
    water_body_code VARCHAR(100) REFERENCES water_bodies(eu_water_body_code),
    eu_protected_area_code VARCHAR(100),
    protected_area_type VARCHAR(100),
    objectives_set BOOLEAN,
    objectives_met BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_protected_areas_wb ON water_body_protected_areas(water_body_code);
""")
print('  ✓ water_body_protected_areas')

# === 4. PLANTS AND AGGLOMERATIONS ===
print('\n[4/5] Creating plants and agglomerations tables...')

cur.execute("""
-- 聚居区表
CREATE TABLE IF NOT EXISTS agglomerations (
    agglomeration_id SERIAL PRIMARY KEY,
    agg_code VARCHAR(100) UNIQUE NOT NULL,
    agg_name VARCHAR(500),
    country_code VARCHAR(10) REFERENCES countries(country_code),
    agg_capacity INT,
    agg_generated NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC,
    rep_code VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 污水处理厂表
CREATE TABLE IF NOT EXISTS plants (
    plant_id SERIAL PRIMARY KEY,
    uwwtp_code VARCHAR(100) UNIQUE NOT NULL,
    plant_name VARCHAR(500),
    country_code VARCHAR(10) REFERENCES countries(country_code),
    latitude NUMERIC,
    longitude NUMERIC,
    capacity_pe INT,
    load_entering NUMERIC,
    
    -- 处理方式
    has_primary_treatment BOOLEAN,
    has_secondary_treatment BOOLEAN,
    has_other_treatment BOOLEAN,
    has_n_removal BOOLEAN,
    has_p_removal BOOLEAN,
    has_uv BOOLEAN,
    has_chlorination BOOLEAN,
    has_ozonation BOOLEAN,
    has_sand_filtration BOOLEAN,
    has_micro_filtration BOOLEAN,
    
    -- 性能指标
    bod5_perf NUMERIC,
    cod_perf NUMERIC,
    tss_perf NUMERIC,
    n_tot_perf NUMERIC,
    p_tot_perf NUMERIC,
    
    rep_code VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 处理厂-聚居区关联
CREATE TABLE IF NOT EXISTS plant_agglomeration_links (
    link_id SERIAL PRIMARY KEY,
    plant_code VARCHAR(100),
    agg_code VARCHAR(100),
    perc_entering_uwwtp NUMERIC,
    UNIQUE(plant_code, agg_code)
);

CREATE INDEX IF NOT EXISTS idx_plants_country ON plants(country_code);
CREATE INDEX IF NOT EXISTS idx_agg_country ON agglomerations(country_code);
""")
print('  ✓ agglomerations, plants, plant_agglomeration_links')

# === 5. DISCHARGE POINTS ===
print('\n[5/5] Creating discharge_points table...')

cur.execute("""
-- 排放点表
CREATE TABLE IF NOT EXISTS discharge_points (
    dcp_id SERIAL PRIMARY KEY,
    dcp_code VARCHAR(100) UNIQUE NOT NULL,
    dcp_name VARCHAR(500),
    plant_code VARCHAR(100),
    country_code VARCHAR(10) REFERENCES countries(country_code),
    latitude NUMERIC,
    longitude NUMERIC,
    
    -- 水体关联
    is_surface_water BOOLEAN NOT NULL,
    water_body_type VARCHAR(10),  -- FW/CW/ES/LF/LC
    water_body_code VARCHAR(100) REFERENCES water_bodies(eu_water_body_code),
    
    -- 其他信息
    receiving_water VARCHAR(500),
    wfd_rbd VARCHAR(100),
    wfd_sub_unit VARCHAR(100),
    
    rep_code VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dcp_plant ON discharge_points(plant_code);
CREATE INDEX IF NOT EXISTS idx_dcp_water_body ON discharge_points(water_body_code);
CREATE INDEX IF NOT EXISTS idx_dcp_surface ON discharge_points(is_surface_water);
""")
print('  ✓ discharge_points')

# === VERIFY ===
print('\n' + '=' * 50)
print('Verifying created tables...')

cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name;
""")

tables = cur.fetchall()
print(f'\nPublic tables ({len(tables)}):')
for table in tables:
    print(f'  ✓ {table[0]}')

cur.close()
conn.close()

print('\n' + '=' * 50)
print('✅ Schema creation completed!')
