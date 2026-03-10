"""创建 plants x uwwtd_requirement 关联表"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

print("=" * 60)
print("Create Plants x Requirement Linking Table")
print("=" * 60)

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# 1. 创建关联表
print("\n1. Creating linking table...")
cur.execute("""
    DROP TABLE IF EXISTS plant_requirement_link CASCADE;
    
    CREATE TABLE plant_requirement_link (
        id SERIAL PRIMARY KEY,
        plant_id INTEGER REFERENCES plants(plant_id),
        requirement_id INTEGER REFERENCES uwwtd_requirement(id),
        applies BOOLEAN DEFAULT TRUE,
        notes TEXT,
        UNIQUE(plant_id, requirement_id)
    );
    
    COMMENT ON TABLE plant_requirement_link IS 'Links plants to applicable UWWTD requirements based on capacity and receiving water sensitivity';
    COMMENT ON COLUMN plant_requirement_link.id IS 'Auto-generated primary key';
    COMMENT ON COLUMN plant_requirement_link.plant_id IS 'Reference to plants table';
    COMMENT ON COLUMN plant_requirement_link.requirement_id IS 'Reference to uwwtd_requirement table';
    COMMENT ON COLUMN plant_requirement_link.applies IS 'Whether the requirement applies to this plant';
    COMMENT ON COLUMN plant_requirement_link.notes IS 'Additional notes about applicability';
""")
conn.commit()
print("   Table created")

# 2. 获取 requirement IDs
print("\n2. Getting requirement IDs...")
cur.execute("SELECT id, treatment_tier FROM uwwtd_requirement")
requirements = {row[1]: row[0] for row in cur.fetchall()}
print(f"   Requirements: {requirements}")

# 3. 获取敏感水体关联的 plants (用于 Tertiary/Quaternary)
print("\n3. Finding plants with sensitive receiving waters...")
cur.execute("""
    SELECT DISTINCT p.plant_id
    FROM plants p
    JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
    JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
    WHERE wpa.protected_area_type IN ('Bathing waters', 'Drinking water protection area', 'Shellfish designated water', 'Sensitive area', 'Nitrate vulnerable zone')
""")
sensitive_plant_ids = set(r[0] for r in cur.fetchall())
print(f"   Found {len(sensitive_plant_ids)} plants with sensitive waters")

# 4. 插入关联数据
print("\n4. Populating links...")

# 获取所有 plants
cur.execute("SELECT plant_id, plant_capacity FROM plants")
plants = cur.fetchall()

inserted = 0
for plant_id, capacity in plants:
    if capacity is None:
        continue
    
    links = []
    
    # Secondary: >= 1,000 PE - BOD/COD/TSS removal required
    if capacity >= 1000:
        links.append((plant_id, requirements['Secondary'], True, 
                      '>= 1,000 PE - BOD/COD/TSS removal required by 2035'))
    
    # Tertiary: >= 150,000 PE 或 >= 10,000 PE + 敏感区域 - N/P removal
    if capacity >= 150000:
        links.append((plant_id, requirements['Tertiary'], True, 
                      '>= 150,000 PE - N/P removal required by 2045'))
    elif capacity >= 10000 and plant_id in sensitive_plant_ids:
        links.append((plant_id, requirements['Tertiary'], True, 
                      '>= 10,000 PE + sensitive area - N/P removal required by 2045'))
    
    # Quaternary: >= 150,000 PE 或 >= 10,000 PE + 敏感区域 - Micropollutant removal
    if capacity >= 150000:
        links.append((plant_id, requirements['Quaternary'], True, 
                      '>= 150,000 PE - Micropollutant removal required by 2045'))
    elif capacity >= 10000 and plant_id in sensitive_plant_ids:
        links.append((plant_id, requirements['Quaternary'], True, 
                      '>= 10,000 PE + sensitive area - Micropollutant removal required by 2045'))
    
    # Energy-self sufficiency: >= 10,000 PE
    if capacity >= 10000:
        links.append((plant_id, requirements['Energy-self suffiency'], True, 
                      '>= 10,000 PE - Energy audits required every 4 years'))
    
    # 插入
    for link in links:
        cur.execute("""
            INSERT INTO plant_requirement_link (plant_id, requirement_id, applies, notes)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (plant_id, requirement_id) DO NOTHING
        """, link)
        inserted += 1

conn.commit()
print(f"   Inserted {inserted} links")

# 5. 统计结果
print("\n5. Summary by requirement...")
cur.execute("""
    SELECT r.treatment_tier, COUNT(prl.id)
    FROM uwwtd_requirement r
    LEFT JOIN plant_requirement_link prl ON r.id = prl.requirement_id
    GROUP BY r.id, r.treatment_tier
    ORDER BY r.id
""")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]} plants")

print("\n6. Sample data...")
cur.execute("""
    SELECT p.uwwtp_code, p.plant_capacity, r.treatment_tier, prl.notes
    FROM plant_requirement_link prl
    JOIN plants p ON prl.plant_id = p.plant_id
    JOIN uwwtd_requirement r ON prl.requirement_id = r.id
    WHERE p.plant_capacity >= 150000
    LIMIT 10
""")
for row in cur.fetchall():
    print(f"   {row[0]} ({row[1]} PE): {row[2]} - {row[3]}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
