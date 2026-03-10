"""导入 requirement.csv 到数据库"""
import csv
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

csv_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current', 'requirement.csv')

print("=" * 60)
print("Import UWWTD Requirement Table")
print("=" * 60)

# 读取 CSV
print("\n1. Reading CSV...")
with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    print(f"   Found {len(rows)} rows")
    for row in rows:
        print(f"   - {row['Treatment tier']}")

# 连接数据库
print("\n2. Connecting to database...")
conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# 创建表
print("\n3. Creating table...")
cur.execute("""
    DROP TABLE IF EXISTS uwwtd_requirement CASCADE;
    
    CREATE TABLE uwwtd_requirement (
        id SERIAL PRIMARY KEY,
        treatment_tier VARCHAR(100) NOT NULL,
        capacity_threshold TEXT,
        intermediary_deadline_1 TEXT,
        intermediary_deadline_2 TEXT,
        intermediary_deadline_3 TEXT,
        final_deadline TEXT,
        description TEXT
    );
    
    COMMENT ON TABLE uwwtd_requirement IS 'UWWTD treatment requirements and deadlines based on revised directive';
    COMMENT ON COLUMN uwwtd_requirement.id IS 'Auto-generated primary key';
    COMMENT ON COLUMN uwwtd_requirement.treatment_tier IS 'Treatment tier: Secondary, Tertiary, Quaternary, Stormwater management, Energy-self sufficiency';
    COMMENT ON COLUMN uwwtd_requirement.capacity_threshold IS 'Capacity threshold for the requirement to apply';
    COMMENT ON COLUMN uwwtd_requirement.intermediary_deadline_1 IS 'First intermediary deadline';
    COMMENT ON COLUMN uwwtd_requirement.intermediary_deadline_2 IS 'Second intermediary deadline';
    COMMENT ON COLUMN uwwtd_requirement.intermediary_deadline_3 IS 'Third intermediary deadline';
    COMMENT ON COLUMN uwwtd_requirement.final_deadline IS 'Final compliance deadline';
    COMMENT ON COLUMN uwwtd_requirement.description IS 'Full description of the requirement';
""")
conn.commit()
print("   Table created")

# 插入数据
print("\n4. Inserting data...")
for row in rows:
    cur.execute("""
        INSERT INTO uwwtd_requirement 
        (treatment_tier, capacity_threshold, intermediary_deadline_1, intermediary_deadline_2, intermediary_deadline_3, final_deadline, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Treatment tier'],
        row['Capacity threshold'],
        row['Intermediary deadline 1'],
        row['Intermediary deadline 2'],
        row['Intermediary deadline 3'],
        row['Final deadline'],
        row['Description'],
    ))

conn.commit()
print(f"   Inserted {len(rows)} rows")

# 验证
print("\n5. Verifying...")
cur.execute("SELECT id, treatment_tier, final_deadline FROM uwwtd_requirement ORDER BY id")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]} (deadline: {row[2]})")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("Done!")
print("=" * 60)
