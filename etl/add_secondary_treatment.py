"""添加 requires_secondary_treatment 字段"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import *

conn = psycopg2.connect(host=SUPABASE_HOST, port=SUPABASE_PORT, database=SUPABASE_DATABASE, user=SUPABASE_USER, password=SUPABASE_PASSWORD)
cur = conn.cursor()

# 1. 添加列
print("1. Adding column...")
cur.execute("ALTER TABLE plants ADD COLUMN IF NOT EXISTS requires_secondary_treatment VARCHAR(10)")
conn.commit()

# 2. 清空
cur.execute("UPDATE plants SET requires_secondary_treatment = NULL")

# 3. 设置 Yes: 1000-2000 PE 且 provides_secondary_treatment = false
print("2. Setting Yes (1000-2000 PE + no secondary treatment)...")
cur.execute("""
    UPDATE plants 
    SET requires_secondary_treatment = 'Yes'
    WHERE plant_waste_load_pe >= 1000 
    AND plant_waste_load_pe < 2000
    AND (provides_secondary_treatment = false OR provides_secondary_treatment IS NULL)
""")
yes_count = cur.rowcount
print(f"   Yes: {yes_count}")

# 4. 设置 No: 其他所有
print("3. Setting No (all others)...")
cur.execute("""
    UPDATE plants 
    SET requires_secondary_treatment = 'No'
    WHERE requires_secondary_treatment IS NULL
""")
no_count = cur.rowcount
print(f"   No: {no_count}")

conn.commit()

# 5. 验证
print("\n4. Distribution:")
cur.execute("SELECT requires_secondary_treatment, COUNT(*) FROM plants GROUP BY requires_secondary_treatment ORDER BY requires_secondary_treatment")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

# 6. 添加注释
cur.execute("""COMMENT ON COLUMN plants.requires_secondary_treatment IS 'Requires secondary treatment: Yes if 1000-2000 PE and no secondary treatment provided, No otherwise'""")
conn.commit()

conn.close()
print("\nDone!")
