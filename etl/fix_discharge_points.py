"""修复 discharge_points 数据导入问题 - 重新导入完整数据"""
import csv
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

# 转换后的 CSV 路径
csv_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current', 'transformed', 'discharge_points.csv')

print("=" * 60)
print("修复 discharge_points 数据")
print("=" * 60)

# 读取转换后的 CSV
print("\n1. 读取转换后的 CSV...")
with open(csv_path, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    print(f"   CSV 总行数: {len(rows)}")
    
    # 检查 plant_code
    plant_codes = [r.get('plant_code', '') for r in rows if r.get('plant_code', '').strip()]
    print(f"   有效 plant_code: {len(plant_codes)}")

# 连接数据库
print("\n2. 连接数据库...")
conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# 检查当前数据
cur.execute("SELECT COUNT(*) FROM discharge_points")
old_count = cur.fetchone()[0]
print(f"   当前数据库记录: {old_count}")

# 删除现有数据
print("\n3. 清空现有数据...")
cur.execute("DELETE FROM discharge_points")
conn.commit()
print(f"   已删除 {old_count} 条记录")

# 准备导入数据
print("\n4. 准备导入数据...")

# 获取有效的 water_body_codes
print("   获取有效的 water_body_codes...")
cur.execute("SELECT eu_water_body_code FROM water_bodies")
valid_water_bodies = set(r[0] for r in cur.fetchall())
print(f"   有效 water_body_code 数量: {len(valid_water_bodies)}")

# 获取有效的 plant_codes (uwwtp_code)
print("   获取有效的 plant_codes...")
cur.execute("SELECT uwwtp_code FROM plants")
valid_plant_codes = set(r[0] for r in cur.fetchall())
print(f"   有效 plant_code 数量: {len(valid_plant_codes)}")

# 数据库列映射
db_columns = [
    'dcp_code', 'plant_code', 'country_code', 'latitude', 'longitude',
    'is_surface_water', 'water_body_type', 'water_body_code', 
    'sensitivity_code', 'rca_code', 'receiving_water', 'wfd_rbd', 'wfd_sub_unit'
]

def parse_bool(val):
    if val is None or val == '':
        return None
    val_lower = str(val).lower().strip()
    if val_lower in ('true', '1', 'yes', 't'):
        return True
    elif val_lower in ('false', '0', 'no', 'f'):
        return False
    return None

def parse_float(val):
    if val is None or val == '':
        return None
    try:
        return float(val)
    except:
        return None

def get_water_body_code(row):
    """根据 is_surface_water 选择正确的 water_body_code"""
    is_surface = parse_bool(row.get('is_surface_water', ''))
    if is_surface is True:
        return row.get('water_body_code', '') or None
    elif is_surface is False:
        # 对于地下水，使用 gwb_water_body_code 或 dcpGroundWater
        return row.get('water_body_code', '') or None
    return row.get('water_body_code', '') or None

# 准备数据
data = []
invalid_wb_count = 0
invalid_plant_count = 0
for row in rows:
    # 检查 plant_code 是否有效
    plant_code = row.get('plant_code', '') or None
    if plant_code and plant_code not in valid_plant_codes:
        invalid_plant_count += 1
        plant_code = None  # 设为 NULL
    
    # 检查 water_body_code 是否有效
    wb_code = row.get('water_body_code', '') or None
    if wb_code and wb_code not in valid_water_bodies:
        invalid_wb_count += 1
        wb_code = None  # 设为 NULL
    
    record = (
        row.get('dcp_code', '') or None,
        plant_code,
        row.get('country_code', '').replace('\ufeff', '') or None,  # 移除 BOM
        parse_float(row.get('latitude', '')),
        parse_float(row.get('longitude', '')),
        parse_bool(row.get('is_surface_water', '')),
        row.get('water_body_type', '') or None,
        wb_code,
        row.get('sensitivity_code', '') or None,
        row.get('rca_code', '') or None,
        row.get('receiving_water', '') or None,
        row.get('wfd_rbd', '') or None,
        row.get('wfd_sub_unit', '') or None,
    )
    data.append(record)

print(f"   无效 plant_code (设为NULL): {invalid_plant_count}")
print(f"   无效 water_body_code (设为NULL): {invalid_wb_count}")

print(f"   准备导入 {len(data)} 条记录")

# 批量导入
print("\n5. 批量导入数据...")
batch_size = 1000
total_inserted = 0

sql = f"""
    INSERT INTO discharge_points ({', '.join(db_columns)})
    VALUES %s
"""

for i in range(0, len(data), batch_size):
    batch = data[i:i+batch_size]
    execute_values(cur, sql, batch)
    total_inserted += len(batch)
    if (i + batch_size) % 5000 == 0 or i + batch_size >= len(data):
        print(f"   已导入 {total_inserted}/{len(data)} 条...")

conn.commit()

# 验证结果
print("\n6. 验证导入结果...")
cur.execute("SELECT COUNT(*) FROM discharge_points")
new_count = cur.fetchone()[0]
print(f"   总记录数: {new_count}")

cur.execute("SELECT COUNT(*) FROM discharge_points WHERE plant_code IS NOT NULL")
with_plant = cur.fetchone()[0]
print(f"   有 plant_code: {with_plant} ({with_plant/new_count*100:.1f}%)")

cur.execute("SELECT COUNT(*) FROM discharge_points WHERE water_body_code IS NOT NULL")
with_wb = cur.fetchone()[0]
print(f"   有 water_body_code: {with_wb} ({with_wb/new_count*100:.1f}%)")

# 检查与 plants 的关联
cur.execute("""
    SELECT COUNT(DISTINCT dp.plant_code) 
    FROM discharge_points dp
    WHERE EXISTS (SELECT 1 FROM plants p WHERE p.uwwtp_code = dp.plant_code)
""")
matched = cur.fetchone()[0]
print(f"   关联到 plants: {matched}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("完成！")
print("=" * 60)
