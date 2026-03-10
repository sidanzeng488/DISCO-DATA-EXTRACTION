"""检查 uwwLoadEnteringUWWTP 和 uwwWasteWaterTreated 字段"""
import csv
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

# 1. 检查 plants 表当前列
print("=" * 60)
print("1. Plants 表当前所有列:")
print("=" * 60)

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT, 
    database=SUPABASE_DATABASE, user=SUPABASE_USER, 
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

cur.execute('''
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = 'public' AND table_name = 'plants' 
    ORDER BY ordinal_position
''')
cols = cur.fetchall()
col_names = [c[0] for c in cols]
for col in cols:
    print(f"  {col[0]}: {col[1]}")

# 检查是否有相关列
print("\n" + "=" * 60)
print("2. 检查是否存在相关列:")
print("=" * 60)
check_cols = ['load_entering', 'uww_load', 'volume_wastewater', 'wastewater_treated', 'wastewater_reused']
for check in check_cols:
    matches = [c for c in col_names if check.lower() in c.lower()]
    if matches:
        print(f"  包含 '{check}': {matches}")

cur.close()
conn.close()

# 2. 检查 CSV 中的原始列
print("\n" + "=" * 60)
print("3. plants.csv 原始列 (查找 uwwLoad 和 uwwWasteWater):")
print("=" * 60)

csv_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current', 'plants.csv')
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames
    
    # 查找相关列
    load_cols = [h for h in headers if 'load' in h.lower() or 'Load' in h]
    waste_cols = [h for h in headers if 'waste' in h.lower() or 'Waste' in h]
    
    print(f"  包含 'load' 的列: {load_cols}")
    print(f"  包含 'waste' 的列: {waste_cols}")
    
    # 检查具体字段
    print(f"\n  uwwLoadEnteringUWWTP 存在: {'uwwLoadEnteringUWWTP' in headers}")
    print(f"  uwwWasteWaterTreated 存在: {'uwwWasteWaterTreated' in headers}")
    print(f"  uwwWasteWaterReuse 存在: {'uwwWasteWaterReuse' in headers}")
    
    # 检查数据示例
    print("\n" + "=" * 60)
    print("4. 数据示例 (前5行有值的):")
    print("=" * 60)
    
    f.seek(0)
    reader = csv.DictReader(f)
    
    count = 0
    for row in reader:
        load_val = row.get('uwwLoadEnteringUWWTP', '')
        treated_val = row.get('uwwWasteWaterTreated', '')
        reuse_val = row.get('uwwWasteWaterReuse', '')
        
        if load_val or treated_val:
            print(f"  uwwCode: {row.get('uwwCode', 'N/A')}")
            print(f"    uwwLoadEnteringUWWTP: {load_val}")
            print(f"    uwwWasteWaterTreated: {treated_val}")
            print(f"    uwwWasteWaterReuse: {reuse_val}")
            count += 1
            if count >= 5:
                break
    
    # 统计非空值
    print("\n" + "=" * 60)
    print("5. 字段填充率统计:")
    print("=" * 60)
    
    f.seek(0)
    reader = csv.DictReader(f)
    
    total = 0
    load_count = 0
    treated_count = 0
    reuse_count = 0
    
    for row in reader:
        total += 1
        if row.get('uwwLoadEnteringUWWTP', '').strip():
            load_count += 1
        if row.get('uwwWasteWaterTreated', '').strip():
            treated_count += 1
        if row.get('uwwWasteWaterReuse', '').strip():
            reuse_count += 1
    
    print(f"  总记录数: {total}")
    print(f"  uwwLoadEnteringUWWTP: {load_count} ({load_count/total*100:.1f}%)")
    print(f"  uwwWasteWaterTreated: {treated_count} ({treated_count/total*100:.1f}%)")
    print(f"  uwwWasteWaterReuse: {reuse_count} ({reuse_count/total*100:.1f}%)")
