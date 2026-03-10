"""检查原始 CSV 中的 discharge_points 数据量"""
import csv
import os

base_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')

# 检查 discharge_points.csv 原始数据
dcp_path = os.path.join(base_path, 'discharge_points.csv')
if os.path.exists(dcp_path):
    with open(dcp_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        print(f"discharge_points.csv total rows: {len(rows)}")
        
        # 统计唯一 uwwCode
        uww_codes = set(r.get('uwwCode', '') for r in rows if r.get('uwwCode'))
        print(f"Unique uwwCode in discharge_points.csv: {len(uww_codes)}")
        
        # 样本
        sample = list(uww_codes)[:5]
        print(f"Sample uwwCode: {sample}")
else:
    print("discharge_points.csv not found")

# 检查 plants.csv
plants_path = os.path.join(base_path, 'plants.csv')
if os.path.exists(plants_path):
    with open(plants_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        print(f"\nplants.csv total rows: {len(rows)}")
        plant_codes = set(r.get('uwwCode', '') for r in rows if r.get('uwwCode'))
        print(f"Unique uwwCode in plants.csv: {len(plant_codes)}")

# 检查数据库导入是否完整
print("\n=== Database vs CSV comparison ===")
print(f"CSV discharge_points: should have {len(rows) if 'dcp_path' in dir() else 'N/A'} rows")
