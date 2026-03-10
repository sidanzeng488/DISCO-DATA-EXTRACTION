"""检查 plants.csv 中的容量相关字段"""
import csv
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'DATA', 'current')

filepath = os.path.join(DATA_DIR, 'plants.csv')

with open(filepath, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    
    # 找容量相关的列
    capacity_cols = [c for c in reader.fieldnames if 'capacity' in c.lower() or 'capac' in c.lower()]
    print(f"Capacity related columns: {capacity_cols}")
    
    # 显示样本数据
    print("\nSample data:")
    for i, row in enumerate(reader):
        if i >= 5:
            break
        print(f"\nRow {i+1}: uwwCode={row.get('uwwCode')}")
        for col in capacity_cols:
            print(f"  {col}: '{row.get(col, '')}'")
    
    # 统计有数据的记录数
    f.seek(0)
    reader = csv.DictReader(f)
    
    print("\n\nData statistics:")
    for col in capacity_cols:
        f.seek(0)
        reader = csv.DictReader(f)
        count = sum(1 for r in reader if r.get(col, '').strip())
        print(f"  {col}: {count} non-empty records")
