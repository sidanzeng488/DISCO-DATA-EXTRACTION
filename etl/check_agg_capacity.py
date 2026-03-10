"""检查 aggCapacity 数据"""
import csv
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'DATA', 'current')

with open(os.path.join(DATA_DIR, 'agglomerations.csv'), 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    
    # 检查列名
    print(f"Columns: {reader.fieldnames}")
    print()
    
    # 找有 capacity 的记录
    has_capacity = []
    no_capacity = []
    
    for i, row in enumerate(reader):
        cap = row.get('aggCapacity', '')
        if cap and cap.strip():
            has_capacity.append((row.get('aggCode'), cap))
        else:
            no_capacity.append(row.get('aggCode'))
        
        if i >= 100:
            break
    
    print(f"First 100 rows:")
    print(f"  With capacity: {len(has_capacity)}")
    print(f"  Without capacity: {len(no_capacity)}")
    
    if has_capacity:
        print(f"\nSamples with capacity:")
        for code, cap in has_capacity[:10]:
            print(f"  {code}: '{cap}'")
    
    if no_capacity:
        print(f"\nSamples without capacity:")
        for code in no_capacity[:5]:
            print(f"  {code}")
