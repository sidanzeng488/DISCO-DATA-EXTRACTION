"""检查 aggCapacity 数据 - 详细版"""
import csv
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'DATA', 'current')

filepath = os.path.join(DATA_DIR, 'agglomerations.csv')

# 直接读取前几行
print("Raw lines:")
with open(filepath, 'r', encoding='utf-8-sig') as f:
    for i, line in enumerate(f):
        if i >= 3:
            break
        # 分割并显示第11列 (aggCapacity) 和第21列 (aggGenerated)
        parts = line.strip().split(',')
        if i == 0:
            print(f"  Column 11: {parts[10] if len(parts) > 10 else 'N/A'}")
            print(f"  Column 21: {parts[20] if len(parts) > 20 else 'N/A'}")
        else:
            print(f"\nRow {i}:")
            print(f"  aggCode (col 3): {parts[2] if len(parts) > 2 else 'N/A'}")
            print(f"  aggCapacity (col 11): '{parts[10] if len(parts) > 10 else 'N/A'}'")
            print(f"  aggGenerated (col 21): '{parts[20] if len(parts) > 20 else 'N/A'}'")

print("\n\n" + "="*60)
print("Searching for non-empty aggCapacity...")

# 遍历所有数据找非空的 aggCapacity
with open(filepath, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    found = 0
    for i, row in enumerate(reader):
        cap = row.get('aggCapacity', '').strip()
        if cap:
            found += 1
            if found <= 5:
                print(f"  Row {i+1}: aggCode={row.get('aggCode')}, aggCapacity='{cap}'")
    
    print(f"\nTotal rows with non-empty aggCapacity: {found}")
