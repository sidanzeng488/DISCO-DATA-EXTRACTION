"""填充 plants.plant_waste_load_pe 从 uwwLoadEnteringUWWTP"""
import csv
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

csv_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current', 'plants.csv')

# 读取 CSV 数据
print("Reading plants.csv...")
updates = []
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        uww_code = row.get('uwwCode', '').strip()
        load_val = row.get('uwwLoadEnteringUWWTP', '').strip()
        
        if uww_code and load_val:
            try:
                load_int = int(float(load_val))
                updates.append((load_int, uww_code))
            except ValueError:
                pass

print(f"Found {len(updates)} records with uwwLoadEnteringUWWTP values")

# 连接数据库并更新
print("\nConnecting to database...")
conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

# 批量更新
print("Updating plant_waste_load_pe...")
batch_size = 1000
updated = 0

for i in range(0, len(updates), batch_size):
    batch = updates[i:i+batch_size]
    
    # 使用 CASE WHEN 批量更新
    cases = []
    codes = []
    for load_val, uww_code in batch:
        cases.append(f"WHEN uwwtp_code = %s THEN %s")
        codes.extend([uww_code, load_val])
    
    uww_codes = [u[1] for u in batch]
    
    sql = f"""
        UPDATE plants 
        SET plant_waste_load_pe = CASE 
            {' '.join(cases)}
        END
        WHERE uwwtp_code IN ({','.join(['%s'] * len(uww_codes))})
    """
    
    cur.execute(sql, codes + uww_codes)
    updated += cur.rowcount
    
    if (i + batch_size) % 5000 == 0 or i + batch_size >= len(updates):
        print(f"  Processed {min(i + batch_size, len(updates))}/{len(updates)} records...")

conn.commit()

# 验证结果
cur.execute("SELECT COUNT(*) FROM plants WHERE plant_waste_load_pe IS NOT NULL")
filled = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM plants")
total = cur.fetchone()[0]

print(f"\nDone! Updated {updated} rows")
print(f"plant_waste_load_pe fill rate: {filled}/{total} ({filled/total*100:.1f}%)")

# 显示一些样本
print("\nSample data:")
cur.execute("""
    SELECT uwwtp_code, plant_name, plant_waste_load_pe, plant_capacity 
    FROM plants 
    WHERE plant_waste_load_pe IS NOT NULL 
    LIMIT 5
""")
for row in cur.fetchall():
    print(f"  {row[0]}: load={row[2]} p.e., capacity={row[3]} p.e.")

cur.close()
conn.close()
