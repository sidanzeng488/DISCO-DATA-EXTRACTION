"""检查 DISCO CSV 和数据库的数据完整性对比"""
import csv
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

base_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')

conn = psycopg2.connect(
    host=SUPABASE_HOST, port=SUPABASE_PORT,
    database=SUPABASE_DATABASE, user=SUPABASE_USER,
    password=SUPABASE_PASSWORD
)
cur = conn.cursor()

print("=" * 80)
print("DISCO CSV vs Database Completeness Check")
print("=" * 80)

# 定义要检查的表
checks = [
    {
        'name': 'plants',
        'csv': 'plants.csv',
        'table': 'plants',
        'csv_key': 'uwwCode',
        'db_key': 'uwwtp_code',
    },
    {
        'name': 'agglomerations', 
        'csv': 'agglomerations.csv',
        'table': 'agglomeration',
        'csv_key': 'aggCode',
        'db_key': 'agg_code',
    },
    {
        'name': 'discharge_points',
        'csv': 'discharge_points.csv',
        'table': 'discharge_points',
        'csv_key': 'dcpCode',
        'db_key': 'dcp_code',
    },
    {
        'name': 'report_periods',
        'csv': 'report_periods.csv',
        'table': 'report_code',
        'csv_key': 'repCode',
        'db_key': 'rep_code',
    },
    {
        'name': 'water_bodies (SWB)',
        'csv': 'swb_surface_water.csv',
        'table': 'water_bodies',
        'csv_key': 'euSurfaceWaterBodyCode',
        'db_key': 'eu_water_body_code',
        'db_filter': "water_type = 'SWB'",
    },
    {
        'name': 'water_bodies (GWB)',
        'csv': 'gwb_groundwater.csv',
        'table': 'water_bodies',
        'csv_key': 'euGroundWaterBodyCode',
        'db_key': 'eu_water_body_code',
        'db_filter': "water_type = 'GWB'",
    },
    {
        'name': 'protected_areas (SWB)',
        'csv': 'swb_protected_areas.csv',
        'table': 'water_body_protected_areas',
        'csv_key': None,  # 无唯一键
        'db_key': None,
    },
    {
        'name': 'protected_areas (GWB)',
        'csv': 'gwb_protected_areas.csv',
        'table': 'water_body_protected_areas',
        'csv_key': None,
        'db_key': None,
    },
    {
        'name': 'art17_investments',
        'csv': 'art17_investments.csv',
        'table': None,  # 合并到 plants
        'csv_key': 'uwwCode',
        'db_key': None,
    },
]

results = []

for check in checks:
    csv_path = os.path.join(base_path, check['csv'])
    
    # CSV 统计
    csv_count = 0
    csv_keys = set()
    if os.path.exists(csv_path):
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            csv_count = len(rows)
            if check['csv_key']:
                csv_keys = set(r.get(check['csv_key'], '') for r in rows if r.get(check['csv_key'], '').strip())
    else:
        csv_count = -1  # 文件不存在
    
    # 数据库统计
    db_count = 0
    if check['table']:
        sql = f"SELECT COUNT(*) FROM {check['table']}"
        if check.get('db_filter'):
            sql += f" WHERE {check['db_filter']}"
        cur.execute(sql)
        db_count = cur.fetchone()[0]
    
    # 计算差异
    diff = csv_count - db_count if csv_count >= 0 else 'N/A'
    pct = (db_count / csv_count * 100) if csv_count > 0 else 0
    
    results.append({
        'name': check['name'],
        'csv': csv_count,
        'db': db_count,
        'diff': diff,
        'pct': pct,
    })

# 打印结果
print(f"\n{'Table':<25} {'CSV Rows':>12} {'DB Rows':>12} {'Diff':>10} {'Coverage':>10}")
print("-" * 80)

issues = []
for r in results:
    csv_str = str(r['csv']) if r['csv'] >= 0 else 'NOT FOUND'
    diff_str = str(r['diff']) if r['diff'] != 'N/A' else 'N/A'
    pct_str = f"{r['pct']:.1f}%" if r['csv'] > 0 else 'N/A'
    
    # 标记问题
    flag = ''
    if r['csv'] > 0 and r['pct'] < 90:
        flag = ' ⚠️ LOW'
        issues.append(r)
    elif r['csv'] == -1:
        flag = ' ❌ MISSING'
    
    print(f"{r['name']:<25} {csv_str:>12} {r['db']:>12} {diff_str:>10} {pct_str:>10}{flag}")

# 检查关键字段填充率
print("\n" + "=" * 80)
print("Key Field Fill Rates in Database")
print("=" * 80)

field_checks = [
    ('plants', 'plant_code (uwwtp_code)', 'uwwtp_code'),
    ('plants', 'plant_capacity', 'plant_capacity'),
    ('plants', 'rep_code', 'rep_code'),
    ('plants', 'agglomeration_id', 'agglomeration_id'),
    ('discharge_points', 'plant_code', 'plant_code'),
    ('discharge_points', 'water_body_code', 'water_body_code'),
    ('agglomeration', 'agg_code', 'agg_code'),
    ('agglomeration', 'agg_generated', 'agg_generated'),
]

print(f"\n{'Table':<20} {'Field':<25} {'Filled':>10} {'Total':>10} {'Rate':>10}")
print("-" * 80)

for table, field_name, column in field_checks:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL")
    filled = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = cur.fetchone()[0]
    rate = filled / total * 100 if total > 0 else 0
    
    flag = ' ⚠️' if rate < 90 else ''
    print(f"{table:<20} {field_name:<25} {filled:>10} {total:>10} {rate:>9.1f}%{flag}")

# 总结
print("\n" + "=" * 80)
print("Summary")
print("=" * 80)

if issues:
    print("\n⚠️ Tables with low coverage (<90%):")
    for i in issues:
        print(f"  - {i['name']}: {i['pct']:.1f}% ({i['db']}/{i['csv']})")
else:
    print("\n✅ All tables have good coverage (>=90%)")

cur.close()
conn.close()
