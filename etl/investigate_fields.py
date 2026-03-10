"""调查缺失字段的源数据"""
import os
import csv

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'DATA', 'current')

def read_csv_sample(filepath, n=5):
    if not os.path.exists(filepath):
        return None, []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = []
        for i, row in enumerate(reader):
            if i >= n:
                break
            rows.append(row)
        return list(rows[0].keys()) if rows else [], rows

print('=' * 70)
print('调查 COD Outgoing 字段')
print('=' * 70)

cols, samples = read_csv_sample(os.path.join(DATA_DIR, 'plants.csv'))

# 查找 COD 相关列
cod_cols = [c for c in cols if 'cod' in c.lower()]
print(f'\nCOD 相关列 ({len(cod_cols)}):')
for c in cod_cols:
    # 显示样本值
    values = [s.get(c, '') for s in samples]
    non_empty = [v for v in values if v]
    print(f'  {c}: {non_empty[:3] if non_empty else "[empty]"}')

# 查找 Discharge 相关列（可能 COD outgoing 用的是 discharge）
discharge_cols = [c for c in cols if 'discharge' in c.lower()]
print(f'\nDischarge 相关列 ({len(discharge_cols)}):')
for c in discharge_cols:
    values = [s.get(c, '') for s in samples]
    non_empty = [v for v in values if v]
    print(f'  {c}: {non_empty[:3] if non_empty else "[empty]"}')

print('\n' + '=' * 70)
print('调查 Article 17 字段')
print('=' * 70)

cols2, samples2 = read_csv_sample(os.path.join(DATA_DIR, 'art17_investments.csv'))

print(f'\n所有 Art17 列 ({len(cols2)}):')
for c in cols2:
    values = [s.get(c, '') for s in samples2]
    non_empty = [v for v in values if v]
    print(f'  {c}: {non_empty[:2] if non_empty else "[empty]"}')

# 特别查找 investment/measures 相关
print('\n\n查找 investment_planned 对应的列:')
invest_cols = [c for c in cols2 if 'measure' in c.lower() or 'invest' in c.lower() or 'planned' in c.lower()]
for c in invest_cols:
    values = [s.get(c, '') for s in samples2]
    print(f'  {c}: {values}')

# 查找 reason 相关（investment_need）
print('\n查找 investment_need 对应的列:')
need_cols = [c for c in cols2 if 'reason' in c.lower() or 'need' in c.lower()]
for c in need_cols:
    values = [s.get(c, '') for s in samples2]
    print(f'  {c}: {values}')

# 查找 type 相关（investment_type）
print('\n查找 investment_type 对应的列:')
type_cols = [c for c in cols2 if 'treatment' in c.lower() or 'type' in c.lower()]
for c in type_cols:
    values = [s.get(c, '') for s in samples2]
    print(f'  {c}: {values}')
