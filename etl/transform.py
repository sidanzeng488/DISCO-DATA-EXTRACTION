"""
数据转换脚本
- 读取 DATA/current/ 中的原始 DISCO 数据
- 按照 Schema 映射转换字段名
- 合并 SWB + GWB 为 water_bodies
- 保存转换后的数据到 DATA/current/transformed/
"""
import os
import sys
import csv
import json
from datetime import datetime

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.config import (
    CURRENT_DIR,
    FIELD_MAPPINGS,
    SENSITIVITY_DATA,
)

TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')


def read_csv(filepath):
    """读取 CSV 文件"""
    if not os.path.exists(filepath):
        return []
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv(filepath, data, fieldnames=None):
    """写入 CSV 文件"""
    if not data:
        return
    
    if fieldnames is None:
        # 收集所有行的所有字段（处理字段不一致的情况）
        all_fields = set()
        for row in data:
            all_fields.update(row.keys())
        fieldnames = sorted(list(all_fields))
    
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)


def transform_fields(data, mapping):
    """转换字段名"""
    if not data:
        return []
    
    transformed = []
    for row in data:
        new_row = {}
        for old_key, value in row.items():
            # 使用映射的新字段名，如果没有映射则保留原名
            new_key = mapping.get(old_key, old_key)
            new_row[new_key] = value
        transformed.append(new_row)
    
    return transformed


def merge_water_bodies():
    """合并 SWB 和 GWB 为统一的 water_bodies 表"""
    print('\n[Transform] Merging SWB + GWB to water_bodies...')
    
    water_bodies = []
    
    # 读取 SWB 数据
    swb_file = os.path.join(CURRENT_DIR, 'swb_surface_water.csv')
    swb_data = read_csv(swb_file)
    
    if swb_data:
        swb_mapping = FIELD_MAPPINGS.get('water_bodies_swb', {})
        for row in swb_data:
            wb = {'water_type': 'SWB'}
            for old_key, value in row.items():
                new_key = swb_mapping.get(old_key, old_key)
                wb[new_key] = value
            water_bodies.append(wb)
        print(f'  SWB records: {len(swb_data)}')
    else:
        print(f'  SWB records: 0 (file not found - manual download needed)')
    
    # 读取 GWB 数据
    gwb_file = os.path.join(CURRENT_DIR, 'gwb_groundwater.csv')
    gwb_data = read_csv(gwb_file)
    
    if gwb_data:
        gwb_mapping = FIELD_MAPPINGS.get('water_bodies_gwb', {})
        for row in gwb_data:
            wb = {'water_type': 'GWB'}
            for old_key, value in row.items():
                new_key = gwb_mapping.get(old_key, old_key)
                wb[new_key] = value
            water_bodies.append(wb)
        print(f'  GWB records: {len(gwb_data)}')
    else:
        print(f'  GWB records: 0 (file not found)')
    
    print(f'  Total water_bodies: {len(water_bodies)}')
    
    return water_bodies


def merge_protected_areas():
    """合并 SWB 和 GWB 保护区为统一表"""
    print('\n[Transform] Merging protected areas...')
    
    protected_areas = []
    
    # 读取 SWB 保护区
    swb_pa_file = os.path.join(CURRENT_DIR, 'swb_protected_areas.csv')
    swb_pa = read_csv(swb_pa_file)
    
    if swb_pa:
        for row in swb_pa:
            pa = {
                'water_body_code': row.get('euSurfaceWaterBodyCode'),
                'eu_protected_area_code': row.get('euProtectedAreaCode'),
                'protected_area_type': row.get('protectedAreaType'),
                'objectives_set': row.get('protectedAreaObjectivesSet'),
                'objectives_met': row.get('protectedAreaObjectivesMet'),
            }
            protected_areas.append(pa)
        print(f'  SWB protected areas: {len(swb_pa)}')
    else:
        print(f'  SWB protected areas: 0 (file not found - manual download needed)')
    
    # 读取 GWB 保护区
    gwb_pa_file = os.path.join(CURRENT_DIR, 'gwb_protected_areas.csv')
    gwb_pa = read_csv(gwb_pa_file)
    
    if gwb_pa:
        for row in gwb_pa:
            pa = {
                'water_body_code': row.get('euGroundWaterBodyCode'),
                'eu_protected_area_code': row.get('euProtectedAreaCode'),
                'protected_area_type': row.get('protectedAreaType'),
                'objectives_set': row.get('protectedAreaObjectivesSet'),
                'objectives_met': row.get('protectedAreaObjectivesMet'),
            }
            protected_areas.append(pa)
        print(f'  GWB protected areas: {len(gwb_pa)}')
    else:
        print(f'  GWB protected areas: 0 (file not found - manual download needed)')
    
    print(f'  Total protected_areas: {len(protected_areas)}')
    
    return protected_areas


def transform_discharge_points():
    """转换排放点数据，合并水体代码"""
    print('\n[Transform] Processing discharge_points...')
    
    dcp_file = os.path.join(CURRENT_DIR, 'discharge_points.csv')
    dcp_data = read_csv(dcp_file)
    
    mapping = FIELD_MAPPINGS.get('discharge_points', {})
    transformed = []
    
    for row in dcp_data:
        new_row = {}
        for old_key, value in row.items():
            new_key = mapping.get(old_key, old_key)
            new_row[new_key] = value
        
        # 根据 is_surface_water 确定 water_body_code
        is_surface = new_row.get('is_surface_water', '').lower() == 'true'
        if is_surface:
            new_row['water_body_code'] = new_row.get('swb_water_body_code')
        else:
            new_row['water_body_code'] = new_row.get('gwb_water_body_code')
        
        # 清理临时字段
        new_row.pop('swb_water_body_code', None)
        new_row.pop('gwb_water_body_code', None)
        
        transformed.append(new_row)
    
    print(f'  Discharge points: {len(transformed)}')
    return transformed


def extract_countries():
    """从各表中提取唯一的国家代码"""
    print('\n[Transform] Extracting countries...')
    
    countries = set()
    
    # 从 plants 提取
    plants_file = os.path.join(CURRENT_DIR, 'plants.csv')
    plants = read_csv(plants_file)
    for row in plants:
        code = row.get('CountryCode')
        if code:
            countries.add(code)
    
    # 从 water bodies 提取
    swb_file = os.path.join(CURRENT_DIR, 'swb_surface_water.csv')
    swb = read_csv(swb_file)
    for row in swb:
        code = row.get('countryCode')
        if code:
            countries.add(code)
    
    result = [{'country_code': code, 'country_name': ''} for code in sorted(countries)]
    print(f'  Countries: {len(result)}')
    return result


def create_sensitivity_table():
    """创建 sensitivity 查找表"""
    print('\n[Transform] Creating sensitivity lookup table...')
    print(f'  Sensitivity codes: {len(SENSITIVITY_DATA)}')
    return SENSITIVITY_DATA


def main():
    """主转换流程"""
    print('=' * 60)
    print('DISCO Data Transform')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Source: {CURRENT_DIR}')
    
    # 确保目录存在
    os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    
    # 1. 提取国家
    countries = extract_countries()
    write_csv(os.path.join(TRANSFORMED_DIR, 'countries.csv'), countries)
    
    # 2. 创建 sensitivity 表
    sensitivity = create_sensitivity_table()
    write_csv(os.path.join(TRANSFORMED_DIR, 'sensitivity.csv'), sensitivity)
    
    # 3. 转换 plants
    print('\n[Transform] Processing plants...')
    plants_data = read_csv(os.path.join(CURRENT_DIR, 'plants.csv'))
    plants_transformed = transform_fields(plants_data, FIELD_MAPPINGS.get('plants', {}))
    write_csv(os.path.join(TRANSFORMED_DIR, 'plants.csv'), plants_transformed)
    print(f'  Plants: {len(plants_transformed)}')
    
    # 4. 转换 agglomerations
    print('\n[Transform] Processing agglomerations...')
    agg_data = read_csv(os.path.join(CURRENT_DIR, 'agglomerations.csv'))
    agg_transformed = transform_fields(agg_data, FIELD_MAPPINGS.get('agglomerations', {}))
    write_csv(os.path.join(TRANSFORMED_DIR, 'agglomerations.csv'), agg_transformed)
    print(f'  Agglomerations: {len(agg_transformed)}')
    
    # 5. 转换 report_periods
    print('\n[Transform] Processing report_periods...')
    rep_data = read_csv(os.path.join(CURRENT_DIR, 'report_periods.csv'))
    rep_transformed = transform_fields(rep_data, FIELD_MAPPINGS.get('report_periods', {}))
    write_csv(os.path.join(TRANSFORMED_DIR, 'report_periods.csv'), rep_transformed)
    print(f'  Report periods: {len(rep_transformed)}')
    
    # 6. 合并 water_bodies
    water_bodies = merge_water_bodies()
    write_csv(os.path.join(TRANSFORMED_DIR, 'water_bodies.csv'), water_bodies)
    
    # 7. 合并 protected_areas
    protected_areas = merge_protected_areas()
    write_csv(os.path.join(TRANSFORMED_DIR, 'water_body_protected_areas.csv'), protected_areas)
    
    # 8. 转换 discharge_points
    discharge_points = transform_discharge_points()
    write_csv(os.path.join(TRANSFORMED_DIR, 'discharge_points.csv'), discharge_points)
    
    # 打印总结
    print('\n' + '=' * 60)
    print('Transform Summary')
    print('=' * 60)
    files = [f for f in os.listdir(TRANSFORMED_DIR) if f.endswith('.csv')]
    for f in sorted(files):
        filepath = os.path.join(TRANSFORMED_DIR, f)
        data = read_csv(filepath)
        print(f'  ✓ {f}: {len(data)} rows')
    
    print('\n' + '=' * 60)
    print('✅ Transform completed!')
    print(f'   Data saved to: {TRANSFORMED_DIR}')


if __name__ == '__main__':
    main()
