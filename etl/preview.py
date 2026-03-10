"""
预览 DATA 文件夹中的数据
不会对 Supabase 做任何修改
"""
import os
import sys
import csv
import json
from datetime import datetime

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.config import CURRENT_DIR, MANIFEST_FILE

TRANSFORMED_DIR = os.path.join(CURRENT_DIR, 'transformed')


def read_csv_preview(filepath, max_rows=5):
    """读取 CSV 文件预览"""
    if not os.path.exists(filepath):
        return None, 0, []
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        
    columns = list(rows[0].keys()) if rows else []
    preview = rows[:max_rows]
    return preview, len(rows), columns


def main():
    """预览数据"""
    print('=' * 60)
    print('DISCO Data Preview')
    print('=' * 60)
    
    # 显示 manifest 信息
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
        print(f"\nLast Extract: {manifest.get('extract_date')} {manifest.get('extract_time')}")
        print(f"Total Rows: {manifest.get('total_rows', 0)}")
    else:
        print('\n⚠ No manifest found. Run extract.py first.')
    
    # 显示原始数据
    print('\n' + '-' * 40)
    print('Raw Data (DATA/current/)')
    print('-' * 40)
    
    if os.path.exists(CURRENT_DIR):
        files = [f for f in os.listdir(CURRENT_DIR) if f.endswith('.csv')]
        for f in sorted(files):
            filepath = os.path.join(CURRENT_DIR, f)
            _, total, columns = read_csv_preview(filepath)
            print(f'\n  {f}')
            print(f'    Rows: {total}, Columns: {len(columns)}')
    else:
        print('  No data found.')
    
    # 显示转换后数据
    print('\n' + '-' * 40)
    print('Transformed Data (DATA/current/transformed/)')
    print('-' * 40)
    
    if os.path.exists(TRANSFORMED_DIR):
        files = [f for f in os.listdir(TRANSFORMED_DIR) if f.endswith('.csv')]
        for f in sorted(files):
            filepath = os.path.join(TRANSFORMED_DIR, f)
            preview, total, columns = read_csv_preview(filepath, max_rows=3)
            print(f'\n  {f}')
            print(f'    Rows: {total}, Columns: {len(columns)}')
            if columns:
                print(f'    Columns: {", ".join(columns[:8])}...')
    else:
        print('  No transformed data. Run transform.py first.')
    
    print('\n' + '=' * 60)
    print('Commands:')
    print('  python etl/extract.py   - Download from DISCO')
    print('  python etl/transform.py - Transform data')
    print('  python etl/deploy.py    - Deploy to Supabase')


if __name__ == '__main__':
    main()
