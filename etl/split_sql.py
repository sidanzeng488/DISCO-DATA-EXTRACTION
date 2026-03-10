"""
拆分大 SQL 文件为多个小文件
每个文件约 5000 条记录
"""
import os
import re

SQL_DIR = r"C:\Users\Sidan.Zeng\OneDrive - Media Analytics Limited\Desktop\DISCO link\DATA\sql"

def split_sql_file(filename, records_per_file=5000):
    """拆分 SQL 文件"""
    filepath = os.path.join(SQL_DIR, filename)
    
    if not os.path.exists(filepath):
        print(f'File not found: {filename}')
        return
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 找到所有 INSERT 语句块
    # 匹配 "-- Batch X" 开头的块
    batches = re.split(r'-- Batch \d+\n', content)
    batches = [b.strip() for b in batches if b.strip() and 'INSERT' in b]
    
    if not batches:
        print(f'No batches found in {filename}')
        return
    
    print(f'\n[Split] {filename}')
    print(f'  Found {len(batches)} batches')
    
    # 获取文件名前缀
    base_name = filename.replace('.sql', '')
    
    # 每 N 个批次写入一个文件
    batches_per_file = records_per_file // 1000  # 每批 1000 条
    if batches_per_file < 1:
        batches_per_file = 1
    
    file_count = 0
    for i in range(0, len(batches), batches_per_file):
        file_count += 1
        chunk = batches[i:i+batches_per_file]
        
        out_file = os.path.join(SQL_DIR, f'{base_name}_part{file_count:02d}.sql')
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(f'-- {base_name} Part {file_count}\n')
            f.write(f'-- Records: ~{len(chunk) * 1000}\n\n')
            for batch in chunk:
                f.write(batch)
                f.write('\n\n')
        
        print(f'  Created: {base_name}_part{file_count:02d}.sql')
    
    print(f'  Total: {file_count} files')


def main():
    print('=' * 50)
    print('Split SQL Files')
    print('=' * 50)
    
    # 拆分大文件
    split_sql_file('water_bodies.sql', records_per_file=10000)
    split_sql_file('agglomeration.sql', records_per_file=10000)
    split_sql_file('plants.sql', records_per_file=10000)
    split_sql_file('discharge_points.sql', records_per_file=10000)
    split_sql_file('water_body_protected_areas.sql', records_per_file=10000)
    
    print('\n' + '=' * 50)
    print('Done! Execute files in order:')
    print('  water_bodies_part01.sql, part02.sql, ...')
    print('  agglomeration_part01.sql, ...')
    print('  etc.')


if __name__ == '__main__':
    main()
