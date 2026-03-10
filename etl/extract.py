"""
从 DISCO API 提取数据到 DATA/current/
自动备份旧数据到 DATA/backup/
"""
import os
import sys
import csv
import json
import shutil
from datetime import datetime

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discodata_client import DiscoDataClient
from etl.config import (
    DISCO_TABLES_AUTO,
    DISCO_TABLES_MANUAL,
    CURRENT_DIR, 
    BACKUP_DIR, 
    MANIFEST_FILE,
    MAX_BACKUPS
)


def backup_current_data():
    """将当前数据备份到 backup 文件夹"""
    if not os.path.exists(CURRENT_DIR):
        return
    
    # 检查 current 文件夹是否有数据
    files = [f for f in os.listdir(CURRENT_DIR) if f.endswith('.csv')]
    if not files:
        return
    
    # 读取上次提取时间，如果没有则使用当前时间
    extract_date = datetime.now().strftime('%Y-%m-%d')
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
            manifest = json.load(f)
            saved_date = manifest.get('extract_date')
            if saved_date:  # 只有当有值时才使用
                extract_date = saved_date
    
    # 创建备份目录
    backup_path = os.path.join(BACKUP_DIR, extract_date)
    
    # 如果备份已存在，添加时间戳
    if os.path.exists(backup_path):
        timestamp = datetime.now().strftime('%H%M%S')
        backup_path = os.path.join(BACKUP_DIR, f'{extract_date}_{timestamp}')
    
    print(f'\n[Backup] Moving current data to {backup_path}')
    shutil.move(CURRENT_DIR, backup_path)
    os.makedirs(CURRENT_DIR, exist_ok=True)
    
    # 清理旧备份，只保留最新的 MAX_BACKUPS 个
    cleanup_old_backups()


def cleanup_old_backups():
    """清理旧备份，只保留最新的 MAX_BACKUPS 个"""
    if not os.path.exists(BACKUP_DIR):
        return
    
    backups = sorted([
        d for d in os.listdir(BACKUP_DIR) 
        if os.path.isdir(os.path.join(BACKUP_DIR, d))
    ], reverse=True)
    
    # 删除超出数量的旧备份
    for old_backup in backups[MAX_BACKUPS:]:
        old_path = os.path.join(BACKUP_DIR, old_backup)
        print(f'[Cleanup] Removing old backup: {old_backup}')
        try:
            shutil.rmtree(old_path)
        except PermissionError:
            print(f'  Warning: Could not delete {old_backup} (file in use)')
        except Exception as e:
            print(f'  Warning: {e}')


def extract_table(client, table_name, table_config):
    """从 DISCO 提取单个表的数据"""
    disco_table = table_config['disco_table']
    filename = table_config['filename']
    columns = table_config.get('columns')
    
    print(f'\n[Extract] {table_name}')
    print(f'  Source: {disco_table}')
    
    try:
        # 构建查询
        if columns:
            cols = ', '.join(columns)
            query = f'SELECT {cols} FROM {disco_table}'
        else:
            query = f'SELECT * FROM {disco_table}'
        
        # 获取数据
        data = client.query(query)
        
        if not data:
            print(f'  Warning: No data returned')
            return 0
        
        # 保存为 CSV
        filepath = os.path.join(CURRENT_DIR, filename)
        fieldnames = list(data[0].keys())
        
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f'  Saved: {filename} ({len(data)} rows, {len(fieldnames)} columns)')
        return len(data)
        
    except Exception as e:
        print(f'  Error: {e}')
        return 0


def update_manifest(stats):
    """更新 manifest.json"""
    manifest = {
        'extract_date': datetime.now().strftime('%Y-%m-%d'),
        'extract_time': datetime.now().strftime('%H:%M:%S'),
        'extract_timestamp': datetime.now().isoformat(),
        'tables': stats,
        'total_rows': sum(stats.values()),
    }
    
    with open(MANIFEST_FILE, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    
    print(f'\n[Manifest] Updated {MANIFEST_FILE}')


def check_manual_tables():
    """检查需要手动下载的大表是否存在"""
    missing = []
    existing = []
    
    for table_name, table_config in DISCO_TABLES_MANUAL.items():
        filepath = os.path.join(CURRENT_DIR, table_config['filename'])
        if os.path.exists(filepath):
            # 检查文件大小
            size = os.path.getsize(filepath)
            if size > 1000:  # 大于 1KB 认为有效
                existing.append((table_name, filepath))
            else:
                missing.append(table_config)
        else:
            missing.append(table_config)
    
    return missing, existing


def main():
    """主提取流程"""
    print('=' * 60)
    print('DISCO Data Extract')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # 确保目录存在
    os.makedirs(CURRENT_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    
    # 备份当前数据
    backup_current_data()
    
    # 初始化客户端
    print('\n[Init] Connecting to DISCO API...')
    client = DiscoDataClient()
    
    # 提取小表（通过 API）
    stats = {}
    print('\n' + '-' * 40)
    print('Auto Download (via API)')
    print('-' * 40)
    
    for table_name, table_config in DISCO_TABLES_AUTO.items():
        rows = extract_table(client, table_name, table_config)
        stats[table_name] = rows
    
    # 检查大表
    missing_manual, existing_manual = check_manual_tables()
    
    for table_name, filepath in existing_manual:
        # 统计已有文件的行数
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            rows = sum(1 for _ in f) - 1  # 减去表头
        stats[table_name] = rows
    
    # 更新 manifest
    update_manifest(stats)
    
    # 打印总结
    print('\n' + '=' * 60)
    print('Extract Summary')
    print('=' * 60)
    for table_name, rows in stats.items():
        status = '✓' if rows > 0 else '✗'
        print(f'  {status} {table_name}: {rows} rows')
    print(f'\n  Total: {sum(stats.values())} rows')
    
    # 提示手动下载大表
    if missing_manual:
        print('\n' + '=' * 60)
        print('⚠️  Manual Download Required')
        print('=' * 60)
        print('The following large tables need manual export from DISCO website:')
        print('(Click Export -> CSV on each page)\n')
        
        for config in missing_manual:
            print(f'  📥 {config["filename"]}')
            print(f'     URL: {config["url"]}')
            print(f'     Estimated: {config["estimated_rows"]} rows')
            print(f'     Save to: DATA/current/{config["filename"]}')
            print()
    
    print('=' * 60)
    if missing_manual:
        print('⏳ Waiting for manual downloads...')
        print('   After downloading, run: python etl/transform.py')
    else:
        print('✅ Extract completed!')
        print('   Next step: python etl/transform.py')
    print(f'   Data folder: {CURRENT_DIR}')


if __name__ == '__main__':
    main()
