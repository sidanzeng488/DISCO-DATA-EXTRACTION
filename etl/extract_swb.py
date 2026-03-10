"""
单独重新下载 SWB (Surface Water Body) 表
用于补充因 API 超时而中断的下载
"""
import os
import sys
import csv
from datetime import datetime

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discodata_client import DiscoDataClient
from etl.config import CURRENT_DIR

def main():
    print('=' * 60)
    print('SWB (Surface Water Body) Re-download')
    print('=' * 60)
    print(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
    # 初始化客户端
    print('\n[Init] Connecting to DISCO API...')
    client = DiscoDataClient()
    
    disco_table = '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]'
    filename = 'swb_surface_water.csv'
    filepath = os.path.join(CURRENT_DIR, filename)
    
    print(f'\n[Extract] swb_surface_water')
    print(f'  Source: {disco_table}')
    
    try:
        query = f'SELECT * FROM {disco_table}'
        data = client.query(query)
        
        if not data:
            print(f'  Warning: No data returned')
            return
        
        # 保存为 CSV
        fieldnames = list(data[0].keys())
        
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        print(f'\n  ✅ Saved: {filename} ({len(data)} rows, {len(fieldnames)} columns)')
        
    except Exception as e:
        print(f'  ❌ Error: {e}')

if __name__ == '__main__':
    main()
