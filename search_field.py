"""
在 WFD 表中搜索 ReportNetEnvelopeFileId 字段
"""
from discodata_client import DiscoDataClient

client = DiscoDataClient()

wfd_tables = [
    '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]',
    '[WISE_WFD].[v2r1].[GWB_GroundWaterBody]',
    '[WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]',
    # 可能的保护区相关表
    '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]',
    '[WISE_WFD].[v2r1].[ProtectedArea]',
    '[WISE_WFD].[v2r1].[SWB_ProtectedArea]',
]

search_term = 'reportnetenvelopefileid'

print('在 WFD 表中搜索 ReportNetEnvelopeFileId...\n')
print('=' * 60)

for table in wfd_tables:
    try:
        cols = client.get_columns(table)
        print(f'\n{table}:')
        print(f'  共 {len(cols)} 列')
        
        # 查找包含 report/envelope/file 的字段
        related = [c for c in cols if 'report' in c.lower() or 'envelope' in c.lower() or 'file' in c.lower()]
        if related:
            print(f'  相关字段: {related}')
        
        # 精确匹配
        for col in cols:
            if col.lower() == search_term:
                print(f'  ✅ 找到: {col}')
    except Exception as e:
        print(f'\n{table}: 表不存在或无法访问')
