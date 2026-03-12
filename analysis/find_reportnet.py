"""
搜索 ReportNetEnvelopeFileId 字段
尝试更多可能的 UWWTD 表
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.discodata_client import DiscoDataClient

client = DiscoDataClient()

# 可能的表名（基于 UWWTD Art17 相关）
possible_tables = [
    # 已知的表
    '[WISE_UWWTD].[v1r1].[T_UWWTPS]',
    '[WISE_UWWTD].[v1r1].[T_Agglomerations]',
    '[WISE_UWWTD].[v1r1].[T_UWWTPAgglos]',
    '[WISE_UWWTD].[v1r1].[T_DischargePoints]',
    '[WISE_UWWTD].[v1r1].[T_Art17_FLAUWWTP]',
    # Art17 相关可能的表
    '[WISE_UWWTD].[v1r1].[T_FLAUWWTPs]',
    '[WISE_UWWTD].[v1r1].[FLAUWWTPs]',
    '[WISE_UWWTD].[v1r1].[T_Art17]',
    '[WISE_UWWTD].[v1r1].[UWWTDArt17]',
    '[WISE_UWWTD].[v1r1].[T_UWWTDArt17]',
    # 报告相关
    '[WISE_UWWTD].[v1r1].[T_Reports]',
    '[WISE_UWWTD].[v1r1].[T_Envelope]',
    '[WISE_UWWTD].[v1r1].[T_Envelopes]',
    '[WISE_UWWTD].[v1r1].[Reports]',
    # 敏感区域相关
    '[WISE_UWWTD].[v1r1].[T_ReceivingAreas]',
    '[WISE_UWWTD].[v1r1].[T_SensitiveAreas]',
    '[WISE_UWWTD].[v1r1].[T_ReceivingAreasUWWTPs]',
    # FLA 相关
    '[WISE_UWWTD].[v1r1].[T_FLAAgglo]',
    '[WISE_UWWTD].[v1r1].[T_Art17_FLAAgglo]',
]

search_field = 'reportnetenvelopefileid'

print("搜索 ReportNetEnvelopeFileId 字段...")
print("=" * 60)

found_tables = []
valid_tables = []

for table in possible_tables:
    try:
        cols = client.get_columns(table)
        valid_tables.append(table)
        
        # 搜索字段
        for col in cols:
            if search_field in col.lower():
                print(f"\n✅ 找到！表: {table}")
                print(f"   字段: {col}")
                found_tables.append((table, col))
        
        # 也列出包含 'report' 或 'envelope' 或 'file' 的字段
        related = [c for c in cols if 'report' in c.lower() or 'envelope' in c.lower() or 'fileid' in c.lower()]
        if related:
            print(f"\n{table} 有相关字段:")
            for r in related:
                print(f"   - {r}")
    except Exception as e:
        # 表不存在
        pass

print("\n" + "=" * 60)
print(f"有效的表 ({len(valid_tables)} 个):")
for t in valid_tables:
    print(f"  {t}")

print("\n" + "=" * 60)
if found_tables:
    print(f"找到 ReportNetEnvelopeFileId 在 {len(found_tables)} 个表中")
else:
    print("❌ 在所有搜索的表中都没有找到 ReportNetEnvelopeFileId 字段")
    print("\n这个字段可能:")
    print("  1. 在 DISCODATA API 不公开的表中")
    print("  2. 是 ReportNet 系统内部使用的元数据")
    print("  3. 需要通过其他 EEA 数据接口获取")
