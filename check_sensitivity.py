"""查看 DISCO 中的敏感区域数据"""
from discodata_client import DiscoDataClient
client = DiscoDataClient()

print("T_DischargePoints 中的敏感区域字段:")
print("=" * 60)

data = client.query("""
    SELECT DISTINCT dcpTypeOfReceivingArea, rcaCode, COUNT(*) as cnt
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    GROUP BY dcpTypeOfReceivingArea, rcaCode
    ORDER BY cnt DESC
""")

print(f"\n找到 {len(data)} 种组合:")
for row in data:
    print(f"  Type: {row.get('dcpTypeOfReceivingArea'):10}, rcaCode: {row.get('rcaCode')}, Count: {row.get('cnt')}")

# 尝试查找敏感区域表
print("\n" + "=" * 60)
print("尝试查找 T_ReceivingAreas 或 T_SensitiveAreas 表:")
print("=" * 60)

try:
    rca_cols = client.get_columns("[WISE_UWWTD].[v1r1].[T_ReceivingAreas]")
    print(f"\nT_ReceivingAreas 表存在！列: {rca_cols}")
except:
    print("\nT_ReceivingAreas 表不存在")

try:
    sa_cols = client.get_columns("[WISE_UWWTD].[v1r1].[T_SensitiveAreas]")
    print(f"\nT_SensitiveAreas 表存在！列: {sa_cols}")
except:
    print("\nT_SensitiveAreas 表不存在")
