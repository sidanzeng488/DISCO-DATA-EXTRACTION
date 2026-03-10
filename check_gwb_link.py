"""
检查 dcpWaterbodyID 是否也链接到 GWB (Ground Water Body)
"""
from discodata_client import DiscoDataClient

client = DiscoDataClient()

print("=" * 60)
print("检查 T_DischargePoints 中的水体类型字段")
print("=" * 60)

# 查看 DischargePoints 表中是否有水体类型字段
cols = client.get_columns("[WISE_UWWTD].[v1r1].[T_DischargePoints]")
water_related = [c for c in cols if 'water' in c.lower() or 'gwb' in c.lower() or 'swb' in c.lower()]
print(f"\n水体相关字段: {water_related}")

# 查看 dcpWaterBodyType 字段的可能值
print("\n" + "=" * 60)
print("查看 dcpWaterBodyType 的值")
print("=" * 60)

type_data = client.query("""
    SELECT DISTINCT dcpWaterBodyType, COUNT(*) as cnt
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    GROUP BY dcpWaterBodyType
""")
print("\ndcpWaterBodyType 值分布:")
for row in type_data:
    print(f"  {row.get('dcpWaterBodyType')}: {row.get('cnt')} 条记录")

# 检查是否有地下水排放点
print("\n" + "=" * 60)
print("检查是否有地下水 (GroundWater) 排放点")
print("=" * 60)

gw_data = client.query("""
    SELECT TOP 10 dcpCode, dcpWaterbodyID, dcpWaterBodyType, dcpGroundWater
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    WHERE dcpGroundWater IS NOT NULL AND dcpGroundWater != ''
""")
print(f"\n有 dcpGroundWater 值的记录数: {len(gw_data)}")
if gw_data:
    print("示例:")
    for row in gw_data[:5]:
        print(f"  dcpCode: {row.get('dcpCode')}")
        print(f"    dcpWaterbodyID: {row.get('dcpWaterbodyID')}")
        print(f"    dcpWaterBodyType: {row.get('dcpWaterBodyType')}")
        print(f"    dcpGroundWater: {row.get('dcpGroundWater')}")
        print()

# 尝试用 dcpWaterbodyID 匹配 GWB 表
print("\n" + "=" * 60)
print("尝试用 dcpWaterbodyID 匹配 GWB 表")
print("=" * 60)

# 获取一些 dcpWaterbodyID 样本
sample_data = client.query("""
    SELECT TOP 50 dcpWaterbodyID
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    WHERE dcpWaterbodyID IS NOT NULL
""")

# 查看 GWB 表的 ID 格式
gwb_sample = client.query("""
    SELECT TOP 5 euGroundWaterBodyCode, countryCode
    FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody]
""")
print("\nGWB 表 ID 格式示例:")
for row in gwb_sample:
    print(f"  euGroundWaterBodyCode: {row.get('euGroundWaterBodyCode')}")

print("\n对比 dcpWaterbodyID 格式:")
for row in sample_data[:5]:
    print(f"  dcpWaterbodyID: {row.get('dcpWaterbodyID')}")

# 尝试匹配
if sample_data:
    sample_id = sample_data[0].get('dcpWaterbodyID')
    try:
        gwb_match = client.query(f"""
            SELECT TOP 1 euGroundWaterBodyCode
            FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody]
            WHERE euGroundWaterBodyCode = '{sample_id}'
        """)
        if gwb_match:
            print(f"\n✅ dcpWaterbodyID '{sample_id}' 在 GWB 表中找到匹配!")
        else:
            print(f"\n❌ dcpWaterbodyID '{sample_id}' 在 GWB 表中未找到匹配")
    except Exception as e:
        print(f"查询错误: {e}")

print("\n" + "=" * 60)
print("结论")
print("=" * 60)
