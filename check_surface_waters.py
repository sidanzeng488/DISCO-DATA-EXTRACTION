"""
分析 dcpSurfaceWaters 与 SWB/GWB 的关系
"""
from discodata_client import DiscoDataClient
client = DiscoDataClient()

print("=" * 60)
print("分析 dcpSurfaceWaters 与 SWB/GWB 的关系")
print("=" * 60)

# 查看 dcpSurfaceWaters 的值分布
print("\n1. dcpSurfaceWaters 值分布:")
dist = client.query("""
    SELECT dcpSurfaceWaters, COUNT(*) as cnt
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    GROUP BY dcpSurfaceWaters
""")
for row in dist:
    print(f"   {row.get('dcpSurfaceWaters')}: {row.get('cnt')} 条")

# 验证: dcpSurfaceWaters=1 时是否有 dcpWaterbodyID (SWB)
print("\n2. dcpSurfaceWaters=1 时，dcpWaterbodyID 和 dcpGroundWater 情况:")
swb_check = client.query("""
    SELECT TOP 10 dcpCode, dcpSurfaceWaters, dcpWaterbodyID, dcpGroundWater
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    WHERE dcpSurfaceWaters = 1
""")
has_waterbody = 0
has_groundwater = 0
for row in swb_check:
    if row.get('dcpWaterbodyID'):
        has_waterbody += 1
    if row.get('dcpGroundWater'):
        has_groundwater += 1
    print(f"   WaterbodyID={row.get('dcpWaterbodyID')}, GroundWater={row.get('dcpGroundWater')}")
print(f"\n   统计: 有WaterbodyID={has_waterbody}/10, 有GroundWater={has_groundwater}/10")

# 验证: dcpSurfaceWaters=0 时是否有 dcpGroundWater (GWB)
print("\n3. dcpSurfaceWaters=0 时，dcpWaterbodyID 和 dcpGroundWater 情况:")
gwb_check = client.query("""
    SELECT TOP 10 dcpCode, dcpSurfaceWaters, dcpWaterbodyID, dcpGroundWater
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    WHERE dcpSurfaceWaters = 0
""")
has_waterbody = 0
has_groundwater = 0
for row in gwb_check:
    if row.get('dcpWaterbodyID'):
        has_waterbody += 1
    if row.get('dcpGroundWater'):
        has_groundwater += 1
    print(f"   WaterbodyID={row.get('dcpWaterbodyID')}, GroundWater={row.get('dcpGroundWater')}")
print(f"\n   统计: 有WaterbodyID={has_waterbody}/10, 有GroundWater={has_groundwater}/10")

# 也看看 dcpWaterBodyType 的关系
print("\n4. dcpSurfaceWaters 与 dcpWaterBodyType 的关系:")
relation = client.query("""
    SELECT dcpSurfaceWaters, dcpWaterBodyType, COUNT(*) as cnt
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    GROUP BY dcpSurfaceWaters, dcpWaterBodyType
    ORDER BY dcpSurfaceWaters, cnt DESC
""")
for row in relation:
    print(f"   SurfaceWaters={row.get('dcpSurfaceWaters')}, WaterBodyType={row.get('dcpWaterBodyType')}: {row.get('cnt')} 条")

print("\n" + "=" * 60)
print("结论")
print("=" * 60)
