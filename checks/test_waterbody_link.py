"""
测试 dcpWaterbodyID 链接到哪个主表
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.discodata_client import DiscoDataClient

client = DiscoDataClient()

print("=" * 60)
print("1. 查看 T_DischargePoints 表中 dcpWaterbodyID 的样本数据")
print("=" * 60)

# 获取 DischargePoints 表中带有 dcpWaterbodyID 的数据
discharge_data = client.query("""
    SELECT TOP 10 dcpCode, dcpWaterbodyID, dcpReceivingWater 
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    WHERE dcpWaterbodyID IS NOT NULL
""")
print("\nDischarge Points with WaterbodyID:")
for row in discharge_data:
    print(f"  dcpCode: {row.get('dcpCode')}, dcpWaterbodyID: {row.get('dcpWaterbodyID')}")

# 获取一个样本 WaterbodyID
if discharge_data:
    sample_waterbody_id = discharge_data[0].get('dcpWaterbodyID')
    print(f"\n样本 dcpWaterbodyID: {sample_waterbody_id}")
    
    print("\n" + "=" * 60)
    print("2. 查看 SWB_SurfaceWaterBody 表的 ID 字段")
    print("=" * 60)
    
    # 看 SWB 表有什么 ID 字段
    swb_columns = client.get_columns("[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]")
    id_columns = [c for c in swb_columns if 'id' in c.lower() or 'code' in c.lower()]
    print(f"\nSWB 表中包含 'id' 或 'code' 的列: {id_columns}")
    
    # 查看 SWB 表的 ID 样本
    print("\n" + "=" * 60)
    print("3. 查看 SWB 表的 ID 样本数据")
    print("=" * 60)
    
    swb_sample = client.query("""
        SELECT TOP 5 euSurfaceWaterBodyCode, surfaceWaterBodyName, countryCode
        FROM [WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]
    """)
    print("\nSWB 表 ID 样本:")
    for row in swb_sample:
        print(f"  euSurfaceWaterBodyCode: {row.get('euSurfaceWaterBodyCode')}, Name: {row.get('surfaceWaterBodyName')}")
    
    # 尝试匹配
    print("\n" + "=" * 60)
    print("4. 尝试用 dcpWaterbodyID 匹配 SWB 表")
    print("=" * 60)
    
    # 尝试用 euSurfaceWaterBodyCode 匹配
    try:
        match1 = client.query(f"""
            SELECT TOP 1 euSurfaceWaterBodyCode, surfaceWaterBodyName, countryCode
            FROM [WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]
            WHERE euSurfaceWaterBodyCode = '{sample_waterbody_id}'
        """)
        if match1:
            print(f"\n✅ 匹配成功！dcpWaterbodyID 链接到 SWB_SurfaceWaterBody.euSurfaceWaterBodyCode")
            print(f"   匹配数据: {match1[0]}")
        else:
            print(f"\n❌ euSurfaceWaterBodyCode 未匹配到: {sample_waterbody_id}")
            
            # 尝试部分匹配或 LIKE 查询
            print("\n尝试 LIKE 查询，看奥地利(AT)的 SWB 数据格式...")
            match_like = client.query(f"""
                SELECT TOP 5 euSurfaceWaterBodyCode, surfaceWaterBodyName, countryCode
                FROM [WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]
                WHERE countryCode = 'AT'
            """)
            if match_like:
                print("奥地利(AT)的 SWB 样本数据:")
                for row in match_like:
                    print(f"  euCode: {row.get('euSurfaceWaterBodyCode')}, Name: {row.get('surfaceWaterBodyName')}")
    except Exception as e:
        print(f"   查询错误: {e}")

    # 也检查 GWB 表
    print("\n" + "=" * 60)
    print("5. 查看 GWB_GroundWaterBody 表的 ID 字段")
    print("=" * 60)
    
    gwb_columns = client.get_columns("[WISE_WFD].[v2r1].[GWB_GroundWaterBody]")
    id_columns = [c for c in gwb_columns if 'id' in c.lower() or 'code' in c.lower()]
    print(f"\nGWB 表中包含 'id' 或 'code' 的列: {id_columns}")

print("\n" + "=" * 60)
print("测试完成！")
print("=" * 60)
