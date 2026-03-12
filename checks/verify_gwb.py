"""验证 dcpGroundWater 是否链接到 GWB 表"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.discodata_client import DiscoDataClient
client = DiscoDataClient()

# 取一个 dcpGroundWater 值
gw_sample = client.query("""
    SELECT TOP 1 dcpGroundWater
    FROM [WISE_UWWTD].[v1r1].[T_DischargePoints]
    WHERE dcpGroundWater IS NOT NULL
""")
sample_id = gw_sample[0].get('dcpGroundWater')
print(f"dcpGroundWater 样本值: {sample_id}")

# 尝试在 GWB 表中匹配
match = client.query(f"""
    SELECT TOP 1 euGroundWaterBodyCode, countryCode
    FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody]
    WHERE euGroundWaterBodyCode = '{sample_id}'
""")
if match:
    print(f"✅ GWB 匹配成功: {match[0]}")
else:
    print("❌ GWB 未匹配，查看奥地利(AT) GWB ID 格式...")
    # 尝试 LIKE 匹配
    like_match = client.query("""
        SELECT TOP 5 euGroundWaterBodyCode
        FROM [WISE_WFD].[v2r1].[GWB_GroundWaterBody]
        WHERE countryCode = 'AT'
    """)
    print("奥地利(AT) GWB euGroundWaterBodyCode 格式:")
    for row in like_match:
        print(f"  {row.get('euGroundWaterBodyCode')}")
