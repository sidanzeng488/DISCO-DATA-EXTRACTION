"""检查 T_UWWTPS 表是否有重复值"""
from discodata_client import DiscoDataClient
client = DiscoDataClient()

print("=" * 60)
print("检查 T_UWWTPS (Plants) 表是否有重复值")
print("=" * 60)

# 1. 总记录数
total = client.query("""
    SELECT COUNT(*) as total FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
""")
print(f"\n总记录数: {total[0].get('total')}")

# 2. 唯一 uwwCode 数量
unique = client.query("""
    SELECT COUNT(DISTINCT uwwCode) as unique_count FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
""")
print(f"唯一 uwwCode 数量: {unique[0].get('unique_count')}")

# 3. 查找重复的 uwwCode
print("\n" + "=" * 60)
print("查找重复的 uwwCode:")
print("=" * 60)

duplicates = client.query("""
    SELECT uwwCode, COUNT(*) as cnt
    FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
    GROUP BY uwwCode
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
""")

if duplicates:
    print(f"\n发现 {len(duplicates)} 个重复的 uwwCode:")
    for row in duplicates[:20]:
        print(f"  {row.get('uwwCode')}: {row.get('cnt')} 条记录")
    
    # 查看一个重复记录的详情
    if duplicates:
        sample_code = duplicates[0].get('uwwCode')
        print(f"\n查看重复记录详情 (uwwCode = {sample_code}):")
        details = client.query(f"""
            SELECT uwwCode, uwwName, CountryCode, repCode, uwwCapacity
            FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
            WHERE uwwCode = '{sample_code}'
        """)
        for row in details:
            print(f"  Name: {row.get('uwwName')}, Country: {row.get('CountryCode')}, repCode: {row.get('repCode')}, Capacity: {row.get('uwwCapacity')}")
else:
    print("\n✅ 没有重复的 uwwCode，每个 uwwCode 都是唯一的！")

# 4. 检查 uwwUWWTPSID 是否唯一
print("\n" + "=" * 60)
print("检查 uwwUWWTPSID 是否唯一:")
print("=" * 60)

unique_id = client.query("""
    SELECT COUNT(DISTINCT uwwUWWTPSID) as unique_count FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]
""")
print(f"唯一 uwwUWWTPSID 数量: {unique_id[0].get('unique_count')}")
