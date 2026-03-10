"""检查 plants 表结构，确认需要填充的字段"""
import psycopg2
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD

def main():
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    print("=" * 60)
    print("plants 表结构")
    print("=" * 60)
    
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'plants'
        ORDER BY ordinal_position;
    """)
    
    columns = cur.fetchall()
    print(f"共 {len(columns)} 列:\n")
    
    for col_name, col_type, nullable in columns:
        print(f"  {col_name:<40} {col_type:<20} {'NULL' if nullable == 'YES' else 'NOT NULL'}")
    
    # 检查是否有日期相关字段
    print("\n" + "=" * 60)
    print("查找日期相关字段")
    print("=" * 60)
    
    date_related = [c for c in columns if 'date' in c[0].lower() or 'published' in c[0].lower() or 'situation' in c[0].lower()]
    
    if date_related:
        print("找到日期相关字段:")
        for col_name, col_type, _ in date_related:
            print(f"  {col_name}: {col_type}")
    else:
        print("未找到 date_published 或 date_situation_at 字段")
        print("\n建议添加以下字段:")
        print("  - date_published (date)")
        print("  - date_situation_at (date)")
    
    # 检查 report_code 表
    print("\n" + "=" * 60)
    print("report_code 表结构")
    print("=" * 60)
    
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'report_code'
        ORDER BY ordinal_position;
    """)
    
    rc_columns = cur.fetchall()
    if rc_columns:
        for col_name, col_type in rc_columns:
            print(f"  {col_name}: {col_type}")
        
        # 显示样本数据
        cur.execute("SELECT * FROM report_code LIMIT 5")
        rows = cur.fetchall()
        print(f"\n样本数据 ({len(rows)} rows):")
        for row in rows:
            print(f"  {row}")
    else:
        print("report_code 表不存在或为空")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
