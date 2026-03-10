"""
Supabase 数据库连接测试
"""
import psycopg2
from config import (
    SUPABASE_HOST, 
    SUPABASE_PORT, 
    SUPABASE_DATABASE, 
    SUPABASE_USER, 
    SUPABASE_PASSWORD
)

def get_connection():
    """获取数据库连接"""
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )

if __name__ == '__main__':
    print('=' * 50)
    print('Testing Supabase Connection...')
    print('=' * 50)
    
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        print(f'✅ Connected to: {SUPABASE_HOST}')
        print()
        
        # 测试连接
        cur.execute('SELECT version();')
        print('PostgreSQL version:', cur.fetchone()[0][:50] + '...')
        print()
        
        # 列出所有公共表
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        tables = cur.fetchall()
        print(f'Public tables ({len(tables)}):')
        print('-' * 40)
        if tables:
            for table in tables:
                print(f'  - {table[0]}')
        else:
            print('  (empty - no tables yet)')
        
        cur.close()
        conn.close()
        print()
        print('✅ Connection test successful!')
        
    except Exception as e:
        print(f'❌ Connection failed: {e}')
        print()
        print('Please check your config.py settings.')
