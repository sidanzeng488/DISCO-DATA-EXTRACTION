"""
执行 Energy Self-Sufficiency 字段添加和更新
基于修订后的 UWWTD (2024) Article 11 要求
"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD


def main():
    print('=' * 60)
    print('Adding Energy Self-Sufficiency Field to Plants Table')
    print('Based on revised UWWTD Article 11: >= 10,000 PE')
    print('=' * 60)
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    # 1. Check if column already exists
    print('\n[1/5] Checking if column exists...')
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'plants' 
        AND column_name = 'requires_energy_self_sufficiency'
    """)
    exists = cur.fetchone()
    
    if exists:
        print('  [OK] Column already exists')
    else:
        print('  Adding column...')
        cur.execute("""
            ALTER TABLE plants 
            ADD COLUMN requires_energy_self_sufficiency BOOLEAN DEFAULT FALSE
        """)
        conn.commit()
        print('  [OK] Column added')
    
    # 2. Add comment
    print('\n[2/5] Adding column comment...')
    cur.execute("""
        COMMENT ON COLUMN plants.requires_energy_self_sufficiency IS 
        'Energy self-sufficiency requirement under revised UWWTD Article 11: TRUE if plant_capacity >= 10,000 PE. Deadline targets: 20%% renewable by 2030, 40%% by 2035, 70%% by 2040, 100%% by 2045.'
    """)
    conn.commit()
    print('  [OK] Comment added')
    
    # 3. Update plants >= 10,000 PE
    print('\n[3/5] Tagging plants >= 10,000 PE...')
    cur.execute("""
        UPDATE plants 
        SET requires_energy_self_sufficiency = TRUE 
        WHERE plant_capacity >= 10000
    """)
    tagged_count = cur.rowcount
    conn.commit()
    print(f'  [OK] Tagged {tagged_count} plants')
    
    # 4. Set FALSE for plants < 10,000 PE
    print('\n[4/5] Setting FALSE for plants < 10,000 PE...')
    cur.execute("""
        UPDATE plants 
        SET requires_energy_self_sufficiency = FALSE 
        WHERE plant_capacity < 10000 OR plant_capacity IS NULL
    """)
    not_tagged_count = cur.rowcount
    conn.commit()
    print(f'  [OK] Updated {not_tagged_count} plants')
    
    # 5. Verify and show statistics
    print('\n[5/5] Verification...')
    cur.execute("""
        SELECT 
            requires_energy_self_sufficiency,
            COUNT(*) as count,
            MIN(plant_capacity) as min_capacity,
            MAX(plant_capacity) as max_capacity,
            ROUND(AVG(plant_capacity)) as avg_capacity
        FROM plants 
        GROUP BY requires_energy_self_sufficiency
        ORDER BY requires_energy_self_sufficiency DESC NULLS LAST
    """)
    
    print('\n' + '=' * 60)
    print('RESULTS: Energy Self-Sufficiency Requirement')
    print('=' * 60)
    print(f'{"Required":<12} {"Count":<10} {"Min PE":<12} {"Max PE":<12} {"Avg PE":<12}')
    print('-' * 60)
    
    for row in cur.fetchall():
        required = 'Yes' if row[0] else ('No' if row[0] is False else 'NULL')
        print(f'{required:<12} {row[1]:<10} {row[2] or "N/A":<12} {row[3] or "N/A":<12} {row[4] or "N/A":<12}')
    
    # Show by country
    print('\n' + '-' * 60)
    print('Plants requiring energy self-sufficiency by country:')
    print('-' * 60)
    cur.execute("""
        SELECT 
            country_code,
            COUNT(*) as count,
            SUM(plant_capacity) as total_capacity
        FROM plants 
        WHERE requires_energy_self_sufficiency = TRUE
        GROUP BY country_code
        ORDER BY count DESC
        LIMIT 15
    """)
    
    print(f'{"Country":<10} {"Count":<10} {"Total PE":<15}')
    for row in cur.fetchall():
        print(f'{row[0]:<10} {row[1]:<10} {row[2]:,.0f}')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('[DONE] Completed!')
    print('=' * 60)


if __name__ == '__main__':
    main()
