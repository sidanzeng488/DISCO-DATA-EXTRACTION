"""
更新 plant_requirement_link 表中的 notes
使其更加清晰一目了然
"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD


def main():
    print('=' * 70)
    print('Update plant_requirement_link Notes')
    print('Making notes clear and informative')
    print('=' * 70)
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    # Get requirement IDs
    print('\n[1/5] Getting requirement IDs...')
    cur.execute("SELECT id, treatment_tier FROM uwwtd_requirement")
    requirements = {row[1]: row[0] for row in cur.fetchall()}
    print(f'  Requirements: {requirements}')
    
    # Get plants with sensitive receiving waters
    print('\n[2/5] Finding plants with sensitive receiving waters...')
    cur.execute("""
        SELECT DISTINCT p.plant_id
        FROM plants p
        JOIN discharge_points dp ON p.uwwtp_code = dp.plant_code
        JOIN water_body_protected_areas wpa ON dp.water_body_code = wpa.water_body_code
        WHERE wpa.protected_area_type IN (
            'Bathing waters', 
            'Drinking water protection area', 
            'Shellfish designated water', 
            'Sensitive area', 
            'Nitrate vulnerable zone'
        )
    """)
    sensitive_plant_ids = set(r[0] for r in cur.fetchall())
    print(f'  Found {len(sensitive_plant_ids)} plants with sensitive waters')
    
    # Update Secondary notes
    print('\n[3/5] Updating Secondary treatment notes...')
    if 'Secondary' in requirements:
        cur.execute("""
            UPDATE plant_requirement_link 
            SET notes = '>= 1,000 PE - BOD/COD/TSS removal required by 2035'
            WHERE requirement_id = %s
        """, (requirements['Secondary'],))
        print(f'  [OK] Updated {cur.rowcount} Secondary links')
        conn.commit()
    
    # Update Tertiary notes
    print('\n[4/5] Updating Tertiary treatment notes...')
    if 'Tertiary' in requirements:
        # For >= 150,000 PE plants
        cur.execute("""
            UPDATE plant_requirement_link prl
            SET notes = '>= 150,000 PE - N/P removal required by 2045'
            FROM plants p
            WHERE prl.plant_id = p.plant_id 
            AND prl.requirement_id = %s
            AND p.plant_capacity >= 150000
        """, (requirements['Tertiary'],))
        count_150k = cur.rowcount
        
        # For >= 10,000 PE + sensitive area plants
        cur.execute("""
            UPDATE plant_requirement_link prl
            SET notes = '>= 10,000 PE + sensitive area - N/P removal required by 2045'
            FROM plants p
            WHERE prl.plant_id = p.plant_id 
            AND prl.requirement_id = %s
            AND p.plant_capacity >= 10000 
            AND p.plant_capacity < 150000
        """, (requirements['Tertiary'],))
        count_10k = cur.rowcount
        
        print(f'  [OK] Updated {count_150k} (>= 150k PE) + {count_10k} (>= 10k PE + sensitive) Tertiary links')
        conn.commit()
    
    # Update Quaternary notes
    print('\n[5/5] Updating Quaternary treatment notes...')
    if 'Quaternary' in requirements:
        # For >= 150,000 PE plants
        cur.execute("""
            UPDATE plant_requirement_link prl
            SET notes = '>= 150,000 PE - Micropollutant removal required by 2045'
            FROM plants p
            WHERE prl.plant_id = p.plant_id 
            AND prl.requirement_id = %s
            AND p.plant_capacity >= 150000
        """, (requirements['Quaternary'],))
        count_150k = cur.rowcount
        
        # For >= 10,000 PE + sensitive area plants
        cur.execute("""
            UPDATE plant_requirement_link prl
            SET notes = '>= 10,000 PE + sensitive area - Micropollutant removal required by 2045'
            FROM plants p
            WHERE prl.plant_id = p.plant_id 
            AND prl.requirement_id = %s
            AND p.plant_capacity >= 10000 
            AND p.plant_capacity < 150000
        """, (requirements['Quaternary'],))
        count_10k = cur.rowcount
        
        print(f'  [OK] Updated {count_150k} (>= 150k PE) + {count_10k} (>= 10k PE + sensitive) Quaternary links')
        conn.commit()
    
    # Energy-self sufficiency notes already updated by previous script
    print('\n  Energy-self suffiency notes already set to:')
    print('  ">= 10,000 PE - Energy audits required every 4 years"')
    
    # Show summary
    print('\n' + '=' * 70)
    print('SUMMARY: Updated Notes')
    print('=' * 70)
    cur.execute("""
        SELECT 
            r.treatment_tier,
            prl.notes,
            COUNT(*) as count
        FROM plant_requirement_link prl
        JOIN uwwtd_requirement r ON prl.requirement_id = r.id
        GROUP BY r.id, r.treatment_tier, prl.notes
        ORDER BY r.id, prl.notes
    """)
    
    current_tier = None
    for row in cur.fetchall():
        tier, notes, count = row
        if tier != current_tier:
            print(f'\n{tier}:')
            current_tier = tier
        print(f'  [{count:,} plants] {notes}')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 70)
    print('[DONE] All notes updated!')
    print('=' * 70)


if __name__ == '__main__':
    main()
