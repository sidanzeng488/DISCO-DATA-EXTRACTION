"""
更新 Energy Self-Sufficiency 字段
1. 把 BOOLEAN (TRUE/FALSE) 改成 VARCHAR ('Yes'/'No')
2. 确保与 plant_requirement_link 表关联正确
3. 完善 notes
"""
import psycopg2
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD


def main():
    print('=' * 60)
    print('Update Energy Self-Sufficiency Field')
    print('Convert BOOLEAN to VARCHAR (Yes/No)')
    print('=' * 60)
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    
    # 1. Change column type from BOOLEAN to VARCHAR
    print('\n[1/5] Converting column type to VARCHAR...')
    
    # Check current column type
    cur.execute("""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = 'plants' 
        AND column_name = 'requires_energy_self_sufficiency'
    """)
    result = cur.fetchone()
    
    if result:
        current_type = result[0]
        print(f'  Current type: {current_type}')
        
        if current_type == 'boolean':
            # Convert BOOLEAN to VARCHAR with Yes/No
            cur.execute("""
                ALTER TABLE plants 
                ALTER COLUMN requires_energy_self_sufficiency TYPE VARCHAR(10)
                USING CASE 
                    WHEN requires_energy_self_sufficiency = TRUE THEN 'Yes'
                    WHEN requires_energy_self_sufficiency = FALSE THEN 'No'
                    ELSE NULL 
                END
            """)
            conn.commit()
            print('  [OK] Converted to VARCHAR')
        else:
            print('  [OK] Already VARCHAR, updating values...')
            # Update any remaining TRUE/FALSE to Yes/No
            cur.execute("""
                UPDATE plants 
                SET requires_energy_self_sufficiency = 'Yes'
                WHERE requires_energy_self_sufficiency IN ('true', 'TRUE', 't', '1')
            """)
            cur.execute("""
                UPDATE plants 
                SET requires_energy_self_sufficiency = 'No'
                WHERE requires_energy_self_sufficiency IN ('false', 'FALSE', 'f', '0')
            """)
            conn.commit()
    else:
        print('  Column does not exist, creating...')
        cur.execute("""
            ALTER TABLE plants 
            ADD COLUMN requires_energy_self_sufficiency VARCHAR(10) DEFAULT 'No'
        """)
        conn.commit()
    
    # 2. Update values based on capacity
    print('\n[2/5] Updating values based on plant capacity...')
    cur.execute("""
        UPDATE plants 
        SET requires_energy_self_sufficiency = 'Yes'
        WHERE plant_capacity >= 10000
    """)
    yes_count = cur.rowcount
    
    cur.execute("""
        UPDATE plants 
        SET requires_energy_self_sufficiency = 'No'
        WHERE plant_capacity < 10000 OR plant_capacity IS NULL
    """)
    no_count = cur.rowcount
    conn.commit()
    print(f'  [OK] Yes: {yes_count}, No: {no_count}')
    
    # 3. Update column comment
    print('\n[3/5] Updating column comment...')
    cur.execute("""
        COMMENT ON COLUMN plants.requires_energy_self_sufficiency IS 
        'Energy self-sufficiency requirement under revised UWWTD Article 11 (linked to uwwtd_requirement.treatment_tier = Energy-self suffiency). 
Values: Yes = plant_capacity >= 10,000 PE, No = plant_capacity < 10,000 PE.
Deadline targets: 20%% renewable by 2030, 40%% by 2035, 70%% by 2040, 100%% by 2045.
All applicable plants must achieve energy neutrality and reduce greenhouse gas emissions through solar energy, biogas from sludge, heat/kinetic energy, or other renewable sources. Energy audits required every 4 years.'
    """)
    conn.commit()
    print('  [OK] Comment updated')
    
    # 4. Verify plant_requirement_link has Energy-self suffiency entries
    print('\n[4/5] Verifying plant_requirement_link...')
    
    # Get Energy-self suffiency requirement ID
    cur.execute("""
        SELECT id FROM uwwtd_requirement 
        WHERE treatment_tier = 'Energy-self suffiency'
    """)
    req_result = cur.fetchone()
    
    if req_result:
        req_id = req_result[0]
        print(f'  Energy-self suffiency requirement ID: {req_id}')
        
        # Count existing links
        cur.execute("""
            SELECT COUNT(*) FROM plant_requirement_link 
            WHERE requirement_id = %s
        """, (req_id,))
        existing_links = cur.fetchone()[0]
        print(f'  Existing links: {existing_links}')
        
        # Insert missing links for plants >= 10,000 PE
        cur.execute("""
            INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
            SELECT 
                p.plant_id, 
                %s, 
                '>= 10,000 PE - Energy audits required every 4 years'
            FROM plants p
            WHERE p.plant_capacity >= 10000
            AND NOT EXISTS (
                SELECT 1 FROM plant_requirement_link prl 
                WHERE prl.plant_id = p.plant_id AND prl.requirement_id = %s
            )
        """, (req_id, req_id))
        new_links = cur.rowcount
        conn.commit()
        print(f'  [OK] Added {new_links} new links')
        
        # Update notes for existing links
        cur.execute("""
            UPDATE plant_requirement_link 
            SET notes = '>= 10,000 PE - Energy audits required every 4 years'
            WHERE requirement_id = %s AND (notes IS NULL OR notes = '>= 10,000 PE')
        """, (req_id,))
        updated_notes = cur.rowcount
        conn.commit()
        print(f'  [OK] Updated {updated_notes} link notes')
    else:
        print('  [WARNING] Energy-self suffiency requirement not found in uwwtd_requirement table')
        print('  Run import_requirement.py first')
    
    # 5. Show results
    print('\n[5/5] Final verification...')
    
    print('\n' + '-' * 60)
    print('Plants table - requires_energy_self_sufficiency:')
    print('-' * 60)
    cur.execute("""
        SELECT 
            requires_energy_self_sufficiency,
            COUNT(*) as count,
            MIN(plant_capacity) as min_pe,
            MAX(plant_capacity) as max_pe
        FROM plants 
        GROUP BY requires_energy_self_sufficiency
        ORDER BY requires_energy_self_sufficiency DESC NULLS LAST
    """)
    print(f'{"Value":<10} {"Count":<10} {"Min PE":<12} {"Max PE":<12}')
    for row in cur.fetchall():
        val = row[0] if row[0] else 'NULL'
        print(f'{val:<10} {row[1]:<10} {row[2] or "N/A":<12} {row[3] or "N/A":<12}')
    
    print('\n' + '-' * 60)
    print('plant_requirement_link - Energy-self suffiency:')
    print('-' * 60)
    cur.execute("""
        SELECT 
            r.treatment_tier,
            COUNT(prl.id) as link_count,
            prl.notes
        FROM uwwtd_requirement r
        LEFT JOIN plant_requirement_link prl ON r.id = prl.requirement_id
        WHERE r.treatment_tier = 'Energy-self suffiency'
        GROUP BY r.treatment_tier, prl.notes
    """)
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]} plants')
        print(f'  Notes: {row[2]}')
    
    print('\n' + '-' * 60)
    print('Sample plants with Energy self-sufficiency requirement:')
    print('-' * 60)
    cur.execute("""
        SELECT 
            p.uwwtp_code, 
            p.plant_name,
            p.plant_capacity,
            p.requires_energy_self_sufficiency,
            prl.notes
        FROM plants p
        JOIN plant_requirement_link prl ON p.plant_id = prl.plant_id
        JOIN uwwtd_requirement r ON prl.requirement_id = r.id
        WHERE r.treatment_tier = 'Energy-self suffiency'
        ORDER BY p.plant_capacity DESC
        LIMIT 10
    """)
    print(f'{"Code":<25} {"Name":<25} {"PE":<12} {"Required":<10}')
    for row in cur.fetchall():
        name = row[1][:24] if row[1] else ''
        # Handle encoding issues
        name = name.encode('ascii', 'replace').decode('ascii')
        print(f'{row[0]:<25} {name:<25} {row[2]:<12} {row[3]:<10}')
    
    cur.close()
    conn.close()
    
    print('\n' + '=' * 60)
    print('[DONE] Update completed!')
    print('=' * 60)


if __name__ == '__main__':
    main()
