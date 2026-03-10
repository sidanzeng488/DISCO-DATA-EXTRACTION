-- ============================================================
-- Add Energy Self-Sufficiency Field to Plants Table
-- Based on revised UWWTD (2024) Article 11 requirements
-- WWTPs >= 10,000 PE must achieve energy neutrality
-- ============================================================

-- 1. Add new VARCHAR column to plants table (Yes/No values)
ALTER TABLE plants 
ADD COLUMN IF NOT EXISTS requires_energy_self_sufficiency VARCHAR(10) DEFAULT 'No';

-- 2. Add column comment with link to uwwtd_requirement table
COMMENT ON COLUMN plants.requires_energy_self_sufficiency IS 
'Energy self-sufficiency requirement under revised UWWTD Article 11 (linked to uwwtd_requirement.treatment_tier = Energy-self suffiency). 
Values: Yes = plant_capacity >= 10,000 PE, No = plant_capacity < 10,000 PE.
Deadline targets: 20% renewable by 2030, 40% by 2035, 70% by 2040, 100% by 2045.
All applicable plants must achieve energy neutrality and reduce greenhouse gas emissions through solar energy, biogas from sludge, heat/kinetic energy, or other renewable sources. Energy audits required every 4 years.';

-- 3. Update all plants with capacity >= 10,000 PE to 'Yes'
UPDATE plants 
SET requires_energy_self_sufficiency = 'Yes' 
WHERE plant_capacity >= 10000;

-- 4. Set to 'No' for plants under 10,000 PE (explicit)
UPDATE plants 
SET requires_energy_self_sufficiency = 'No' 
WHERE plant_capacity < 10000 OR plant_capacity IS NULL;

-- 5. Insert into plant_requirement_link (link to Energy-self suffiency requirement)
INSERT INTO plant_requirement_link (plant_id, requirement_id, notes)
SELECT 
    p.plant_id, 
    (SELECT id FROM uwwtd_requirement WHERE treatment_tier = 'Energy-self suffiency'),
    '>= 10,000 PE - Energy audits required every 4 years'
FROM plants p
WHERE p.plant_capacity >= 10000
ON CONFLICT (plant_id, requirement_id) DO NOTHING;

-- 6. Verify the update
SELECT 
    requires_energy_self_sufficiency,
    COUNT(*) as count,
    MIN(plant_capacity) as min_capacity,
    MAX(plant_capacity) as max_capacity
FROM plants 
GROUP BY requires_energy_self_sufficiency
ORDER BY requires_energy_self_sufficiency DESC;

-- 7. Verify plant_requirement_link
SELECT 
    r.treatment_tier,
    COUNT(prl.id) as link_count
FROM uwwtd_requirement r
LEFT JOIN plant_requirement_link prl ON r.id = prl.requirement_id
WHERE r.treatment_tier = 'Energy-self suffiency'
GROUP BY r.treatment_tier;

-- 8. Show sample of tagged plants
SELECT 
    uwwtp_code, 
    plant_name, 
    country_code, 
    plant_capacity,
    requires_energy_self_sufficiency
FROM plants 
WHERE requires_energy_self_sufficiency = 'Yes' 
ORDER BY plant_capacity DESC 
LIMIT 20;
