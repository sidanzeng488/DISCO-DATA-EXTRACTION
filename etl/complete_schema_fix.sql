-- ============================================================
-- Complete Schema Fix based on feilds mapping with DISCO.xlsx
-- ============================================================

-- 1. Add missing date columns
ALTER TABLE plants ADD COLUMN IF NOT EXISTS date_published DATE;
ALTER TABLE plants ADD COLUMN IF NOT EXISTS date_situation_at DATE;

-- Add comments
COMMENT ON COLUMN plants.date_published IS 'Report published date from T_ReportPeriod.repVersion';
COMMENT ON COLUMN plants.date_situation_at IS 'Situation at date from T_ReportPeriod.repSituationAt';

-- 2. Calculate removal percentages
-- BOD removal %
UPDATE plants 
SET bod_removal_pct = 
    CASE 
        WHEN bod_incoming_measured > 0 AND bod_outgoing_measured IS NOT NULL 
        THEN ROUND(((bod_incoming_measured - bod_outgoing_measured) / bod_incoming_measured) * 100, 2)
        ELSE NULL 
    END
WHERE bod_incoming_measured IS NOT NULL AND bod_outgoing_measured IS NOT NULL;

-- COD removal %
UPDATE plants 
SET cod_removal_pct = 
    CASE 
        WHEN cod_incoming_measured > 0 AND cod_outgoing_measured IS NOT NULL 
        THEN ROUND(((cod_incoming_measured - cod_outgoing_measured) / cod_incoming_measured) * 100, 2)
        ELSE NULL 
    END
WHERE cod_incoming_measured IS NOT NULL AND cod_outgoing_measured IS NOT NULL;

-- Nitrogen removal %
UPDATE plants 
SET nitrogen_removal_pct = 
    CASE 
        WHEN nitrogen_incoming_measured > 0 AND nitrogen_outgoing_measured IS NOT NULL 
        THEN ROUND(((nitrogen_incoming_measured - nitrogen_outgoing_measured) / nitrogen_incoming_measured) * 100, 2)
        ELSE NULL 
    END
WHERE nitrogen_incoming_measured IS NOT NULL AND nitrogen_outgoing_measured IS NOT NULL;

-- Phosphorus removal %
UPDATE plants 
SET phosphorus_removal_pct = 
    CASE 
        WHEN phosphorus_incoming_measured > 0 AND phosphorus_outgoing_measured IS NOT NULL 
        THEN ROUND(((phosphorus_incoming_measured - phosphorus_outgoing_measured) / phosphorus_incoming_measured) * 100, 2)
        ELSE NULL 
    END
WHERE phosphorus_incoming_measured IS NOT NULL AND phosphorus_outgoing_measured IS NOT NULL;

-- 3. Verify results
SELECT 
    'date_published' as field, COUNT(*) as filled FROM plants WHERE date_published IS NOT NULL
UNION ALL
SELECT 'date_situation_at', COUNT(*) FROM plants WHERE date_situation_at IS NOT NULL
UNION ALL
SELECT 'bod_removal_pct', COUNT(*) FROM plants WHERE bod_removal_pct IS NOT NULL
UNION ALL
SELECT 'cod_removal_pct', COUNT(*) FROM plants WHERE cod_removal_pct IS NOT NULL
UNION ALL
SELECT 'nitrogen_removal_pct', COUNT(*) FROM plants WHERE nitrogen_removal_pct IS NOT NULL
UNION ALL
SELECT 'phosphorus_removal_pct', COUNT(*) FROM plants WHERE phosphorus_removal_pct IS NOT NULL;
