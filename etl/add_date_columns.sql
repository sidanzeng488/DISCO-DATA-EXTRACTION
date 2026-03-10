-- 添加缺失的日期字段
ALTER TABLE plants ADD COLUMN IF NOT EXISTS date_published DATE;
ALTER TABLE plants ADD COLUMN IF NOT EXISTS date_situation_at DATE;

-- 添加注释
COMMENT ON COLUMN plants.date_published IS 'Report published date from T_ReportPeriod.repVersion';
COMMENT ON COLUMN plants.date_situation_at IS 'Situation at date from T_ReportPeriod.repSituationAt';
