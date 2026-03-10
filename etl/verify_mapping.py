"""
根据 feilds mapping with DISCO.xlsx 验证并修复字段映射
"""
import psycopg2
import csv
import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'supabase'))
from config import SUPABASE_HOST, SUPABASE_PORT, SUPABASE_DATABASE, SUPABASE_USER, SUPABASE_PASSWORD
from psycopg2.extras import execute_batch

# 数据目录
CURRENT_DIR = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'current')

def read_csv(filepath):
    with open(filepath, mode='r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def clean_value(val):
    if val is None or val == '' or val == 'NULL':
        return None
    return str(val).strip()

def parse_date(val):
    if not val or val == '' or val == 'NULL':
        return None
    try:
        # 处理 ISO 格式: 2024-06-25T00:00:00
        if 'T' in str(val):
            return datetime.fromisoformat(val.replace('T00:00:00', '')).date()
        return datetime.strptime(val, '%Y-%m-%d').date()
    except:
        return None

def get_row_count(cur, table_name):
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]

def main():
    print("=" * 70)
    print("根据 Excel 映射验证并修复数据")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        database=SUPABASE_DATABASE,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD
    )
    cur = conn.cursor()
    print("Connected to Supabase")
    
    # ================================================================
    # 1. 检查 plants 表是否有 date_published 和 date_situation_at
    # ================================================================
    print("\n" + "=" * 70)
    print("1. 检查缺失字段")
    print("=" * 70)
    
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'plants' AND column_name IN ('date_published', 'date_situation_at')
    """)
    existing_date_cols = [r[0] for r in cur.fetchall()]
    
    missing_cols = []
    if 'date_published' not in existing_date_cols:
        missing_cols.append('date_published')
    if 'date_situation_at' not in existing_date_cols:
        missing_cols.append('date_situation_at')
    
    if missing_cols:
        print(f"plants 表缺少字段: {missing_cols}")
        print("\n需要执行的 SQL:")
        for col in missing_cols:
            print(f"  ALTER TABLE plants ADD COLUMN {col} DATE;")
        print("\n请先在 Supabase 执行以上 SQL，然后重新运行此脚本")
        
        # 生成完整 SQL
        sql_file = os.path.join(os.path.dirname(__file__), 'add_date_columns.sql')
        with open(sql_file, 'w', encoding='utf-8') as f:
            f.write("-- 添加缺失的日期字段\n")
            for col in missing_cols:
                f.write(f"ALTER TABLE plants ADD COLUMN IF NOT EXISTS {col} DATE;\n")
            f.write("\n-- 添加注释\n")
            f.write("COMMENT ON COLUMN plants.date_published IS 'Report published date from T_ReportPeriod.repVersion';\n")
            f.write("COMMENT ON COLUMN plants.date_situation_at IS 'Situation at date from T_ReportPeriod.repSituationAt';\n")
        print(f"\nSQL 已保存到: {sql_file}")
        
    else:
        print("✓ plants 表已有 date_published 和 date_situation_at 字段")
        
        # ================================================================
        # 2. 填充 date_published 和 date_situation_at
        # ================================================================
        print("\n" + "=" * 70)
        print("2. 填充日期字段 (通过 rep_code 链接)")
        print("=" * 70)
        
        # 读取 report_periods.csv
        report_periods_path = os.path.join(CURRENT_DIR, 'report_periods.csv')
        report_data = read_csv(report_periods_path)
        print(f"report_periods.csv: {len(report_data)} 条记录")
        
        # 构建 rep_code -> (date_published, date_situation_at) 映射
        rep_code_dates = {}
        for row in report_data:
            rep_code = clean_value(row.get('repCode'))
            rep_version = parse_date(row.get('repVersion'))
            rep_situation_at = parse_date(row.get('repSituationAt'))
            if rep_code:
                rep_code_dates[rep_code] = (rep_version, rep_situation_at)
        
        print(f"rep_code 映射: {len(rep_code_dates)} 条")
        print(f"样本: {list(rep_code_dates.items())[:3]}")
        
        # 获取 plants 中的 rep_code
        cur.execute("SELECT DISTINCT rep_code FROM plants WHERE rep_code IS NOT NULL")
        plant_rep_codes = [r[0] for r in cur.fetchall()]
        print(f"plants 中的 rep_code: {len(plant_rep_codes)} 种")
        
        # 更新 plants
        updates = []
        for rep_code in plant_rep_codes:
            if rep_code in rep_code_dates:
                date_pub, date_sit = rep_code_dates[rep_code]
                if date_pub or date_sit:
                    updates.append((date_pub, date_sit, rep_code))
        
        print(f"准备更新: {len(updates)} 条")
        
        if updates:
            query = """
                UPDATE plants 
                SET date_published = %s, date_situation_at = %s
                WHERE rep_code = %s
            """
            execute_batch(cur, query, updates, page_size=100)
            conn.commit()
            print("✓ 日期字段已更新")
            
            # 验证
            cur.execute("SELECT COUNT(*) FROM plants WHERE date_published IS NOT NULL")
            pub_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM plants WHERE date_situation_at IS NOT NULL")
            sit_count = cur.fetchone()[0]
            total = get_row_count(cur, 'plants')
            
            print(f"\n填充结果:")
            print(f"  date_published: {pub_count}/{total} ({pub_count/total*100:.1f}%)")
            print(f"  date_situation_at: {sit_count}/{total} ({sit_count/total*100:.1f}%)")
    
    # ================================================================
    # 3. 验证所有映射字段
    # ================================================================
    print("\n" + "=" * 70)
    print("3. 验证映射字段填充率")
    print("=" * 70)
    
    # 根据 Excel 定义的映射
    field_checks = [
        ('country_code', 'CountryCode'),
        ('provides_primary_treatment', 'uwwPrimaryTreatment'),
        ('provides_secondary_treatment', 'uwwSecondaryTreatment'),
        ('other_treatment_provided', 'uwwOtherTreatment'),
        ('provides_nitrogen_removal', 'uwwNRemoval'),
        ('provides_phosphorus_removal', 'uwwPRemoval'),
        ('includes_uv_treatment', 'uwwUV'),
        ('includes_chlorination', 'uwwChlorination'),
        ('includes_ozonation', 'uwwOzonation'),
        ('includes_sand_filtration', 'uwwSandFiltration'),
        ('includes_microfiltration', 'uwwMicroFiltration'),
        ('bod_compliance', 'uwwBOD5Perf'),
        ('cod_compliance', 'uwwCODPerf'),
        ('tss_compliance', 'uwwTSSPerf'),
        ('nitrogen_compliance', 'uwwNTotPerf'),
        ('phosphorus_compliance', 'uwwPTotPerf'),
        ('other_compliance', 'uwwOtherPerf'),
        ('failure_notes', 'uwwInformation'),
        ('commissioning_date', 'uwwBeginLife'),
        ('plant_notes', 'uwwRemarks'),
        ('pct_wastewater_reused', 'uwwWasteWaterReuse'),
        ('volume_wastewater_reused_m3_per_year', 'uwwWasteWaterTreated'),
        ('bod_incoming_measured', 'uwwBODIncomingMeasured'),
        ('bod_outgoing_measured', 'uwwBODDischargeMeasured'),
        ('cod_incoming_measured', 'uwwCODIncomingMeasured'),
        ('cod_outgoing_measured', 'uwwCODDischargeMeasured'),
        ('nitrogen_incoming_measured', 'uwwNIncomingMeasured'),
        ('nitrogen_outgoing_measured', 'uwwNDischargeMeasured'),
        ('phosphorus_incoming_measured', 'uwwPIncomingMeasured'),
        ('phosphorus_outgoing_measured', 'uwwPDischargeMeasured'),
        ('article17_report_date', 'flaconSituationAt (via link)'),
        ('article17_compliance_status', 'flatpStatus'),
        ('article17_investment_planned', 'flatpMeasures'),
        ('article17_investment_type', 'flatpExpecTreatment'),
        ('article17_investment_need', 'flatpReasons'),
        ('article17_investment_cost', 'flatpInv'),
        ('eu_funding_scheme', 'flatpEUFundName'),
        ('eu_funding_amount', 'flatpEUFund'),
        ('other_funding_scheme', 'flatpOtherFundName'),
        ('other_funding_amount', 'flatpOtherFund'),
        ('planning_start_date', 'flatpExpecDateStart'),
        ('construction_start_date', 'flatpExpecDateStartWork'),
        ('construction_completion_date', 'flatpExpecDateCompletion'),
        ('expected_commissioning_date', 'flatpExpecDatePerformance'),
        ('capacity_expansion', 'flatpExpCapacity'),
    ]
    
    total = get_row_count(cur, 'plants')
    print(f"plants 总记录: {total}\n")
    print(f"{'Supabase 字段':<45} {'DISCO 字段':<35} {'填充率':<10}")
    print("-" * 95)
    
    for db_col, disco_col in field_checks:
        try:
            cur.execute(f"SELECT COUNT(*) FROM plants WHERE \"{db_col}\" IS NOT NULL")
            filled = cur.fetchone()[0]
            pct = filled / total * 100
            status = "✓" if pct > 80 else ("○" if pct > 0 else "✗")
            print(f"{status} {db_col:<43} {disco_col:<35} {pct:5.1f}%")
        except Exception as e:
            print(f"✗ {db_col:<43} {disco_col:<35} ERROR: {e}")
    
    # ================================================================
    # 4. 计算字段 (removal %)
    # ================================================================
    print("\n" + "=" * 70)
    print("4. 计算字段 (removal %)")
    print("=" * 70)
    
    # BOD removal %
    cur.execute("""
        SELECT COUNT(*) FROM plants 
        WHERE bod_incoming_measured IS NOT NULL 
        AND bod_outgoing_measured IS NOT NULL
    """)
    bod_calc_possible = cur.fetchone()[0]
    print(f"可计算 bod_removal_pct: {bod_calc_possible} 条 ({bod_calc_possible/total*100:.1f}%)")
    
    cur.execute("""
        SELECT COUNT(*) FROM plants 
        WHERE cod_incoming_measured IS NOT NULL 
        AND cod_outgoing_measured IS NOT NULL
    """)
    cod_calc_possible = cur.fetchone()[0]
    print(f"可计算 cod_removal_pct: {cod_calc_possible} 条 ({cod_calc_possible/total*100:.1f}%)")
    
    cur.execute("""
        SELECT COUNT(*) FROM plants 
        WHERE nitrogen_incoming_measured IS NOT NULL 
        AND nitrogen_outgoing_measured IS NOT NULL
    """)
    n_calc_possible = cur.fetchone()[0]
    print(f"可计算 nitrogen_removal_pct: {n_calc_possible} 条 ({n_calc_possible/total*100:.1f}%)")
    
    cur.execute("""
        SELECT COUNT(*) FROM plants 
        WHERE phosphorus_incoming_measured IS NOT NULL 
        AND phosphorus_outgoing_measured IS NOT NULL
    """)
    p_calc_possible = cur.fetchone()[0]
    print(f"可计算 phosphorus_removal_pct: {p_calc_possible} 条 ({p_calc_possible/total*100:.1f}%)")
    
    print("\n✅ 验证完成")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
