"""
ETL 配置文件
定义提取的表、字段映射和目标 Schema
"""
import os

# 路径配置
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'DATA')
CURRENT_DIR = os.path.join(DATA_DIR, 'current')
BACKUP_DIR = os.path.join(DATA_DIR, 'backup')
MANIFEST_FILE = os.path.join(DATA_DIR, 'manifest.json')

# 备份策略：保留最新版本 + 上一个版本
MAX_BACKUPS = 1

# ============================================
# DISCO 表配置 - 要提取的表
# ============================================

# 小表：通过 API 自动下载
DISCO_TABLES_AUTO = {
    'plants': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_UWWTPS]',
        'filename': 'plants.csv',
    },
    'agglomerations': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_Agglomerations]',
        'filename': 'agglomerations.csv',
    },
    'plant_agglo_links': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_UWWTPAgglos]',
        'filename': 'plant_agglo_links.csv',
    },
    'discharge_points': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_DischargePoints]',
        'filename': 'discharge_points.csv',
    },
    'art17_investments': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_Art17_FLAUWWTP]',
        'filename': 'art17_investments.csv',
    },
    'art17_contacts': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_Art17_FLAContact]',
        'filename': 'art17_contacts.csv',
    },
    'report_periods': {
        'disco_table': '[WISE_UWWTD].[v1r1].[T_ReportPeriod]',
        'filename': 'report_periods.csv',
    },
    'gwb_groundwater': {
        'disco_table': '[WISE_WFD].[v2r1].[GWB_GroundWaterBody]',
        'filename': 'gwb_groundwater.csv',
    },
}

# 大表：需要手动从 DISCO 网站导出（数据量大，API 容易超时）
DISCO_TABLES_MANUAL = {
    'swb_surface_water': {
        'disco_table': '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]',
        'filename': 'swb_surface_water.csv',
        'url': 'https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]',
        'estimated_rows': '~200,000',
    },
    'swb_protected_areas': {
        'disco_table': '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]',
        'filename': 'swb_protected_areas.csv',
        'url': 'https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]',
        'estimated_rows': '~500,000+',
    },
    'gwb_protected_areas': {
        'disco_table': '[WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]',
        'filename': 'gwb_protected_areas.csv',
        'url': 'https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=[WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]',
        'estimated_rows': '~100,000+',
    },
}

# 合并（用于 transform.py 等需要完整列表的地方）
DISCO_TABLES = {**DISCO_TABLES_AUTO, **DISCO_TABLES_MANUAL}

# ============================================
# 字段映射 - DISCO → Supabase
# ============================================
FIELD_MAPPINGS = {
    'plants': {
        'uwwCode': 'uwwtp_code',
        'uwwName': 'plant_name',
        'CountryCode': 'country_code',
        'uwwLatitude': 'lat',
        'uwwLongitude': 'longitude',
        'uwwCapacity': 'plant_capacity',
        'uwwPrimaryTreatment': 'provides_primary_treatment',
        'uwwSecondaryTreatment': 'provides_secondary_treatment',
        'uwwOtherTreatment': 'other_treatment_provided',
        'uwwNRemoval': 'provides_nitrogen_removal',
        'uwwPRemoval': 'provides_phosphorus_removal',
        'uwwUV': 'includes_uv_treatment',
        'uwwChlorination': 'includes_chlorination',
        'uwwOzonation': 'includes_ozonation',
        'uwwSandFiltration': 'includes_sand_filtration',
        'uwwMicroFiltration': 'includes_microfiltration',
        # 合规性字段 (Compliance fields from DISCO)
        'uwwBOD5Perf': 'bod_compliance',
        'uwwCODPerf': 'cod_compliance',
        'uwwTSSPerf': 'tss_compliance',
        'uwwNTotPerf': 'nitrogen_compliance',
        'uwwPTotPerf': 'phosphorus_compliance',
        'uwwOtherPerf': 'other_compliance',
        # 注意: removal_pct 字段需要在数据库中计算，不从DISCO导入
        # BOD/COD/N/P 数据
        'uwwBODIncomingMeasured': 'bod_incoming_measured',
        'uwwBODIncomingCalculated': 'bod_incoming_calculated',
        'uwwBODIncomingEstimated': 'bod_incoming_estimated',
        'uwwBODDischargeMeasured': 'bod_outgoing_measured',
        'uwwBODDischargeCalculated': 'bod_outgoing_calculated',
        'uwwBODDischargeEstimated': 'bod_outgoing_estimated',
        'uwwCODIncomingMeasured': 'cod_incoming_measured',
        'uwwCODIncomingCalculated': 'cod_incoming_calculated',
        'uwwCODIncomingEstimated': 'cod_incoming_estimated',
        'uwwCODDischargeMeasured': 'cod_outgoing_measured',
        'uwwCODDischargeCalculated': 'cod_outgoing_calculated',
        'uwwCODDischargeEstimated': 'cod_outgoing_estimated',
        'uwwNIncomingMeasured': 'nitrogen_incoming_measured',
        'uwwNIncomingCalculated': 'nitrogen_incoming_calculated',
        'uwwNIncomingEstimated': 'nitrogen_incoming_estimated',
        'uwwNDischargeMeasured': 'nitrogen_outgoing_measured',
        'uwwNDischargeCalculated': 'nitrogen_outgoing_calculated',
        'uwwNDischargeEstimated': 'nitrogen_outgoing_estimated',
        'uwwPIncomingMeasured': 'phosphorus_incoming_measured',
        'uwwPIncomingCalculated': 'phosphorus_incoming_calculated',
        'uwwPIncomingEstimated': 'phosphorus_incoming_estimated',
        'uwwPDischargeMeasured': 'phosphorus_outgoing_measured',
        'uwwPDischargeCalculated': 'phosphorus_outgoing_calculated',
        'uwwPDischargeEstimated': 'phosphorus_outgoing_estimated',
        'uwwInformation': 'failure_notes',
        'uwwRemarks': 'plant_notes',
        'uwwWasteWaterReuse': 'pct_wastewater_reused',
        'uwwWasteWaterTreated': 'volume_wastewater_reused_m3_per_year',
        'uwwBeginLife': 'commissioning_date',
        'repCode': 'rep_code',
        'aggCode': 'agg_code',
    },
    'discharge_points': {
        'dcpCode': 'dcp_code',
        'dcpName': 'dcp_name',
        'uwwCode': 'plant_code',
        'CountryCode': 'country_code',
        'dcpLatitude': 'latitude',
        'dcpLongitude': 'longitude',
        'dcpSurfaceWaters': 'is_surface_water',
        'dcpWaterBodyType': 'water_body_type',
        'dcpWaterbodyID': 'swb_water_body_code',
        'dcpGroundWater': 'gwb_water_body_code',
        'dcpTypeOfReceivingArea': 'sensitivity_code',
        'rcaCode': 'rca_code',
        'dcpReceivingWater': 'receiving_water',
        'dcpWFDRBD': 'wfd_rbd',
        'dcpWFDSubUnit': 'wfd_sub_unit',
        'repCode': 'rep_code',
    },
    'water_bodies_swb': {
        'euSurfaceWaterBodyCode': 'eu_water_body_code',
        'surfaceWaterBodyName': 'water_body_name',
        'countryCode': 'country_code',
        'euRBDCode': 'eu_rbd_code',
        'rbdName': 'rbd_name',
        'cYear': 'c_year',
        'surfaceWaterBodyCategory': 'surface_water_category',
        'cArea': 'c_area',
        'cLength': 'c_length',
        'swEcologicalStatusOrPotentialValue': 'sw_ecological_status',
        'swChemicalStatusValue': 'sw_chemical_status',
        'fileUrl': 'file_url',
    },
    'water_bodies_gwb': {
        'euGroundWaterBodyCode': 'eu_water_body_code',
        'countryCode': 'country_code',
        'euRBDCode': 'eu_rbd_code',
        'rbdName': 'rbd_name',
        'cYear': 'c_year',
        'gwQuantitativeStatusValue': 'gw_quantitative_status',
        'gwChemicalStatusValue': 'gw_chemical_status',
        'fileUrl': 'file_url',
    },
    'agglomerations': {
        'aggCode': 'agg_code',
        'aggName': 'agglomeration_name',
        'CountryCode': 'country_code',
        'aggCapacity': 'agg_capacity',
        'aggGenerated': 'agg_generated',
        'aggLatitude': 'latitude',
        'aggLongitude': 'longitude',
        'repCode': 'rep_code',
    },
    'report_periods': {
        'repCode': 'rep_code',
        'CountryCode': 'country_code',
        'repReportedPeriod': 'reported_period',
        'repSituationAt': 'situation_at',
        'repVersion': 'version',
    },
}

# ============================================
# Sensitivity 查找表数据
# ============================================
SENSITIVITY_DATA = [
    {'code': 'A54', 'definition': 'Art. 5(4) area', 'nutrient_sensitivity': True},
    {'code': 'A523', 'definition': 'Sensitive area Art. 5(2,3)', 'nutrient_sensitivity': True},
    {'code': 'A5854', 'definition': 'Art. 5(8) area (entire Member State) + Art. 5(4)', 'nutrient_sensitivity': True},
    {'code': 'A58523', 'definition': 'Art. 5(8) area (entire Member State) + Art. 5(2,3)', 'nutrient_sensitivity': True},
    {'code': 'CSA', 'definition': 'Catchment in the sense of Art. 5(5)', 'nutrient_sensitivity': True},
    {'code': 'LSA', 'definition': 'Less sensitive area', 'nutrient_sensitivity': False},
    {'code': 'NA', 'definition': 'Normal area', 'nutrient_sensitivity': False},
]
