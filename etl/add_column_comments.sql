-- ============================================================
-- Column Comments for Supabase Database
-- Based on DISCO Data Dictionary (dd.eionet.europa.eu)
-- Generated based on actual table structure
-- ============================================================

-- ============================================================
-- TABLE: plants (T_UWWTPS)
-- Source: https://dd.eionet.europa.eu/datasets/3540
-- ============================================================

-- Primary identifiers
COMMENT ON COLUMN plants.plant_id IS 'Auto-generated primary key';
COMMENT ON COLUMN plants.uwwtp_code IS 'Unique code for the urban waste water treatment plant (uwwCode)';
COMMENT ON COLUMN plants.plant_name IS 'Name of the urban waste water treatment plant (uwwName)';
COMMENT ON COLUMN plants.country_code IS 'ISO 3166-1 alpha-2 country code (CountryCode)';
COMMENT ON COLUMN plants.rep_code IS 'Report code linking to T_ReportPeriod for report metadata';

-- Location
COMMENT ON COLUMN plants.lat IS 'Latitude of the UWWTP in decimal degrees (uwwLatitude)';
COMMENT ON COLUMN plants.longitude IS 'Longitude of the UWWTP in decimal degrees (uwwLongitude)';

-- Capacity
COMMENT ON COLUMN plants.plant_capacity IS 'Design capacity of the UWWTP in population equivalent (p.e.) (uwwCapacity)';
COMMENT ON COLUMN plants.plant_waste_load_pe IS 'Actual organic load entering the UWWTP in population equivalent (p.e.) - the real load received by the plant (uwwLoadEnteringUWWTP)';
COMMENT ON COLUMN plants.agglomeration_id IS 'Foreign key to agglomeration table';

-- Calculated fields (revised UWWTD)
COMMENT ON COLUMN plants.actual_pe_greater_than_plant_design_pe IS 'Whether actual load exceeds design capacity: TRUE if plant_waste_load_pe > plant_capacity (calculated field)';
COMMENT ON COLUMN plants.requires_quaternary_treatment IS 'Quaternary treatment requirement under revised UWWTD: Yes (>=150k PE), Likely (>=10k PE + sensitive receiving water), No (others) - based on bathing/drinking/shellfish protected areas';
COMMENT ON COLUMN plants.requires_energy_self_sufficiency IS 'Energy self-sufficiency requirement under revised UWWTD Article 11 (linked to uwwtd_requirement.treatment_tier = Energy-self suffiency via plant_requirement_link table). Values: Yes = plant_capacity >= 10,000 PE, No = otherwise. Deadline targets: 20% renewable by 2030, 40% by 2035, 70% by 2040, 100% by 2045. Energy audits required every 4 years.';

-- Treatment types (Boolean flags)
COMMENT ON COLUMN plants.provides_primary_treatment IS 'Whether plant provides primary treatment (uwwPrimaryTreatment)';
COMMENT ON COLUMN plants.provides_secondary_treatment IS 'Whether plant provides secondary treatment (uwwSecondaryTreatment)';
COMMENT ON COLUMN plants.other_treatment_provided IS 'Whether plant provides other treatment types (uwwOtherTreatment)';
COMMENT ON COLUMN plants.provides_nitrogen_removal IS 'Whether plant provides nitrogen removal (uwwNRemoval)';
COMMENT ON COLUMN plants.provides_phosphorus_removal IS 'Whether plant provides phosphorus removal (uwwPRemoval)';
COMMENT ON COLUMN plants.includes_uv_treatment IS 'Whether plant includes UV disinfection (uwwUV)';
COMMENT ON COLUMN plants.includes_chlorination IS 'Whether plant includes chlorination (uwwChlorination)';
COMMENT ON COLUMN plants.includes_ozonation IS 'Whether plant includes ozonation (uwwOzonation)';
COMMENT ON COLUMN plants.includes_sand_filtration IS 'Whether plant includes sand filtration (uwwSandFiltration)';
COMMENT ON COLUMN plants.includes_microfiltration IS 'Whether plant includes microfiltration (uwwMicroFiltration)';

-- Compliance status (dd.eionet.europa.eu/dataelements/104947)
COMMENT ON COLUMN plants.bod_compliance IS 'BOD5 performance compliance status: C=compliant, NC=non-compliant, NI=no information (uwwBOD5Perf)';
COMMENT ON COLUMN plants.cod_compliance IS 'COD performance compliance status: C=compliant, NC=non-compliant, NI=no information (uwwCODPerf)';
COMMENT ON COLUMN plants.tss_compliance IS 'TSS performance compliance status: C=compliant, NC=non-compliant, NI=no information (uwwTSSPerf)';
COMMENT ON COLUMN plants.nitrogen_compliance IS 'Total Nitrogen performance compliance status (uwwNTotPerf)';
COMMENT ON COLUMN plants.phosphorus_compliance IS 'Total Phosphorus performance compliance status (uwwPTotPerf)';
COMMENT ON COLUMN plants.other_compliance IS 'Other parameters performance compliance status (uwwOtherPerf)';

-- BOD measurements (mg/l)
COMMENT ON COLUMN plants.bod_incoming_measured IS 'BOD5 incoming load - measured value in mg/l (uwwBODIncomingMeasured)';
COMMENT ON COLUMN plants.bod_incoming_calculated IS 'BOD5 incoming load - calculated value in mg/l (uwwBODIncomingCalculated)';
COMMENT ON COLUMN plants.bod_incoming_estimated IS 'BOD5 incoming load - estimated value in mg/l (uwwBODIncomingEstimated)';
COMMENT ON COLUMN plants.bod_outgoing_measured IS 'BOD5 discharge load - measured value in mg/l (uwwBODDischargeMeasured)';
COMMENT ON COLUMN plants.bod_outgoing_calculated IS 'BOD5 discharge load - calculated value in mg/l (uwwBODDischargeCalculated)';
COMMENT ON COLUMN plants.bod_outgoing_estimated IS 'BOD5 discharge load - estimated value in mg/l (uwwBODDischargeEstimated)';
COMMENT ON COLUMN plants.bod_removal_pct IS 'BOD5 removal percentage - calculated: (incoming-outgoing)/incoming*100';

-- COD measurements (mg/l)
COMMENT ON COLUMN plants.cod_incoming_measured IS 'COD incoming load - measured value in mg/l (uwwCODIncomingMeasured)';
COMMENT ON COLUMN plants.cod_incoming_calculated IS 'COD incoming load - calculated value in mg/l (uwwCODIncomingCalculated)';
COMMENT ON COLUMN plants.cod_incoming_estimated IS 'COD incoming load - estimated value in mg/l (uwwCODIncomingEstimated)';
COMMENT ON COLUMN plants.cod_outgoing_measured IS 'COD discharge load - measured value in mg/l (uwwCODDischargeMeasured)';
COMMENT ON COLUMN plants.cod_outgoing_calculated IS 'COD discharge load - calculated value in mg/l (uwwCODDischargeCalculated)';
COMMENT ON COLUMN plants.cod_outgoing_estimated IS 'COD discharge load - estimated value in mg/l (uwwCODDischargeEstimated)';
COMMENT ON COLUMN plants.cod_removal_pct IS 'COD removal percentage - calculated: (incoming-outgoing)/incoming*100';

-- Nitrogen measurements (mg/l)
COMMENT ON COLUMN plants.nitrogen_incoming_measured IS 'Total Nitrogen incoming load - measured value in mg/l (uwwNIncomingMeasured)';
COMMENT ON COLUMN plants.nitrogen_incoming_calculated IS 'Total Nitrogen incoming load - calculated value in mg/l (uwwNIncomingCalculated)';
COMMENT ON COLUMN plants.nitrogen_incoming_estimated IS 'Total Nitrogen incoming load - estimated value in mg/l (uwwNIncomingEstimated)';
COMMENT ON COLUMN plants.nitrogen_outgoing_measured IS 'Total Nitrogen discharge load - measured value in mg/l (uwwNDischargeMeasured)';
COMMENT ON COLUMN plants.nitrogen_outgoing_calculated IS 'Total Nitrogen discharge load - calculated value in mg/l (uwwNDischargeCalculated)';
COMMENT ON COLUMN plants.nitrogen_outgoing_estimated IS 'Total Nitrogen discharge load - estimated value in mg/l (uwwNDischargeEstimated)';
COMMENT ON COLUMN plants.nitrogen_removal_pct IS 'Total Nitrogen removal percentage - calculated: (incoming-outgoing)/incoming*100';

-- Phosphorus measurements (mg/l)
COMMENT ON COLUMN plants.phosphorus_incoming_measured IS 'Total Phosphorus incoming load - measured value in mg/l (uwwPIncomingMeasured)';
COMMENT ON COLUMN plants.phosphorus_incoming_calculated IS 'Total Phosphorus incoming load - calculated value in mg/l (uwwPIncomingCalculated)';
COMMENT ON COLUMN plants.phosphorus_incoming_estimated IS 'Total Phosphorus incoming load - estimated value in mg/l (uwwPIncomingEstimated)';
COMMENT ON COLUMN plants.phosphorus_outgoing_measured IS 'Total Phosphorus discharge load - measured value in mg/l (uwwPDischargeMeasured)';
COMMENT ON COLUMN plants.phosphorus_outgoing_calculated IS 'Total Phosphorus discharge load - calculated value in mg/l (uwwPDischargeCalculated)';
COMMENT ON COLUMN plants.phosphorus_outgoing_estimated IS 'Total Phosphorus discharge load - estimated value in mg/l (uwwPDischargeEstimated)';
COMMENT ON COLUMN plants.phosphorus_removal_pct IS 'Total Phosphorus removal percentage - calculated: (incoming-outgoing)/incoming*100';

-- Wastewater reuse
COMMENT ON COLUMN plants.pct_wastewater_reused IS 'Percentage of treated wastewater that is reused (uwwWasteWaterReuse)';
COMMENT ON COLUMN plants.volume_wastewater_reused_m3_per_year IS 'Mean annual volume of waste water treated in m3/year (uwwWasteWaterTreated)';

-- Dates and notes
COMMENT ON COLUMN plants.commissioning_date IS 'Date when the UWWTP started operation (uwwBeginLife)';
COMMENT ON COLUMN plants.date_published IS 'Report publication date from T_ReportPeriod.repVersion';
COMMENT ON COLUMN plants.date_situation_at IS 'Data situation date from T_ReportPeriod.repSituationAt';
COMMENT ON COLUMN plants.failure_notes IS 'Further information on cause of non-compliance failure (uwwInformation)';
COMMENT ON COLUMN plants.plant_notes IS 'General remarks about the UWWTP (uwwRemarks)';

-- Article 17 Investment fields (T_Art17_FLAUWWTP)
COMMENT ON COLUMN plants.article17_report_date IS 'Article 17 report situation date from T_Art17_FLAContact.flaconSituationAt';
COMMENT ON COLUMN plants.article17_compliance_status IS 'Article 17 compliance status: C=compliant, NC=non-compliant, NI=no information (flatpStatus)';
COMMENT ON COLUMN plants.article17_investment_planned IS 'Planned measures for Article 17 compliance (flatpMeasures)';
COMMENT ON COLUMN plants.article17_investment_type IS 'Expected treatment type after investment (flatpExpecTreatment)';
COMMENT ON COLUMN plants.article17_investment_need IS 'Reasons for investment need (flatpReasons)';
COMMENT ON COLUMN plants.article17_investment_cost IS 'Total investment cost in million EUR (flatpInv)';
COMMENT ON COLUMN plants.eu_funding_scheme IS 'Name of EU funding scheme (flatpEUFundName)';
COMMENT ON COLUMN plants.eu_funding_amount IS 'EU funding amount in million EUR (flatpEUFund)';
COMMENT ON COLUMN plants.other_funding_scheme IS 'Name of other funding scheme (flatpOtherFundName)';
COMMENT ON COLUMN plants.other_funding_amount IS 'Other funding amount in million EUR (flatpOtherFund)';
COMMENT ON COLUMN plants.planning_start_date IS 'Expected start date of planning phase (flatpExpecDateStart)';
COMMENT ON COLUMN plants.construction_start_date IS 'Expected start date of construction works (flatpExpecDateStartWork)';
COMMENT ON COLUMN plants.construction_completion_date IS 'Expected completion date of construction (flatpExpecDateCompletion)';
COMMENT ON COLUMN plants.expected_commissioning_date IS 'Expected date of commissioning/performance (flatpExpecDatePerformance)';
COMMENT ON COLUMN plants.capacity_expansion IS 'Expected capacity expansion in p.e. (flatpExpCapacity)';

-- ============================================================
-- TABLE: agglomeration (T_Agglomerations)
-- Actual columns: agglomeration_id, agglomeration_name, agg_code, country_code, agg_generated, latitude, longitude
-- ============================================================

COMMENT ON COLUMN agglomeration.agglomeration_id IS 'Auto-generated primary key';
COMMENT ON COLUMN agglomeration.agg_code IS 'Unique agglomeration code (aggCode)';
COMMENT ON COLUMN agglomeration.agglomeration_name IS 'Name of the agglomeration (aggName)';
COMMENT ON COLUMN agglomeration.country_code IS 'ISO 3166-1 alpha-2 country code (CountryCode)';
COMMENT ON COLUMN agglomeration.latitude IS 'Latitude of the agglomeration centroid in decimal degrees (aggLatitude)';
COMMENT ON COLUMN agglomeration.longitude IS 'Longitude of the agglomeration centroid in decimal degrees (aggLongitude)';
COMMENT ON COLUMN agglomeration.agg_generated IS 'Generated load of the agglomeration in p.e. (aggGenerated)';

-- ============================================================
-- TABLE: discharge_points (T_DischargePoints)
-- Actual columns: dcp_id, dcp_code, plant_code, country_code, latitude, longitude, is_surface_water, water_body_type, water_body_code, sensitivity_code, rca_code, receiving_water, wfd_rbd, wfd_sub_unit, rep_code
-- ============================================================

COMMENT ON COLUMN discharge_points.dcp_id IS 'Auto-generated primary key';
COMMENT ON COLUMN discharge_points.dcp_code IS 'Unique discharge point code (dcpCode)';
COMMENT ON COLUMN discharge_points.plant_code IS 'Reference to the UWWTP code (uwwCode)';
COMMENT ON COLUMN discharge_points.country_code IS 'ISO 3166-1 alpha-2 country code (CountryCode)';
COMMENT ON COLUMN discharge_points.latitude IS 'Latitude of the discharge point in decimal degrees (dcpLatitude)';
COMMENT ON COLUMN discharge_points.longitude IS 'Longitude of the discharge point in decimal degrees (dcpLongitude)';
COMMENT ON COLUMN discharge_points.is_surface_water IS 'True if discharge is to surface water, False if to groundwater (dcpSurfaceWaters)';
COMMENT ON COLUMN discharge_points.water_body_type IS 'Type of receiving water body: FW=freshwater, CW=coastal, TW=transitional (dcpWaterBodyType)';
COMMENT ON COLUMN discharge_points.water_body_code IS 'EU Water Body code - links to water_bodies table (dcpWaterbodyID or dcpGroundWater)';
COMMENT ON COLUMN discharge_points.sensitivity_code IS 'Type of receiving area sensitivity: A54, A523, A5854, A58523, CSA, LSA, or Normal (dcpTypeOfReceivingArea) - see https://dd.eionet.europa.eu/dataelements/105020';
COMMENT ON COLUMN discharge_points.rca_code IS 'Receiving catchment area code (rcaCode)';
COMMENT ON COLUMN discharge_points.receiving_water IS 'Name of the receiving water (dcpReceivingWater)';
COMMENT ON COLUMN discharge_points.wfd_rbd IS 'Water Framework Directive River Basin District code (dcpWFDRBD)';
COMMENT ON COLUMN discharge_points.wfd_sub_unit IS 'WFD Sub-unit code (dcpWFDSubUnit)';
COMMENT ON COLUMN discharge_points.rep_code IS 'Report code linking to report_code table';

-- ============================================================
-- TABLE: water_bodies (SWB_SurfaceWaterBody + GWB_GroundWaterBody)
-- Actual columns: water_body_id, eu_water_body_code, water_type, water_body_name, country_code, eu_rbd_code, rbd_name, c_year, surface_water_category, c_area, c_length, sw_ecological_status, sw_chemical_status, gw_quantitative_status, gw_chemical_status, file_url
-- ============================================================

COMMENT ON COLUMN water_bodies.water_body_id IS 'Auto-generated primary key';
COMMENT ON COLUMN water_bodies.eu_water_body_code IS 'EU Water Body unique code (euSurfaceWaterBodyCode or euGroundWaterBodyCode)';
COMMENT ON COLUMN water_bodies.water_type IS 'Type of water body: SWB=Surface Water Body, GWB=Groundwater Body';
COMMENT ON COLUMN water_bodies.water_body_name IS 'Name of the water body (surfaceWaterBodyName)';
COMMENT ON COLUMN water_bodies.country_code IS 'ISO 3166-1 alpha-2 country code (countryCode)';
COMMENT ON COLUMN water_bodies.eu_rbd_code IS 'EU River Basin District code (euRBDCode)';
COMMENT ON COLUMN water_bodies.rbd_name IS 'River Basin District name (rbdName)';
COMMENT ON COLUMN water_bodies.c_year IS 'Reporting cycle year (cYear)';
COMMENT ON COLUMN water_bodies.surface_water_category IS 'SWB category: RW=river, LW=lake, TW=transitional, CW=coastal (surfaceWaterBodyCategory)';
COMMENT ON COLUMN water_bodies.c_area IS 'Surface area in km2 (cArea)';
COMMENT ON COLUMN water_bodies.c_length IS 'Length in km for rivers (cLength)';
COMMENT ON COLUMN water_bodies.sw_ecological_status IS 'SWB Ecological status/potential: High, Good, Moderate, Poor, Bad (swEcologicalStatusOrPotentialValue)';
COMMENT ON COLUMN water_bodies.sw_chemical_status IS 'SWB Chemical status: Good, Fail to achieve good (swChemicalStatusValue)';
COMMENT ON COLUMN water_bodies.gw_quantitative_status IS 'GWB Quantitative status: Good, Poor (gwQuantitativeStatusValue)';
COMMENT ON COLUMN water_bodies.gw_chemical_status IS 'GWB Chemical status: Good, Poor (gwChemicalStatusValue)';
COMMENT ON COLUMN water_bodies.file_url IS 'URL to source file (fileUrl)';

-- ============================================================
-- TABLE: water_body_protected_areas
-- Actual columns: protected_area_id, water_body_code, eu_protected_area_code, protected_area_type, objectives_set, objectives_met
-- ============================================================

COMMENT ON COLUMN water_body_protected_areas.protected_area_id IS 'Auto-generated primary key';
COMMENT ON COLUMN water_body_protected_areas.water_body_code IS 'Reference to water_bodies.eu_water_body_code';
COMMENT ON COLUMN water_body_protected_areas.eu_protected_area_code IS 'EU Protected Area unique code';
COMMENT ON COLUMN water_body_protected_areas.protected_area_type IS 'Type of protected area: drinkingWater, bathing, birds, fish, habitats, shellfish, nitrates, UWWT';
COMMENT ON COLUMN water_body_protected_areas.objectives_set IS 'Whether objectives have been set for this protected area';
COMMENT ON COLUMN water_body_protected_areas.objectives_met IS 'Whether objectives have been met for this protected area';

-- ============================================================
-- TABLE: sensitivity
-- Actual columns: id, code, definition, nutrient_sensitivity
-- ============================================================

COMMENT ON COLUMN sensitivity.id IS 'Auto-generated primary key';
COMMENT ON COLUMN sensitivity.code IS 'Sensitivity area code: A54, A523, A5854, A58523, CSA, LSA, or NULL for Normal';
COMMENT ON COLUMN sensitivity.definition IS 'Full name/definition of the sensitivity area type';
COMMENT ON COLUMN sensitivity.nutrient_sensitivity IS 'Whether the area is sensitive to nutrients (nitrogen/phosphorus)';

-- ============================================================
-- TABLE: country
-- Actual columns: country_id, country_code, country_name
-- ============================================================

COMMENT ON COLUMN country.country_id IS 'Auto-generated primary key';
COMMENT ON COLUMN country.country_code IS 'ISO 3166-1 alpha-2 country code';
COMMENT ON COLUMN country.country_name IS 'Full country name in English';

-- ============================================================
-- TABLE: report_code (T_ReportPeriod)
-- Actual columns: rep_code_id, rep_code, country_code, year, version
-- ============================================================

COMMENT ON COLUMN report_code.rep_code_id IS 'Auto-generated primary key';
COMMENT ON COLUMN report_code.rep_code IS 'Unique report code (repCode)';
COMMENT ON COLUMN report_code.country_code IS 'ISO 3166-1 alpha-2 country code';
COMMENT ON COLUMN report_code.year IS 'Reporting period year (repReportedPeriod)';
COMMENT ON COLUMN report_code.version IS 'Report version/publication date (repVersion)';

-- ============================================================
-- Table-level comments
-- ============================================================

COMMENT ON TABLE plants IS 'Urban Waste Water Treatment Plants - Source: DISCO T_UWWTPS + T_Art17_FLAUWWTP';
COMMENT ON TABLE agglomeration IS 'Agglomerations (populated areas) - Source: DISCO T_Agglomerations';
COMMENT ON TABLE discharge_points IS 'Discharge points from UWWTPs to receiving waters - Source: DISCO T_DischargePoints';
COMMENT ON TABLE water_bodies IS 'Water bodies (Surface + Groundwater) - Source: DISCO SWB_SurfaceWaterBody + GWB_GroundWaterBody';
COMMENT ON TABLE water_body_protected_areas IS 'Protected areas associated with water bodies - Source: DISCO SWB/GWB AssociatedProtectedArea';
COMMENT ON TABLE sensitivity IS 'Receiving area sensitivity types under UWWTD Article 5';
COMMENT ON TABLE country IS 'Country reference table with ISO codes';
COMMENT ON TABLE report_code IS 'Report periods and versions - Source: DISCO T_ReportPeriod';
