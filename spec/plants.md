# Plants Specification (T_UWWTPS)

**DISCO Table**: `[WISE_UWWTD].[v1r1].[T_UWWTPS]`  
**Description**: Urban Waste Water Treatment Plants  
**Record Count**: ~26,000  

## Primary Keys and Identifiers

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `uwwCode` | `uwwtp_code` | VARCHAR(100) | NO | Unique plant identifier code | Must be unique; Format: `{CountryCode}TP_*` |
| `uwwUWWTPSID` | - | INTEGER | NO | Internal DISCO ID | **Not mapped to DB** |
| `CountryCode` | `country_code` | VARCHAR(10) | NO | ISO country code (2 letters) | Must match valid EU country code |
| `repCode` | `rep_code` | VARCHAR(50) | NO | Report period reference | Format: `{CountryCode}_UWWTD_{Year}` |
| `rptMStateKey` | - | VARCHAR(10) | NO | Member state key | **Not mapped to DB** |

## Basic Information

| DISCO Column | DB Column | Type | Nullable | Description | Source Logic |
|--------------|-----------|------|----------|-------------|--------------|
| `uwwName` | `plant_name` | VARCHAR(500) | YES | Name of the treatment plant | Direct from DISCO |
| `uwwState` | - | INTEGER | NO | Plant state (1=active) | **Not mapped to DB** - 1 = active, 0 = inactive, 2 = temporary inactive |
| `aggCode` | `agg_code` | VARCHAR(100) | YES | Associated agglomeration code | Links to agglomerations table |

## Location Data

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `uwwLatitude` | `lat` | NUMERIC | YES | Latitude (decimal degrees) | Range: -90 to 90; NULL if not reported |
| `uwwLongitude` | `longitude` | NUMERIC | YES | Longitude (decimal degrees) | Range: -180 to 180 |
| `uwwNUTS` | - | VARCHAR(20) | YES | NUTS region code | **Not mapped to DB** |

## Capacity and Load

| DISCO Column | DB Column | Type | Nullable | Description | Source Logic |
|--------------|-----------|------|----------|-------------|--------------|
| `uwwCapacity` | `plant_capacity` | INTEGER | YES | Design capacity (PE - Population Equivalent) | Number of PE the plant is designed for |
| `uwwLoadEnteringUWWTP` | `plant_waste_load_pe` | NUMERIC | YES | Actual organic load entering the UWWTP in population equivalent (p.e.) | Direct copy as numeric |
| `uwwCollectingSystem` | - | VARCHAR(20) | YES | Type of collecting system | **Not mapped to DB** - Values: `ISCON`, `NOTCON`, etc. |

## Treatment Types (Boolean Fields)

| DISCO Column | DB Column | Type | Nullable | Description | Source Logic |
|--------------|-----------|------|----------|-------------|--------------|
| `uwwPrimaryTreatment` | `provides_primary_treatment` | BOOLEAN | YES | Has primary treatment | `True` = mechanical treatment exists |
| `uwwSecondaryTreatment` | `provides_secondary_treatment` | BOOLEAN | YES | Has secondary treatment | `True` = biological treatment exists |
| `uwwOtherTreatment` | `other_treatment_provided` | BOOLEAN | YES | Has tertiary/other treatment | `True` = advanced treatment exists |
| `uwwNRemoval` | `provides_nitrogen_removal` | BOOLEAN | YES | Has nitrogen removal | `True` = nitrogen treatment capability |
| `uwwPRemoval` | `provides_phosphorus_removal` | BOOLEAN | YES | Has phosphorus removal | `True` = phosphorus treatment capability |
| `uwwUV` | `includes_uv_treatment` | BOOLEAN | YES | Has UV disinfection | `True` = UV treatment installed |
| `uwwChlorination` | `includes_chlorination` | BOOLEAN | YES | Has chlorination | `True` = chlorination process |
| `uwwOzonation` | `includes_ozonation` | BOOLEAN | YES | Has ozonation | `True` = ozone treatment |
| `uwwSandFiltration` | `includes_sand_filtration` | BOOLEAN | YES | Has sand filtration | `True` = sand filter installed |
| `uwwMicroFiltration` | `includes_microfiltration` | BOOLEAN | YES | Has microfiltration | `True` = membrane filtration |
| `uwwOther` | - | BOOLEAN | YES | Has other treatment type | **Not mapped to DB** |
| `uwwSpecification` | - | TEXT | YES | Specification of other treatment | **Not mapped to DB** |

## Compliance Performance Fields

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `uwwBOD5Perf` | `bod_compliance` | VARCHAR(10) | YES | BOD5 compliance status | `P` = Pass, `F` = Fail, `NR` = Not Relevant, `NA` = Data not available |
| `uwwCODPerf` | `cod_compliance` | VARCHAR(10) | YES | COD compliance status | `P` = Pass, `F` = Fail, `NR` = Not Relevant, `NA` = Data not available |
| `uwwTSSPerf` | `tss_compliance` | VARCHAR(10) | YES | TSS compliance status | `P` = Pass, `F` = Fail, `NR` = Not Relevant, `NA` = Data not available |
| `uwwNTotPerf` | `nitrogen_compliance` | VARCHAR(10) | YES | Total Nitrogen compliance | `P` = Pass, `F` = Fail, `NR` = Not Relevant, `NA` = Data not available |
| `uwwPTotPerf` | `phosphorus_compliance` | VARCHAR(10) | YES | Total Phosphorus compliance | `P` = Pass, `F` = Fail, `NR` = Not Relevant, `NA` = Data not available |
| `uwwOtherPerf` | `other_compliance` | VARCHAR(10) | YES | Other parameter compliance | `P` = Pass, `F` = Fail, `NR` = Not Relevant, `NA` = Data not available |

## BOD5 Data (Biochemical Oxygen Demand)

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `uwwBODIncomingMeasured` | `bod_incoming_measured` | NUMERIC | YES | Measured BOD5 entering | tons/year |
| `uwwBODIncomingCalculated` | `bod_incoming_calculated` | NUMERIC | YES | Calculated BOD5 entering | tons/year |
| `uwwBODIncomingEstimated` | `bod_incoming_estimated` | NUMERIC | YES | Estimated BOD5 entering | tons/year |
| `uwwBODDischargeMeasured` | `bod_outgoing_measured` | NUMERIC | YES | Measured BOD5 discharged |tons/year |
| `uwwBODDischargeCalculated` | `bod_outgoing_calculated` | NUMERIC | YES | Calculated BOD5 discharged | tons/year |
| `uwwBODDischargeEstimated` | `bod_outgoing_estimated` | NUMERIC | YES | Estimated BOD5 discharged | tons/year |

**Calculated Field**: `bod_removal_pct` = (1 - discharge/incoming) × 100%

## COD Data (Chemical Oxygen Demand)

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `uwwCODIncomingMeasured` | `cod_incoming_measured` | NUMERIC | YES | Measured COD entering | tons/year |
| `uwwCODIncomingCalculated` | `cod_incoming_calculated` | NUMERIC | YES | Calculated COD entering | tons/year |
| `uwwCODIncomingEstimated` | `cod_incoming_estimated` | NUMERIC | YES | Estimated COD entering | tons/year |
| `uwwCODDischargeMeasured` | `cod_outgoing_measured` | NUMERIC | YES | Measured COD discharged |tons/year |
| `uwwCODDischargeCalculated` | `cod_outgoing_calculated` | NUMERIC | YES | Calculated COD discharged | tons/year |
| `uwwCODDischargeEstimated` | `cod_outgoing_estimated` | NUMERIC | YES | Estimated COD discharged | tons/year |

**Calculated Field**: `cod_removal_pct` = (1 - discharge/incoming) × 100%

## Nitrogen Data (Total Nitrogen)

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `uwwNIncomingMeasured` | `nitrogen_incoming_measured` | NUMERIC | YES | Measured N entering |tons/year |
| `uwwNIncomingCalculated` | `nitrogen_incoming_calculated` | NUMERIC | YES | Calculated N entering |tons/year |
| `uwwNIncomingEstimated` | `nitrogen_incoming_estimated` | NUMERIC | YES | Estimated N entering |tons/year |
| `uwwNDischargeMeasured` | `nitrogen_outgoing_measured` | NUMERIC | YES | Measured N discharged |tons/year |
| `uwwNDischargeCalculated` | `nitrogen_outgoing_calculated` | NUMERIC | YES | Calculated N discharged |tons/year |
| `uwwNDischargeEstimated` | `nitrogen_outgoing_estimated` | NUMERIC | YES | Estimated N discharged |tons/year |

**Calculated Field**: `nitrogen_removal_pct` = (1 - discharge/incoming) × 100%

## Phosphorus Data (Total Phosphorus)

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `uwwPIncomingMeasured` | `phosphorus_incoming_measured` | NUMERIC | YES | Measured P entering |tons/year |
| `uwwPIncomingCalculated` | `phosphorus_incoming_calculated` | NUMERIC | YES | Calculated P entering |tons/year |
| `uwwPIncomingEstimated` | `phosphorus_incoming_estimated` | NUMERIC | YES | Estimated P entering |tons/year |
| `uwwPDischargeMeasured` | `phosphorus_outgoing_measured` | NUMERIC | YES | Measured P discharged |tons/year |
| `uwwPDischargeCalculated` | `phosphorus_outgoing_calculated` | NUMERIC | YES | Calculated P discharged |tons/year |
| `uwwPDischargeEstimated` | `phosphorus_outgoing_estimated` | NUMERIC | YES | Estimated P discharged |tons/year |

**Calculated Field**: `phosphorus_removal_pct` = (1 - discharge/incoming) × 100%

## Failure and Performance Issues

| DISCO Column | DB Column | Type | Nullable | Description | Source Logic |
|--------------|-----------|------|----------|-------------|--------------|
| `uwwBadPerformance` | - | BOOLEAN | YES | Performance issues flag | **Not mapped to DB** |
| `uwwAccidents` | - | BOOLEAN | YES | Accidents occurred | **Not mapped to DB** |
| `uwwBadDesign` | - | BOOLEAN | YES | Design issues flag | **Not mapped to DB** |
| `uwwInformation` | `failure_notes` | TEXT | YES | Failure explanation (notes about plant failures/issues) | Free text describing issues |

## Water Reuse

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `uwwWasteWaterReuse` | `pct_wastewater_reused` | NUMERIC | YES | Percentage of wastewater reused as annual mean percentage| % (0-100) |
| `uwwWasteWaterTreated` | `volume_wastewater_reused_m3_per_year` | NUMERIC | YES | Volume of wastewater treated | m3/year |
| `uwwMethodWasteWaterTreated` | - | VARCHAR(10) | YES | Measurement method | **Not mapped to DB** - `M` = Measured, `C` = Calculated, `E` = Estimated |

## Lifecycle Dates

| DISCO Column | DB Column | Type | Nullable | Description | Format |
|--------------|-----------|------|----------|-------------|--------|
| `uwwBeginLife` | `initial_designation_date` | DATE | YES | the date of first designation by the official| ISO date (YYYY-MM-DD) |
| `uwwBeginLife_original` | - | TEXT | YES | Original date string | **Not mapped to DB** |
| `uwwDateClosing` | - | DATE | YES | Plant closing date | **Not mapped to DB** |
| `uwwDateClosing_original` | - | TEXT | YES | Original closing date | **Not mapped to DB** |
| `uwwEndLife` | - | DATE | YES | End of reporting life | **Not mapped to DB** |
| `uwwEndLife_orginal` | - | TEXT | YES | Original end date | **Not mapped to DB** |
| `uwwHistorie` | - | BOOLEAN | YES | Historical record flag | **Not mapped to DB** |

## Additional Fields

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `uwwRemarks` | `plant_notes` | TEXT | YES | General remarks/comments |
| `uwwHyperlink` | `hyperlink` | TEXT | YES | External URL link to plant information | Direct copy |
| `uwwInspireIDFacility` | - | VARCHAR(100) | YES | INSPIRE facility ID | **Not mapped to DB** |
| `aggGenLT2000` | - | INTEGER | YES | Associated agglomeration < 2000 PE flag | **Not mapped to DB** |

## Upload Logic

1. **Extract**: Download from DISCO API using SQL query
2. **Validate**: 
   - Check `uwwCode` uniqueness
   - Validate country codes against EU member states
   - Verify numeric ranges (lat/lon, capacity > 0)
3. **Transform**:
   - Map DISCO column names to database columns (see `etl/config.py`)
   - Convert boolean strings (`True`/`False`) to boolean type
   - Parse dates from various formats
   - Calculate removal percentages from incoming/discharge values
4. **Load**: Insert into `plants` table with upsert logic (update on conflict)

## Judgment Logic

### Secondary Treatment Required
```python
def is_secondary_treatment_required(plant_capacity, agglomeration, discharge_point):
    """
    Determine if secondary treatment is required based on UWWTD Article 3 and 4.
    """
    # Plants >= 2000 PE always require secondary treatment
    if plant_capacity >= 2000:
        return True
    
    # Plants in sensitive areas require secondary treatment
    if discharge_point and discharge_point.sensitivity_code in ['A54', 'A523', 'A5854', 'A58523', 'CSA']:
        return True
    
    # Check if agglomeration is in sensitive area
    if agglomeration and agglomeration.is_sensitive_area:
        return True
    
    return False
```

### Nutrient Removal Required
```python
def get_nutrient_removal_requirements(plant_capacity, sensitivity_code):
    """
    Determine nutrient removal requirements based on UWWTD Article 5.
    """
    sensitive_codes = ['A54', 'A523', 'A5854', 'A58523', 'CSA']
    
    result = {
        'nutrient_removal_required': False,
        'phosphorus_removal_required': False,
        'nitrogen_removal_required': False
    }
    
    if sensitivity_code not in sensitive_codes:
        return result
    
    result['nutrient_removal_required'] = True
    
    # Phosphorus removal: >= 10,000 PE in sensitive areas
    if plant_capacity >= 10000:
        result['phosphorus_removal_required'] = True
    
    # Nitrogen removal: >= 100,000 PE in sensitive areas
    if plant_capacity >= 100000:
        result['nitrogen_removal_required'] = True
    
    return result
```

### Quaternary Treatment Required (Micropollutant Removal)
```python
def get_quaternary_treatment_requirement(plant_capacity, discharge_points, protected_areas):
    """
    Determine quaternary treatment (micropollutant removal) requirement 
    under revised UWWTD.
    
    Quaternary treatment targets removal of micropollutants such as:
    - Pharmaceuticals
    - Personal care products
    - Pesticides
    - Microplastics
    
    Deadline: 2045
    
    Args:
        plant_capacity: Plant capacity in PE (or plant_waste_load_pe)
        discharge_points: List of discharge points for this plant
        protected_areas: List of protected areas associated with water bodies
    
    Returns:
        'Yes' - Required (>= 150,000 PE)
        'Likely' - Required if discharging to sensitive waters (>= 10,000 PE)
        'No' - Not required
    """
    # Threshold 1: >= 150,000 PE always requires quaternary treatment
    if plant_capacity >= 150000:
        return {
            'status': 'Yes',
            'reason': '>= 150,000 PE - Micropollutant removal required by 2045',
            'deadline': '2045-12-31'
        }
    
    # Threshold 2: >= 10,000 PE + sensitive receiving water OR low dilution ratio
    if plant_capacity >= 10000:
        # Check if any discharge point leads to sensitive protected areas
        sensitive_pa_types = ['DrinkingWater', 'Bathing', 'Shellfish']
        
        for dcp in discharge_points:
            water_body_code = dcp.get('water_body_code')
            dilution_ratio = dcp.get('dilution_ratio')
            
            # Check 1: Low dilution ratio (< 10)
            if dilution_ratio is not None and dilution_ratio < 10:
                return {
                    'status': 'Likely',
                    'reason': f'>= 10,000 PE + dilution ratio < 10 ({dilution_ratio})',
                    'deadline': '2045-12-31',
                    'dilution_ratio': dilution_ratio
                }
            
            # Check 2: Sensitive protected areas
            if water_body_code:
                for pa in protected_areas:
                    if pa.get('water_body_code') == water_body_code:
                        if pa.get('protected_area_type') in sensitive_pa_types:
                            return {
                                'status': 'Likely',
                                'reason': f'>= 10,000 PE + {pa.get("protected_area_type")} protected area',
                                'deadline': '2045-12-31',
                                'sensitive_area_type': pa.get('protected_area_type')
                            }
        
        # No sensitive condition found
        return {
            'status': 'No',
            'reason': '>= 10,000 PE but no sensitive receiving water or low dilution',
            'deadline': None
        }
    
    # Below 10,000 PE - not required
    return {
        'status': 'No',
        'reason': '< 10,000 PE - Quaternary treatment not required',
        'deadline': None
    }
```

### Simplified Quaternary Check (Database Column)
```python
def calculate_requires_quaternary_treatment(plant_capacity, has_sensitive_receiving_water, dilution_ratio):
    """
    Simplified quaternary treatment check for database column.
    
    Database column: requires_quaternary_treatment VARCHAR(10)
    Values: 'Yes', 'Likely', 'No'
    
    Args:
        plant_capacity: Plant design capacity (PE)
        has_sensitive_receiving_water: True if discharging to bathing/drinking/shellfish areas
        dilution_ratio: Dilution ratio at discharge point (None if unknown)
    
    Returns:
        'Yes' | 'Likely' | 'No'
    """
    if plant_capacity >= 150000:
        return 'Yes'
    elif plant_capacity >= 10000:
        # Check sensitive conditions
        if has_sensitive_receiving_water:
            return 'Likely'
        if dilution_ratio is not None and dilution_ratio < 10:
            return 'Likely'
    return 'No'
```

### Treatment Tier Assignment
```python
def assign_treatment_tier(plant_capacity):
    """
    Assign treatment tier based on plant capacity.
    """
    if plant_capacity < 1000:
        return 'Individual/Small (< 1000 PE)'
    elif plant_capacity < 2000:
        return 'Tier 1 (1000-1999 PE)'
    elif plant_capacity < 10000:
        return 'Tier 2 (2000-9999 PE)'
    elif plant_capacity < 100000:
        return 'Tier 3 (10000-99999 PE)'
    else:
        return 'Tier 4 (>= 100000 PE)'
```

### Removal Percentage Calculation
```python
def calculate_removal_percentage(incoming, outgoing):
    """
    Calculate pollutant removal percentage.
    Returns None if data is insufficient.
    """
    # Get best available incoming value (measured > calculated > estimated)
    incoming_value = incoming.get('measured') or incoming.get('calculated') or incoming.get('estimated')
    
    # Get best available outgoing value
    outgoing_value = outgoing.get('measured') or outgoing.get('calculated') or outgoing.get('estimated')
    
    if incoming_value is None or outgoing_value is None or incoming_value == 0:
        return None
    
    removal_pct = (1 - outgoing_value / incoming_value) * 100
    return max(0, min(100, removal_pct))  # Clamp to 0-100%
```

### Compliance Status Check
```python
def check_compliance_status(parameter, outgoing_value, threshold, is_required):
    """
    Check compliance status for a parameter.
    
    Args:
        parameter: 'BOD', 'COD', 'TSS', 'N', 'P'
        outgoing_value: Measured/calculated/estimated discharge value
        threshold: Regulatory threshold value
        is_required: Whether this parameter check is required
    
    Returns:
        'P' = Pass, 'F' = Fail, 'NR' = Not Required, 'NA' = Data not available
    """
    if not is_required:
        return 'NR'
    
    if outgoing_value is None:
        return 'NA'
    
    # Thresholds (concentration mg/L or % reduction)
    thresholds = {
        'BOD': {'concentration': 25, 'reduction': 70},      # mg/L or 70-90% reduction
        'COD': {'concentration': 125, 'reduction': 75},     # mg/L or 75% reduction
        'TSS': {'concentration': 35, 'reduction': 90},      # mg/L or 90% reduction
        'N':   {'concentration': 15, 'reduction': 70},      # mg/L (10-15) or 70-80% reduction
        'P':   {'concentration': 2, 'reduction': 80}        # mg/L (1-2) or 80% reduction
    }
    
    param_threshold = thresholds.get(parameter, {})
    if outgoing_value <= param_threshold.get('concentration', float('inf')):
        return 'P'
    else:
        return 'F'
```

### Data Quality Assessment
```python
def assess_data_quality(plant_row):
    """
    Assess data quality and completeness for a plant record.
    """
    quality_score = 0
    max_score = 10
    issues = []
    
    # Required fields
    if plant_row.get('uwwCode'):
        quality_score += 1
    else:
        issues.append('Missing plant code')
    
    if plant_row.get('uwwName'):
        quality_score += 1
    else:
        issues.append('Missing plant name')
    
    # Location data
    lat, lon = plant_row.get('uwwLatitude'), plant_row.get('uwwLongitude')
    if lat and lon and -90 <= lat <= 90 and -180 <= lon <= 180:
        quality_score += 1
    else:
        issues.append('Invalid or missing coordinates')
    
    # Capacity
    capacity = plant_row.get('uwwCapacity')
    if capacity and capacity > 0:
        quality_score += 1
    else:
        issues.append('Invalid or missing capacity')
    
    # Treatment information
    if plant_row.get('uwwSecondaryTreatment') is not None:
        quality_score += 1
    else:
        issues.append('Missing treatment information')
    
    # Performance data (BOD/COD/N/P)
    has_performance_data = any([
        plant_row.get('uwwBODDischargeMeasured'),
        plant_row.get('uwwCODDischargeMeasured'),
        plant_row.get('uwwNDischargeMeasured'),
        plant_row.get('uwwPDischargeMeasured')
    ])
    if has_performance_data:
        quality_score += 2
    else:
        issues.append('Missing discharge performance data')
    
    # Compliance status
    if plant_row.get('uwwBOD5Perf') and plant_row.get('uwwCODPerf'):
        quality_score += 2
    else:
        issues.append('Missing compliance status')
    
    # Report code
    if plant_row.get('repCode'):
        quality_score += 1
    else:
        issues.append('Missing report code')
    
    return {
        'score': quality_score,
        'max_score': max_score,
        'percentage': (quality_score / max_score) * 100,
        'issues': issues
    }
```

### Plant Status Validation
```python
def validate_plant_status(plant_row):
    """
    Validate plant record consistency and status.
    """
    errors = []
    warnings = []
    
    # Check capacity vs load consistency
    capacity = plant_row.get('uwwCapacity', 0)
    load = plant_row.get('uwwLoadEnteringUWWTP', 0)
    if load and capacity and load > capacity * 1.5:
        warnings.append(f'Load ({load} PE) significantly exceeds capacity ({capacity} PE)')
    
    # Check treatment consistency
    if plant_row.get('uwwNRemoval') and not plant_row.get('uwwSecondaryTreatment'):
        errors.append('Nitrogen removal without secondary treatment')
    
    if plant_row.get('uwwPRemoval') and not plant_row.get('uwwSecondaryTreatment'):
        errors.append('Phosphorus removal without secondary treatment')
    
    # Check discharge vs incoming values
    bod_in = plant_row.get('uwwBODIncomingMeasured') or plant_row.get('uwwBODIncomingCalculated')
    bod_out = plant_row.get('uwwBODDischargeMeasured') or plant_row.get('uwwBODDischargeCalculated')
    if bod_in and bod_out and bod_out > bod_in:
        warnings.append('BOD discharge exceeds incoming (possible data error)')
    
    # Check date consistency
    begin_life = plant_row.get('uwwBeginLife')
    date_closing = plant_row.get('uwwDateClosing')
    if begin_life and date_closing and begin_life > date_closing:
        errors.append('Begin life date is after closing date')
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }
```
