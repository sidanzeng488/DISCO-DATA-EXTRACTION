# Discharge Points Specification (T_DischargePoints)

**DISCO Table**: `[WISE_UWWTD].[v1r1].[T_DischargePoints]`  
**Description**: Points where treated wastewater is discharged to receiving water bodies  
**Record Count**: ~26,000  

## Primary Keys and Identifiers

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `dcpCode` | `dcp_code` | VARCHAR(100) | NO | Unique discharge point identifier | Format: `{CountryCode}DP_*` |
| `dcpDischargePointsID` | `dcpDischargePointsID` | INTEGER | NO | Internal DISCO ID | Auto-generated |
| `CountryCode` | `country_code` | VARCHAR(10) | NO | ISO country code | EU member state |
| `uwwCode` | `plant_code` | VARCHAR(100) | YES | Associated plant code | Links to plants table |
| `rptMStateKey` | `rptMStateKey` | VARCHAR(10) | NO | Member state key | Same as CountryCode |

## Basic Information

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `dcpName` | `dcp_name` | VARCHAR(500) | YES | Name of discharge point |
| `dcpState` | `dcpState` | BOOLEAN | NO | Active status |
| `dcpNuts` | `dcpNuts` | VARCHAR(20) | YES | NUTS region code |

## Location Data

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `dcpLatitude` | `latitude` | NUMERIC | YES | Latitude (decimal degrees) | Range: -90 to 90 |
| `dcpLongitude` | `longitude` | NUMERIC | YES | Longitude (decimal degrees) | Range: -180 to 180 |
| `dcpGeometry` | `dcpGeometry` | TEXT | YES | WKT geometry | Format: `POINT (lat lon)` |

## Water Body Type Classification

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `dcpWaterBodyType` | `water_body_type` | VARCHAR(10) | YES | Type of receiving water | See table below |
| `dcpSurfaceWaters` | `is_surface_water` | BOOLEAN | YES | Is discharge to surface water | `True` = surface, `False` = groundwater |
| `dcpIrrigation` | `dcpIrrigation` | BOOLEAN | YES | Discharge used for irrigation | `True` = irrigation use |

### Water Body Type Codes

| Code | Description | Category |
|------|-------------|----------|
| `FW` | Freshwater | Surface Water |
| `CW` | Coastal Water | Surface Water |
| `ES` | Estuary | Surface Water |
| `LF` | Lake - Freshwater | Surface Water |
| `LC` | Lake - Coastal | Surface Water |
| `TW` | Transitional Water | Surface Water |
| `GW` | Groundwater | Groundwater |
| `O` | Other | Other |

## Water Body Links

| DISCO Column | DB Column | Type | Nullable | Description | Source Logic |
|--------------|-----------|------|----------|-------------|--------------|
| `dcpWaterbodyID` | `swb_water_body_code` | VARCHAR(100) | YES | Surface water body code | Links to SWB_SurfaceWaterBody |
| `dcpGroundWater` | `gwb_water_body_code` | VARCHAR(100) | YES | Ground water body code | Links to GWB_GroundWaterBody |
| `dcpReceivingWater` | `receiving_water` | VARCHAR(500) | YES | Name of receiving water | Free text name |

### Water Body Link Logic

```python
# Determine which water body code to use
if dcpSurfaceWaters == True:
    water_body_code = dcpWaterbodyID  # Link to SWB table
else:
    water_body_code = dcpGroundWater  # Link to GWB table
```

## Water Framework Directive (WFD) References

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `dcpWFDRBD` | `wfd_rbd` | VARCHAR(100) | YES | WFD River Basin District code |
| `dcpWFDSubUnit` | `wfd_sub_unit` | VARCHAR(100) | YES | WFD Sub-unit code |

## Sensitivity Classification

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `dcpTypeOfReceivingArea` | `sensitivity_code` | VARCHAR(20) | YES | Sensitivity area designation | See sensitivity.md |
| `rcaCode` | `rca_code` | VARCHAR(100) | YES | Receiving area code | Links to receiving areas |

### Sensitivity Area Types

| Code | Description | Nutrient Sensitive |
|------|-------------|-------------------|
| `A54` | Art. 5(4) area | YES |
| `A523` | Sensitive area Art. 5(2,3) | YES |
| `A5854` | Art. 5(8) + Art. 5(4) | YES |
| `A58523` | Art. 5(8) + Art. 5(2,3) | YES |
| `CSA` | Catchment Art. 5(5) | YES |
| `LSA` | Less sensitive area | NO |
| `NA` | Normal area | NO |

## Commission Acceptance Fields

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `dcpNotAffect` | `dcpNotAffect` | BOOLEAN | YES | Does not affect water quality |
| `dcpMSProvide` | `dcpMSProvide` | BOOLEAN | YES | Member State provided evidence |
| `dcpCOMAccept` | `dcpCOMAccept` | BOOLEAN | YES | Commission accepted evidence |

## Reference Dates

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `dcpWFDRBDReferenceDate` | `dcpWFDRBDReferenceDate` | DATE | YES | WFD RBD reference date |
| `dcpWaterBodyReferenceDate` | `dcpWaterBodyReferenceDate` | DATE | YES | Water body reference date |
| `dcpGroundWaterReferenceDate` | `dcpGroundWaterReferenceDate` | DATE | YES | Groundwater reference date |
| `dcpReceivingWaterReferenceDate` | `dcpReceivingWaterReferenceDate` | DATE | YES | Receiving water reference date |
| `dcpWFDSubUnitReferenceDate` | `dcpWFDSubUnitReferenceDate` | DATE | YES | WFD sub-unit reference date |

## Lifecycle Dates

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `dcpBeginLife` | `dcpBeginLife` | DATE | YES | Record begin date |
| `dcpEndLife` | `dcpEndLife` | DATE | YES | Record end date |

## Additional Fields

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `dcpRemarks` | `dcpRemarks` | TEXT | YES | General remarks |

## Upload Logic

1. **Extract**: Download from DISCO API
2. **Validate**:
   - Check `dcpCode` uniqueness
   - Validate `uwwCode` exists in plants table
   - Verify water body type codes
   - Check lat/lon ranges
3. **Transform**:
   - Map DISCO columns to database columns
   - Determine `water_body_code` based on `is_surface_water`
   - Merge SWB/GWB codes into single water_body_code
4. **Load**: Insert with upsert logic

## Judgment Logic

### Water Body Link Resolution

```python
def resolve_water_body_link(dcp_row):
    """
    Determine which water body table to link to based on 
    dcpSurfaceWaters flag and available water body codes.
    """
    if dcp_row['dcpSurfaceWaters'] == True:
        # Link to Surface Water Body (SWB)
        water_body_code = dcp_row['dcpWaterbodyID']
        water_type = 'SWB'
    else:
        # Link to Ground Water Body (GWB)
        water_body_code = dcp_row['dcpGroundWater']
        water_type = 'GWB'
    
    return {
        'water_body_code': water_body_code,
        'water_type': water_type
    }
```

### Sensitivity Impact Assessment

```python
def assess_sensitivity_impact(dcp_row, plant_row):
    """
    Determine if discharge requires nutrient removal based on 
    sensitivity classification and plant capacity.
    """
    sensitivity_code = dcp_row['dcpTypeOfReceivingArea']
    plant_capacity = plant_row['uwwCapacity']
    
    # Sensitive areas requiring nutrient removal
    sensitive_codes = ['A54', 'A523', 'A5854', 'A58523', 'CSA']
    
    if sensitivity_code in sensitive_codes:
        requires_nutrient_removal = True
        
        # Phosphorus threshold: 10,000 PE
        requires_p_removal = plant_capacity >= 10000
        
        # Nitrogen threshold: 100,000 PE
        requires_n_removal = plant_capacity >= 100000
    else:
        requires_nutrient_removal = False
        requires_p_removal = False
        requires_n_removal = False
    
    return {
        'requires_nutrient_removal': requires_nutrient_removal,
        'requires_p_removal': requires_p_removal,
        'requires_n_removal': requires_n_removal
    }
```

### Water Body Type Validation

```python
def validate_water_body_type(dcp_row):
    """
    Validate consistency between water body type and surface water flag.
    """
    water_body_type = dcp_row['dcpWaterBodyType']
    is_surface_water = dcp_row['dcpSurfaceWaters']
    
    surface_types = ['FW', 'CW', 'ES', 'LF', 'LC', 'TW']
    ground_types = ['GW']
    
    if is_surface_water and water_body_type not in surface_types:
        return False, "Mismatch: surface water flag but non-surface type"
    
    if not is_surface_water and water_body_type not in ground_types:
        return False, "Mismatch: groundwater flag but non-ground type"
    
    return True, "Valid"
```

## Relationships

### Plant → Discharge Points (1:N)
One plant can have multiple discharge points.

```sql
SELECT p.uwwCode, p.uwwName, d.dcpCode, d.dcpName
FROM plants p
JOIN discharge_points d ON p.uwwCode = d.uwwCode
```

### Discharge Point → Water Body (N:1)
Multiple discharge points can discharge to the same water body.

```sql
SELECT d.dcpCode, wb.eu_water_body_code, wb.water_body_name
FROM discharge_points d
JOIN water_bodies wb ON d.water_body_code = wb.eu_water_body_code
```
