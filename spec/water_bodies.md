# Water Bodies Specification (SWB + GWB)

**DISCO Tables**:
- `[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]` - Surface Water Bodies
- `[WISE_WFD].[v2r1].[GWB_GroundWaterBody]` - Ground Water Bodies

**Description**: Water bodies reported under the Water Framework Directive (WFD)  
**Record Count**: ~200,000+ (combined)

## Overview

Water bodies are the receiving environments for wastewater discharge. The database merges Surface Water Bodies (SWB) and Ground Water Bodies (GWB) into a unified `water_bodies` table with a `water_type` discriminator.

## Unified Schema

| DB Column | Type | Nullable | Description | SWB Source | GWB Source |
|-----------|------|----------|-------------|------------|------------|
| `water_body_id` | SERIAL | NO | Auto-generated primary key | - | - |
| `eu_water_body_code` | VARCHAR(100) | NO | Unique EU water body code | `euSurfaceWaterBodyCode` | `euGroundWaterBodyCode` |
| `water_type` | VARCHAR(10) | NO | Type indicator | 'SWB' | 'GWB' |
| `water_body_name` | VARCHAR(500) | YES | Name of water body | `surfaceWaterBodyName` | - |
| `country_code` | VARCHAR(10) | NO | ISO country code | `countryCode` | `countryCode` |

## Surface Water Body (SWB) Specific Fields

### Primary Identifiers

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `euSurfaceWaterBodyCode` | `eu_water_body_code` | VARCHAR(100) | NO | Unique EU identifier |
| `surfaceWaterBodyName` | `water_body_name` | VARCHAR(500) | YES | Name of water body |
| `countryCode` | `country_code` | VARCHAR(10) | NO | ISO country code |

### River Basin District

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `euRBDCode` | `eu_rbd_code` | VARCHAR(50) | YES | EU River Basin District code |
| `rbdName` | `rbd_name` | VARCHAR(200) | YES | River Basin District name |
| `euSubUnitCode` | `eu_sub_unit_code` | VARCHAR(50) | YES | Sub-unit code |
| `subUnitName` | `sub_unit_name` | VARCHAR(200) | YES | Sub-unit name |

### Water Body Characteristics

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `surfaceWaterBodyCategory` | `surface_water_category` | VARCHAR(50) | YES | Category of surface water | River, Lake, Coastal, Transitional |
| `naturalAWBHMWB` | `natural_awb_hmwb` | VARCHAR(50) | YES | Natural/Artificial/Heavily Modified | Natural, AWB, HMWB |
| `cYear` | `c_year` | INTEGER | YES | Reference year for classification | |
| `cArea` | `c_area` | NUMERIC | YES | Area (for lakes, coastal) | km² |
| `cLength` | `c_length` | NUMERIC | YES | Length (for rivers) | km |

### Ecological Status

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `swEcologicalStatusOrPotentialValue` | `sw_ecological_status` | VARCHAR(50) | YES | Ecological status/potential | High, Good, Moderate, Poor, Bad, Unknown |
| `swEcologicalAssessmentYear` | `sw_ecological_assessment_year` | INTEGER | YES | Year of assessment | |

### Chemical Status

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `swChemicalStatusValue` | `sw_chemical_status` | VARCHAR(50) | YES | Chemical status | Good, Failing to achieve good, Unknown |
| `swChemicalAssessmentYear` | `sw_chemical_assessment_year` | INTEGER | YES | Year of assessment | |

## Ground Water Body (GWB) Specific Fields

### Primary Identifiers

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `euGroundWaterBodyCode` | `eu_water_body_code` | VARCHAR(100) | NO | Unique EU identifier |
| `countryCode` | `country_code` | VARCHAR(10) | NO | ISO country code |

### River Basin District

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `euRBDCode` | `eu_rbd_code` | VARCHAR(50) | YES | EU River Basin District code |
| `rbdName` | `rbd_name` | VARCHAR(200) | YES | River Basin District name |

### Quantitative Status

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `gwQuantitativeStatusValue` | `gw_quantitative_status` | VARCHAR(50) | YES | Quantitative status | Good, Poor, Unknown |
| `gwQuantitativeAssessmentYear` | `gw_quantitative_assessment_year` | INTEGER | YES | Year of assessment | |

### Chemical Status

| DISCO Column | DB Column | Type | Nullable | Description | Values |
|--------------|-----------|------|----------|-------------|--------|
| `gwChemicalStatusValue` | `gw_chemical_status` | VARCHAR(50) | YES | Chemical status | Good, Poor, Unknown |
| `gwChemicalAssessmentYear` | `gw_chemical_assessment_year` | INTEGER | YES | Year of assessment | |

## Reference Information

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `cYear` | `c_year` | INTEGER | YES | Classification year |
| `fileUrl` | `file_url` | TEXT | YES | Source file URL |

## Upload Logic

### Extraction

SWB and GWB tables are large and may require manual download:

```python
# Auto download (may timeout for large datasets)
DISCO_TABLES_MANUAL = {
    'swb_surface_water': {
        'disco_table': '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]',
        'url': 'https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=...',
        'estimated_rows': '~200,000',
    },
    'gwb_groundwater': {
        'disco_table': '[WISE_WFD].[v2r1].[GWB_GroundWaterBody]',
        'filename': 'gwb_groundwater.csv',
    },
}
```

### Transformation (Merge Logic)

```python
def merge_water_bodies():
    """
    Merge SWB and GWB into unified water_bodies table.
    """
    water_bodies = []
    
    # Process SWB
    for row in swb_data:
        wb = {
            'water_type': 'SWB',
            'eu_water_body_code': row['euSurfaceWaterBodyCode'],
            'water_body_name': row['surfaceWaterBodyName'],
            'country_code': row['countryCode'],
            'eu_rbd_code': row['euRBDCode'],
            'rbd_name': row['rbdName'],
            'c_year': row['cYear'],
            # SWB-specific
            'surface_water_category': row['surfaceWaterBodyCategory'],
            'sw_ecological_status': row['swEcologicalStatusOrPotentialValue'],
            'sw_chemical_status': row['swChemicalStatusValue'],
            # GWB-specific (NULL for SWB)
            'gw_quantitative_status': None,
            'gw_chemical_status': None,
        }
        water_bodies.append(wb)
    
    # Process GWB
    for row in gwb_data:
        wb = {
            'water_type': 'GWB',
            'eu_water_body_code': row['euGroundWaterBodyCode'],
            'water_body_name': None,  # GWB doesn't have names
            'country_code': row['countryCode'],
            'eu_rbd_code': row['euRBDCode'],
            'rbd_name': row['rbdName'],
            'c_year': row['cYear'],
            # SWB-specific (NULL for GWB)
            'surface_water_category': None,
            'sw_ecological_status': None,
            'sw_chemical_status': None,
            # GWB-specific
            'gw_quantitative_status': row['gwQuantitativeStatusValue'],
            'gw_chemical_status': row['gwChemicalStatusValue'],
        }
        water_bodies.append(wb)
    
    return water_bodies
```

## Judgment Logic

### Water Body Status Assessment

```python
def assess_water_body_status(water_body):
    """
    Assess overall water body status based on ecological/quantitative 
    and chemical status.
    """
    if water_body['water_type'] == 'SWB':
        ecological = water_body['sw_ecological_status']
        chemical = water_body['sw_chemical_status']
        
        # Good status requires both ecological and chemical to be Good
        if ecological in ['High', 'Good'] and chemical == 'Good':
            return 'Good'
        elif ecological in ['Bad', 'Poor'] or chemical == 'Failing to achieve good':
            return 'Failing'
        else:
            return 'Moderate'
    
    else:  # GWB
        quantitative = water_body['gw_quantitative_status']
        chemical = water_body['gw_chemical_status']
        
        if quantitative == 'Good' and chemical == 'Good':
            return 'Good'
        elif quantitative == 'Poor' or chemical == 'Poor':
            return 'Poor'
        else:
            return 'Unknown'
```

### Discharge Point Water Body Linking

```python
def link_discharge_to_water_body(discharge_point):
    """
    Link discharge point to appropriate water body.
    """
    if discharge_point['dcpSurfaceWaters'] == True:
        # Link to SWB
        water_body_code = discharge_point['dcpWaterbodyID']
        expected_type = 'SWB'
    else:
        # Link to GWB
        water_body_code = discharge_point['dcpGroundWater']
        expected_type = 'GWB'
    
    # Verify water body exists
    water_body = lookup_water_body(water_body_code)
    
    if water_body is None:
        return {
            'success': False,
            'error': f'Water body {water_body_code} not found'
        }
    
    if water_body['water_type'] != expected_type:
        return {
            'success': False,
            'error': f'Type mismatch: expected {expected_type}, found {water_body["water_type"]}'
        }
    
    return {
        'success': True,
        'water_body': water_body
    }
```

## Relationships

### Water Body ← Discharge Points (1:N)

```sql
SELECT 
    wb.eu_water_body_code,
    wb.water_body_name,
    wb.water_type,
    COUNT(dcp.dcp_code) as discharge_count
FROM water_bodies wb
LEFT JOIN discharge_points dcp ON wb.eu_water_body_code = dcp.water_body_code
GROUP BY wb.eu_water_body_code, wb.water_body_name, wb.water_type
HAVING COUNT(dcp.dcp_code) > 0
ORDER BY discharge_count DESC
```

### Water Body → Protected Areas (1:N)

```sql
SELECT 
    wb.eu_water_body_code,
    wb.water_type,
    pa.eu_protected_area_code,
    pa.protected_area_type
FROM water_bodies wb
JOIN water_body_protected_areas pa ON wb.eu_water_body_code = pa.water_body_code
```
