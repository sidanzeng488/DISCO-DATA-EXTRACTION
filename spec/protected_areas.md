# Protected Areas Specification

**DISCO Tables**:
- `[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]`
- `[WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]`

**Description**: Protected areas associated with water bodies under various EU directives  
**Record Count**: ~600,000+ (combined)

## Overview

Protected areas are designated zones requiring special protection under EU environmental legislation. They are linked to water bodies and may include:
- Drinking water protection zones
- Natura 2000 sites
- Bathing water areas
- Nutrient-sensitive areas
- Shellfish water areas

## Unified Schema

| DB Column | Type | Nullable | Description |
|-----------|------|----------|-------------|
| `protected_area_id` | SERIAL | NO | Auto-generated primary key |
| `water_body_code` | VARCHAR(100) | NO | Links to water_bodies table |
| `eu_protected_area_code` | VARCHAR(100) | YES | EU protected area identifier |
| `protected_area_type` | VARCHAR(100) | YES | Type of protection designation |
| `objectives_set` | BOOLEAN | YES | Are protection objectives defined |
| `objectives_met` | BOOLEAN | YES | Are objectives being achieved |

## Surface Water Protected Areas (SWB)

### Source Fields

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `euSurfaceWaterBodyCode` | `water_body_code` | VARCHAR(100) | NO | Surface water body code |
| `euProtectedAreaCode` | `eu_protected_area_code` | VARCHAR(100) | YES | EU protected area code |
| `protectedAreaType` | `protected_area_type` | VARCHAR(100) | YES | Type code |
| `protectedAreaObjectivesSet` | `objectives_set` | BOOLEAN | YES | Objectives defined |
| `protectedAreaObjectivesMet` | `objectives_met` | BOOLEAN | YES | Objectives achieved |

### Additional SWB Fields

| DISCO Column | Type | Nullable | Description |
|--------------|------|----------|-------------|
| `countryCode` | VARCHAR(10) | NO | ISO country code |
| `euRBDCode` | VARCHAR(50) | YES | River Basin District code |
| `cYear` | INTEGER | YES | Classification year |
| `fileUrl` | TEXT | YES | Source file URL |

## Ground Water Protected Areas (GWB)

### Source Fields

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `euGroundWaterBodyCode` | `water_body_code` | VARCHAR(100) | NO | Ground water body code |
| `euProtectedAreaCode` | `eu_protected_area_code` | VARCHAR(100) | YES | EU protected area code |
| `protectedAreaType` | `protected_area_type` | VARCHAR(100) | YES | Type code |
| `protectedAreaObjectivesSet` | `objectives_set` | BOOLEAN | YES | Objectives defined |
| `protectedAreaObjectivesMet` | `objectives_met` | BOOLEAN | YES | Objectives achieved |

## Protected Area Types

| Type Code | Description | Directive |
|-----------|-------------|-----------|
| `DrinkingWater` | Drinking water protection zone | Drinking Water Directive (98/83/EC) |
| `Bathing` | Bathing water | Bathing Water Directive (2006/7/EC) |
| `Birds` | Bird protection area | Birds Directive (2009/147/EC) |
| `Habitats` | Habitat protection area | Habitats Directive (92/43/EEC) |
| `Nitrates` | Nitrate vulnerable zone | Nitrates Directive (91/676/EEC) |
| `Shellfish` | Shellfish waters | Former Shellfish Directive |
| `UWWTD` | Urban wastewater sensitive area | UWWTD (91/271/EEC) |
| `Fish` | Freshwater fish waters | Former Freshwater Fish Directive |
| `Other` | Other protected areas | Various |

## Upload Logic

### Extraction

Protected area tables are very large and typically require manual download:

```python
DISCO_TABLES_MANUAL = {
    'swb_protected_areas': {
        'disco_table': '[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]',
        'estimated_rows': '~500,000+',
    },
    'gwb_protected_areas': {
        'disco_table': '[WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]',
        'estimated_rows': '~100,000+',
    },
}
```

### Transformation (Merge Logic)

```python
def merge_protected_areas():
    """
    Merge SWB and GWB protected areas into unified table.
    """
    protected_areas = []
    
    # Process SWB protected areas
    for row in swb_pa_data:
        pa = {
            'water_body_code': row['euSurfaceWaterBodyCode'],
            'eu_protected_area_code': row['euProtectedAreaCode'],
            'protected_area_type': row['protectedAreaType'],
            'objectives_set': parse_boolean(row['protectedAreaObjectivesSet']),
            'objectives_met': parse_boolean(row['protectedAreaObjectivesMet']),
        }
        protected_areas.append(pa)
    
    # Process GWB protected areas
    for row in gwb_pa_data:
        pa = {
            'water_body_code': row['euGroundWaterBodyCode'],
            'eu_protected_area_code': row['euProtectedAreaCode'],
            'protected_area_type': row['protectedAreaType'],
            'objectives_set': parse_boolean(row['protectedAreaObjectivesSet']),
            'objectives_met': parse_boolean(row['protectedAreaObjectivesMet']),
        }
        protected_areas.append(pa)
    
    return protected_areas
```

## Judgment Logic

### Protection Level Assessment

```python
def assess_water_body_protection(water_body_code):
    """
    Assess the protection level for a water body based on 
    associated protected areas.
    """
    protected_areas = get_protected_areas_for_water_body(water_body_code)
    
    if not protected_areas:
        return {
            'protection_level': 'None',
            'protected_area_count': 0,
            'types': []
        }
    
    # Categorize protection types
    high_protection_types = ['DrinkingWater', 'Bathing', 'Birds', 'Habitats']
    moderate_protection_types = ['Nitrates', 'UWWTD', 'Shellfish']
    
    types = set(pa['protected_area_type'] for pa in protected_areas)
    
    if types & set(high_protection_types):
        protection_level = 'High'
    elif types & set(moderate_protection_types):
        protection_level = 'Moderate'
    else:
        protection_level = 'Low'
    
    return {
        'protection_level': protection_level,
        'protected_area_count': len(protected_areas),
        'types': list(types)
    }
```

### Objectives Achievement Analysis

```python
def analyze_objectives_achievement(protected_areas):
    """
    Analyze how well protection objectives are being met.
    """
    total = len(protected_areas)
    objectives_set = sum(1 for pa in protected_areas if pa['objectives_set'])
    objectives_met = sum(1 for pa in protected_areas if pa['objectives_met'])
    
    return {
        'total_areas': total,
        'with_objectives': objectives_set,
        'objectives_met': objectives_met,
        'pct_with_objectives': (objectives_set / total * 100) if total > 0 else 0,
        'pct_objectives_met': (objectives_met / objectives_set * 100) if objectives_set > 0 else 0
    }
```

### Nutrient Sensitivity Check

```python
def check_nutrient_sensitivity(water_body_code):
    """
    Check if a water body is in a nutrient-sensitive area based on 
    its protected areas.
    """
    protected_areas = get_protected_areas_for_water_body(water_body_code)
    
    nutrient_sensitive_types = ['Nitrates', 'UWWTD']
    
    for pa in protected_areas:
        if pa['protected_area_type'] in nutrient_sensitive_types:
            return True
    
    return False
```

## Relationships

### Water Body → Protected Areas (1:N)

```sql
-- Get all protected areas for a water body
SELECT 
    pa.eu_protected_area_code,
    pa.protected_area_type,
    pa.objectives_set,
    pa.objectives_met
FROM water_body_protected_areas pa
WHERE pa.water_body_code = 'ATOK801780000'

-- Count protected areas by type
SELECT 
    protected_area_type,
    COUNT(*) as count,
    SUM(CASE WHEN objectives_met THEN 1 ELSE 0 END) as objectives_met_count
FROM water_body_protected_areas
GROUP BY protected_area_type
ORDER BY count DESC
```

### Discharge Impact on Protected Areas

```sql
-- Find discharges to water bodies with drinking water protection
SELECT 
    dcp.dcp_code,
    dcp.plant_code,
    wb.eu_water_body_code,
    pa.protected_area_type
FROM discharge_points dcp
JOIN water_bodies wb ON dcp.water_body_code = wb.eu_water_body_code
JOIN water_body_protected_areas pa ON wb.eu_water_body_code = pa.water_body_code
WHERE pa.protected_area_type = 'DrinkingWater'
```

## Usage Notes

1. **Large Dataset**: Protected area tables contain hundreds of thousands of records. Consider filtering by country or RBD when querying.

2. **Many-to-Many**: A water body can have multiple protected areas, and a protected area can span multiple water bodies.

3. **Objective Status**: `objectives_met` may be NULL if objectives haven't been assessed yet.

4. **EU vs National**: Some protected areas have EU-level codes (`euProtectedAreaCode`), while others may only have national designations.
