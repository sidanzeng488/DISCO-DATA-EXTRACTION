# Sensitivity Codes Specification

**Source**: Lookup table derived from UWWTD Article 5  
**Description**: Sensitivity classifications for receiving areas  
**Record Count**: 7 (static lookup table)

## Overview

Sensitivity codes define the environmental sensitivity of receiving areas for wastewater discharge. These codes determine whether enhanced nutrient removal is required at treatment plants.

Under the Urban Waste Water Treatment Directive (91/271/EEC), Member States must designate:
- **Sensitive areas** - require more stringent treatment (nutrient removal)
- **Less sensitive areas** - may allow less stringent treatment under certain conditions
- **Normal areas** - standard secondary treatment requirements apply

## Sensitivity Code Table

| Code | Definition | Nutrient Sensitive | Treatment Requirement |
|------|------------|-------------------|----------------------|
| `A54` | Article 5(4) area | YES | Nutrient removal required |
| `A523` | Sensitive area Art. 5(2,3) | YES | Nutrient removal required |
| `A5854` | Art. 5(8) area + Art. 5(4) | YES | Entire MS sensitive + nutrient removal |
| `A58523` | Art. 5(8) area + Art. 5(2,3) | YES | Entire MS sensitive + specific designation |
| `CSA` | Catchment Art. 5(5) | YES | Catchment of sensitive area |
| `LSA` | Less sensitive area | NO | May allow primary treatment only |
| `NA` | Normal area | NO | Standard secondary treatment |

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS sensitivity (
    code VARCHAR(20) PRIMARY KEY,
    definition VARCHAR(200) NOT NULL,
    nutrient_sensitivity BOOLEAN NOT NULL
);
```

## Data Definition

```python
SENSITIVITY_DATA = [
    {
        'code': 'A54',
        'definition': 'Art. 5(4) area',
        'nutrient_sensitivity': True
    },
    {
        'code': 'A523',
        'definition': 'Sensitive area Art. 5(2,3)',
        'nutrient_sensitivity': True
    },
    {
        'code': 'A5854',
        'definition': 'Art. 5(8) area (entire Member State) + Art. 5(4)',
        'nutrient_sensitivity': True
    },
    {
        'code': 'A58523',
        'definition': 'Art. 5(8) area (entire Member State) + Art. 5(2,3)',
        'nutrient_sensitivity': True
    },
    {
        'code': 'CSA',
        'definition': 'Catchment in the sense of Art. 5(5)',
        'nutrient_sensitivity': True
    },
    {
        'code': 'LSA',
        'definition': 'Less sensitive area',
        'nutrient_sensitivity': False
    },
    {
        'code': 'NA',
        'definition': 'Normal area',
        'nutrient_sensitivity': False
    },
]
```

## Article 5 Breakdown

### Article 5(2) - Eutrophication Sensitivity
Areas designated as sensitive due to:
- **Eutrophication** risk - excessive algae growth from nutrient enrichment
- **Natural freshwater lakes and reservoirs** showing eutrophication signs
- **Estuaries and coastal waters** susceptible to eutrophication

### Article 5(3) - Drinking Water Sensitivity
Areas where surface freshwater is used for:
- **Drinking water abstraction** (current or planned)
- Areas where nitrate concentration exceeds or could exceed 50 mg/L

### Article 5(4) - Nutrient Removal
Applies to all discharges from agglomerations > 10,000 PE to sensitive areas:
- **Phosphorus removal** required for > 10,000 PE
- **Nitrogen removal** required for > 100,000 PE

### Article 5(5) - Catchment Areas
Areas designated as:
- **Catchments of sensitive areas** - upstream areas contributing to sensitive water bodies

### Article 5(8) - Entire Territory
Member States may designate their **entire territory** as sensitive:
- Applies EU-wide treatment standards
- Simplifies compliance monitoring
- Countries using this: Austria, Denmark, Finland, Luxembourg, Netherlands, etc.

## Judgment Logic

### Nutrient Removal Requirement

```python
def determine_nutrient_requirements(discharge_point, plant):
    """
    Determine nutrient removal requirements based on sensitivity 
    and plant capacity.
    """
    sensitivity_code = discharge_point['dcpTypeOfReceivingArea']
    plant_capacity = plant['uwwCapacity']
    
    # Check if area is nutrient sensitive
    nutrient_sensitive_codes = ['A54', 'A523', 'A5854', 'A58523', 'CSA']
    
    if sensitivity_code not in nutrient_sensitive_codes:
        return {
            'requires_nutrient_removal': False,
            'requires_p_removal': False,
            'requires_n_removal': False,
            'reason': f'Area type {sensitivity_code} is not nutrient sensitive'
        }
    
    # Nutrient sensitive area - check capacity thresholds
    requires_p_removal = plant_capacity >= 10000
    requires_n_removal = plant_capacity >= 100000
    
    return {
        'requires_nutrient_removal': True,
        'requires_p_removal': requires_p_removal,
        'requires_n_removal': requires_n_removal,
        'reason': f'Sensitive area {sensitivity_code}, capacity {plant_capacity} PE'
    }
```

### Treatment Level Determination

```python
def determine_required_treatment(sensitivity_code, plant_capacity, population_equivalent):
    """
    Determine the required treatment level based on sensitivity 
    and population equivalent.
    """
    # Less sensitive areas (Article 5(6))
    if sensitivity_code == 'LSA':
        if population_equivalent >= 10000:
            # Primary treatment may be acceptable with specific conditions
            return 'PRIMARY_WITH_MONITORING'
        else:
            return 'PRIMARY'
    
    # Normal areas
    if sensitivity_code == 'NA':
        if population_equivalent >= 2000:
            return 'SECONDARY'
        else:
            return 'APPROPRIATE'  # Appropriate treatment for <2000 PE
    
    # Sensitive areas
    if sensitivity_code in ['A54', 'A523', 'A5854', 'A58523', 'CSA']:
        if plant_capacity >= 100000:
            return 'TERTIARY_N_P'  # Both N and P removal
        elif plant_capacity >= 10000:
            return 'TERTIARY_P'   # P removal required
        elif plant_capacity >= 2000:
            return 'SECONDARY'    # Secondary + may need nutrients
        else:
            return 'SECONDARY'    # Secondary for sensitive areas
    
    return 'UNKNOWN'
```

### Compliance Assessment

```python
def assess_compliance(plant, discharge_point):
    """
    Assess if plant treatment meets requirements for its sensitivity area.
    """
    sensitivity_code = discharge_point['dcpTypeOfReceivingArea']
    requirements = determine_nutrient_requirements(discharge_point, plant)
    
    issues = []
    
    # Check P removal
    if requirements['requires_p_removal'] and not plant['uwwPRemoval']:
        issues.append('Phosphorus removal required but not provided')
    
    # Check N removal
    if requirements['requires_n_removal'] and not plant['uwwNRemoval']:
        issues.append('Nitrogen removal required but not provided')
    
    # Check P performance
    if requirements['requires_p_removal'] and plant['uwwPTotPerf'] == 'F':
        issues.append('Phosphorus removal not meeting performance standards')
    
    # Check N performance
    if requirements['requires_n_removal'] and plant['uwwNTotPerf'] == 'F':
        issues.append('Nitrogen removal not meeting performance standards')
    
    return {
        'compliant': len(issues) == 0,
        'issues': issues,
        'sensitivity_code': sensitivity_code
    }
```

## Relationships

### Sensitivity → Discharge Points

```sql
-- Count discharge points by sensitivity type
SELECT 
    dcpTypeOfReceivingArea as sensitivity_code,
    s.definition,
    s.nutrient_sensitivity,
    COUNT(*) as discharge_count
FROM discharge_points dp
LEFT JOIN sensitivity s ON dp.dcpTypeOfReceivingArea = s.code
GROUP BY dcpTypeOfReceivingArea, s.definition, s.nutrient_sensitivity
ORDER BY discharge_count DESC
```

### Plants Requiring Nutrient Removal

```sql
-- Find plants that should have nutrient removal
SELECT DISTINCT
    p.uwwCode,
    p.uwwName,
    p.uwwCapacity,
    p.uwwNRemoval,
    p.uwwPRemoval,
    dp.dcpTypeOfReceivingArea
FROM plants p
JOIN discharge_points dp ON p.uwwCode = dp.uwwCode
JOIN sensitivity s ON dp.dcpTypeOfReceivingArea = s.code
WHERE s.nutrient_sensitivity = TRUE
  AND p.uwwCapacity >= 10000
ORDER BY p.uwwCapacity DESC
```

## Upload Logic

This is a static lookup table that should be populated once during database initialization:

```python
def create_sensitivity_table():
    """Create and populate sensitivity lookup table."""
    for row in SENSITIVITY_DATA:
        db.execute("""
            INSERT INTO sensitivity (code, definition, nutrient_sensitivity)
            VALUES (%s, %s, %s)
            ON CONFLICT (code) DO UPDATE SET
                definition = EXCLUDED.definition,
                nutrient_sensitivity = EXCLUDED.nutrient_sensitivity
        """, (row['code'], row['definition'], row['nutrient_sensitivity']))
```

## Usage Notes

1. **Static Data**: This table rarely changes as it's based on EU directive definitions.

2. **Member State Variations**: Some Member States apply Article 5(8) (entire territory sensitive), which affects how codes are assigned.

3. **Validation**: The `dcpTypeOfReceivingArea` field in discharge_points should always match one of these codes.

4. **Historical Context**: Sensitivity designations can change over time. The current data reflects the most recent reporting period.
