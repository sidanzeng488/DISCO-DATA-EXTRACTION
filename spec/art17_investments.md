# Article 17 Investments Specification (T_Art17_FLAUWWTP)

**DISCO Table**: `[WISE_UWWTD].[v1r1].[T_Art17_FLAUWWTP]`  
**Description**: Future Load Analysis - Investment projects for wastewater treatment  
**Record Count**: ~5,000  

## Overview

Article 17 of the Urban Waste Water Treatment Directive (91/271/EEC) requires Member States to report on:
- Current non-compliance situations
- Investment plans to achieve compliance
- Expected completion dates

This table tracks investment projects planned or underway at treatment plants.

## Primary Keys and Identifiers

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `Art17_FLAUWWTPID` | `Art17_FLAUWWTPID` | INTEGER | NO | Internal DISCO ID | Auto-generated |
| `CountryCode` | `country_code` | VARCHAR(10) | NO | ISO country code | EU member state |
| `uwwCode` | `uwwtp_code` | VARCHAR(100) | YES | Associated plant code | Links to plants table |
| `flarepCode` | `flarepCode` | VARCHAR(50) | YES | FLA report code | Format: `{CountryCode}_FLA_{Year}` |

## Plant Reference

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `uwwCode` | `uwwtp_code` | VARCHAR(100) | YES | Plant code |
| `uwwName` | `plant_name` | VARCHAR(500) | YES | Plant name |

## Investment Details

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `flatpInv` | `investment_amount` | NUMERIC | YES | Total investment amount | EUR (millions) |
| `flatpStatus` | `investment_status` | VARCHAR(50) | YES | Current investment status | See status codes |

### Investment Status Codes

| Code | Description |
|------|-------------|
| `planned` | Investment planned but not started |
| `ongoing` | Construction/implementation in progress |
| `completed` | Investment completed |
| `delayed` | Behind original schedule |
| `cancelled` | Investment cancelled |

## Expected Capacity and Load

| DISCO Column | DB Column | Type | Nullable | Description | Unit |
|--------------|-----------|------|----------|-------------|------|
| `flatpExpCapacity` | `expected_capacity` | INTEGER | YES | Expected final capacity | PE |
| `flatpExpLoad` | `expected_load` | NUMERIC | YES | Expected incoming load | PE |
| `flatpExpLoadTruck` | `expected_load_truck` | NUMERIC | YES | Expected load from trucks | PE |

## Expected Treatment Level

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flatpExpecTreatment` | `expected_treatment` | VARCHAR(50) | YES | Expected treatment level after completion |

### Treatment Level Codes

| Code | Description |
|------|-------------|
| `P` | Primary treatment only |
| `S` | Secondary treatment |
| `MS` | More stringent treatment (nutrient removal) |
| `N` | Nitrogen removal |
| `P` | Phosphorus removal |
| `NP` | Both nitrogen and phosphorus removal |

## Timeline Dates

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flatpExpecDateStart` | `expected_date_start` | DATE | YES | Expected project start date |
| `flatpExpecDateStart_original` | - | TEXT | YES | Original date string |
| `flatpExpecDateStartWork` | `expected_date_work_start` | DATE | YES | Expected construction start |
| `flatpExpecDateStartWork_original` | - | TEXT | YES | Original date string |
| `flatpExpecDateCompletion` | `expected_date_completion` | DATE | YES | Expected completion date |
| `flatpExpecDateCompletion_original` | - | TEXT | YES | Original date string |
| `flatpExpecDatePerformance` | `expected_date_performance` | DATE | YES | Expected full performance date |
| `flatpExpecDatePerformance_original` | - | TEXT | YES | Original date string |

## Funding Sources

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flatpEUFund` | `has_eu_funding` | BOOLEAN | YES | Received EU funding |
| `flatpEUFundName` | `eu_fund_name` | VARCHAR(200) | YES | Name of EU fund |
| `flatpLoan` | `has_loan` | BOOLEAN | YES | Has loan financing |
| `flatpLoanName` | `loan_name` | VARCHAR(200) | YES | Name of loan source |
| `flatpOtherFund` | `has_other_funding` | BOOLEAN | YES | Has other funding |
| `flatpOtherFundName` | `other_fund_name` | VARCHAR(200) | YES | Name of other funding source |

### Common EU Funding Sources

| Fund Name | Description |
|-----------|-------------|
| Cohesion Fund | For Member States with GNI < 90% EU average |
| ERDF | European Regional Development Fund |
| CEF | Connecting Europe Facility |
| LIFE | Environment and Climate Action programme |

## Measures and Reasons

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flatpMeasures` | `planned_measures` | TEXT | YES | Description of planned measures |
| `flatpReasons` | `reasons` | TEXT | YES | Reasons for non-compliance |
| `flatpComments` | `comments` | TEXT | YES | Additional comments |

## Upload Logic

1. **Extract**: Download from DISCO API
2. **Validate**:
   - Check `uwwCode` exists in plants table (if provided)
   - Validate date sequences (start < completion)
   - Verify investment amount is positive
3. **Transform**:
   - Map DISCO columns to database columns
   - Parse dates from various formats
   - Convert boolean funding flags
4. **Load**: Insert with upsert logic

## Judgment Logic

### Investment Timeline Validation

```python
def validate_timeline(investment_row):
    """
    Validate that investment timeline is logically consistent.
    """
    start = investment_row['flatpExpecDateStart']
    work_start = investment_row['flatpExpecDateStartWork']
    completion = investment_row['flatpExpecDateCompletion']
    performance = investment_row['flatpExpecDatePerformance']
    
    errors = []
    
    # Check sequence
    if start and work_start and start > work_start:
        errors.append("Project start should be before work start")
    
    if work_start and completion and work_start > completion:
        errors.append("Work start should be before completion")
    
    if completion and performance and completion > performance:
        errors.append("Completion should be before full performance")
    
    return len(errors) == 0, errors
```

### Compliance Achievement Assessment

```python
def assess_compliance_achievement(investment_row, plant_row, deadline):
    """
    Determine if investment will achieve compliance by deadline.
    """
    expected_completion = investment_row['flatpExpecDateCompletion']
    expected_treatment = investment_row['flatpExpecTreatment']
    plant_capacity = plant_row['uwwCapacity']
    
    if expected_completion is None:
        return 'UNKNOWN', "No expected completion date"
    
    if expected_completion <= deadline:
        # Check if expected treatment meets requirements
        if plant_capacity >= 2000 and expected_treatment in ['S', 'MS', 'N', 'P', 'NP']:
            return 'ON_TRACK', "Secondary treatment expected by deadline"
        elif plant_capacity < 2000:
            return 'ON_TRACK', "Below threshold, appropriate treatment planned"
        else:
            return 'INSUFFICIENT', "Treatment level may not meet requirements"
    else:
        return 'DELAYED', f"Expected completion {expected_completion} is after deadline {deadline}"
```

### Funding Analysis

```python
def analyze_funding(investment_row):
    """
    Analyze investment funding sources.
    """
    funding_sources = []
    
    if investment_row['flatpEUFund']:
        funding_sources.append({
            'type': 'EU',
            'name': investment_row['flatpEUFundName']
        })
    
    if investment_row['flatpLoan']:
        funding_sources.append({
            'type': 'Loan',
            'name': investment_row['flatpLoanName']
        })
    
    if investment_row['flatpOtherFund']:
        funding_sources.append({
            'type': 'Other',
            'name': investment_row['flatpOtherFundName']
        })
    
    has_eu_support = any(f['type'] == 'EU' for f in funding_sources)
    
    return {
        'funding_sources': funding_sources,
        'has_eu_support': has_eu_support,
        'source_count': len(funding_sources)
    }
```

## Relationships

### Investment → Plant (N:1)
Multiple investment projects can be associated with one plant.

```sql
SELECT 
    p.uwwCode,
    p.uwwName,
    i.flatpInv as investment_amount,
    i.flatpExpecDateCompletion as expected_completion
FROM plants p
JOIN art17_investments i ON p.uwwCode = i.uwwCode
ORDER BY i.flatpInv DESC
```

### Country Investment Summary

```sql
SELECT 
    CountryCode,
    COUNT(*) as project_count,
    SUM(flatpInv) as total_investment,
    AVG(flatpInv) as avg_investment
FROM art17_investments
WHERE flatpInv IS NOT NULL
GROUP BY CountryCode
ORDER BY total_investment DESC
```
