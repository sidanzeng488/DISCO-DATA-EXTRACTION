# Report Periods Specification (T_ReportPeriod)

**DISCO Table**: `[WISE_UWWTD].[v1r1].[T_ReportPeriod]`  
**Description**: Metadata about reporting periods for each Member State  
**Record Count**: ~30  

## Overview

This table contains information about when each Member State submitted their UWWTD reports and what time period the data represents.

## Primary Keys and Identifiers

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `repReportPeriodID` | `report_period_id` | INTEGER | NO | Internal DISCO ID | Auto-generated |
| `repCode` | `rep_code` | VARCHAR(50) | NO | Unique report code | Format: `{CountryCode}_UWWTD_{Year}` |
| `CountryCode` | `country_code` | VARCHAR(10) | NO | ISO country code | EU member state |
| `rptMStateKey` | `rptMStateKey` | VARCHAR(10) | NO | Member state key | Same as CountryCode |

## Reporting Period Information

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `repReportedPeriod` | `reported_period` | VARCHAR(20) | YES | Reporting year (e.g., "2022") |
| `repSituationAt` | `situation_at` | DATE | YES | Date of data snapshot |
| `repSituationAt_original` | - | TEXT | YES | Original date string |

## Version Information

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `repVersion` | `version` | DATE | YES | Report version date |
| `repVersion_original` | - | TEXT | YES | Original version string |

## Reference System

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `repReferenceSystem` | `reference_system` | VARCHAR(20) | YES | Coordinate reference system (e.g., "ETRS89") |

## Upload Logic

1. **Extract**: Download from DISCO API
2. **Validate**:
   - Check `repCode` uniqueness
   - Validate country codes
   - Verify date formats
3. **Transform**:
   - Map DISCO columns to database columns
   - Parse dates from various formats
4. **Load**: Insert with upsert logic

## Judgment Logic

### Report Code Parsing

```python
def parse_rep_code(rep_code):
    """
    Parse report code to extract components.
    Example: "AT_UWWTD_2022" -> {'country': 'AT', 'type': 'UWWTD', 'year': 2022}
    """
    parts = rep_code.strip().split('_')
    
    if len(parts) >= 3:
        return {
            'country_code': parts[0],
            'report_type': parts[1],
            'year': int(parts[2]) if parts[2].isdigit() else None
        }
    else:
        return {
            'country_code': None,
            'report_type': None,
            'year': None
        }
```

### Reporting Cycle Validation

```python
def validate_reporting_cycle(report_periods_df):
    """
    Validate that reporting follows expected cycle (every 2 years).
    """
    issues = []
    
    for country, group in report_periods_df.groupby('CountryCode'):
        years = sorted([int(rp['repReportedPeriod']) for rp in group if rp['repReportedPeriod']])
        
        for i in range(1, len(years)):
            gap = years[i] - years[i-1]
            if gap != 2:
                issues.append({
                    'country': country,
                    'from_year': years[i-1],
                    'to_year': years[i],
                    'gap': gap,
                    'expected': 2
                })
    
    return issues
```

### Latest Report Identification

```python
def get_latest_reports(report_periods_df):
    """
    Get the most recent report for each country.
    """
    latest = {}
    
    for country, group in report_periods_df.groupby('CountryCode'):
        sorted_reports = sorted(
            group.to_dict('records'),
            key=lambda x: x['repReportedPeriod'],
            reverse=True
        )
        if sorted_reports:
            latest[country] = sorted_reports[0]
    
    return latest
```

## Relationships

### Report Period → All UWWTD Tables

The `repCode` is used as a foreign key in all UWWTD tables to link records to their reporting period.

```sql
-- Find all plants reported in a specific period
SELECT p.*
FROM plants p
WHERE p.repCode = 'AT_UWWTD_2022'

-- Count records per report period
SELECT 
    repCode,
    (SELECT COUNT(*) FROM plants WHERE plants.repCode = rp.repCode) as plant_count,
    (SELECT COUNT(*) FROM agglomerations WHERE agglomerations.repCode = rp.repCode) as agg_count,
    (SELECT COUNT(*) FROM discharge_points WHERE discharge_points.repCode = rp.repCode) as dcp_count
FROM report_periods rp
ORDER BY repCode
```

## Historical Data

Each reporting cycle creates a new snapshot of data. Historical records are retained with their respective `repCode` to enable:
- Trend analysis over time
- Compliance progress tracking
- Comparison between reporting periods

```sql
-- Compare plant count between periods
SELECT 
    CountryCode,
    repCode,
    COUNT(*) as plant_count
FROM plants
WHERE CountryCode = 'DE'
GROUP BY CountryCode, repCode
ORDER BY repCode
```
