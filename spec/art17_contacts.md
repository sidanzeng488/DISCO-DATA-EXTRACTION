# Article 17 Contacts Specification (T_Art17_FLAContact)

**DISCO Table**: `[WISE_UWWTD].[v1r1].[T_Art17_FLAContact]`  
**Description**: Contact persons responsible for Article 17 reporting in each Member State  
**Record Count**: ~40  

## Overview

This table contains contact information for the officials responsible for submitting Article 17 reports on behalf of each EU Member State.

## Primary Keys and Identifiers

| DISCO Column | DB Column | Type | Nullable | Description | Validation |
|--------------|-----------|------|----------|-------------|------------|
| `Art17_FLAconContactsID` | `contact_id` | INTEGER | NO | Internal DISCO ID | Auto-generated |
| `CountryCode` | `country_code` | VARCHAR(10) | NO | ISO country code | EU member state |
| `flarepCode` | `flarepCode` | VARCHAR(50) | YES | FLA report code | Format: `{CountryCode}_UWWTD_{Year}` |
| `rptMStateKey` | `rptMStateKey` | VARCHAR(10) | NO | Member state key | Same as CountryCode |

## Contact Person Details

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flaconName` | `contact_name` | VARCHAR(200) | YES | Full name of contact person |
| `flaconInstitution` | `institution` | VARCHAR(500) | YES | Name of organization/ministry |
| `flaconMemberState` | `member_state_name` | VARCHAR(100) | YES | Full country name |

## Address Information

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flaconStreet` | `street_address` | VARCHAR(500) | YES | Street address |
| `flaconZIP` | `postal_code` | VARCHAR(20) | YES | Postal/ZIP code |
| `flaconCity` | `city` | VARCHAR(100) | YES | City name |

## Communication Details

| DISCO Column | DB Column | Type | Nullable | Description | Format |
|--------------|-----------|------|----------|-------------|--------|
| `flaconPhone` | `phone` | VARCHAR(50) | YES | Phone number | International format |
| `flaconFax` | `fax` | VARCHAR(50) | YES | Fax number | International format |
| `flaconEmail` | `email` | VARCHAR(200) | YES | Email address | Valid email format |

## Reporting Period Information

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flaconReportedPeriod` | `reported_period` | VARCHAR(20) | YES | Reporting year (e.g., "2022") |
| `flaconSituationAt` | `situation_at` | DATE | YES | Data reference date |
| `flaconSituationAt_original` | - | TEXT | YES | Original date string |
| `flaconVersion` | `version` | DATE | YES | Report version date |
| `flaconVersion_original` | - | TEXT | YES | Original version string |

## Additional Fields

| DISCO Column | DB Column | Type | Nullable | Description |
|--------------|-----------|------|----------|-------------|
| `flaconRemarks` | `remarks` | TEXT | YES | Additional remarks |

## Upload Logic

1. **Extract**: Download from DISCO API
2. **Validate**:
   - Validate country codes
   - Verify email format if provided
   - Check for duplicate contacts per country/period
3. **Transform**:
   - Map DISCO columns to database columns
   - Parse dates
   - Standardize phone number format
4. **Load**: Insert with upsert logic (update on country + period conflict)

## Judgment Logic

### Contact Completeness Check

```python
def check_contact_completeness(contact_row):
    """
    Check if contact information is complete.
    """
    required_fields = ['flaconName', 'flaconInstitution', 'flaconEmail']
    optional_fields = ['flaconPhone', 'flaconStreet', 'flaconCity']
    
    missing_required = []
    missing_optional = []
    
    for field in required_fields:
        if not contact_row.get(field):
            missing_required.append(field)
    
    for field in optional_fields:
        if not contact_row.get(field):
            missing_optional.append(field)
    
    completeness_score = (
        len(required_fields) + len(optional_fields) - 
        len(missing_required) - len(missing_optional)
    ) / (len(required_fields) + len(optional_fields)) * 100
    
    return {
        'is_complete': len(missing_required) == 0,
        'missing_required': missing_required,
        'missing_optional': missing_optional,
        'completeness_score': completeness_score
    }
```

### Email Validation

```python
import re

def validate_email(email):
    """
    Validate email format.
    """
    if not email:
        return False, "Email is empty"
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if re.match(pattern, email):
        return True, "Valid email format"
    else:
        return False, "Invalid email format"
```

### Country Coverage Check

```python
def check_country_coverage(contacts_df, expected_countries):
    """
    Check if all expected countries have contacts.
    """
    reported_countries = set(contacts_df['CountryCode'].unique())
    expected_countries = set(expected_countries)
    
    missing = expected_countries - reported_countries
    extra = reported_countries - expected_countries
    
    return {
        'complete': len(missing) == 0,
        'missing_countries': list(missing),
        'coverage_rate': len(reported_countries & expected_countries) / len(expected_countries) * 100
    }
```

## Relationships

### Contact → Country (N:1)
Each country may have multiple contacts over different reporting periods.

```sql
SELECT 
    flaconMemberState as country,
    COUNT(*) as contact_count,
    MAX(flaconReportedPeriod) as latest_period
FROM art17_contacts
GROUP BY flaconMemberState
ORDER BY country
```

## Usage Notes

1. **Privacy Considerations**: Contact information may be subject to GDPR. Handle appropriately.
2. **Historical Records**: Old contacts are retained for audit purposes.
3. **Updates**: Contacts are typically updated with each reporting cycle (every 2 years).
