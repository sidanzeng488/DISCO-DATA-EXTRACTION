# DISCO Data Specification

This folder contains detailed specifications for all database columns related to DISCO (European Environment Agency DISCODATA).

## Overview

The data originates from the EEA DISCODATA service:
- **Source**: https://discodata.eea.europa.eu/
- **Databases**: 
  - `WISE_UWWTD` (Urban Waste Water Treatment Directive) - Version v1r1
  - `WISE_WFD` (Water Framework Directive) - Version v2r1
- **Data Dictionary**: https://dd.eionet.europa.eu/tables/12105 

## Table Index

| Spec File | DISCO Table | Description | Records |
|-----------|-------------|-------------|---------|
| [plants.md](plants.md) | T_UWWTPS | Urban Waste Water Treatment Plants | ~26,000 |
| [agglomerations.md](agglomerations.md) | T_Agglomerations | Population agglomerations | ~25,000 |
| [discharge_points.md](discharge_points.md) | T_DischargePoints | Discharge points linking plants to water bodies | ~26,000 |
| [art17_investments.md](art17_investments.md) | T_Art17_FLAUWWTP | Article 17 investment projects | ~5,000 |
| [art17_contacts.md](art17_contacts.md) | T_Art17_FLAContact | Article 17 contact persons | ~40 |
| [report_periods.md](report_periods.md) | T_ReportPeriod | Reporting period metadata | ~30 |
| [water_bodies.md](water_bodies.md) | SWB + GWB | Surface and Ground Water Bodies | ~200,000+ |
| [protected_areas.md](protected_areas.md) | Protected Area tables | Water body protected areas | ~600,000+ |
| [sensitivity.md](sensitivity.md) | N/A (lookup table) | Sensitivity area codes | 7 |

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      DISCO API (EEA)                            │
│          https://discodata.eea.europa.eu/sql                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ETL Extract (etl/extract.py)                │
│   - Auto download: plants, agglomerations, discharge_points    │
│   - Manual download: SWB, GWB (large datasets)                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA/current/*.csv                          │
│              Raw CSV files from DISCO                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ETL Transform (etl/transform.py)               │
│   - Field name mapping (DISCO → Database)                       │
│   - Merge SWB + GWB into unified water_bodies                   │
│   - Data validation and type conversion                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│               DATA/current/transformed/*.csv                    │
│           Transformed CSV ready for database import             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Supabase Database                          │
└─────────────────────────────────────────────────────────────────┘
```

## Column Naming Convention

| DISCO Prefix | Meaning | Example |
|--------------|---------|---------|
| `uww` | Urban Waste Water (Plant) | `uwwCode`, `uwwCapacity` |
| `agg` | Agglomeration | `aggCode`, `aggGenerated` |
| `dcp` | Discharge Point | `dcpCode`, `dcpLatitude` |
| `rep` | Report | `repCode`, `repReportedPeriod` |
| `rca` | Receiving Area | `rcaCode` |
| `fla` | FLA (Future Load Analysis) - Art17 | `flatpInv`, `flarepCode` |
| `eu` | EU-level code | `euSurfaceWaterBodyCode` |
| `sw` / `gw` | Surface Water / Ground Water | `swEcologicalStatus` |

## Validation Rules

Each spec file contains:
1. **Column Name** - Original DISCO column name
2. **Data Type** - Expected data type
3. **Nullable** - Whether NULL values are allowed
4. **Description** - Field meaning and context
5. **Source Logic** - How to extract/calculate the value
6. **Validation** - Rules for data quality checks
7. **Mapping** - Target database column name

## Related Files

- `etl/config.py` - Field mapping configuration
- `etl/transform.py` - Data transformation logic
- `supabase/create_schema.py` - Database schema definition
- `databases.json` - DISCO table configurations
