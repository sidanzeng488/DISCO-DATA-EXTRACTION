# DISCODATA Explorer

A simple web tool for querying and downloading data from the European Environment Agency (EEA) DISCODATA database.

## Project Structure

```
DISCO link/
├── core/               # Core module - API client
│   └── discodata_client.py
├── web/                # Web application
│   ├── app.py
│   └── templates/
├── tools/              # Interactive tools
│   └── disco_explorer.py
├── checks/             # Data validation scripts
│   ├── check_duplicates.py
│   ├── check_gwb_link.py
│   └── ...
├── analysis/           # Data analysis tools
│   ├── read_excel.py
│   └── ...
├── examples/           # Examples and tests
│   ├── examples.py
│   └── test_api.py
├── scripts/            # Startup scripts
│   ├── start.bat
│   ├── start.ps1
│   └── start-share.ps1
├── etl/                # ETL data processing module
├── supabase/           # Database connection module
├── DATA/               # Data files
│   ├── current/        # Current data
│   ├── backup/         # Backup data
│   └── GWI/            # GWI data
├── databases.json      # Table configuration
├── requirements.txt    # Python dependencies
└── README.md
```

## Features

- Browse available tables (UWWTD, Water Framework Directive)
- Select specific columns to download
- Add WHERE filters
- Preview data before downloading
- Export to CSV

## Available Tables

| Table | Description | Columns |
|-------|-------------|---------|
| T_UWWTPS | Treatment Plants | 74 |
| T_Agglomerations | Agglomerations | 54 |
| T_UWWTPAgglos | Plant-Agglomeration Links | 13 |
| T_DischargePoints | Discharge Points | 32 |
| T_Art17_FLAUWWTP | Art17 Investments | 28 |
| SWB_SurfaceWaterBody | Surface Water Bodies | 34 |
| GWB_GroundWaterBody | Groundwater Bodies | 31 |
| GWB_GroundWaterBody_GWAssociatedProtectedArea | Groundwater Protected Areas | 33 |

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Web Interface

```bash
# Option 1: Use startup scripts (recommended)
.\scripts\start.ps1     # PowerShell
.\scripts\start.bat     # CMD

# Option 2: Run directly
python -m waitress --host=127.0.0.1 --port=5000 web.app:app
```

Then open http://127.0.0.1:5000 in your browser.

### Command Line

```bash
python tools/disco_explorer.py
```

### Python API

```python
from core.discodata_client import create_client

client = create_client()

# Select specific columns
data = client.select(
    "[WISE_UWWTD].[v1r1].[T_UWWTPS]",
    columns=['uwwCode', 'uwwName', 'CountryCode', 'uwwCapacity'],
    where="CountryCode = 'DE'",
    limit=1000
)

# Export to CSV
client.to_csv(data, "output.csv")
```

## API Reference

### `create_client(hits_per_page=500)`

Create a DISCODATA client instance.

### `client.preview(table_full_name, rows=10)`

Preview table data.

### `client.get_columns(table_full_name)`

Get list of column names for a table.

### `client.select(table_full_name, columns=None, where=None, limit=None)`

Query data with optional column selection, filtering, and limit.

### `client.to_csv(data, filename)`

Export data to CSV file.

### `client.to_json(data, filename)`

Export data to JSON file.

## Table Name Format

DISCODATA tables use the format: `[Database].[Version].[TableName]`

Examples:
- `[WISE_UWWTD].[v1r1].[T_UWWTPS]`
- `[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]`

## Data Source

All data comes from the European Environment Agency DISCODATA service:
https://discodata.eea.europa.eu/

## License

For personal/research use. Data is subject to EEA terms of use.
