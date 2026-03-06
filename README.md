# DISCODATA Explorer

A simple web tool for querying and downloading data from the European Environment Agency (EEA) DISCODATA database.

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
python app.py
```

Then open http://127.0.0.1:5000 in your browser.

### Command Line

```bash
python disco_explorer.py
```

### Python API

```python
from discodata_client import create_client

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
