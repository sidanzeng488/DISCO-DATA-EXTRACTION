"""Read fields mapping Excel file"""
import pandas as pd
import os

excel_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'feilds mapping with DISCO.xlsx')
print(f'Reading: {excel_path}\n')

# Get all sheet names
xl = pd.ExcelFile(excel_path)
print(f'Sheets: {xl.sheet_names}\n')

# Read each sheet
for sheet_name in xl.sheet_names:
    print('=' * 80)
    print(f'Sheet: {sheet_name}')
    print('=' * 80)
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    print(f'Rows: {len(df)}, Columns: {len(df.columns)}')
    print(f'\nColumns: {list(df.columns)}\n')
    print(df.to_string())
    print('\n')
