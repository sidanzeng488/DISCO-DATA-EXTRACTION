"""Check dilution data from GWI Excel file"""
import pandas as pd
import os

excel_path = os.path.join(os.path.dirname(__file__), '..', 'DATA', 'GWI', 'micropollutant costings EU data - Lara.xlsx')

df = pd.read_excel(excel_path)

print('=== Dilution Column Analysis ===')
dilution_col = 'Dilution '  # Note the trailing space

print(f'Column name: "{dilution_col}"')
print(f'Total rows: {len(df)}')
print(f'Non-null values: {df[dilution_col].notna().sum()}')
print(f'Null values: {df[dilution_col].isna().sum()}')

print(f'\nData type: {df[dilution_col].dtype}')
print(f'\nBasic stats:')
print(df[dilution_col].describe())

print(f'\nSample data (uwwCode + Dilution):')
sample = df[['uwwCode', dilution_col]].dropna().head(10)
for _, row in sample.iterrows():
    print(f'  {row["uwwCode"]}: {row[dilution_col]}')

# Check unique uwwCode count
unique_uww = df['uwwCode'].nunique()
print(f'\nUnique uwwCode values: {unique_uww}')

# Check for duplicates
dup_count = df.groupby('uwwCode')[dilution_col].count()
multi_dilution = (dup_count > 1).sum()
print(f'uwwCodes with multiple dilution values: {multi_dilution}')
