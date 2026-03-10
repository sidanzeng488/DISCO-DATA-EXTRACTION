"""读取 Excel 文件"""
import pandas as pd

# 读取所有 sheets
xlsx = pd.ExcelFile('feilds mapping with DISCO.xlsx')
print("Sheet names:", xlsx.sheet_names)
print()

# 读取 Sheet 2 (index 1)
for i, sheet_name in enumerate(xlsx.sheet_names):
    print(f"\n{'=' * 60}")
    print(f"Sheet {i+1}: {sheet_name}")
    print('=' * 60)
    df = pd.read_excel(xlsx, sheet_name=sheet_name)
    print(f"Columns: {list(df.columns)}")
    print(f"Rows: {len(df)}")
    print()
    print(df.head(20).to_string())
