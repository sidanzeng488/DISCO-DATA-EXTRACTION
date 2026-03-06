"""
DISCODATA Interactive Explorer

Browse and download data from EEA DISCODATA
https://discodata.eea.europa.eu/
"""

import json
import os
from discodata_client import create_client


def load_config():
    """Load databases config"""
    config_path = os.path.join(os.path.dirname(__file__), 'databases.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def print_divider(char='=', width=60):
    print(char * width)


def print_header(title):
    print()
    print_divider()
    print(f"  {title}")
    print_divider()


def show_my_tables(config):
    """Display user's configured tables"""
    print_header("My Tables")
    
    tables = config.get('my_tables', [])
    
    for i, t in enumerate(tables, 1):
        status = "OK" if not t['full_name'].startswith('待') else "待确认"
        print(f"\n  [{i}] {t['name']}")
        print(f"      Table: {t['full_name']}")
        if t.get('columns'):
            print(f"      Columns: {t['columns']}")
        if status != "OK":
            print(f"      Status: {status}")
    
    print(f"\n  [0] Back to menu")
    return tables


def show_columns(client, full_name):
    """Display and return columns of a table"""
    print(f"\n  Fetching columns from {full_name}...")
    
    try:
        columns = client.get_columns(full_name)
        
        print(f"\n  Found {len(columns)} columns:")
        for i, col in enumerate(columns, 1):
            print(f"    {i:3d}. {col}")
        
        return columns
    except Exception as e:
        print(f"\n  Error: {e}")
        return []


def select_columns(columns):
    """Interactive column selection"""
    print("\n  Select columns to download:")
    print("    - Enter numbers: 1,3,5,7")
    print("    - Enter range: 1-10")
    print("    - Enter 'all' for all columns")
    print("    - Enter 0 to cancel")
    
    while True:
        choice = input("\n  Selection: ").strip().lower()
        
        if choice == '0':
            return None
        
        if choice == 'all':
            return columns
        
        try:
            selected = []
            parts = choice.replace(' ', '').split(',')
            
            for part in parts:
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    for i in range(start, end + 1):
                        if 1 <= i <= len(columns):
                            col = columns[i - 1]
                            if col not in selected:
                                selected.append(col)
                else:
                    idx = int(part)
                    if 1 <= idx <= len(columns):
                        col = columns[idx - 1]
                        if col not in selected:
                            selected.append(col)
            
            if selected:
                print(f"\n  Selected {len(selected)} columns:")
                for col in selected:
                    print(f"    - {col}")
                
                confirm = input("\n  Confirm? (y/n): ").strip().lower()
                if confirm == 'y':
                    return selected
            else:
                print("  No valid columns selected")
                
        except ValueError:
            print("  Invalid format")


def get_filter():
    """Get WHERE clause"""
    print("\n  Add filter? (optional)")
    print("    Example: CountryCode = 'DE'")
    print("    Press Enter to skip")
    
    where = input("\n  WHERE: ").strip()
    return where if where else None


def get_limit():
    """Get record limit"""
    print("\n  Limit records? (optional)")
    print("    Enter number or press Enter for no limit")
    
    while True:
        limit = input("\n  Limit: ").strip()
        if not limit:
            return None
        try:
            return int(limit)
        except ValueError:
            print("  Enter a valid number")


def download_data(client, full_name, columns, where, limit):
    """Download and export data"""
    print_header("Downloading")
    
    # Count
    print(f"  Counting records...")
    try:
        total = client.count(full_name, where)
        print(f"  Matching records: {total}")
        if limit:
            print(f"  Will download: {min(total, limit)}")
    except Exception as e:
        print(f"  Could not count: {e}")
    
    confirm = input("\n  Start? (y/n): ").strip().lower()
    if confirm != 'y':
        print("  Cancelled")
        return
    
    # Download
    print("\n  Downloading...")
    try:
        data = client.select(full_name, columns=columns, where=where, limit=limit)
        
        if not data:
            print("  No data")
            return
        
        print(f"\n  Downloaded {len(data)} records")
        
        # Export
        print("\n  Format: [1] CSV  [2] JSON")
        fmt = input("  Choice: ").strip()
        filename = input("  Filename (no ext): ").strip() or "output"
        
        if fmt == '2':
            client.to_json(data, f"{filename}.json")
        else:
            client.to_csv(data, f"{filename}.csv")
            
    except Exception as e:
        print(f"\n  Error: {e}")


def custom_query_mode(client):
    """Custom SQL query"""
    print_header("Custom SQL")
    print("  Table format: [Database].[Version].[Table]")
    print("  Example: SELECT TOP 100 * FROM [WISE_UWWTD].[v1r1].[T_UWWTPS]")
    print("  Type 'exit' to go back")
    
    while True:
        print()
        sql = input("  SQL> ").strip()
        
        if sql.lower() == 'exit':
            break
        
        if not sql:
            continue
        
        try:
            data = client.query(sql, max_records=10000)
            
            if not data:
                print("  No results")
                continue
            
            print(f"\n  Got {len(data)} records")
            print(f"  Columns: {', '.join(list(data[0].keys())[:5])}...")
            
            export = input("\n  Export? (y/n): ").strip().lower()
            if export == 'y':
                filename = input("  Filename: ").strip() or "result"
                fmt = input("  Format (csv/json): ").strip().lower()
                
                if fmt == 'json':
                    client.to_json(data, f"{filename}.json")
                else:
                    client.to_csv(data, f"{filename}.csv")
                    
        except Exception as e:
            print(f"\n  Error: {e}")


def main():
    print_header("DISCODATA Explorer")
    print("  EEA Data Query Tool")
    print("  https://discodata.eea.europa.eu/")
    
    client = create_client()
    config = load_config()
    
    while True:
        print("\n  Menu:")
        print("    [1] My Tables (Quick Access)")
        print("    [2] Custom SQL Query")
        print("    [0] Exit")
        
        choice = input("\n  Select: ").strip()
        
        if choice == '0':
            print("\n  Bye!")
            break
            
        elif choice == '1':
            tables = show_my_tables(config)
            t_choice = input("\n  Select table: ").strip()
            
            if t_choice == '0' or not t_choice:
                continue
            
            try:
                t_idx = int(t_choice) - 1
                if 0 <= t_idx < len(tables):
                    table = tables[t_idx]
                    full_name = table['full_name']
                    
                    if full_name.startswith('待'):
                        print("\n  This table needs to be configured first!")
                        print("  Please find the correct table name on DISCODATA website")
                        input("\n  Press Enter...")
                        continue
                    
                    # Show columns
                    columns = show_columns(client, full_name)
                    if not columns:
                        input("\n  Press Enter...")
                        continue
                    
                    # Select columns
                    selected = select_columns(columns)
                    if not selected:
                        continue
                    
                    # Filter and limit
                    where = get_filter()
                    limit = get_limit()
                    
                    # Download
                    download_data(client, full_name, selected, where, limit)
                    input("\n  Press Enter...")
                    
            except (ValueError, IndexError):
                print("  Invalid selection")
                
        elif choice == '2':
            custom_query_mode(client)
            
        else:
            print("  Invalid option")


if __name__ == "__main__":
    main()
