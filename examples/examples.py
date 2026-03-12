"""
DISCODATA Usage Examples

Your configured tables:
- T_UWWTPS (污水处理厂)
- T_Agglomerations (集聚区)
- T_UWWTPAgglos (处理厂-集聚区关系)
- T_DischargePoints (排放点)
- T_Art17_FLAUWWTP (投资项目)
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.discodata_client import create_client


# ============================================================
# Example 1: Download treatment plants with selected columns
# ============================================================
def example_uwwtps():
    """Download treatment plant data"""
    print("\n" + "=" * 50)
    print("Example: Download T_UWWTPS (Treatment Plants)")
    print("=" * 50)

    client = create_client()

    # Select only the columns you need
    columns = [
        'uwwCode',
        'uwwName', 
        'CountryCode',
        'uwwLatitude',
        'uwwLongitude',
        'uwwCapacity',
        'uwwLoadEnteringUWWTP',
        'uwwPrimaryTreatment',
        'uwwSecondaryTreatment',
        'uwwOtherTreatment',
        'uwwNRemoval',
        'uwwPRemoval'
    ]

    # Download data (with optional filter)
    data = client.select(
        "[WISE_UWWTD].[v1r1].[T_UWWTPS]",
        columns=columns,
        where="CountryCode = 'DE'",  # Germany only
        limit=500
    )

    print(f"\nDownloaded {len(data)} records")
    
    # Export to CSV
    client.to_csv(data, "uwwtps_germany.csv")


# ============================================================
# Example 2: Download agglomerations
# ============================================================
def example_agglomerations():
    """Download agglomeration data"""
    print("\n" + "=" * 50)
    print("Example: Download T_Agglomerations")
    print("=" * 50)

    client = create_client()

    columns = [
        'aggCode',
        'aggName',
        'CountryCode',
        'aggLatitude',
        'aggLongitude',
        'aggGenerated',
        'aggC1',
        'aggC2'
    ]

    data = client.select(
        "[WISE_UWWTD].[v1r1].[T_Agglomerations]",
        columns=columns,
        limit=1000
    )

    print(f"\nDownloaded {len(data)} records")
    client.to_csv(data, "agglomerations.csv")


# ============================================================
# Example 3: Download discharge points
# ============================================================
def example_discharge_points():
    """Download discharge point data"""
    print("\n" + "=" * 50)
    print("Example: Download T_DischargePoints")
    print("=" * 50)

    client = create_client()

    columns = [
        'dcpCode',
        'dcpName',
        'uwwCode',
        'dcpLatitude',
        'dcpLongitude',
        'dcpWaterBodyType',
        'dcpNuts',
        'CountryCode'
    ]

    data = client.select(
        "[WISE_UWWTD].[v1r1].[T_DischargePoints]",
        columns=columns,
        limit=1000
    )

    print(f"\nDownloaded {len(data)} records")
    client.to_csv(data, "discharge_points.csv")


# ============================================================
# Example 4: Download investment data
# ============================================================
def example_investments():
    """Download Art17 investment data"""
    print("\n" + "=" * 50)
    print("Example: Download T_Art17_FLAUWWTP (Investments)")
    print("=" * 50)

    client = create_client()

    columns = [
        'uwwCode',
        'uwwName',
        'flatpInv',
        'flatpExpecDateCompletion',
        'flatpStatus',
        'flarepCode',
        'flatpEUFund',
        'flatpEUFundName',
        'CountryCode'
    ]

    data = client.select(
        "[WISE_UWWTD].[v1r1].[T_Art17_FLAUWWTP]",
        columns=columns,
        where="flatpInv > 0",  # Only with investment data
        limit=500
    )

    print(f"\nDownloaded {len(data)} records")
    client.to_csv(data, "investments.csv")


# ============================================================
# Example 5: Download all tables for a country
# ============================================================
def example_all_tables_for_country(country='FR'):
    """Download all tables for a specific country"""
    print("\n" + "=" * 50)
    print(f"Example: Download all data for {country}")
    print("=" * 50)

    client = create_client()

    tables = {
        'uwwtps': {
            'table': '[WISE_UWWTD].[v1r1].[T_UWWTPS]',
            'columns': ['uwwCode', 'uwwName', 'uwwLatitude', 'uwwLongitude', 'uwwCapacity']
        },
        'agglomerations': {
            'table': '[WISE_UWWTD].[v1r1].[T_Agglomerations]',
            'columns': ['aggCode', 'aggName', 'aggLatitude', 'aggLongitude', 'aggGenerated']
        },
        'discharge': {
            'table': '[WISE_UWWTD].[v1r1].[T_DischargePoints]',
            'columns': ['dcpCode', 'dcpName', 'uwwCode', 'dcpWaterBodyType']
        }
    }

    for name, info in tables.items():
        print(f"\n  Downloading {name}...")
        try:
            data = client.select(
                info['table'],
                columns=info['columns'],
                where=f"CountryCode = '{country}'"
            )
            filename = f"{country.lower()}_{name}.csv"
            client.to_csv(data, filename)
        except Exception as e:
            print(f"    Error: {e}")


# ============================================================
# Run examples
# ============================================================
if __name__ == "__main__":
    print("=" * 50)
    print("DISCODATA Examples")
    print("=" * 50)
    print("\nAvailable examples:")
    print("  1. example_uwwtps() - Treatment plants")
    print("  2. example_agglomerations() - Agglomerations")
    print("  3. example_discharge_points() - Discharge points")
    print("  4. example_investments() - Investment projects")
    print("  5. example_all_tables_for_country('FR') - All for France")

    # Uncomment to run:
    # example_uwwtps()
    # example_agglomerations()
    # example_discharge_points()
    # example_investments()
    # example_all_tables_for_country('FR')

    print("\n\nTip: Uncomment examples above or run:")
    print("  python disco_explorer.py  (interactive mode)")
