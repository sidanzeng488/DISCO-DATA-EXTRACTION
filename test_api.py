"""
DISCODATA API Connection Test
Tests the connection to EEA DISCODATA API
"""

from discodata_client import create_client
import json

def test_connection():
    """Test basic API connection"""
    print("=" * 50)
    print("DISCODATA API Connection Test")
    print("=" * 50)
    print(f"\nAPI URL: https://discodata.eea.europa.eu/sql")
    
    client = create_client()
    
    # Test 1: Preview T_UWWTPS
    print("\n[Test 1] Fetching T_UWWTPS (Treatment Plants)...")
    try:
        data = client.preview("[WISE_UWWTD].[v1r1].[T_UWWTPS]", rows=3)
        print(f"  ✓ Success! Got {len(data)} rows")
        print(f"  Columns: {len(data[0].keys())}")
        print(f"  Sample: {data[0]['uwwName']} ({data[0]['CountryCode']})")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False
    
    # Test 2: Preview T_Agglomerations
    print("\n[Test 2] Fetching T_Agglomerations...")
    try:
        data = client.preview("[WISE_UWWTD].[v1r1].[T_Agglomerations]", rows=3)
        print(f"  ✓ Success! Got {len(data)} rows")
        print(f"  Columns: {len(data[0].keys())}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False
    
    # Test 3: Preview Surface Water Body
    print("\n[Test 3] Fetching SWB_SurfaceWaterBody...")
    try:
        data = client.preview("[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]", rows=3)
        print(f"  ✓ Success! Got {len(data)} rows")
        print(f"  Columns: {len(data[0].keys())}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False
    
    # Test 4: Preview Groundwater Body
    print("\n[Test 4] Fetching GWB_GroundWaterBody...")
    try:
        data = client.preview("[WISE_WFD].[v2r1].[GWB_GroundWaterBody]", rows=3)
        print(f"  ✓ Success! Got {len(data)} rows")
        print(f"  Columns: {len(data[0].keys())}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False
    
    # Test 5: Select specific columns with filter
    print("\n[Test 5] Query with column selection and filter...")
    try:
        data = client.select(
            "[WISE_UWWTD].[v1r1].[T_UWWTPS]",
            columns=['uwwCode', 'uwwName', 'CountryCode', 'uwwCapacity'],
            where="CountryCode = 'DE'",
            limit=5
        )
        print(f"  ✓ Success! Got {len(data)} German plants")
        for row in data:
            print(f"    - {row['uwwName']}: {row['uwwCapacity']} PE")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False
    
    # Test 6: Count records
    print("\n[Test 6] Count total records...")
    try:
        count = client.count("[WISE_UWWTD].[v1r1].[T_UWWTPS]")
        print(f"  ✓ Success! Total treatment plants: {count}")
    except Exception as e:
        print(f"  ✗ Failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("All tests passed! API connection is working.")
    print("=" * 50)
    return True


if __name__ == "__main__":
    test_connection()
