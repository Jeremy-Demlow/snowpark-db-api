#!/usr/bin/env python3
"""
Test High-Level API with Current SQL Server Setup
Demonstrates the simple, flexible high-level API for analysts
"""

from snowpark_db_api import transfer, transfer_sample, transfer_with_validation

def test_basic_transfer():
    """Test simple table transfer with configuration overrides."""
    print("🚀 Testing Basic Transfer with Runtime Config Overrides")
    print("=" * 60)
    
    # Simple query with just 100 rows for testing
    result = transfer(
        "(SELECT TOP 100 * FROM dbo.RandomDataWith100Columns) AS high_level_test",
        snowflake_database="DB_API_MSSQL",
        snowflake_schema="PUBLIC", 
        create_db_if_missing=False  # Don't try to create - assume it exists
    )
    
    print(f"Transfer result: {result}")
    return result

def test_sample_transfer():
    """Test the transfer_sample function."""
    print("\n🧪 Testing Sample Transfer Function")
    print("=" * 60)
    
    # Test with small sample - always safe
    result = transfer_sample(
        "(SELECT TOP 500 ID, Column0, Column1, Column2, Column3 FROM dbo.RandomDataWith100Columns) AS sample_test",
        rows=50,  # Even smaller sample
        snowflake_database="DB_API_MSSQL",
        snowflake_schema="PUBLIC"
    )
    
    print(f"Sample transfer result: {result}")
    return result

def test_different_queries():
    """Test various query patterns."""
    print("\n📊 Testing Different Query Patterns")
    print("=" * 60)
    
    queries = [
        # Small user profile data
        ("(SELECT TOP 10 Id, FullName, Country FROM dbo.UserProfile WHERE Id IS NOT NULL) AS user_sample", "User profile data"),
        
        # Order data with date filter
        ("(SELECT TOP 20 o_orderkey, o_custkey, o_totalprice FROM dbo.ORDERS WHERE o_orderdate >= '2020-01-01') AS recent_orders", "Recent orders"),
        
        # Random data with specific columns
        ("(SELECT TOP 25 ID, Column0, Column1, Column5 as timestamp_col FROM dbo.RandomDataWith100Columns) AS minimal_test", "Minimal column set"),
    ]
    
    results = []
    for query, description in queries:
        print(f"\n📝 Testing: {description}")
        result = transfer(
            query,
            snowflake_database="DB_API_MSSQL",
            snowflake_schema="PUBLIC",
            create_db_if_missing=False
        )
        results.append((description, result))
        print(f"   Result: {'✅ Success' if result else '❌ Failed'}")
    
    return results

def test_validation():
    """Test transfer with validation."""
    print("\n✅ Testing Transfer with Validation")
    print("=" * 60)
    
    validation_rules = {
        'min_rows': 1,
        'max_rows': 100,
        'required_columns': ['ID', 'Column0']
    }
    
    result = transfer_with_validation(
        "(SELECT TOP 15 ID, Column0, Column1 FROM dbo.RandomDataWith100Columns) AS validated_test",
        validation_rules=validation_rules,
        snowflake_database="DB_API_MSSQL",
        snowflake_schema="PUBLIC",
        create_db_if_missing=False
    )
    
    print(f"Validation result: {result}")
    return result

def test_flexibility_showcase():
    """Showcase the flexibility of runtime configuration."""
    print("\n🎯 Testing Configuration Flexibility")
    print("=" * 60)
    
    # Same data, different destinations
    base_query = "(SELECT TOP 5 ID, Column0 FROM dbo.RandomDataWith100Columns) AS flex_test"
    
    # Test 1: Different schema
    print("📍 Test 1: Using different schema")
    result1 = transfer(
        base_query.replace("flex_test", "flex_test_public"),
        snowflake_database="DB_API_MSSQL",
        snowflake_schema="PUBLIC"
    )
    
    # Test 2: Show what overrides are applied
    print("\n📍 Test 2: With explicit create_db_if_missing=False")
    result2 = transfer(
        base_query.replace("flex_test", "flex_test_safe"),
        snowflake_database="DB_API_MSSQL", 
        snowflake_schema="PUBLIC",
        create_db_if_missing=False
    )
    
    print(f"Flexibility test results: {result1}, {result2}")
    return result1 and result2

def main():
    """Run all high-level API tests."""
    print("🧪 HIGH-LEVEL API TEST SUITE")
    print("Testing the simple, flexible API for analysts")
    print("=" * 80)
    
    tests = [
        ("Basic Transfer", test_basic_transfer),
        ("Sample Transfer", test_sample_transfer), 
        ("Different Queries", test_different_queries),
        ("Validation", test_validation),
        ("Flexibility Showcase", test_flexibility_showcase),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            print(f"\n🔄 Running: {test_name}")
            result = test_func()
            success = bool(result) if not isinstance(result, list) else all(r[1] for r in result)
            results.append((test_name, success))
            print(f"✅ {test_name}: {'PASSED' if success else 'FAILED'}")
        except Exception as e:
            print(f"❌ {test_name}: FAILED with error: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 TEST SUMMARY")
    print("=" * 80)
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"  {status} {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All high-level API tests PASSED!")
        print("   The flexible, simple API is working perfectly!")
    elif passed > total // 2:
        print("⚠️  Most tests passed - API is mostly functional")
    else:
        print("❌ Multiple failures - needs investigation")
    
    return passed == total

if __name__ == "__main__":
    main() 