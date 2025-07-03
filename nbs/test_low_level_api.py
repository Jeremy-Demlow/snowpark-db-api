#!/usr/bin/env python3
"""
Test Low-Level API with Current SQL Server Setup
Demonstrates complete control over transfer operations and raw primitives
"""

from snowpark_db_api import LowLevelTransferEngine
from snowpark_db_api.core import DataTransfer
from snowpark_db_api.config import get_config
from snowpark_db_api.snowflake_connection import SnowflakeConnection, ConnectionConfig

def test_raw_engine():
    """Test the low-level transfer engine with complete control."""
    print("⚙️ Testing Raw Transfer Engine - Complete Control")
    print("=" * 60)
    
    try:
        config = get_config()
        engine = LowLevelTransferEngine(config)
        
        print("Establishing raw connections...")
        connections = engine.establish_raw_connections()
        print(f"Connection success: {connections['success']}")
        
        if connections['success']:
            # Execute raw query
            raw_query = "SELECT TOP 10 ID, Column0 FROM dbo.RandomDataWith100Columns"
            source_results = engine.execute_raw_query(raw_query)
            print(f"Source query returned {len(source_results)} rows")
            
            # Test with a smaller table or proper query
            print("Testing with smaller data...")
            try:
                # Use a small table first - UserProfile is much smaller
                df = engine.create_snowpark_dataframe("dbo.UserProfile")
                print(f"DataFrame created from UserProfile table: {type(df)}")
                
                # Limit to just a few rows to be safe
                df_limited = df.limit(5)
                print(f"Limited DataFrame to 5 rows")
                
                # Write with raw control
                write_result = engine.write_dataframe_raw(df_limited, "RAW_ENGINE_TEST", "overwrite")
                print(f"Write completed successfully")
                
            except Exception as e:
                print(f"Small table access failed: {e}")
                print("Trying with explicit query instead...")
                
                try:
                    # Use explicit query with TOP to limit at the database level
                    df = engine.create_snowpark_dataframe("SELECT TOP 10 ID, Column0 FROM dbo.RandomDataWith100Columns")
                    print(f"DataFrame created from limited query: {type(df)}")
                    
                    # Write with raw control
                    write_result = engine.write_dataframe_raw(df, "RAW_ENGINE_TEST", "overwrite")
                    print(f"Write completed with query approach")
                    
                except Exception as e2:
                    print(f"Query approach also failed: {e2}")
                    print("Raw engine has implementation issues with DataFrame creation")
                    
                    # At least test that we can get raw query results
                    print("Testing raw query execution...")
                    simple_raw_query = "SELECT TOP 5 ID, Column0 FROM dbo.RandomDataWith100Columns"
                    raw_results = engine.execute_raw_query(simple_raw_query)
                    print(f"Raw query returned {len(raw_results)} rows: {raw_results[0] if raw_results else 'No data'}")
                    
                    # Consider this a partial success since raw queries work
                    print("Raw engine partially functional: raw queries work, DataFrame creation needs refinement")
                    return False  # Mark as failed since DataFrame creation doesn't work
            
            engine.cleanup_raw_connections()
            return True
        
        return False
        
    except Exception as e:
        print(f"❌ Raw engine test failed: {e}")
        return False

def test_direct_data_transfer():
    """Test direct DataTransfer class usage."""
    print("\n🔧 Testing Direct DataTransfer Class")
    print("=" * 60)
    
    try:
        config = get_config()
        config.snowflake.database = "DB_API_MSSQL"
        config.snowflake.create_db_if_missing = False
        
        transfer = DataTransfer(config)
        success = transfer.setup_connections()
        
        if success:
            # Test direct connection access
            source_conn = transfer.source_connection()
            cursor = source_conn.cursor()
            cursor.execute("SELECT TOP 5 ID FROM dbo.RandomDataWith100Columns")
            results = cursor.fetchall()
            cursor.close()
            print(f"Direct query returned {len(results)} rows")
            
            # Test Snowflake session
            sf_result = transfer.session.sql("SELECT CURRENT_DATABASE()").collect()
            print(f"Current database: {sf_result[0][0]}")
            
            transfer.cleanup()
            return True
        
        return False
        
    except Exception as e:
        print(f"❌ Direct transfer test failed: {e}")
        return False

def test_raw_snowflake_connection():
    """Test raw Snowflake connection management."""
    print("\n❄️ Testing Raw Snowflake Connection")
    print("=" * 60)
    
    try:
        config = get_config()
        sf_config = ConnectionConfig(
            account=config.snowflake.account,
            user=config.snowflake.user,
            password=config.snowflake.password,
            role=config.snowflake.role,
            warehouse=config.snowflake.warehouse,
            database="DB_API_MSSQL",
            schema="PUBLIC",
            create_db_if_missing=False
        )
        
        sf_conn = SnowflakeConnection.from_config(sf_config)
        
        # Test basic operations
        test_result = sf_conn.test_connection()
        print(f"Connection test: {test_result}")
        
        print(f"Database: {sf_conn.current_database}")
        print(f"Schema: {sf_conn.current_schema}")
        
        # Simple query
        result = sf_conn.sql("SELECT CURRENT_TIMESTAMP()").collect()
        print(f"Current time: {result[0][0]}")
        
        sf_conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Raw connection test failed: {e}")
        return False

def main():
    """Run all low-level API tests."""
    print("🧪 LOW-LEVEL API TEST SUITE")
    print("Testing complete control over transfer operations")
    print("=" * 80)
    
    tests = [
        ("Raw Engine", test_raw_engine),
        ("Direct DataTransfer", test_direct_data_transfer),
        ("Raw Snowflake Connection", test_raw_snowflake_connection),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            print(f"\n🔄 Running: {test_name}")
            result = test_func()
            success = bool(result)
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
        print("🎉 All low-level API tests PASSED!")
    elif passed > 0:
        print("⚠️  Some tests passed - partial functionality")
    else:
        print("❌ All tests failed - needs investigation")
    
    return passed == total

if __name__ == "__main__":
    main() 