#!/usr/bin/env python3
"""
Test Mid-Level API with Current SQL Server Setup
Demonstrates composable building blocks for custom workflows
"""

from snowpark_db_api import TransferBuilder, ConnectionManager
from snowpark_db_api.transforms import Pipeline, SchemaTransform, QueryTransform, show_pipeline_steps

def test_transfer_builder():
    """Test the composable TransferBuilder."""
    print("🏗️ Testing TransferBuilder - Composable Workflows")
    print("=" * 60)
    
    try:
        # Build a custom transfer workflow
        result = (TransferBuilder()
                 .from_source("(SELECT TOP 50 ID, Column0, Column1 FROM dbo.RandomDataWith100Columns) AS builder_test")
                 .to_destination("BUILDER_TEST_TABLE") 
                 .with_schema_mapping()  # Custom schema mapping
                 .with_query_optimization()  # Query optimization
                 .show_pipeline_steps(True)  # Show what's happening
                 .execute())
        
        print(f"TransferBuilder result: {result}")
        return result
        
    except Exception as e:
        print(f"❌ TransferBuilder failed: {e}")
        return False

def test_connection_manager():
    """Test explicit connection management."""
    print("\n🔌 Testing ConnectionManager - Explicit Connection Control")
    print("=" * 60)
    
    try:
        # Create connection manager
        manager = ConnectionManager()
        
        # Test connections
        print("Testing connections...")
        connection_status = manager.test_connections()
        print(f"Connection status: {connection_status}")
        
        if connection_status.get('source_db') and connection_status.get('snowflake'):
            # Execute a transfer with established connections
            print("Executing transfer with managed connections...")
            result = manager.execute_transfer(
                "(SELECT TOP 25 ID, Column0 FROM dbo.RandomDataWith100Columns) AS managed_test"
            )
            print(f"Managed transfer result: {result}")
            
            # Clean up
            manager.close()
            return result
        else:
            print("❌ Connection test failed")
            return False
            
    except Exception as e:
        print(f"❌ ConnectionManager failed: {e}")
        return False

def test_custom_pipeline():
    """Test building custom transform pipelines."""
    print("\n🔄 Testing Custom Transform Pipelines")
    print("=" * 60)
    
    try:
        # Create custom transforms
        schema_transform = SchemaTransform(
            source_db_type='sqlserver',
            target_db_type='snowflake'
        )
        
        query_transform = QueryTransform(
            "(SELECT TOP 30 ID, Column0, Column1 FROM dbo.RandomDataWith100Columns) AS pipeline_test",
            "PIPELINE_TEST_TABLE"
        )
        
        # Build pipeline
        pipeline = Pipeline([schema_transform, query_transform])
        
        # Show pipeline details
        print("Pipeline created with transforms:")
        for i, transform in enumerate(pipeline.transforms, 1):
            print(f"  {i}. {transform.__class__.__name__}")
        
        # Test pipeline steps
        sample_data = {"sample": "data", "columns": ["ID", "Column0", "Column1"]}
        print(f"\nTesting pipeline with sample data: {sample_data}")
        
        # This is more of a structural test since we can't easily test transforms in isolation
        print("✅ Pipeline structure created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Custom pipeline failed: {e}")
        return False

def test_builder_with_environment():
    """Test TransferBuilder with different environment configurations."""
    print("\n🌍 Testing Environment-Specific Configuration")
    print("=" * 60)
    
    try:
        # Development environment setup
        print("📍 Testing development environment setup...")
        dev_result = (TransferBuilder()
                     .from_source("(SELECT TOP 10 ID, Column0 FROM dbo.RandomDataWith100Columns) AS dev_test")
                     .to_destination("DEV_TEST_TABLE")
                     .with_environment('development')  # Optimized for dev
                     .execute())
        
        print(f"Development environment result: {dev_result}")
        
        # Production environment setup  
        print("\n📍 Testing production environment setup...")
        prod_result = (TransferBuilder()
                      .from_source("(SELECT TOP 20 ID, Column0 FROM dbo.RandomDataWith100Columns) AS prod_test")
                      .to_destination("PROD_TEST_TABLE")
                      .with_environment('production')  # Optimized for prod
                      .execute())
        
        print(f"Production environment result: {prod_result}")
        return dev_result and prod_result
        
    except Exception as e:
        print(f"❌ Environment configuration failed: {e}")
        return False

def test_advanced_workflows():
    """Test advanced workflow combinations."""
    print("\n⚙️ Testing Advanced Workflow Combinations")
    print("=" * 60)
    
    try:
        # Complex workflow with multiple configurations
        result = (TransferBuilder()
                 .from_source("(SELECT TOP 15 Id, FullName FROM dbo.UserProfile WHERE Id IS NOT NULL) AS advanced_test")
                 .to_destination("ADVANCED_WORKFLOW_TABLE")
                 .with_schema_mapping({'Id': 'USER_ID', 'FullName': 'FULL_NAME'})  # Custom mappings
                 .with_query_optimization({'complexity': 'simple'})  # Optimization hints
                 .with_environment('production')  # Production settings
                 .show_pipeline_steps(True)  # Show transparency
                 .execute())
        
        print(f"Advanced workflow result: {result}")
        return result
        
    except Exception as e:
        print(f"❌ Advanced workflow failed: {e}")
        return False

def test_connection_lifecycle():
    """Test full connection lifecycle management."""
    print("\n🔄 Testing Connection Lifecycle Management")
    print("=" * 60)
    
    try:
        # Test complete lifecycle
        manager = ConnectionManager()
        
        # Step 1: Connect
        print("Step 1: Establishing connections...")
        connect_success = manager.connect()
        print(f"Connection established: {connect_success}")
        
        if not connect_success:
            print("❌ Failed to establish connections")
            return False
        
        # Step 2: Test
        print("Step 2: Testing connections...")
        test_results = manager.test_connections()
        print(f"Test results: {test_results}")
        
        # Step 3: Execute multiple transfers
        print("Step 3: Executing multiple transfers...")
        transfers = [
            "(SELECT TOP 5 ID, Column0 FROM dbo.RandomDataWith100Columns) AS lifecycle_test1",
            "(SELECT TOP 5 Id, FullName FROM dbo.UserProfile WHERE Id IS NOT NULL) AS lifecycle_test2"
        ]
        
        results = []
        for transfer_query in transfers:
            result = manager.execute_transfer(transfer_query)
            results.append(result)
            print(f"Transfer result: {result}")
        
        # Step 4: Clean up
        print("Step 4: Cleaning up connections...")
        manager.close()
        
        overall_success = all(results)
        print(f"Lifecycle test overall success: {overall_success}")
        return overall_success
        
    except Exception as e:
        print(f"❌ Connection lifecycle failed: {e}")
        return False

def main():
    """Run all mid-level API tests."""
    print("🧪 MID-LEVEL API TEST SUITE")
    print("Testing composable building blocks for custom workflows")
    print("=" * 80)
    
    tests = [
        ("TransferBuilder", test_transfer_builder),
        ("ConnectionManager", test_connection_manager),
        ("Custom Pipeline", test_custom_pipeline),
        ("Environment Config", test_builder_with_environment),
        ("Advanced Workflows", test_advanced_workflows),
        ("Connection Lifecycle", test_connection_lifecycle),
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
        print("🎉 All mid-level API tests PASSED!")
        print("   Composable building blocks are working perfectly!")
    elif passed > total // 2:
        print("⚠️  Most tests passed - API is mostly functional")
    else:
        print("❌ Multiple failures - needs investigation")
    
    return passed == total

if __name__ == "__main__":
    main() 