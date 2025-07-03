"""
Functional tests for real data transfers.
Tests actual working functionality with real database connections.
"""

import pytest
import time
from snowpark_db_api import transfer, transfer_sample
from snowpark_db_api.core import transfer_data, transfer_query, transfer_query_transparent
from snowpark_db_api.api import TransferBuilder, ConnectionManager

@pytest.mark.functional
class TestHighLevelAPI:
    """Test the high-level API functions that we know work."""
    
    def test_transfer_simple_query(self, sample_queries, expected_results):
        """Test simple transfer with known working query."""
        query = sample_queries['small_orders']
        expected = expected_results['customer_orders']
        
        # This should work - we've tested it
        result = transfer(query, show_progress=True)
        
        assert result is True, "Transfer should succeed"
        # Note: We don't validate table contents here, just that it completes
    
    def test_transfer_sample_function(self, sample_queries):
        """Test transfer_sample function with known data."""
        query = sample_queries['filtered_data']
        
        # Sample transfer should work and be safer
        result = transfer_sample(query, rows=50)
        
        assert result is True, "Sample transfer should succeed"
    
    def test_transfer_with_limit(self, sample_queries):
        """Test transfer with row limit."""
        query = sample_queries['customer_data']
        
        result = transfer(query, limit=10, show_progress=True)
        
        assert result is True, "Limited transfer should succeed"

@pytest.mark.functional  
class TestCoreAPI:
    """Test core transfer functions directly."""
    
    def test_transfer_data_function(self, real_transfer, sample_queries):
        """Test core transfer_data function."""
        if not real_transfer.setup_connections():
            pytest.skip("Could not establish connections")
        
        query = sample_queries['small_orders']
        
        # Use the core function directly
        result = transfer_data(real_transfer.config, query=query)
        
        assert result is True, "Core transfer_data should work"
    
    def test_transfer_query_function(self, sample_queries):
        """Test the transfer_query function."""
        query = sample_queries['customer_data']
        
        result = transfer_query(query)
        
        assert result is True, "transfer_query function should work"
    
    def test_transfer_query_transparent(self, sample_queries):
        """Test transparent transfer function."""
        query = sample_queries['small_orders']
        
        result = transfer_query_transparent(query, verbose=True)
        
        assert result is True, "Transparent transfer should work"

@pytest.mark.functional
class TestMidLevelAPI:
    """Test mid-level API components."""
    
    def test_connection_manager(self, test_config):
        """Test ConnectionManager functionality."""
        manager = ConnectionManager(test_config)
        
        connected = manager.connect()
        assert connected is True, "ConnectionManager should connect"
        
        # Test connections
        status = manager.test_connections()
        # At least one connection should work
        assert any(status.values()), "At least one connection should be working"
        
        manager.close()
    
    def test_transfer_builder_basic(self, sample_queries):
        """Test basic TransferBuilder functionality."""
        query = sample_queries['customer_data']
        
        builder = TransferBuilder()
        result = (builder
                 .from_source(query)
                 .to_destination("TEST_CUSTOMER_DATA")
                 .show_pipeline_steps(True)
                 .execute())
        
        assert result is True, "TransferBuilder should execute successfully"

@pytest.mark.functional
class TestEndToEndScenarios:
    """Test complete end-to-end scenarios that match our validated examples."""
    
    def test_customer_orders_scenario(self, sample_queries, expected_results):
        """Test the customer orders scenario we validated."""
        query = sample_queries['small_orders']
        expected = expected_results['customer_orders']
        
        start_time = time.time()
        result = transfer(query, show_progress=True)
        duration = time.time() - start_time
        
        assert result is True, "Customer orders transfer should succeed"
        assert duration < 15.0, f"Transfer took too long: {duration:.1f}s"
        # We expect this to complete in under 15 seconds based on our testing
    
    def test_filtered_data_scenario(self, sample_queries, expected_results):
        """Test the filtered data scenario we validated."""
        query = sample_queries['filtered_data']
        expected = expected_results['filtered_data']
        
        start_time = time.time()
        result = transfer(query, show_progress=True)
        duration = time.time() - start_time
        
        assert result is True, "Filtered data transfer should succeed"
        assert duration < 20.0, f"Transfer took too long: {duration:.1f}s"
    
    def test_customer_data_scenario(self, sample_queries, expected_results):
        """Test the customer data scenario we validated."""
        query = sample_queries['customer_data']
        expected = expected_results['customer_data']
        
        start_time = time.time()
        result = transfer(query, show_progress=True) 
        duration = time.time() - start_time
        
        assert result is True, "Customer data transfer should succeed"
        assert duration < 10.0, f"Transfer took too long: {duration:.1f}s"

@pytest.mark.functional
@pytest.mark.slow
class TestLargeDataScenarios:
    """Test scenarios with larger datasets."""
    
    def test_large_sample_transfer(self):
        """Test transfer with larger sample size."""
        query = """(SELECT TOP 1000 ID, Column0, Column1, Column2, Column3, Column4, 
                          Column5 as last_updated, Column6, Column7, Column8
                   FROM dbo.RandomDataWith100Columns
                   WHERE Column5 > CAST('2023-01-01 00:00:00' AS DATETIME)) AS large_sample"""
        
        start_time = time.time()
        result = transfer(query, show_progress=True)
        duration = time.time() - start_time
        
        assert result is True, "Large sample transfer should succeed"
        assert duration < 30.0, f"Large transfer took too long: {duration:.1f}s"
    
    def test_sample_function_safety(self):
        """Test that transfer_sample is always safe with large tables."""
        # This should safely limit to small sample even from huge table
        result = transfer_sample("dbo.RandomDataWith100Columns", rows=100)
        
        assert result is True, "Sample function should always be safe"

@pytest.mark.functional
class TestErrorHandling:
    """Test error handling with real scenarios."""
    
    def test_invalid_query(self):
        """Test handling of invalid SQL."""
        invalid_query = "(SELECT * FROM nonexistent_table) AS test"
        
        result = transfer(invalid_query, show_progress=False)
        
        assert result is False, "Invalid query should return False, not crash"
    
    def test_invalid_table(self):
        """Test handling of invalid table name."""
        result = transfer("dbo.NonExistentTable", show_progress=False)
        
        assert result is False, "Invalid table should return False, not crash"
    
    def test_connection_failure_graceful(self):
        """Test that connection failures are handled gracefully."""
        # We can't easily simulate this in functional tests,
        # but we can test that the functions don't crash unexpectedly
        
        try:
            # Try with minimal query
            result = transfer("(SELECT 1 AS test_col) AS connection_test", show_progress=False)
            # Result could be True or False, but shouldn't crash
            assert isinstance(result, bool), "Should return boolean even on connection issues"
        except Exception as e:
            pytest.fail(f"Should handle connection issues gracefully, not crash: {e}")

@pytest.mark.functional 
class TestConfigurationRobustness:
    """Test that configuration works robustly."""
    
    def test_config_loading(self, test_config):
        """Test that configuration loads properly."""
        assert test_config is not None, "Configuration should load"
        assert hasattr(test_config, 'database_type'), "Config should have database_type"
        assert hasattr(test_config, 'source'), "Config should have source"
        assert hasattr(test_config, 'snowflake'), "Config should have snowflake"
    
    def test_transfer_with_real_config(self, test_config, sample_queries):
        """Test transfer using real configuration object."""
        query = sample_queries['customer_data']
        
        # Pass config explicitly
        from snowpark_db_api.core import transfer_data
        result = transfer_data(test_config, query=query)
        
        assert result is True, "Transfer with explicit config should work"
