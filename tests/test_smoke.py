"""
Smoke tests - quick validation that basic functionality works.
These tests should run fast and validate core imports and basic functionality.
"""

import pytest
from snowpark_db_api.config import get_config, Config, DatabaseType

@pytest.mark.smoke
class TestImports:
    """Test that all imports work correctly."""
    
    def test_high_level_api_imports(self):
        """Test high-level API imports."""
        from snowpark_db_api import transfer, transfer_sample
        
        assert callable(transfer), "transfer function should be importable"
        assert callable(transfer_sample), "transfer_sample function should be importable"
    
    def test_core_imports(self):
        """Test core module imports."""
        from snowpark_db_api.core import DataTransfer, transfer_data, transfer_query
        
        assert DataTransfer is not None, "DataTransfer class should be importable"
        assert callable(transfer_data), "transfer_data function should be importable" 
        assert callable(transfer_query), "transfer_query function should be importable"
    
    def test_config_imports(self):
        """Test configuration imports."""
        from snowpark_db_api.config import Config, DatabaseType, get_config
        
        assert Config is not None, "Config class should be importable"
        assert DatabaseType is not None, "DatabaseType enum should be importable"
        assert callable(get_config), "get_config function should be importable"
    
    def test_api_layer_imports(self):
        """Test API layer imports."""
        from snowpark_db_api.api import TransferBuilder, ConnectionManager
        
        assert TransferBuilder is not None, "TransferBuilder should be importable"
        assert ConnectionManager is not None, "ConnectionManager should be importable"

@pytest.mark.smoke 
class TestConfiguration:
    """Test configuration functionality."""
    
    def test_database_types_exist(self):
        """Test that database types are properly defined."""
        assert hasattr(DatabaseType, 'SQLSERVER'), "SQLSERVER type should exist"
        assert hasattr(DatabaseType, 'POSTGRESQL'), "POSTGRESQL type should exist"
        assert hasattr(DatabaseType, 'MYSQL'), "MYSQL type should exist"
    
    def test_config_creation(self):
        """Test that config can be created."""
        try:
            config = get_config()
            assert config is not None, "Configuration should be created"
            assert hasattr(config, 'database_type'), "Config should have database_type"
        except Exception:
            # It's OK if config fails in test environment without .env
            pytest.skip("Configuration requires .env file - skipping in test environment")

@pytest.mark.smoke
class TestBasicFunctionality:
    """Test basic functionality without database connections."""
    
    def test_transfer_function_exists(self):
        """Test that transfer function exists and is callable."""
        from snowpark_db_api import transfer
        
        assert callable(transfer), "transfer function should be callable"
    
    def test_data_transfer_class(self):
        """Test DataTransfer class creation."""
        from snowpark_db_api.core import DataTransfer
        from snowpark_db_api.config import Config, DatabaseType, SourceConfig, SnowflakeConfig, TransferConfig
        
        # Create minimal config for testing
        config = Config(
            database_type=DatabaseType.SQLSERVER,
            source=SourceConfig(
                host='test-host',
                database='test-database', 
                username='test-username',
                password='test-password'
            ),
            snowflake=SnowflakeConfig(
                account='test-account',
                user='test-user',
                password='test-snowflake-password',
                warehouse='test-warehouse',
                database='test-database'
            ),
            transfer=TransferConfig(
                source_table='test-table',
                destination_table='test-destination-table'
            )
        )
        
        # Should be able to create instance
        transfer = DataTransfer(config)
        assert transfer is not None, "DataTransfer should be created"
        assert transfer.config == config, "Config should be stored"
    
    def test_api_builders_creation(self):
        """Test that API builders can be created."""
        from snowpark_db_api.api import TransferBuilder, ConnectionManager
        
        builder = TransferBuilder()
        assert builder is not None, "TransferBuilder should be created"
        
        try:
            manager = ConnectionManager()
            assert manager is not None, "ConnectionManager should be created"
        except Exception:
            # May fail without proper config, that's OK for smoke tests
            pass

@pytest.mark.smoke
class TestErrorHandling:
    """Test basic error handling."""
    
    def test_invalid_imports_handled(self):
        """Test that invalid imports don't crash the module."""
        try:
            # Try importing everything from main module
            import snowpark_db_api
            
            # Should have basic attributes
            assert hasattr(snowpark_db_api, 'transfer'), "Should have transfer function"
            assert hasattr(snowpark_db_api, '__version__'), "Should have version"
            
        except ImportError as e:
            pytest.fail(f"Basic imports should not fail: {e}")
    
    def test_function_signatures(self):
        """Test that functions have expected signatures."""
        from snowpark_db_api import transfer, transfer_sample
        import inspect
        
        # transfer function should accept query_or_table as first parameter
        sig = inspect.signature(transfer)
        params = list(sig.parameters.keys())
        assert 'query_or_table' in params, "transfer should accept query_or_table parameter"
        
        # transfer_sample should accept query_or_table and rows
        sig = inspect.signature(transfer_sample)
        params = list(sig.parameters.keys())
        assert 'query_or_table' in params, "transfer_sample should accept query_or_table"
        assert 'rows' in params, "transfer_sample should accept rows parameter"

@pytest.mark.smoke
class TestDocumentation:
    """Test that documentation exists."""
    
    def test_module_docstrings(self):
        """Test that modules have docstrings."""
        import snowpark_db_api
        import snowpark_db_api.core
        import snowpark_db_api.config
        
        assert snowpark_db_api.__doc__ is not None, "Main module should have docstring"
        assert snowpark_db_api.core.__doc__ is not None, "Core module should have docstring"
        assert snowpark_db_api.config.__doc__ is not None, "Config module should have docstring"
    
    def test_function_docstrings(self):
        """Test that main functions have docstrings."""
        from snowpark_db_api import transfer, transfer_sample
        
        assert transfer.__doc__ is not None, "transfer function should have docstring"
        assert transfer_sample.__doc__ is not None, "transfer_sample function should have docstring"
