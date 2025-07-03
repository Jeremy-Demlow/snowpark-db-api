"""
Pytest configuration for functional testing.
Real connections, real data, real functionality.
Uses isolated test databases to avoid polluting production data.
"""

import pytest
import os
import logging
from pathlib import Path

from snowpark_db_api.config import get_config
from snowpark_db_api.core import DataTransfer
from tests.test_database_manager import isolated_test_database, cleanup_old_test_databases

# Set up logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def isolated_test_db():
    """Create an isolated test database for the entire test session."""
    # Clean up any old test databases first
    cleanup_old_test_databases()
    
    with isolated_test_database() as (test_db_name, db_manager):
        logger.info(f"🗄️  Test session using isolated database: {test_db_name}")
        yield test_db_name, db_manager

@pytest.fixture(scope="session")
def test_config(isolated_test_db):
    """Get real configuration with isolated test database."""
    test_db_name, db_manager = isolated_test_db
    config = get_config()
    
    # Verify we're using the test database
    if not db_manager.verify_test_isolation():
        pytest.fail(f"Test database isolation failed - not using {test_db_name}")
    
    logger.info(f"✅ Configuration using test database: {test_db_name}")
    return config

@pytest.fixture(scope="session")
def real_transfer(test_config):
    """Create a real DataTransfer instance for testing with isolated database."""
    transfer = DataTransfer(test_config)
    yield transfer
    transfer.cleanup()

# Pytest markers
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "smoke: Quick validation tests")
    config.addinivalue_line("markers", "functional: End-to-end functional tests")
    config.addinivalue_line("markers", "slow: Slow tests that transfer larger datasets")
    config.addinivalue_line("markers", "isolated_db: Tests that require isolated test database")

@pytest.fixture
def sample_queries():
    """Real queries that we know work from testing."""
    return {
        'small_orders': """(SELECT o_orderkey as order_id, o_custkey as customer_id, 
                               o_totalprice as order_amount, 'COMPLETED' as order_status,
                               o_orderdate as last_updated
                           FROM dbo.ORDERS
                           WHERE o_orderdate >= CAST('2020-01-01 00:00:00' AS DATE)) AS customer_orders""",
        
        'filtered_data': """(SELECT TOP 500 ID, Column0, Column1, Column2, Column3, Column4, 
                                  Column5 as last_updated, Column6, Column7, Column8
                           FROM dbo.RandomDataWith100Columns
                           WHERE Column5 > CAST('2023-01-01 00:00:00' AS DATETIME)) AS filtered_data""",
        
        'customer_data': """(SELECT Id as customer_id, 
                                   FullName as customer_name, 
                                   Country as customer_country, 
                                   Notes as customer_notes
                            FROM dbo.UserProfile
                            WHERE Id IS NOT NULL) AS customer_data"""
    }

@pytest.fixture
def expected_results():
    """Expected results from our real testing."""
    return {
        'customer_orders': {'expected_rows': 4, 'table_name': 'CUSTOMER_ORDERS'},
        'filtered_data': {'expected_rows': 500, 'table_name': 'FILTERED_DATA'}, 
        'customer_data': {'expected_rows': 1, 'table_name': 'CUSTOMER_DATA'}
    }

@pytest.fixture
def test_table_tracker(isolated_test_db):
    """Track tables created during testing for transparency."""
    test_db_name, db_manager = isolated_test_db
    
    # Get initial table count
    initial_tables = set(db_manager.list_test_tables())
    logger.info(f"📊 Initial tables in {test_db_name}: {len(initial_tables)}")
    
    yield db_manager
    
    # Show what tables were created during the test
    final_tables = set(db_manager.list_test_tables())
    new_tables = final_tables - initial_tables
    
    if new_tables:
        logger.info(f"📊 Test created {len(new_tables)} new tables: {list(new_tables)}")
    else:
        logger.info("📊 No new tables created during test") 