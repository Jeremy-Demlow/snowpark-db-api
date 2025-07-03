"""
Test to verify database isolation is working correctly.
This test will show which database is actually being used.
"""

import pytest
import logging
from snowpark_db_api import transfer

logger = logging.getLogger(__name__)

@pytest.mark.functional
def test_database_isolation_verification(isolated_test_db, test_table_tracker):
    """Verify that we're actually using an isolated test database."""
    test_db_name, db_manager = isolated_test_db
    
    # Show current database info
    logger.info(f"🔍 Test should be using database: {test_db_name}")
    
    # Verify isolation
    assert db_manager.verify_test_isolation(), f"Not using isolated database {test_db_name}"
    
    # List current tables
    current_tables = db_manager.list_test_tables()
    logger.info(f"📊 Current tables in {test_db_name}: {current_tables}")
    
    # Perform a simple transfer to create a table
    query = "(SELECT 'test' as test_column, 123 as test_number) AS isolation_test"
    
    logger.info(f"🚀 Creating test table in {test_db_name}")
    result = transfer(query, show_progress=True)
    
    assert result is True, "Test transfer should succeed"
    
    # Verify the table was created in the test database
    updated_tables = db_manager.list_test_tables()
    new_tables = set(updated_tables) - set(current_tables)
    
    logger.info(f"📊 New tables created: {list(new_tables)}")
    
    # Should have created ISOLATION_TEST table
    assert "ISOLATION_TEST" in updated_tables, f"ISOLATION_TEST table not found in {test_db_name}"
    
    logger.info(f"✅ Database isolation verified: Using {test_db_name}")
    logger.info(f"✅ Table created successfully: ISOLATION_TEST")

@pytest.mark.functional
def test_transfer_shows_correct_database(isolated_test_db):
    """Test that transfers show they're going to the test database."""
    test_db_name, db_manager = isolated_test_db
    
    # This should show the test database in the output
    query = "(SELECT 'verification' as test_col) AS database_verification"
    
    logger.info(f"🔍 Expected target database: {test_db_name}")
    
    # Transfer with verbose output to see which database it targets
    result = transfer(query, show_progress=True)
    
    assert result is True, "Transfer should succeed"
    
    # Verify we can list the created table
    tables = db_manager.list_test_tables()
    assert "DATABASE_VERIFICATION" in tables, f"DATABASE_VERIFICATION not found in {test_db_name}"
    
    logger.info(f"✅ Confirmed: Transfer went to isolated database {test_db_name}") 