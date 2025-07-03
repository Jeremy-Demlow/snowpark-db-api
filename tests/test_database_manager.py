"""
Test Database Manager - Creates isolated test databases for testing.
Ensures tests don't pollute real databases and cleans up after testing.
"""

import os
import logging
from datetime import datetime
from typing import Optional
from contextlib import contextmanager

from snowpark_db_api.config import get_config
from snowpark_db_api.snowflake_connection import SnowflakeConnection, ConnectionConfig

logger = logging.getLogger(__name__)

class TestDatabaseManager:
    """Manages isolated test databases for functional testing."""
    
    def __init__(self, base_name: str = "DB_API_TEST"):
        """Initialize with a base name for test databases."""
        self.base_name = base_name
        self.test_db_name = None
        self.original_db_name = None
        self.snowflake_connection = None
        self._session = None
    
    def create_test_database(self) -> str:
        """Create a unique test database and return its name."""
        # Generate unique test database name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.test_db_name = f"{self.base_name}_{timestamp}"
        
        # Get config and set up Snowflake connection
        config = get_config()
        self.original_db_name = config.snowflake.database
        
        sf_config = ConnectionConfig(
            account=config.snowflake.account,
            user=config.snowflake.user,
            password=config.snowflake.password,
            role=config.snowflake.role,
            warehouse=config.snowflake.warehouse,
            database=config.snowflake.database,  # Connect with admin rights first
            schema=config.snowflake.db_schema
        )
        
        self.snowflake_connection = SnowflakeConnection.from_config(sf_config)
        self._session = self.snowflake_connection.session
        
        # Create the test database
        logger.info(f"Creating test database: {self.test_db_name}")
        self._session.sql(f"CREATE DATABASE IF NOT EXISTS {self.test_db_name}").collect()
        self._session.sql(f"USE DATABASE {self.test_db_name}").collect()
        self._session.sql(f"CREATE SCHEMA IF NOT EXISTS PUBLIC").collect()
        
        # Update environment variable so tests use the test database
        os.environ['SNOWFLAKE_DATABASE'] = self.test_db_name
        
        logger.info(f"✅ Test database created: {self.test_db_name}")
        return self.test_db_name
    
    def cleanup_test_database(self):
        """Drop the test database and restore original configuration."""
        if self.test_db_name and self._session:
            try:
                logger.info(f"Cleaning up test database: {self.test_db_name}")
                
                # Switch back to original database first
                if self.original_db_name:
                    self._session.sql(f"USE DATABASE {self.original_db_name}").collect()
                
                # Drop the test database
                self._session.sql(f"DROP DATABASE IF EXISTS {self.test_db_name}").collect()
                logger.info(f"✅ Test database cleaned up: {self.test_db_name}")
                
                # Restore original environment
                if self.original_db_name:
                    os.environ['SNOWFLAKE_DATABASE'] = self.original_db_name
                
            except Exception as e:
                logger.warning(f"Failed to cleanup test database {self.test_db_name}: {e}")
            
            finally:
                if self.snowflake_connection:
                    self.snowflake_connection.close()
    
    def list_test_tables(self) -> list:
        """List all tables in the test database."""
        if not self._session or not self.test_db_name:
            return []
        
        try:
            result = self._session.sql(f"SHOW TABLES IN DATABASE {self.test_db_name}").collect()
            return [row['name'] for row in result]
        except Exception as e:
            logger.warning(f"Failed to list tables: {e}")
            return []
    
    def verify_test_isolation(self) -> bool:
        """Verify that we're actually using the test database."""
        if not self._session:
            return False
        
        try:
            result = self._session.sql("SELECT CURRENT_DATABASE()").collect()
            current_db = result[0][0] if result else None
            is_isolated = current_db == self.test_db_name
            
            if is_isolated:
                logger.info(f"✅ Test isolation verified: Using {current_db}")
            else:
                logger.error(f"❌ Test isolation failed: Expected {self.test_db_name}, got {current_db}")
            
            return is_isolated
            
        except Exception as e:
            logger.error(f"Failed to verify test isolation: {e}")
            return False


@contextmanager
def isolated_test_database():
    """Context manager for isolated test database."""
    manager = TestDatabaseManager()
    try:
        test_db = manager.create_test_database()
        if not manager.verify_test_isolation():
            raise RuntimeError("Test database isolation failed")
        
        yield test_db, manager
        
    finally:
        manager.cleanup_test_database()


def cleanup_old_test_databases():
    """Clean up any old test databases that might be left over."""
    try:
        config = get_config()
        sf_config = ConnectionConfig(
            account=config.snowflake.account,
            user=config.snowflake.user,
            password=config.snowflake.password,
            role=config.snowflake.role,
            warehouse=config.snowflake.warehouse,
            database=config.snowflake.database,
            schema=config.snowflake.db_schema
        )
        
        with SnowflakeConnection.from_config(sf_config) as conn:
            # List all databases
            result = conn.session.sql("SHOW DATABASES").collect()
            
            test_databases = [
                row['name'] for row in result 
                if row['name'].startswith('DB_API_TEST_')
            ]
            
            if test_databases:
                logger.info(f"Found {len(test_databases)} old test databases to clean up")
                
                for db_name in test_databases:
                    try:
                        conn.session.sql(f"DROP DATABASE IF EXISTS {db_name}").collect()
                        logger.info(f"Cleaned up old test database: {db_name}")
                    except Exception as e:
                        logger.warning(f"Failed to clean up {db_name}: {e}")
            else:
                logger.info("No old test databases found")
                
    except Exception as e:
        logger.warning(f"Failed to cleanup old test databases: {e}")


if __name__ == "__main__":
    # Test the database manager
    with isolated_test_database() as (test_db, manager):
        print(f"Created test database: {test_db}")
        tables = manager.list_test_tables()
        print(f"Tables in test database: {tables}") 