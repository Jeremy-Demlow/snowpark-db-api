"""Core data transfer functionality using Snowpark DB-API.

This module provides the main data transfer logic using the new Snowpark DB-API
for efficient, scalable data movement from various databases to Snowflake.
"""

from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
from multiprocessing import freeze_support
import traceback

from .config import AppConfig, DatabaseType
from .connections import get_connection_factory, test_connection
from .utils import ProgressTracker, validate_table_name, save_transfer_metadata, print_banner

__all__ = ['DataTransfer', 'transfer_data', 'create_snowflake_session']

logger = logging.getLogger(__name__)

class DataTransfer:
    """Main class for handling data transfers using Snowpark DB-API."""
    
    def __init__(self, config: AppConfig):
        """Initialize data transfer with configuration.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.session: Optional[Session] = None
        self.connection_factory = None
        self.transfer_stats = {
            'start_time': None,
            'end_time': None,
            'rows_transferred': 0,
            'errors': 0,
            'warnings': 0
        }
    
    def setup_connections(self) -> bool:
        """Setup database connections.
        
        Returns:
            True if connections are successful, False otherwise
        """
        try:
            logger.info("Setting up database connections...")
            
            # Create source database connection factory
            self.connection_factory = get_connection_factory(
                self.config.database_type, 
                self.config.source_db
            )
            
            # Test source connection
            logger.info("Testing source database connection...")
            if not test_connection(self.connection_factory):
                logger.error("Failed to connect to source database")
                return False
            
            # Create Snowflake session
            logger.info("Creating Snowflake session...")
            self.session = create_snowflake_session(self.config.snowflake)
            
            # Test Snowflake connection
            if not self._test_snowflake_connection():
                logger.error("Failed to connect to Snowflake")
                return False
            
            logger.info("All connections established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up connections: {e}")
            return False
    
    def _test_snowflake_connection(self) -> bool:
        """Test Snowflake connection."""
        try:
            result = self.session.sql("SELECT 1").collect()
            return len(result) == 1
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return False
    
    def transfer_table(self, query: Optional[str] = None) -> bool:
        """Transfer data from source to Snowflake.
        
        Args:
            query: Optional custom SQL query. If not provided, transfers entire table.
            
        Returns:
            True if transfer is successful, False otherwise
        """
        try:
            self.transfer_stats['start_time'] = datetime.now()
            
            # Validate and prepare table names
            source_table = validate_table_name(self.config.transfer.source_table)
            dest_table = validate_table_name(self.config.transfer.destination_table)
            
            logger.info(f"Starting transfer: {source_table} -> {dest_table}")
            
            # Build dbapi parameters
            dbapi_params = self._build_dbapi_params(source_table, query)
            
            # Execute transfer using Snowpark DB-API
            logger.info("Executing data transfer using Snowpark DB-API...")
            df = self.session.read.dbapi(
                self.connection_factory,
                **dbapi_params
            )
            
            # Get row count for progress tracking
            logger.info("Getting row count...")
            row_count = df.count()
            logger.info(f"Transferring {row_count:,} rows")
            
            # Write to Snowflake table
            logger.info(f"Writing data to Snowflake table: {dest_table}")
            write_result = df.write.mode(self.config.transfer.mode).save_as_table(dest_table)
            
            # Update statistics
            self.transfer_stats['rows_transferred'] = row_count
            self.transfer_stats['end_time'] = datetime.now()
            
            # Save metadata
            self._save_transfer_metadata(source_table, dest_table, row_count)
            
            logger.info(f"Transfer completed successfully: {row_count:,} rows")
            return True
            
        except SnowparkSQLException as e:
            logger.error(f"Snowpark SQL error during transfer: {e}")
            self.transfer_stats['errors'] += 1
            return False
        except Exception as e:
            logger.error(f"Error during transfer: {e}")
            logger.debug(traceback.format_exc())
            self.transfer_stats['errors'] += 1
            return False
    
    def _build_dbapi_params(self, source_table: str, query: Optional[str] = None) -> Dict[str, Any]:
        """Build parameters for dbapi call.
        
        Args:
            source_table: Source table name
            query: Optional custom query
            
        Returns:
            Dictionary of dbapi parameters
        """
        params = {}
        
        # Table or query
        if query:
            params['query'] = query
        else:
            params['table'] = source_table
        
        # Performance parameters
        if self.config.transfer.fetch_size:
            params['fetch_size'] = self.config.transfer.fetch_size
        
        if self.config.transfer.max_workers:
            params['max_workers'] = self.config.transfer.max_workers
        
        if self.config.transfer.query_timeout:
            params['query_timeout'] = self.config.transfer.query_timeout
        
        # Partitioning parameters for large datasets
        if self.config.transfer.partition_column:
            params['column'] = self.config.transfer.partition_column
            
            if self.config.transfer.lower_bound is not None:
                params['lower_bound'] = self.config.transfer.lower_bound
            
            if self.config.transfer.upper_bound is not None:
                params['upper_bound'] = self.config.transfer.upper_bound
            
            if self.config.transfer.num_partitions:
                params['num_partitions'] = self.config.transfer.num_partitions
        
        logger.debug(f"DB-API parameters: {params}")
        return params
    
    def _save_transfer_metadata(self, source_table: str, dest_table: str, row_count: int):
        """Save transfer metadata to file."""
        try:
            source_info = {
                'database_type': self.config.database_type.value,
                'host': self.config.source_db.host,
                'database': self.config.source_db.database,
                'table': source_table
            }
            
            destination_info = {
                'account': self.config.snowflake.account,
                'database': self.config.snowflake.database,
                'schema': self.config.snowflake.schema,
                'table': dest_table
            }
            
            metadata_file = f"transfer_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            save_transfer_metadata(source_info, destination_info, self.transfer_stats, metadata_file)
            logger.info(f"Transfer metadata saved to: {metadata_file}")
            
        except Exception as e:
            logger.warning(f"Failed to save transfer metadata: {e}")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a source table.
        
        Args:
            table_name: Name of the table to inspect
            
        Returns:
            Dictionary with table information
        """
        try:
            conn = self.connection_factory()
            cursor = conn.cursor()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            # Get column information (this will vary by database type)
            if self.config.database_type == DatabaseType.SQLSERVER:
                cursor.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table_name}'
                """)
            elif self.config.database_type == DatabaseType.POSTGRESQL:
                cursor.execute(f"""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
            else:
                # Generic approach
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
                columns = [desc[0] for desc in cursor.description]
                column_info = [{'name': col, 'type': 'unknown'} for col in columns]
            
            if self.config.database_type in [DatabaseType.SQLSERVER, DatabaseType.POSTGRESQL]:
                column_info = []
                for col in cursor.fetchall():
                    column_info.append({
                        'name': col[0],
                        'type': col[1],
                        'max_length': col[2]
                    })
            
            cursor.close()
            conn.close()
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'columns': column_info
            }
            
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {}
    
    def list_tables(self) -> List[str]:
        """List all tables in the source database.
        
        Returns:
            List of table names
        """
        try:
            conn = self.connection_factory()
            cursor = conn.cursor()
            
            if self.config.database_type == DatabaseType.SQLSERVER:
                cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
            elif self.config.database_type == DatabaseType.POSTGRESQL:
                cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            elif self.config.database_type == DatabaseType.MYSQL:
                cursor.execute("SHOW TABLES")
            else:
                logger.warning(f"Table listing not implemented for {self.config.database_type}")
                return []
            
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            return tables
            
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def cleanup(self):
        """Clean up resources."""
        if self.session:
            try:
                self.session.close()
                logger.info("Snowflake session closed")
            except Exception as e:
                logger.warning(f"Error closing Snowflake session: {e}")

def create_snowflake_session(config) -> Session:
    """Create a Snowflake session with the given configuration.
    
    Args:
        config: Snowflake configuration
        
    Returns:
        Snowflake Session object
    """
    connection_params = {
        "account": config.account,
        "user": config.user,
        "password": config.password,
        "role": config.role,
        "warehouse": config.warehouse,
        "database": config.database,
        "schema": config.schema
    }
    
    session = Session.builder.configs(connection_params).create()
    
    # Ensure database and schema exist
    session.sql(f"CREATE DATABASE IF NOT EXISTS {config.database}").collect()
    session.sql(f"USE DATABASE {config.database}").collect()
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema}").collect()
    session.sql(f"USE SCHEMA {config.schema}").collect()
    
    logger.info(f"Snowflake session created: {config.account}.{config.database}.{config.schema}")
    return session

def transfer_data(config: AppConfig, query: Optional[str] = None) -> bool:
    """Main function to transfer data from source database to Snowflake.
    
    Args:
        config: Application configuration
        query: Optional custom SQL query
        
    Returns:
        True if transfer is successful, False otherwise
    """
    # Enable multiprocessing support (required for macOS/Windows)
    freeze_support()
    
    transfer = DataTransfer(config)
    
    try:
        print_banner("SNOWPARK DB-API DATA TRANSFER")
        
        # Setup connections
        if not transfer.setup_connections():
            logger.error("Failed to setup database connections")
            return False
        
        # Execute transfer
        success = transfer.transfer_table(query)
        
        # Print summary
        if success:
            duration = (transfer.transfer_stats['end_time'] - transfer.transfer_stats['start_time']).total_seconds()
            from .utils import print_summary
            print_summary(
                f"{config.source_db.host}.{config.source_db.database}",
                config.transfer.source_table,
                f"{config.snowflake.database}.{config.snowflake.schema}.{config.transfer.destination_table}",
                transfer.transfer_stats['rows_transferred'],
                duration,
                transfer.transfer_stats['errors']
            )
        
        return success
        
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        logger.debug(traceback.format_exc())
        return False
    finally:
        transfer.cleanup()
