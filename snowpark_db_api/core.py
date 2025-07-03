"""
Core data transfer functionality.
Clean, efficient transfer operations
"""

from typing import Optional, Any, Dict, List
import logging
import traceback
import json
from datetime import datetime
from pathlib import Path
import psutil

from snowflake.snowpark import Session
from snowpark_db_api.config import Config, get_config, DatabaseType
from snowpark_db_api.connections import create_connection
from snowpark_db_api.snowflake_connection import SnowflakeConnection, ConnectionConfig
from snowpark_db_api.utils import ProgressTracker, setup_logging

# Set up logging
logger = logging.getLogger(__name__)

class DataTransfer:
    """Main data transfer class. Simple, clean interface."""
    
    def __init__(self, config: Config):
        """Initialize with configuration."""
        self.config = config
        self.source_connection = None
        self.snowflake_connection = None
        self.session = None
        self.transfer_stats = {}
        
        # Set up logging
        setup_logging(config.log_level)
    
    def setup_connections(self) -> bool:
        """Set up database connections. Returns True if successful."""
        try:
            logger.info("Setting up connections")
            
            # Source database connection - create factory function
            logger.debug("Setting up source database connection factory")
            from snowpark_db_api.connections import (
                create_sqlserver_connection, create_postgresql_connection, 
                create_mysql_connection, create_oracle_connection, create_databricks_connection
            )
            from snowpark_db_api.config import DatabaseType
            
            # Map database types to factory functions
            factory_map = {
                DatabaseType.SQLSERVER: create_sqlserver_connection,
                DatabaseType.POSTGRESQL: create_postgresql_connection,
                DatabaseType.MYSQL: create_mysql_connection,
                DatabaseType.ORACLE: create_oracle_connection,
                DatabaseType.DATABRICKS: create_databricks_connection,
            }
            
            if self.config.database_type not in factory_map:
                logger.error(f"Unsupported database type: {self.config.database_type}")
                return False
                
            # Create connection factory (store the factory, not the connection)
            self.source_connection = factory_map[self.config.database_type](self.config.source)
            logger.debug("Source database factory created")
            
            # Snowflake connection
            logger.debug("Connecting to Snowflake")
            sf_config = ConnectionConfig(
                account=self.config.snowflake.account,
                user=self.config.snowflake.user,
                password=self.config.snowflake.password,
                role=self.config.snowflake.role,
                warehouse=self.config.snowflake.warehouse,
                database=self.config.snowflake.database,
                schema=self.config.snowflake.db_schema,
                create_db_if_missing=self.config.snowflake.create_db_if_missing
            )
            self.snowflake_connection = SnowflakeConnection.from_config(sf_config)
            self.session = self.snowflake_connection.session
            logger.debug("Snowflake connected")
            
            logger.info("All connections established")
            return True
            
        except Exception as e:
            logger.error(f"Connection setup failed: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    def cleanup(self):
        """Clean up connections."""
        try:
            # Note: source_connection is a factory function, not a connection object
            # Individual connections are managed by Snowpark internally
            if self.snowflake_connection:
                self.snowflake_connection.close()
            logger.debug("Connections closed")
        except Exception as e:
            logger.warning(f"Cleanup error: {e}")
    
    def transfer_table(self, query: Optional[str] = None, limit_rows: Optional[int] = None) -> bool:
        """Transfer data from source to Snowflake. Main transfer method."""
        start_time = datetime.now()
        source_table = self.config.transfer.source_table
        dest_table = self.config.transfer.destination_table
        
        if query:
            logger.info(f"Starting transfer using custom query -> {dest_table}")
            logger.debug(f"Query: {query}")
        else:
            logger.info(f"Starting transfer: {source_table} -> {dest_table}")
        
        try:
            logger.debug("Executing data transfer using Snowpark DB-API")
            
            # Determine approach: table-based or query-based
            if query:
                logger.debug(f"Using custom query approach")
                df = self._execute_query_transfer(query)
            else:
                logger.debug(f"Using table-based approach for {source_table}")
                df = self._execute_table_transfer(source_table)
            
            # Apply row limit if specified
            if limit_rows:
                logger.debug(f"Limiting to {limit_rows} rows")
                df = df.limit(limit_rows)
            
            # Get row count and memory usage
            mem_before = self._get_memory_usage()
            row_count = df.count()
            mem_after = self._get_memory_usage()
            
            logger.info(f"Transferring {row_count:,} rows")
            logger.debug(f"Memory for count: {mem_after - mem_before:.1f}MB")
            
            # Write to Snowflake
            logger.debug(f"Writing to table: {dest_table}")
            mem_before_write = self._get_memory_usage()
            df.write.mode(self.config.transfer.mode).save_as_table(dest_table)
            mem_after_write = self._get_memory_usage()
            
            # Record transfer statistics
            end_time = datetime.now()
            self.transfer_stats = {
                'start_time': start_time,
                'end_time': end_time,
                'rows_transferred': row_count,
                'errors': 0,
                'warnings': 0,
                'memory_used_mb': mem_after_write - mem_before,
                'duration_seconds': (end_time - start_time).total_seconds()
            }
            
            # Save metadata
            self._save_transfer_metadata()
            
            logger.info(f"Transfer completed: {row_count:,} rows in {self.transfer_stats['duration_seconds']:.1f}s")
            return True
            
        except Exception as e:
            logger.error(f"Transfer failed: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    def _execute_table_transfer(self, table_name: str):
        """Execute table-based transfer."""
        try:
            df = self.session.read.dbapi(
                self.source_connection,
                table=table_name,
                fetch_size=self.config.transfer.fetch_size,
                query_timeout=self.config.transfer.query_timeout,
                max_workers=self.config.transfer.max_workers
            )
            logger.debug("Table-based transfer executed")
            return df
        except Exception as e:
            logger.error(f"Table transfer failed: {e}")
            raise
    
    def _execute_query_transfer(self, query: str):
        """Execute query-based transfer."""
        try:
            # Try without custom schema first
            df = self.session.read.dbapi(
                self.source_connection,
                query=query,
                fetch_size=self.config.transfer.fetch_size,
                query_timeout=self.config.transfer.query_timeout,
                max_workers=self.config.transfer.max_workers
            )
            logger.debug("Query executed successfully")
            return df
            
        except Exception as query_error:
            logger.debug(f"Query failed without schema: {query_error}")
            logger.debug("Trying with schema generation")
            
            try:
                # Generate custom schema and retry
                custom_schema = self._get_query_schema(query)
                logger.debug(f"Generated schema with {len(custom_schema.fields)} columns")
                
                df = self.session.read.dbapi(
                    self.source_connection,
                    query=query,
                    fetch_size=self.config.transfer.fetch_size,
                    query_timeout=self.config.transfer.query_timeout,
                    max_workers=self.config.transfer.max_workers,
                    custom_schema=custom_schema
                )
                logger.debug("Query with schema executed successfully")
                return df
                
            except Exception as schema_error:
                logger.error(f"Query transfer failed: {schema_error}")
                raise Exception(f"Both approaches failed: {query_error}, {schema_error}")
    
    def _get_query_schema(self, query: str):
        """Generate schema for custom query."""
        # Extract inner SELECT for schema detection
        inner_query = self._extract_inner_query(query)
        schema_query = f"SELECT TOP 0 * FROM ({inner_query}) AS schema_detection"
        
        logger.debug(f"Schema detection query: {schema_query}")
        
        # Execute schema query to get column info - create connection from factory
        conn = self.source_connection()  # Call the factory function to get actual connection
        cursor = conn.cursor()
        cursor.execute(schema_query)
        
        # Get column information
        columns = cursor.description
        
        # Map to Snowpark types
        from snowflake.snowpark.types import StructType, StructField
        
        fields = []
        for col in columns:
            col_name = col[0]
            sql_type_code = col[1]
            snowpark_type = self._map_sql_type_to_snowpark(sql_type_code)
            fields.append(StructField(col_name, snowpark_type))
        
        cursor.close()
        conn.close()  # Close the connection we created
        return StructType(fields)
    
    def _extract_inner_query(self, query: str) -> str:
        """Extract inner SELECT from query pattern."""
        query = query.strip()
        
        # Handle (SELECT ... ) AS alias pattern
        if query.startswith('(') and ')' in query:
            paren_pos = query.rfind(')')
            inner_query = query[1:paren_pos].strip()
            logger.debug(f"Extracted inner query: {inner_query}")
            return inner_query
        
        return query
    
    def _map_sql_type_to_snowpark(self, sql_type_code: int):
        """Map SQL type codes to Snowpark types."""
        from snowflake.snowpark.types import (
            StringType, IntegerType, DoubleType, BooleanType, 
            DateType, TimestampType, DecimalType
        )
        
        # SQL type code mappings (simplified)
        type_mapping = {
        # String types
            1: StringType(),    # CHAR
            12: StringType(),   # VARCHAR
            -1: StringType(),   # LONGVARCHAR
            -9: StringType(),   # NVARCHAR
            
            # Numeric types
            4: IntegerType(),   # INTEGER
            -5: IntegerType(),  # BIGINT
            5: IntegerType(),   # SMALLINT
            -6: IntegerType(),  # TINYINT
            6: DoubleType(),    # FLOAT
            8: DoubleType(),    # DOUBLE
            2: DecimalType(18, 2),  # NUMERIC
            3: DecimalType(18, 2),  # DECIMAL
            
            # Date/Time types
            91: DateType(),     # DATE
            93: TimestampType(), # TIMESTAMP
        
        # Boolean
            16: BooleanType(),  # BOOLEAN
        }
        
        snowpark_type = type_mapping.get(sql_type_code, StringType())
        
        if sql_type_code not in type_mapping:
            logger.debug(f"Unknown SQL type {sql_type_code}, using StringType")
            
        return snowpark_type
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    def _save_transfer_metadata(self):
        """Save transfer metadata to file - only if enabled in config."""
        if not self.config.transfer.save_metadata:
            logger.debug("Metadata saving disabled, skipping")
            return
            
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata_file = f"transfer_metadata_{timestamp}.json"
            
            metadata = {
                'source_table': self.config.transfer.source_table,
                'destination_table': self.config.transfer.destination_table,
                'transfer_stats': {
                    'start_time': self.transfer_stats['start_time'].isoformat(),
                    'end_time': self.transfer_stats['end_time'].isoformat(),
                    'rows_transferred': self.transfer_stats['rows_transferred'],
                    'duration_seconds': self.transfer_stats['duration_seconds'],
                    'memory_used_mb': self.transfer_stats['memory_used_mb']
                },
                'config': {
                    'database_type': self.config.database_type,
                    'mode': self.config.transfer.mode,
                    'fetch_size': self.config.transfer.fetch_size,
                    'max_workers': self.config.transfer.max_workers
                }
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            logger.info(f"Transfer metadata saved to: {metadata_file}")
            
        except Exception as e:
            logger.warning(f"Failed to save transfer metadata: {e}")
    
# Simple transfer function for easy use
def transfer_data(config: Config, query: Optional[str] = None) -> bool:
    """Simple function to transfer data. Main entry point."""
    # If using a query but destination not set, auto-derive it
    if query and not config.transfer.destination_table:
        import re
        match = re.search(r'\)\s+AS\s+(\w+)$', query.strip(), re.IGNORECASE)
        if match:
            config.transfer.destination_table = match.group(1).upper()
        else:
            config.transfer.destination_table = "QUERY_RESULT"
    
    transfer = DataTransfer(config)
    try:
        if not transfer.setup_connections():
            return False
        return transfer.transfer_table(query=query)
    finally:
        transfer.cleanup()

def transfer_query(query: str, destination_table: Optional[str] = None, config: Optional[Config] = None) -> bool:
    """
    Transfer data using a custom query. Much cleaner API for query-based transfers.
    
    Args:
        query: SQL query to execute (should include AS alias for auto-destination)
        destination_table: Optional destination table name (auto-derived if not provided)
        config: Optional config (uses get_config() if not provided)
        
    Examples:
        # Auto-derive destination from query alias
        transfer_query("(SELECT TOP 100 * FROM dbo.LargeTable) AS sample_data")
        
        # Explicit destination  
        transfer_query("SELECT * FROM dbo.Orders", destination_table="ORDERS_COPY")
    """
    import re
    
    # Use provided config or get from environment
    if config is None:
        config = get_config()
    
    # Auto-derive destination table if not provided
    if destination_table is None:
        match = re.search(r'\)\s+AS\s+(\w+)$', query.strip(), re.IGNORECASE)
        if match:
            destination_table = match.group(1).upper()
        else:
            destination_table = "QUERY_RESULT"
    
    # Create a temporary transfer config for the query
    from snowpark_db_api.config import TransferConfig
    config.transfer = TransferConfig(
        source_table=f"<query: {query[:50]}...>",  # Descriptive name for logging
        destination_table=destination_table,
        mode=config.transfer.mode,
        fetch_size=config.transfer.fetch_size,
        query_timeout=config.transfer.query_timeout,
        max_workers=config.transfer.max_workers,
        save_metadata=config.transfer.save_metadata
    )
    
    return transfer_data(config, query=query)

def transfer_query_transparent(query: str, destination_table: Optional[str] = None, 
                              config: Optional[Config] = None, verbose: bool = True) -> bool:
    """Transfer data using a custom query with optional verbose output.
    
    Args:
        query: SQL query to execute (should include AS alias for auto-destination)
        destination_table: Optional destination table name (auto-derived if not provided)
        config: Optional config (uses get_config() if not provided)
        verbose: Show what's happening behind the scenes
    """
    import re
    
    # Use provided config or get from environment
    if config is None:
        if verbose:
            print("No config provided - reading from environment variables")
        config = get_config()
    
    if verbose:
        print(f"Source Database: {config.source.host} ({config.database_type.value})")
        print(f"Snowflake: {config.snowflake.account}.{config.snowflake.database}.{config.snowflake.db_schema}")
        print(f"User: {config.snowflake.user}")
    
    # Auto-derive destination table if not provided
    if destination_table is None:
        match = re.search(r'\)\s+AS\s+(\w+)$', query.strip(), re.IGNORECASE)
        if match:
            destination_table = match.group(1).upper()
            if verbose:
                print(f"Auto-derived destination: {destination_table}")
        else:
            destination_table = "QUERY_RESULT"
            if verbose:
                print(f"Default destination: {destination_table}")
    elif verbose:
        print(f"Explicit destination: {destination_table}")
    
    if verbose:
        print(f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
        print()
    
    # Create a temporary transfer config for the query
    from snowpark_db_api.config import TransferConfig
    config.transfer = TransferConfig(
        source_table=f"<query: {query[:50]}...>",
        destination_table=destination_table,
        mode=config.transfer.mode,
        fetch_size=config.transfer.fetch_size,
        query_timeout=config.transfer.query_timeout,
        max_workers=config.transfer.max_workers,
        save_metadata=config.transfer.save_metadata
    )
    
    return transfer_data(config, query=query)

# Compatibility with existing code
def create_snowflake_session(snowflake_config) -> Session:
    """Create Snowflake session. For compatibility."""
    try:
        sf_config = ConnectionConfig(
            account=snowflake_config.account,
            user=snowflake_config.user,
            password=snowflake_config.password,
            role=snowflake_config.role,
            warehouse=snowflake_config.warehouse,
            database=snowflake_config.database,
            schema=snowflake_config.db_schema
        )
        connection = SnowflakeConnection.from_config(sf_config)
        return connection.session
    except Exception as e:
        logger.error(f"Failed to create Snowflake session: {e}")
        raise

# Enhanced Snowflake exploration functions
def query_snowflake_table(config: Config, table_name: str, limit: int = 10):
    """Enhanced helper to query Snowflake table. Returns Snowpark DataFrame with more info."""
    from snowpark_db_api.snowflake_connection import ConnectionConfig
    
    try:
        # Create Snowflake connection
        sf_config = ConnectionConfig(
            account=config.snowflake.account,
            user=config.snowflake.user,
            password=config.snowflake.password,
            role=config.snowflake.role,
            warehouse=config.snowflake.warehouse,
            database=config.snowflake.database,
            schema=config.snowflake.db_schema,
            create_db_if_missing=config.snowflake.create_db_if_missing
        )
        
        with SnowflakeConnection.from_config(sf_config) as sf_conn:
            logger.info(f"Connected to {sf_conn}")
            
            # Use the improved sql() method
            df = sf_conn.sql(f"SELECT * FROM {table_name} LIMIT {limit}")
            row_count = df.count()
            logger.info(f"Query returned {row_count} rows from {table_name}")
            
            return df
                
    except Exception as e:
        logger.error(f"Failed to query Snowflake table {table_name}: {e}")
        raise

def explore_snowflake_connection(config: Config):
    """Create a Snowflake connection for interactive exploration."""
    from snowpark_db_api.snowflake_connection import ConnectionConfig
    
    sf_config = ConnectionConfig(
        account=config.snowflake.account,
        user=config.snowflake.user,
        password=config.snowflake.password,
        role=config.snowflake.role,
        warehouse=config.snowflake.warehouse,
        database=config.snowflake.database,
        schema=config.snowflake.db_schema,
        create_db_if_missing=config.snowflake.create_db_if_missing
    )
    
    return SnowflakeConnection.from_config(sf_config)
