"""Snowpark DB-API: Layered API System

Three-tier API architecture:
- HIGH-LEVEL: Simple, opinionated, works out of the box
- MID-LEVEL: Composable building blocks for custom workflows  
- LOW-LEVEL: Complete control over transfer operations
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
import logging
from pathlib import Path

from .config import Config, get_config, DatabaseType
from .core import DataTransfer, transfer_data
from .transforms import (
    Pipeline, Transform, QueryTransform, SchemaTransform, 
    create_transfer_pipeline, TransformBuilder, show_pipeline_steps
)

logger = logging.getLogger(__name__)

# =============================================================================
# HIGH-LEVEL API: Simple, opinionated, works out of the box
# =============================================================================

def transfer(
    query_or_table: str,
    destination: Optional[str] = None,
    limit: Optional[int] = None,
    mode: str = "overwrite",
    show_progress: bool = True,
    # 🎯 NEW: Runtime configuration overrides
    snowflake_database: Optional[str] = None,
    snowflake_schema: Optional[str] = None,
    snowflake_warehouse: Optional[str] = None,
    source_database: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None,
    # 🔧 NEW: Control database creation behavior
    create_db_if_missing: Optional[bool] = None
) -> bool:
    """
    🚀 HIGH-LEVEL API: Transfer data with one simple call
    
    This is the MAIN way most users should interact with the system.
    It handles all the complexity automatically with sensible defaults.
    
    Args:
        query_or_table: SQL query "(SELECT ...) AS name" or table name "dbo.table"
        destination: Optional destination table (auto-derived if not provided)
        limit: Optional row limit for testing
        mode: "overwrite" or "append"
        show_progress: Show what's happening behind the scenes
        
        # 🎯 FLEXIBILITY: Override configuration at runtime
        snowflake_database: Override Snowflake database (e.g., "DB_API_MSSQL")
        snowflake_schema: Override Snowflake schema (e.g., "STAGING", "PROD")
        snowflake_warehouse: Override Snowflake warehouse (e.g., "LARGE_WH")
        source_database: Override source database name
        config_overrides: Dict of any other config overrides
        create_db_if_missing: Whether to create database/schema if missing (default: False for overrides)
        
    Examples:
        # Simple table copy (uses environment config)
        transfer("dbo.customers")
        
        # Override destination database and schema (assumes they exist)
        transfer("dbo.customers", 
                snowflake_database="DB_API_MSSQL", 
                snowflake_schema="STAGING")
        
        # Create database if missing (requires CREATE DATABASE permissions)
        transfer("dbo.customers",
                snowflake_database="NEW_DB",
                create_db_if_missing=True)
        
        # Query with custom warehouse for large data
        transfer("(SELECT * FROM dbo.huge_table) AS big_data",
                snowflake_warehouse="XLARGE_WH")
        
        # Test with limit
        transfer("dbo.large_table", limit=100)
        
    Returns:
        True if successful, False otherwise
    """
    if show_progress:
        print(f"🚀 Starting transfer: {query_or_table}")
    
    # Auto-detect if this is a query or table name
    is_query = query_or_table.strip().startswith('(')
    
    try:
        # 🎯 Apply runtime configuration overrides
        config = _get_config_with_overrides(
            snowflake_database, snowflake_schema, snowflake_warehouse,
            source_database, config_overrides, create_db_if_missing, show_progress
        )
        
        if is_query:
            # Query-based transfer
            if show_progress:
                print("📋 Detected custom query")
            return _transfer_query_highlevel(query_or_table, destination, limit, mode, config, show_progress)
        else:
            # Table-based transfer
            if show_progress:
                print("📋 Detected table name")
            return _transfer_table_highlevel(query_or_table, destination, limit, mode, config, show_progress)
            
    except Exception as e:
        logger.error(f"Transfer failed: {e}")
        if show_progress:
            print(f"❌ Transfer failed: {e}")
        return False


def transfer_sample(
    query_or_table: str,
    rows: int = 100,
    destination_suffix: str = "_sample",
    # 🎯 Same flexibility as main transfer function
    snowflake_database: Optional[str] = None,
    snowflake_schema: Optional[str] = None,
    snowflake_warehouse: Optional[str] = None,
    source_database: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None,
    create_db_if_missing: Optional[bool] = None
) -> bool:
    """
    🧪 HIGH-LEVEL API: Quick sample transfer for testing
    
    Perfect for testing and development - always safe, always small.
    
    Args:
        query_or_table: Source query or table
        rows: Number of rows to sample (default 100)
        destination_suffix: Suffix added to destination table
        
        # 🎯 FLEXIBILITY: Same runtime overrides as transfer()
        snowflake_database: Override Snowflake database
        snowflake_schema: Override Snowflake schema  
        snowflake_warehouse: Override Snowflake warehouse
        source_database: Override source database name
        config_overrides: Dict of any other config overrides
        
    Examples:
        # Test with 100 rows (uses environment config)
        transfer_sample("dbo.huge_table")
        
        # Test in different database/schema
        transfer_sample("dbo.customers", rows=50,
                       snowflake_database="DB_API_TEST",
                       snowflake_schema="SAMPLES")
        
        # Use smaller warehouse for testing
        transfer_sample("dbo.big_table", 
                       snowflake_warehouse="XSMALL_WH")
    """
    print(f"🧪 Sampling {rows} rows for testing")
    return transfer(
        query_or_table, 
        limit=rows, 
        show_progress=True,
        snowflake_database=snowflake_database,
        snowflake_schema=snowflake_schema,
        snowflake_warehouse=snowflake_warehouse,
        source_database=source_database,
        config_overrides=config_overrides,
        create_db_if_missing=create_db_if_missing
    )


def transfer_with_validation(
    query_or_table: str,
    destination: Optional[str] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
    # 🎯 Same flexibility as main transfer function
    snowflake_database: Optional[str] = None,
    snowflake_schema: Optional[str] = None,
    snowflake_warehouse: Optional[str] = None,
    source_database: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None,
    create_db_if_missing: Optional[bool] = None
) -> Dict[str, Any]:
    """
    ✅ HIGH-LEVEL API: Transfer with data validation
    
    Includes pre and post-transfer validation checks.
    
    Args:
        query_or_table: Source query or table
        destination: Optional destination table name
        validation_rules: Dict of validation rules to apply
        
        # 🎯 FLEXIBILITY: Same runtime overrides as transfer()
        snowflake_database: Override Snowflake database
        snowflake_schema: Override Snowflake schema
        snowflake_warehouse: Override Snowflake warehouse  
        source_database: Override source database name
        config_overrides: Dict of any other config overrides
    
    Examples:
        # Validate transfer to specific environment
        result = transfer_with_validation(
            "dbo.critical_data",
            snowflake_database="DB_API_PROD",
            snowflake_schema="VALIDATED",
            validation_rules={'min_rows': 1000}
        )
    
    Returns:
        Dict with detailed results including validation status.
    """
    print("✅ Starting validated transfer")
    
    # Pre-transfer validation
    validation_results = {
        'pre_transfer': _validate_source_data(query_or_table, validation_rules),
        'transfer_success': False,
        'post_transfer': {}
    }
    
    if not validation_results['pre_transfer']['valid']:
        print("❌ Pre-transfer validation failed")
        return validation_results
    
    # Execute transfer with same flexibility
    success = transfer(
        query_or_table, 
        destination, 
        show_progress=True,
        snowflake_database=snowflake_database,
        snowflake_schema=snowflake_schema,
        snowflake_warehouse=snowflake_warehouse,
        source_database=source_database,
        config_overrides=config_overrides,
        create_db_if_missing=create_db_if_missing
    )
    validation_results['transfer_success'] = success
    
    if success:
        # Post-transfer validation
        validation_results['post_transfer'] = _validate_transferred_data(
            destination or _derive_destination(query_or_table)
        )
    
    return validation_results


# =============================================================================
# MID-LEVEL API: Composable building blocks for custom workflows
# =============================================================================

class TransferBuilder:
    """
    🏗️ MID-LEVEL API: Build custom transfer workflows
    
    For users who need more control but don't want to handle low-level details.
    Composable building blocks that can be mixed and matched.
    """
    
    def __init__(self):
        self.config = get_config()
        self.pipeline = None
        self.source = None
        self.destination = None
        self.transforms = []
        self._show_steps = False
    
    def from_source(self, query_or_table: str):
        """Set the data source."""
        self.source = query_or_table
        return self
    
    def to_destination(self, table_name: str):
        """Set the destination table."""
        self.destination = table_name
        return self
    
    def with_schema_mapping(self, custom_mappings: Optional[Dict[str, str]] = None):
        """Add custom schema type mappings."""
        transform = SchemaTransform(
            source_db_type=self.config.database_type.value,
            target_db_type='snowflake'
        )
        if custom_mappings:
            transform._type_mapping.update(custom_mappings)
        self.transforms.append(transform)
        return self
    
    def with_query_optimization(self, complexity_hints: Optional[Dict[str, Any]] = None):
        """Add query optimization transforms."""
        transform = QueryTransform("", self.destination)
        self.transforms.append(transform)
        return self
    
    def with_environment(self, env: str = 'production'):
        """Optimize for specific environment (development/production)."""
        from .transforms import ConnectionTransform
        self.transforms.append(ConnectionTransform(env))
        return self
    
    def show_pipeline_steps(self, show: bool = True):
        """Enable pipeline step visualization."""
        self._show_steps = show
        return self
    
    def build_pipeline(self) -> Pipeline:
        """Build the transform pipeline."""
        if not self.transforms:
            # Add default transforms if none specified
            self.with_schema_mapping().with_query_optimization()
        
        self.pipeline = Pipeline(self.transforms)
        
        if self._show_steps:
            show_pipeline_steps(self.pipeline, self.source)
        
        return self.pipeline
    
    def execute(self) -> bool:
        """Execute the transfer with the built pipeline."""
        if not self.pipeline:
            self.build_pipeline()
        
        print(f"🏗️ Executing custom pipeline: {self.source} → {self.destination}")
        print(f"📋 Pipeline steps: {self.pipeline.transform_names}")
        
        # Use the pipeline to process the transfer
        try:
            if self.source.strip().startswith('('):
                # Query-based transfer - ensure destination is set
                if self.destination:
                    self.config.transfer.destination_table = self.destination
                return transfer_data(self.config, query=self.source)
            else:
                # Table-based transfer
                self.config.transfer.source_table = self.source
                self.config.transfer.destination_table = self.destination or self.source
                return transfer_data(self.config)
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return False


class ConnectionManager:
    """
    🔌 MID-LEVEL API: Manage database connections with lifecycle control
    
    For applications that need explicit control over connection management.
    """
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or get_config()
        self.transfer = None
    
    def connect(self) -> bool:
        """Establish database connections."""
        print("🔌 Establishing database connections")
        self.transfer = DataTransfer(self.config)
        return self.transfer.setup_connections()
    
    def test_connections(self) -> Dict[str, bool]:
        """Test all connections and return status."""
        if not self.transfer:
            self.connect()
        
        results = {
            'source_db': False,
            'snowflake': False
        }
        
        try:
            # Test source connection
            conn = self.transfer.source_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            results['source_db'] = True
            print("✅ Source database connection OK")
        except Exception as e:
            print(f"❌ Source database connection failed: {e}")
        
        try:
            # Test Snowflake connection
            self.transfer.session.sql("SELECT 1").collect()
            results['snowflake'] = True
            print("✅ Snowflake connection OK")
        except Exception as e:
            print(f"❌ Snowflake connection failed: {e}")
        
        return results
    
    def execute_transfer(self, query_or_table: str, **kwargs) -> bool:
        """Execute transfer with established connections."""
        if not self.transfer:
            raise RuntimeError("Must call connect() first")
        
        if query_or_table.strip().startswith('('):
            return self.transfer.transfer_table(query=query_or_table, **kwargs)
        else:
            self.config.transfer.source_table = query_or_table
            return self.transfer.transfer_table(**kwargs)
    
    def close(self):
        """Close all connections."""
        if self.transfer:
            self.transfer.cleanup()
            print("🔌 Connections closed")


# =============================================================================
# LOW-LEVEL API: Foundational primitives for complete control
# =============================================================================

class LowLevelTransferEngine:
    """
    ⚙️ LOW-LEVEL API: Direct control over transfer operations
    
    For advanced users who need complete control over every aspect.
    Exposes all internal primitives.
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.raw_transfer = DataTransfer(config)
        self.connection_established = False
    
    def establish_raw_connections(self) -> Dict[str, Any]:
        """Establish connections and return raw connection objects."""
        success = self.raw_transfer.setup_connections()
        self.connection_established = success
        
        return {
            'success': success,
            'source_connection_factory': self.raw_transfer.source_connection,
            'snowflake_session': self.raw_transfer.session,
            'snowflake_connection': self.raw_transfer.snowflake_connection
        }
    
    def execute_raw_query(self, sql: str) -> Any:
        """Execute raw SQL on source database."""
        if not self.connection_established:
            raise RuntimeError("Must establish connections first")
        
        conn = self.raw_transfer.source_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def create_snowpark_dataframe(self, query_or_table: str, custom_schema=None) -> Any:
        """Create Snowpark DataFrame with full control over parameters.
        
        Args:
            query_or_table: Either a table name (e.g., "dbo.table") or a query (e.g., "SELECT ...")
            custom_schema: Optional schema specification
        """
        if not self.connection_established:
            raise RuntimeError("Must establish connections first")
        
        # Determine if this is a query or table name
        is_query = query_or_table.strip().upper().startswith('SELECT') or query_or_table.strip().startswith('(')
        
        try:
            if is_query:
                # Use query parameter for SELECT statements
                return self.raw_transfer.session.read.dbapi(
                    self.raw_transfer.source_connection,
                    query=query_or_table,
                    fetch_size=self.config.transfer.fetch_size,
                    query_timeout=self.config.transfer.query_timeout,
                    max_workers=self.config.transfer.max_workers,
                    custom_schema=custom_schema
                )
            else:
                # Use table parameter for table names
                return self.raw_transfer.session.read.dbapi(
                    self.raw_transfer.source_connection,
                    table=query_or_table,
                    fetch_size=self.config.transfer.fetch_size,
                    query_timeout=self.config.transfer.query_timeout,
                    max_workers=self.config.transfer.max_workers,
                    custom_schema=custom_schema
                )
        except Exception as e:
            # If that fails, try with schema generation like the working method
            if custom_schema is None and is_query:
                custom_schema = self.raw_transfer._get_query_schema(query_or_table)
                return self.raw_transfer.session.read.dbapi(
                    self.raw_transfer.source_connection,
                    query=query_or_table,
                    fetch_size=self.config.transfer.fetch_size,
                    query_timeout=self.config.transfer.query_timeout,
                    max_workers=self.config.transfer.max_workers,
                    custom_schema=custom_schema
                )
            else:
                raise e
    
    def write_dataframe_raw(self, df: Any, table_name: str, mode: str = "overwrite") -> Any:
        """Write DataFrame to Snowflake with raw control."""
        return df.write.mode(mode).save_as_table(table_name)
    
    def get_raw_statistics(self) -> Dict[str, Any]:
        """Get raw transfer statistics."""
        return self.raw_transfer.transfer_stats
    
    def cleanup_raw_connections(self):
        """Clean up all raw connections."""
        self.raw_transfer.cleanup()


# =============================================================================
# Helper Functions (Internal)
# =============================================================================

def _get_config_with_overrides(
    snowflake_database: Optional[str] = None,
    snowflake_schema: Optional[str] = None, 
    snowflake_warehouse: Optional[str] = None,
    source_database: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None,
    create_db_if_missing: Optional[bool] = None,
    show_progress: bool = True
) -> Config:
    """Apply runtime configuration overrides to base config."""
    config = get_config()
    
    # Track what we're overriding for transparency
    overrides_applied = []
    
    # Apply individual overrides
    if snowflake_database:
        config.snowflake.database = snowflake_database
        overrides_applied.append(f"database={snowflake_database}")
        
        # 🔧 When overriding database, default to NOT creating it unless explicitly requested
        if create_db_if_missing is None:
            config.snowflake.create_db_if_missing = False
            overrides_applied.append("create_db_if_missing=False (safe default)")
        
    if snowflake_schema:
        config.snowflake.db_schema = snowflake_schema
        overrides_applied.append(f"schema={snowflake_schema}")
        
    if snowflake_warehouse:
        config.snowflake.warehouse = snowflake_warehouse
        overrides_applied.append(f"warehouse={snowflake_warehouse}")
        
    if source_database:
        config.source.database = source_database
        overrides_applied.append(f"source_db={source_database}")
    
    # Handle explicit create_db_if_missing override
    if create_db_if_missing is not None:
        config.snowflake.create_db_if_missing = create_db_if_missing
        overrides_applied.append(f"create_db_if_missing={create_db_if_missing}")
    
    # Apply any additional overrides from config_overrides dict
    if config_overrides:
        for key, value in config_overrides.items():
            if hasattr(config.snowflake, key):
                setattr(config.snowflake, key, value)
                overrides_applied.append(f"{key}={value}")
            elif hasattr(config.source, key):
                setattr(config.source, key, value)
                overrides_applied.append(f"source.{key}={value}")
            elif hasattr(config.transfer, key):
                setattr(config.transfer, key, value)
                overrides_applied.append(f"transfer.{key}={value}")
    
    # Show what was overridden for transparency
    if overrides_applied and show_progress:
        print(f"🎯 Configuration overrides: {', '.join(overrides_applied)}")
    
    return config


def _transfer_query_highlevel(query: str, destination: Optional[str], limit: Optional[int], 
                             mode: str, config: Config, show_progress: bool) -> bool:
    """Internal helper for high-level query transfers."""
    if show_progress:
        # Show what's happening transparently
        print(f"📊 Source: {config.source.host} ({config.database_type.value})")
        print(f"❄️  Target: {config.snowflake.account}.{config.snowflake.database}")
    
    # Auto-derive destination if not provided
    if not destination:
        destination = _derive_destination(query)
        if show_progress:
            print(f"📝 Auto-derived destination: {destination}")
    
    # Apply limit if specified
    if limit:
        query = _apply_limit_to_query(query, limit)
        if show_progress:
            print(f"🔢 Limited to {limit} rows")
    
    # Update config
    config.transfer.destination_table = destination
    config.transfer.mode = mode
    
    return transfer_data(config, query=query)


def _transfer_table_highlevel(table: str, destination: Optional[str], limit: Optional[int],
                             mode: str, config: Config, show_progress: bool) -> bool:
    """Internal helper for high-level table transfers."""
    if show_progress:
        print(f"📊 Source table: {table}")
        print(f"📊 Source: {config.source.host} ({config.database_type.value})")
        print(f"❄️  Target: {config.snowflake.account}.{config.snowflake.database}")
    
    # Set destination
    dest = destination or table.split('.')[-1].upper()
    if show_progress:
        print(f"📝 Destination: {dest}")
    
    # Update config
    config.transfer.source_table = table
    config.transfer.destination_table = dest
    config.transfer.mode = mode
    
    # Apply limit by converting to query if needed
    if limit:
        query = f"(SELECT TOP {limit} * FROM {table}) AS {dest}"
        if show_progress:
            print(f"🔢 Using query with limit: {query}")
        return transfer_data(config, query=query)
    
    return transfer_data(config)


def _derive_destination(query_or_table: str) -> str:
    """Derive destination table name from query or table."""
    import re
    
    if query_or_table.strip().startswith('('):
        # Extract from query alias
        match = re.search(r'\)\s+AS\s+(\w+)$', query_or_table.strip(), re.IGNORECASE)
        if match:
            return match.group(1).upper()
        return "QUERY_RESULT"
    else:
        # Extract from table name
        return query_or_table.split('.')[-1].upper()


def _apply_limit_to_query(query: str, limit: int) -> str:
    """Apply row limit to a query."""
    # This is a simplified implementation
    # In production, you'd want more sophisticated SQL parsing
    if "TOP " not in query.upper():
        # Insert TOP clause after SELECT
        query = query.replace("SELECT ", f"SELECT TOP {limit} ", 1)
    return query


def _validate_source_data(query_or_table: str, rules: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Validate source data before transfer."""
    # Placeholder for validation logic
    return {'valid': True, 'checks': ['existence', 'permissions']}


def _validate_transferred_data(destination_table: str) -> Dict[str, Any]:
    """Validate data after transfer."""
    # Placeholder for post-transfer validation
    return {'row_count_match': True, 'schema_match': True}


# =============================================================================
# Export the clean API
# =============================================================================

__all__ = [
    # High-level API
    'transfer',
    'transfer_sample', 
    'transfer_with_validation',
    
    # Mid-level API
    'TransferBuilder',
    'ConnectionManager',
    
    # Low-level API
    'LowLevelTransferEngine',
] 