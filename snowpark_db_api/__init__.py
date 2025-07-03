"""Snowpark DB-API: Professional data transfer tool for Snowflake.

Layered Architecture:
- HIGH-LEVEL API: Simple, opinionated, works out of the box
- MID-LEVEL API: Composable building blocks for custom workflows  
- LOW-LEVEL API: Complete control over transfer operations
"""

# =============================================================================
# HIGH-LEVEL API: Simple, works out of the box
# =============================================================================

from .api import (
    transfer,                    # Main transfer function - works like magic
    transfer_sample,            # Quick testing - always safe
    transfer_with_validation,   # Transfer with validation checks
)

# =============================================================================
# MID-LEVEL API: Composable building blocks
# =============================================================================

from .api import (
    TransferBuilder,            # Build custom transfer workflows
    ConnectionManager,          # Explicit connection management
)

from .transforms import (
    Pipeline,                   # Transform pipeline system
    Transform,                  # Base transform class
    QueryTransform,             # Query processing transforms
    SchemaTransform,            # Schema mapping transforms
    create_transfer_pipeline,   # Create complete pipelines
    show_pipeline_steps,        # Pipeline transparency
)

# =============================================================================
# LOW-LEVEL API: Complete control
# =============================================================================

from .api import LowLevelTransferEngine
from .core import DataTransfer, transfer_data, transfer_query, transfer_query_transparent, create_snowflake_session
from .config import Config, DatabaseType, get_config
from .connections import create_connection, test_connection_factory
from .utils import setup_logging, ProgressTracker

# =============================================================================
# Version & Metadata
# =============================================================================

__version__ = "2.0.0"  # Bumped for fast.ai architecture
__author__ = "Snowpark DB-API Team"
__description__ = "Fast.AI Style Layered API for Snowflake Data Transfers"

# =============================================================================
# Clean Public API
# =============================================================================

__all__ = [
    # ===== HIGH-LEVEL API =====
    # These are what 90% of users should use
    "transfer",                     # transfer("dbo.table") or transfer("(SELECT ...) AS name")
    "transfer_sample",              # transfer_sample("dbo.table", rows=100)
    "transfer_with_validation",     # transfer_with_validation("dbo.table")
    
    # ===== MID-LEVEL API =====
    # For users who need more control
    "TransferBuilder",              # Custom workflows: TransferBuilder().from_source().to_destination()
    "ConnectionManager",            # Explicit connection control
    "Pipeline",                     # Transform pipelines
    "Transform",                    # Base transform class
    "QueryTransform",               # Query transforms
    "SchemaTransform",              # Schema transforms
    "create_transfer_pipeline",     # Pipeline factory
    "show_pipeline_steps",          # Pipeline transparency
    
    # ===== LOW-LEVEL API =====
    # For advanced users who need complete control
    "LowLevelTransferEngine",       # Raw transfer operations
    "DataTransfer",                 # Core transfer class
    "transfer_data",                # Core transfer function
    "transfer_query",               # Core transfer function
    "transfer_query_transparent",   # Core transfer function
    "create_snowflake_session",     # Snowflake session creation
    "Config",                       # Configuration class
    "DatabaseType",                 # Database type enum
    "get_config",                   # Configuration factory
    "create_connection",            # Connection factory
    "test_connection_factory",      # Connection testing
    "setup_logging",                # Logging setup
    "ProgressTracker",              # Progress tracking
    
    # ===== METADATA =====
    "__version__",
    "__author__",
    "__description__",
]

# =============================================================================
# Usage Documentation
# =============================================================================

def show_usage_examples():
    """
    Show examples of how to use the layered API.
    
    Run this to see usage patterns for all three API levels.
    """
    print("🚀 Snowpark DB-API Usage Examples")
    print("=" * 50)
    
    print("\n📋 HIGH-LEVEL API (90% of users)")
    print("Simple, opinionated, works out of the box:")
    print()
    print("from snowpark_db_api import transfer, transfer_sample")
    print()
    print("# Simple table copy")
    print('transfer("dbo.customers")')
    print()
    print("# Query with auto-destination")
    print('transfer("(SELECT TOP 1000 * FROM dbo.orders) AS recent_orders")')
    print()
    print("# Quick testing")
    print('transfer_sample("dbo.large_table", rows=100)')
    
    print("\n🏗️ MID-LEVEL API (Composable building blocks)")
    print("For custom workflows:")
    print()
    print("from snowpark_db_api import TransferBuilder, ConnectionManager")
    print()
    print("# Custom pipeline")
    print("builder = TransferBuilder()")
    print('pipeline = (builder.from_source("dbo.sales")')
    print('                 .to_destination("SALES_CLEAN")')
    print('                 .with_schema_mapping()')
    print('                 .with_environment("production")')
    print('                 .show_pipeline_steps()')
    print('                 .execute())')
    print()
    print("# Explicit connection control")
    print("manager = ConnectionManager()")
    print("manager.connect()")
    print("manager.test_connections()")
    print('manager.execute_transfer("dbo.orders")')
    print("manager.close()")
    
    print("\n⚙️ LOW-LEVEL API (Complete control)")
    print("For advanced users:")
    print()
    print("from snowpark_db_api import LowLevelTransferEngine, Config")
    print()
    print("config = Config.from_env()")
    print("engine = LowLevelTransferEngine(config)")
    print("connections = engine.establish_raw_connections()")
    print('df = engine.create_snowpark_dataframe("SELECT * FROM table")')
    print('engine.write_dataframe_raw(df, "target_table")')
    print("stats = engine.get_raw_statistics()")
    print("engine.cleanup_raw_connections()")
    
    print("\n🔍 TRANSPARENCY")
    print("See exactly what's happening:")
    print()
    print("from snowpark_db_api import show_pipeline_steps, create_transfer_pipeline")
    print()
    print('pipeline = create_transfer_pipeline("sqlserver")')
    print('show_pipeline_steps(pipeline, sample_input="dbo.test")')
    
    print("\n💡 Key Benefits:")
    print("✅ No more fake source table names")
    print("✅ Reversible transforms (encode/decode)")
    print("✅ Progressive complexity disclosure")  
    print("✅ Type-aware dispatch system")
    print("✅ Complete transparency")
    print("✅ Composable building blocks")
    
    print(f"\n📖 Version: {__version__}")
    print("Built following Jeremy Howard's Fast.AI principles")


# =============================================================================
# Fast Import Detection
# =============================================================================

def _detect_usage_level():
    """Detect which API level the user is likely to need."""
    import sys
    
    # Check if running in Jupyter
    if 'ipykernel' in sys.modules:
        return "jupyter"
    
    # Check command line usage
    if len(sys.argv) > 1:
        return "cli"
    
    return "python"


# Auto-show usage examples in interactive environments
if __name__ == "__main__":
    show_usage_examples()
