"""Configuration management for Snowpark DB API transfers.

This module provides a clean, fastai-style configuration system with:
- Environment variable support
- YAML/JSON configuration files
- Sensible defaults
- Validation and error handling
"""

from typing import Optional, Dict, Any, Union
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum
import os
import yaml
import json
from urllib.parse import quote_plus

class DatabaseType(str, Enum):
    """Supported source database types."""
    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    DATABRICKS = "databricks"

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str = ""
    port: int = 0
    username: str = ""
    password: str = ""
    database: str = ""
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not all([self.host, self.username, self.password]):
            raise ValueError("Host, username, and password are required")

@dataclass
class SqlServerConfig(DatabaseConfig):
    """SQL Server specific configuration."""
    port: int = 1433
    driver: str = "ODBC Driver 18 for SQL Server"
    trust_server_certificate: bool = True
    
@dataclass
class PostgreSQLConfig(DatabaseConfig):
    """PostgreSQL specific configuration."""
    port: int = 5432
    
@dataclass
class MySQLConfig(DatabaseConfig):
    """MySQL specific configuration."""
    port: int = 3306
    
@dataclass
class OracleConfig(DatabaseConfig):
    """Oracle specific configuration."""
    port: int = 1521
    service_name: str = ""
    
@dataclass
class DatabricksConfig:
    """Databricks specific configuration."""
    server_hostname: str = ""
    http_path: str = ""
    access_token: str = ""
    
    def __post_init__(self):
        if not all([self.server_hostname, self.http_path, self.access_token]):
            raise ValueError("Server hostname, HTTP path, and access token are required")

@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""
    account: str = ""
    user: str = ""
    password: str = ""
    role: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = "PUBLIC"
    
    def __post_init__(self):
        if not all([self.account, self.user, self.password, self.warehouse]):
            raise ValueError("Account, user, password, and warehouse are required")

@dataclass
class TransferConfig:
    """Transfer operation configuration."""
    source_table: str = ""
    destination_table: str = ""
    batch_size: int = 10000
    max_workers: int = 4
    fetch_size: int = 1000
    query_timeout: int = 300
    mode: str = "overwrite"  # overwrite, append, or error
    
    # Partitioning options for large datasets
    partition_column: Optional[str] = None
    lower_bound: Optional[Union[int, str]] = None
    upper_bound: Optional[Union[int, str]] = None
    num_partitions: Optional[int] = None
    
    def __post_init__(self):
        if not self.source_table:
            raise ValueError("Source table is required")
        if not self.destination_table:
            self.destination_table = self.source_table
        if self.mode not in ["overwrite", "append", "error"]:
            raise ValueError("Mode must be 'overwrite', 'append', or 'error'")

@dataclass
class AppConfig:
    """Main application configuration."""
    database_type: DatabaseType = DatabaseType.SQLSERVER
    source_db: DatabaseConfig = field(default_factory=DatabaseConfig)
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig) 
    transfer: TransferConfig = field(default_factory=TransferConfig)
    
    # Logging configuration
    log_level: str = "INFO"
    log_file: Optional[str] = None
    
    # Docker/container configuration
    config_file: Optional[str] = None

class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self):
        self.config: Optional[AppConfig] = None
        
    def load_config(self, config_file: Optional[str] = None) -> AppConfig:
        """Load configuration from environment variables and optional config file."""
        # Start with defaults
        config_data = {}
        
        # Load from file if provided
        if config_file:
            config_data = self._load_config_file(config_file)
        
        # Override with environment variables
        env_config = self._load_from_env()
        config_data.update(env_config)
        
        # Create configuration objects
        self.config = self._create_config(config_data)
        return self.config
    
    def _load_config_file(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file."""
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                return yaml.safe_load(f) or {}
            elif config_path.suffix.lower() == '.json':
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {config_path.suffix}")
    
    def _load_from_env(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}
        
        # Database type
        if db_type := os.getenv('DB_TYPE'):
            config['database_type'] = db_type
        
        # Source database configuration
        source_config = {}
        for key, env_var in [
            ('host', 'SOURCE_HOST'),
            ('port', 'SOURCE_PORT'),
            ('username', 'SOURCE_USERNAME'),
            ('password', 'SOURCE_PASSWORD'),
            ('database', 'SOURCE_DATABASE'),
            ('driver', 'SQLSERVER_DRIVER'),
            ('service_name', 'ORACLE_SERVICE_NAME'),
            ('server_hostname', 'DATABRICKS_SERVER_HOSTNAME'),
            ('http_path', 'DATABRICKS_HTTP_PATH'),
            ('access_token', 'DATABRICKS_ACCESS_TOKEN')
        ]:
            if value := os.getenv(env_var):
                if key == 'port':
                    source_config[key] = int(value)
                elif key == 'trust_server_certificate':
                    source_config[key] = value.lower() == 'true'
                else:
                    source_config[key] = value
        
        if source_config:
            config['source_db'] = source_config
        
        # Snowflake configuration
        snowflake_config = {}
        for key, env_var in [
            ('account', 'SNOWFLAKE_ACCOUNT'),
            ('user', 'SNOWFLAKE_USER'),
            ('password', 'SNOWFLAKE_PASSWORD'),
            ('role', 'SNOWFLAKE_ROLE'),
            ('warehouse', 'SNOWFLAKE_WAREHOUSE'),
            ('database', 'SNOWFLAKE_DATABASE'),
            ('schema', 'SNOWFLAKE_SCHEMA')
        ]:
            if value := os.getenv(env_var):
                snowflake_config[key] = value
        
        if snowflake_config:
            config['snowflake'] = snowflake_config
        
        # Transfer configuration
        transfer_config = {}
        for key, env_var in [
            ('source_table', 'SOURCE_TABLE'),
            ('destination_table', 'DESTINATION_TABLE'),
            ('batch_size', 'BATCH_SIZE'),
            ('max_workers', 'MAX_WORKERS'),
            ('fetch_size', 'FETCH_SIZE'),
            ('mode', 'TRANSFER_MODE'),
            ('partition_column', 'PARTITION_COLUMN'),
            ('lower_bound', 'LOWER_BOUND'),
            ('upper_bound', 'UPPER_BOUND'),
            ('num_partitions', 'NUM_PARTITIONS')
        ]:
            if value := os.getenv(env_var):
                if key in ['batch_size', 'max_workers', 'fetch_size', 'num_partitions']:
                    transfer_config[key] = int(value)
                else:
                    transfer_config[key] = value
        
        if transfer_config:
            config['transfer'] = transfer_config
        
        return config
    
    def _create_config(self, config_data: Dict[str, Any]) -> AppConfig:
        """Create AppConfig instance from configuration data."""
        # Get database type
        db_type = DatabaseType(config_data.get('database_type', 'sqlserver'))
        
        # Create source database config based on type
        source_data = config_data.get('source_db', {})
        if db_type == DatabaseType.SQLSERVER:
            source_db = SqlServerConfig(**source_data)
        elif db_type == DatabaseType.POSTGRESQL:
            source_db = PostgreSQLConfig(**source_data)
        elif db_type == DatabaseType.MYSQL:
            source_db = MySQLConfig(**source_data)
        elif db_type == DatabaseType.ORACLE:
            source_db = OracleConfig(**source_data)
        elif db_type == DatabaseType.DATABRICKS:
            source_db = DatabricksConfig(**source_data)
        else:
            source_db = DatabaseConfig(**source_data)
        
        # Create Snowflake config
        snowflake_data = config_data.get('snowflake', {})
        snowflake = SnowflakeConfig(**snowflake_data)
        
        # Create transfer config
        transfer_data = config_data.get('transfer', {})
        transfer = TransferConfig(**transfer_data)
        
        # Create main config
        return AppConfig(
            database_type=db_type,
            source_db=source_db,
            snowflake=snowflake,
            transfer=transfer,
            log_level=config_data.get('log_level', 'INFO'),
            log_file=config_data.get('log_file'),
            config_file=config_data.get('config_file')
        )

# Global configuration manager instance
config_manager = ConfigManager() 