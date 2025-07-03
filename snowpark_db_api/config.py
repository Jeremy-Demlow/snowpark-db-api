"""Modern configuration system for Snowpark DB API transfers.

Clean, simple configuration using Pydantic with environment variable support.
"""

from typing import Optional
from pathlib import Path
from pydantic import BaseModel, Field
from enum import Enum
import os

class DatabaseType(str, Enum):
    """Supported source database types."""
    SQLSERVER = "sqlserver"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    DATABRICKS = "databricks"

class SourceConfig(BaseModel):
    """Source database configuration with environment variable support."""
    host: str = Field(..., description="Database host")
    port: int = Field(1433, description="Database port")
    username: Optional[str] = Field(None, description="Database username")
    password: Optional[str] = Field(None, description="Database password") 
    database: str = Field(..., description="Database name")
    
    # SQL Server specific
    driver: str = Field("ODBC Driver 18 for SQL Server", description="ODBC driver")
    trust_server_certificate: bool = Field(True, description="Trust server certificate")
    
    # Windows/Kerberos Authentication options for SQL Server
    use_windows_auth: bool = Field(False, description="Use Windows authentication (Trusted_Connection)")
    use_kerberos: bool = Field(False, description="Use Kerberos authentication")
    kerberos_realm: Optional[str] = Field(None, description="Kerberos realm (optional)")
    kerberos_service: Optional[str] = Field(None, description="Kerberos service name (optional)")
    integrated_security: Optional[str] = Field(None, description="Integrated Security setting (SSPI, etc.)")
    
    # Oracle specific  
    service_name: str = Field("", description="Oracle service name")
    
    # Databricks specific
    server_hostname: str = Field("", description="Databricks server hostname")
    http_path: str = Field("", description="Databricks HTTP path")
    access_token: str = Field("", description="Databricks access token")

    @classmethod
    def from_env(cls) -> "SourceConfig":
        """Create from environment variables."""
        return cls(
            host=os.getenv("SOURCE_HOST", ""),
            port=int(os.getenv("SOURCE_PORT", "1433")),
            username=os.getenv("SOURCE_USERNAME"),
            password=os.getenv("SOURCE_PASSWORD"),
            database=os.getenv("SOURCE_DATABASE", ""),
            driver=os.getenv("SOURCE_DRIVER", "ODBC Driver 18 for SQL Server"),
            trust_server_certificate=os.getenv("SOURCE_TRUST_CERT", "true").lower() == "true",
            
            # Windows/Kerberos Authentication
            use_windows_auth=os.getenv("SOURCE_USE_WINDOWS_AUTH", "false").lower() == "true",
            use_kerberos=os.getenv("SOURCE_USE_KERBEROS", "false").lower() == "true",
            kerberos_realm=os.getenv("SOURCE_KERBEROS_REALM"),
            kerberos_service=os.getenv("SOURCE_KERBEROS_SERVICE"),
            integrated_security=os.getenv("SOURCE_INTEGRATED_SECURITY"),
            
            service_name=os.getenv("SOURCE_SERVICE_NAME", ""),
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME", ""),
            http_path=os.getenv("DATABRICKS_HTTP_PATH", ""),
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN", "")
        )

class SnowflakeConfig(BaseModel):
    """Snowflake connection configuration with environment variable support."""
    account: str = Field(..., description="Snowflake account")
    user: str = Field(..., description="Snowflake user")
    password: str = Field(..., description="Snowflake password")
    role: str = Field("ACCOUNTADMIN", description="Snowflake role")
    warehouse: str = Field("COMPUTE_WH", description="Snowflake warehouse")
    database: str = Field(..., description="Snowflake database")
    db_schema: str = Field("PUBLIC", description="Snowflake schema")
    create_db_if_missing: bool = Field(True, description="Create database/schema if they don't exist")
    
    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Create from environment variables."""
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", ""),
            db_schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            create_db_if_missing=os.getenv("SNOWFLAKE_CREATE_DB_IF_MISSING", "true").lower() == "true"
        )

class TransferConfig(BaseModel):
    """Transfer operation configuration."""
    source_table: str = Field(..., description="Source table name")
    destination_table: Optional[str] = Field(None, description="Destination table name")
    mode: str = Field("overwrite", description="Transfer mode: overwrite, append")
    fetch_size: int = Field(1000, description="Batch size for fetching rows")
    query_timeout: int = Field(300, description="Query timeout in seconds")
    max_workers: int = Field(4, description="Maximum parallel workers")
    save_metadata: bool = Field(False, description="Save transfer metadata to JSON file")
    
    def model_post_init(self, __context) -> None:
        """Set destination table to source table if not provided."""
        if not self.destination_table:
            # Clean up source table name for Snowflake (remove schema prefix)
            clean_name = self.source_table.split('.')[-1] if '.' in self.source_table else self.source_table
            self.destination_table = clean_name.upper()

class Config(BaseModel):
    """Main application configuration. One clear, simple way to configure everything."""
    database_type: DatabaseType = Field(DatabaseType.SQLSERVER, description="Source database type")
    source: SourceConfig = Field(..., description="Source database configuration")
    snowflake: SnowflakeConfig = Field(..., description="Snowflake configuration")
    transfer: TransferConfig = Field(..., description="Transfer configuration")
    log_level: str = Field("INFO", description="Logging level")

    @classmethod
    def from_env(cls) -> "Config":
        """Create complete configuration from environment variables. The main way to create config."""
        return cls(
            database_type=DatabaseType(os.getenv("DB_TYPE", "sqlserver")),
            source=SourceConfig.from_env(),
            snowflake=SnowflakeConfig.from_env(), 
            transfer=TransferConfig(
                source_table=os.getenv("SOURCE_TABLE", ""),
                destination_table=os.getenv("DESTINATION_TABLE"),
                mode=os.getenv("TRANSFER_MODE", "overwrite"),
                fetch_size=int(os.getenv("FETCH_SIZE", "1000")),
                query_timeout=int(os.getenv("QUERY_TIMEOUT", "300")),
                max_workers=int(os.getenv("MAX_WORKERS", "4")),
                save_metadata=os.getenv("SAVE_METADATA", "false").lower() == "true"
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip()
        )

# Simple, clean API - one way to get configuration
def get_config() -> Config:
    """Get configuration from environment variables. This is the main entry point."""
    return Config.from_env() 