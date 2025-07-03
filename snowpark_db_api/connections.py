"""Database connection factories for Snowpark DB-API transfers.

Clean connection factories.
"""

from typing import Any, Callable
from .config import DatabaseType, SourceConfig
import logging

logger = logging.getLogger(__name__)

def create_sqlserver_connection(config: SourceConfig) -> Callable[[], Any]:
    """Create a SQL Server connection factory function."""
    def connection_factory():
        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is required for SQL Server connections. "
                "Install with: pip install pyodbc"
            )
        
        # Build connection string
        connection_str = (
            f"DRIVER={{{config.driver}}};"
            f"SERVER={config.host},{config.port};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password};"
        )
        
        if config.trust_server_certificate:
            connection_str += "TrustServerCertificate=yes;"
        
        logger.debug(f"Connecting to SQL Server: {config.host}:{config.port}")
        connection = pyodbc.connect(connection_str)
        logger.info(f"Successfully connected to SQL Server: {config.host}")
        
        return connection
    
    return connection_factory

def create_postgresql_connection(config: SourceConfig) -> Callable[[], Any]:
    """Create a PostgreSQL connection factory function."""
    def connection_factory():
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL connections. "
                "Install with: pip install psycopg2-binary"
            )
        
        logger.debug(f"Connecting to PostgreSQL: {config.host}:{config.port}")
        connection = psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.database,
            user=config.username,
            password=config.password,
        )
        logger.info(f"Successfully connected to PostgreSQL: {config.host}")
        
        return connection
    
    return connection_factory

def create_mysql_connection(config: SourceConfig) -> Callable[[], Any]:
    """Create a MySQL connection factory function."""
    def connection_factory():
        try:
            import pymysql
        except ImportError:
            raise ImportError(
                "pymysql is required for MySQL connections. "
                "Install with: pip install pymysql"
            )
        
        logger.debug(f"Connecting to MySQL: {config.host}:{config.port}")
        connection = pymysql.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.username,
            password=config.password
        )
        logger.info(f"Successfully connected to MySQL: {config.host}")
        
        return connection
    
    return connection_factory

def create_oracle_connection(config: SourceConfig) -> Callable[[], Any]:
    """Create an Oracle connection factory function."""
    def connection_factory():
        try:
            import oracledb
        except ImportError:
            raise ImportError(
                "oracledb is required for Oracle connections. "
                "Install with: pip install oracledb"
            )
        
        # Build DSN
        dsn = f"{config.host}:{config.port}"
        if config.service_name:
            dsn += f"/{config.service_name}"
        
        logger.debug(f"Connecting to Oracle: {dsn}")
        connection = oracledb.connect(
            user=config.username,
            password=config.password,
            dsn=dsn
        )
        logger.info(f"Successfully connected to Oracle: {config.host}")
        
        return connection
    
    return connection_factory

def create_databricks_connection(config: SourceConfig) -> Callable[[], Any]:
    """Create a Databricks connection factory function."""
    def connection_factory():
        try:
            import databricks.sql
        except ImportError:
            raise ImportError(
                "databricks-sql-connector is required for Databricks connections. "
                "Install with: pip install databricks-sql-connector"
            )
        
        logger.debug(f"Connecting to Databricks: {config.server_hostname}")
        connection = databricks.sql.connect(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            access_token=config.access_token
        )
        logger.info(f"Successfully connected to Databricks: {config.server_hostname}")
        
        return connection
    
    return connection_factory

def create_connection(database_type: DatabaseType, config: SourceConfig) -> Any:
    """Create database connection. Main entry point."""
    factories = {
        DatabaseType.SQLSERVER: create_sqlserver_connection,
        DatabaseType.POSTGRESQL: create_postgresql_connection,
        DatabaseType.MYSQL: create_mysql_connection,
        DatabaseType.ORACLE: create_oracle_connection,
        DatabaseType.DATABRICKS: create_databricks_connection,
    }
    
    if database_type not in factories:
        raise ValueError(f"Unsupported database type: {database_type}")
    
    factory = factories[database_type](config)
    return factory()

def test_connection_factory(connection_factory: Callable[[], Any]) -> bool:
    """Test if a connection factory works.
    
    Args:
        connection_factory: Connection factory function to test
        
    Returns:
        True if connection is successful, False otherwise
    """
    try:
        conn = connection_factory()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        logger.info("Connection test successful")
        return True
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False 