# 🔐 SQL Server Windows/Kerberos Authentication Support

This guide explains how to use Windows Authentication and Kerberos Authentication with SQL Server in the `snowpark-db-api` library.

## 🎯 Overview

The `snowpark-db-api` library now supports three authentication methods for SQL Server:

1. **Username/Password Authentication** (default)
2. **Windows Authentication** (Trusted Connection)
3. **Kerberos Authentication** (SSPI)

## 🚀 Quick Start Examples

### Windows Authentication (Trusted Connection)

```python
from snowpark_db_api.config import Config, SourceConfig, SnowflakeConfig, TransferConfig, DatabaseType
from snowpark_db_api import transfer

# Configure Windows Authentication
config = Config(
    database_type=DatabaseType.SQLSERVER,
    source=SourceConfig(
        host="your-server.database.windows.net",
        database="your_database",
        use_windows_auth=True,  # Enable Windows authentication
        port=1433,
        driver="ODBC Driver 18 for SQL Server"
    ),
    snowflake=SnowflakeConfig(
        account="your_account.region.cloud",
        user="your_snowflake_user",
        password="your_snowflake_password",
        role="SYSADMIN",
        warehouse="COMPUTE_WH",
        database="your_snowflake_database",
        db_schema="PUBLIC"
    ),
    transfer=TransferConfig(
        source_table="dbo.orders",
        destination_table="ORDERS_COPY",
        mode="overwrite"
    )
)

# Transfer data using Windows authentication
transfer("dbo.orders", config=config)
```

### Kerberos Authentication

```python
from snowpark_db_api.config import Config, SourceConfig, SnowflakeConfig, TransferConfig, DatabaseType
from snowpark_db_api import transfer

# Configure Kerberos Authentication
config = Config(
    database_type=DatabaseType.SQLSERVER,
    source=SourceConfig(
        host="your-server.database.windows.net",
        database="your_database",
        use_kerberos=True,  # Enable Kerberos authentication
        kerberos_realm="YOUR.DOMAIN.COM",  # Optional: specify realm
        kerberos_service="MSSQLSvc",       # Optional: specify service
        port=1433,
        driver="ODBC Driver 18 for SQL Server"
    ),
    snowflake=SnowflakeConfig(
        account="your_account.region.cloud",
        user="your_snowflake_user",
        password="your_snowflake_password",
        role="SYSADMIN",
        warehouse="COMPUTE_WH",
        database="your_snowflake_database",
        db_schema="PUBLIC"
    ),
    transfer=TransferConfig(
        source_table="dbo.orders",
        destination_table="ORDERS_COPY",
        mode="overwrite"
    )
)

# Transfer data using Kerberos authentication
transfer("dbo.orders", config=config)
```

## 🌍 Environment Variable Configuration

### Windows Authentication

```bash
# .env file for Windows Authentication
DB_TYPE=sqlserver
SOURCE_HOST=your-server.database.windows.net
SOURCE_DATABASE=your_database
SOURCE_PORT=1433
SOURCE_USE_WINDOWS_AUTH=true
SOURCE_DRIVER=ODBC Driver 18 for SQL Server
SOURCE_TRUST_CERT=true

# Snowflake settings
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your_snowflake_database
SNOWFLAKE_SCHEMA=PUBLIC

# Transfer settings
DESTINATION_TABLE=MY_TABLE
MODE=overwrite
```

### Kerberos Authentication

```bash
# .env file for Kerberos Authentication
DB_TYPE=sqlserver
SOURCE_HOST=your-server.database.windows.net
SOURCE_DATABASE=your_database
SOURCE_PORT=1433
SOURCE_USE_KERBEROS=true
SOURCE_KERBEROS_REALM=YOUR.DOMAIN.COM
SOURCE_KERBEROS_SERVICE=MSSQLSvc
SOURCE_DRIVER=ODBC Driver 18 for SQL Server
SOURCE_TRUST_CERT=true

# Snowflake settings
SNOWFLAKE_ACCOUNT=your_account.region.cloud
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=your_snowflake_database
SNOWFLAKE_SCHEMA=PUBLIC

# Transfer settings
DESTINATION_TABLE=MY_TABLE
MODE=overwrite
```

## ⚡ CLI Usage

### Windows Authentication

```bash
# Transfer using Windows authentication
python -m snowpark_db_api transfer \
  --source-table "dbo.orders" \
  --destination-table "ORDERS_COPY" \
  --source-host "your-server.database.windows.net" \
  --source-database "your_database" \
  --source-use-windows-auth \
  --snowflake-account "your_account.region.cloud" \
  --snowflake-user "your_snowflake_user" \
  --snowflake-password "your_snowflake_password" \
  --snowflake-database "your_snowflake_database"
```

### Kerberos Authentication

```bash
# Transfer using Kerberos authentication
python -m snowpark_db_api transfer \
  --source-table "dbo.orders" \
  --destination-table "ORDERS_COPY" \
  --source-host "your-server.database.windows.net" \
  --source-database "your_database" \
  --source-use-kerberos \
  --source-kerberos-realm "YOUR.DOMAIN.COM" \
  --source-kerberos-service "MSSQLSvc" \
  --snowflake-account "your_account.region.cloud" \
  --snowflake-user "your_snowflake_user" \
  --snowflake-password "your_snowflake_password" \
  --snowflake-database "your_snowflake_database"
```

## 📋 Configuration Options

### Windows Authentication Options

| Option | Environment Variable | Description | Default |
|--------|---------------------|-------------|---------|
| `use_windows_auth` | `SOURCE_USE_WINDOWS_AUTH` | Enable Windows authentication | `false` |
| `trust_server_certificate` | `SOURCE_TRUST_CERT` | Trust server certificate | `true` |

### Kerberos Authentication Options

| Option | Environment Variable | Description | Default |
|--------|---------------------|-------------|---------|
| `use_kerberos` | `SOURCE_USE_KERBEROS` | Enable Kerberos authentication | `false` |
| `kerberos_realm` | `SOURCE_KERBEROS_REALM` | Kerberos realm (optional) | `None` |
| `kerberos_service` | `SOURCE_KERBEROS_SERVICE` | Kerberos service name (optional) | `None` |
| `integrated_security` | `SOURCE_INTEGRATED_SECURITY` | Custom integrated security setting | `None` |

### Generated Connection Strings

#### Windows Authentication
```
DRIVER={ODBC Driver 18 for SQL Server};
SERVER=your-server.database.windows.net,1433;
DATABASE=your_database;
Trusted_Connection=yes;
TrustServerCertificate=yes;
```

#### Kerberos Authentication
```
DRIVER={ODBC Driver 18 for SQL Server};
SERVER=your-server.database.windows.net,1433;
DATABASE=your_database;
Integrated Security=SSPI;
Kerberos_Realm=YOUR.DOMAIN.COM;
Kerberos_Service=MSSQLSvc;
TrustServerCertificate=yes;
```

## 🔧 Prerequisites

### Windows Authentication
- The application must be running on a Windows machine
- The user account must have access to the SQL Server instance
- The SQL Server must be configured to accept Windows authentication

### Kerberos Authentication
- Kerberos must be properly configured in your domain
- The SQL Server must be configured with Service Principal Names (SPNs)
- The client machine must be part of the domain or have proper Kerberos configuration

## 🛠️ Advanced Examples

### Mixed Authentication Configuration

```python
from snowpark_db_api.config import Config, SourceConfig, SnowflakeConfig, TransferConfig, DatabaseType

# Production configuration with Windows auth
prod_config = Config(
    database_type=DatabaseType.SQLSERVER,
    source=SourceConfig(
        host="prod-server.company.com",
        database="production_db",
        use_windows_auth=True,
        driver="ODBC Driver 18 for SQL Server",
        trust_server_certificate=False  # More secure in production
    ),
    snowflake=SnowflakeConfig(
        account="company.us-west-2.snowflakecomputing.com",
        user="data_loader",
        password="secure_password",
        role="DATA_LOADER_ROLE",
        warehouse="ETL_WH",
        database="ANALYTICS",
        db_schema="RAW_DATA"
    ),
    transfer=TransferConfig(
        source_table="sales.transactions",
        destination_table="SALES_TRANSACTIONS",
        mode="append",
        fetch_size=25000,
        max_workers=8
    )
)

# Development configuration with username/password
dev_config = Config(
    database_type=DatabaseType.SQLSERVER,
    source=SourceConfig(
        host="dev-server.company.com",
        database="development_db",
        username="dev_user",
        password="dev_password",
        driver="ODBC Driver 18 for SQL Server"
    ),
    # ... rest of configuration
)
```

### Connection Testing

```python
from snowpark_db_api.connections import create_sqlserver_connection, test_connection_factory
from snowpark_db_api.config import SourceConfig

# Test Windows authentication connection
windows_config = SourceConfig(
    host="your-server.database.windows.net",
    database="your_database",
    use_windows_auth=True,
    driver="ODBC Driver 18 for SQL Server"
)

# Create and test connection
connection_factory = create_sqlserver_connection(windows_config)
is_working = test_connection_factory(connection_factory)
print(f"Windows authentication working: {is_working}")

# Test Kerberos authentication connection
kerberos_config = SourceConfig(
    host="your-server.database.windows.net",
    database="your_database",
    use_kerberos=True,
    kerberos_realm="YOUR.DOMAIN.COM",
    driver="ODBC Driver 18 for SQL Server"
)

connection_factory = create_sqlserver_connection(kerberos_config)
is_working = test_connection_factory(connection_factory)
print(f"Kerberos authentication working: {is_working}")
```

## 🚨 Troubleshooting

### Common Issues

#### Windows Authentication
- **Error**: "Login failed for user 'NT AUTHORITY\\ANONYMOUS LOGON'"
  - **Solution**: Ensure the application is running under a user account with SQL Server access

- **Error**: "The target principal name is incorrect"
  - **Solution**: Check that the SQL Server service is running under the correct account

#### Kerberos Authentication
- **Error**: "Cannot generate SSPI context"
  - **Solution**: Verify Kerberos configuration and SPNs are registered correctly

- **Error**: "The specified module could not be found"
  - **Solution**: Ensure the ODBC driver is properly installed

### Debug Connection Strings

To see the actual connection string being used, enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Your transfer code here
```

## 📚 Additional Resources

- [Microsoft SQL Server Authentication Documentation](https://docs.microsoft.com/en-us/sql/relational-databases/security/authentication-access/getting-started-with-database-engine-permissions)
- [Kerberos Authentication for SQL Server](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/register-a-service-principal-name-for-kerberos-connections)
- [pyodbc Connection Strings](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Windows)

## 🤝 Contributing

If you encounter issues with Windows or Kerberos authentication, please:

1. Check the troubleshooting section above
2. Enable debug logging to see connection strings
3. Open an issue with detailed error messages and configuration
4. Consider contributing improvements or additional authentication methods

---

**Note**: This library has been extensively tested with SQL Server Windows Authentication. Kerberos authentication support is based on standard ODBC connection parameters and should work in most environments, but may require environment-specific configuration. 