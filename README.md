# Snowpark DB-API Transfer Tool

A professional, scalable data transfer solution for moving data from various databases to Snowflake using the new Snowpark DB-API. Built following FastAI/Jeremy Howard engineering principles for simplicity, modularity, and ease of use.

## 🚀 Features

- **Modern Snowpark DB-API**: Uses the latest Snowpark DB-API for efficient, parallel data transfers
- **Multi-Database Support**: SQL Server, PostgreSQL, MySQL, Oracle, and Databricks
- **Production Ready**: Docker containerization with all database drivers included
- **Scalable Architecture**: Supports partitioning for large datasets with parallel processing
- **Professional CLI**: Rich terminal interface with progress tracking and error handling
- **Flexible Configuration**: Environment variables, YAML files, or CLI arguments
- **FastAI Principles**: Clean API, sensible defaults, progressive complexity disclosure

## 📋 Prerequisites

- Python 3.11+
- Docker (for containerized deployment)
- Appropriate database drivers for your source database
- Snowflake account with appropriate permissions

## 🛠️ Installation

### Option 1: Docker (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd snowpark-db-api

# Build the Docker image
docker build -t snowpark-transfer .

# Or use docker-compose
docker-compose up --build
```

### Option 2: Local Installation

```bash
# Install the package
pip install -e .

# Or install with all dependencies
pip install -r requirements.txt
```

## 🚀 Quick Start

### Using Docker with Environment Variables

1. Copy the example environment file:
```bash
cp env-example .env
```

2. Edit `.env` with your actual database credentials

3. Run the transfer:
```bash
docker-compose run snowpark-transfer python -m snowpark_db_api.cli transfer \
  --host $SOURCE_HOST \
  --username $SOURCE_USERNAME \
  --password $SOURCE_PASSWORD \
  --database $SOURCE_DATABASE \
  --source-table $SOURCE_TABLE \
  --sf-account $SNOWFLAKE_ACCOUNT \
  --sf-user $SNOWFLAKE_USER \
  --sf-password $SNOWFLAKE_PASSWORD \
  --sf-role $SNOWFLAKE_ROLE \
  --sf-warehouse $SNOWFLAKE_WAREHOUSE \
  --sf-database $SNOWFLAKE_DATABASE
```

### Using Configuration Files

1. Generate a configuration template:
```bash
python -m snowpark_db_api.cli config-template --output my-config.yaml
```

2. Edit the configuration file with your credentials

3. Run the transfer:
```bash
python -m snowpark_db_api.cli transfer --config my-config.yaml
```

### Using Python API

```python
from snowpark_db_api import DataTransfer, config_manager

# Load configuration
config = config_manager.load_config("my-config.yaml")

# Create transfer instance
transfer = DataTransfer(config)

# Setup connections
if transfer.setup_connections():
    # Execute transfer
    success = transfer.transfer_table()
    
    if success:
        print("Transfer completed successfully!")
    else:
        print("Transfer failed!")
        
# Cleanup
transfer.cleanup()
```

## 📖 Usage Examples

### Basic SQL Server to Snowflake Transfer

```bash
python -m snowpark_db_api.cli transfer \
  --host sqlserver.company.com \
  --username myuser \
  --password mypass \
  --database mydb \
  --source-table sales_data \
  --sf-account myaccount.snowflakecomputing.com \
  --sf-user snowflake_user \
  --sf-password snowflake_pass \
  --sf-role SYSADMIN \
  --sf-warehouse COMPUTE_WH \
  --sf-database ANALYTICS
```

### Large Dataset with Partitioning

```bash
python -m snowpark_db_api.cli transfer \
  --host postgres.company.com \
  --username pguser \
  --password pgpass \
  --database mydb \
  --source-table large_table \
  --partition-column id \
  --lower-bound 1 \
  --upper-bound 10000000 \
  --num-partitions 16 \
  --max-workers 8
```

## 🗂️ Supported Databases

| Database | Status | Driver | Notes |
|----------|--------|--------|-------|
| SQL Server | ✅ | pyodbc + ODBC Driver 18 | Fully supported |
| PostgreSQL | ✅ | psycopg2 | Fully supported |
| MySQL | ✅ | PyMySQL | Fully supported |
| Oracle | ✅ | oracledb | Fully supported |
| Databricks | ✅ | databricks-sql-connector | Fully supported |

## ⚙️ Configuration

### Environment Variables

```bash
# Source Database
export DB_TYPE=sqlserver
export SOURCE_HOST=your-host
export SOURCE_USERNAME=your-user
export SOURCE_PASSWORD=your-password
export SOURCE_DATABASE=your-database
export SOURCE_TABLE=your-table

# Snowflake
export SNOWFLAKE_ACCOUNT=your-account
export SNOWFLAKE_USER=your-user
export SNOWFLAKE_PASSWORD=your-password
export SNOWFLAKE_ROLE=your-role
export SNOWFLAKE_WAREHOUSE=your-warehouse
export SNOWFLAKE_DATABASE=your-database
```

### Configuration File Format

```yaml
database_type: sqlserver

source_db:
  host: your-host
  port: 1433
  username: your-username
  password: your-password
  database: your-database

snowflake:
  account: your-account
  user: your-user
  password: your-password
  role: your-role
  warehouse: your-warehouse
  database: your-database
  schema: PUBLIC

transfer:
  source_table: your_table
  destination_table: your_destination_table
  mode: overwrite
  batch_size: 10000
  max_workers: 4
  fetch_size: 1000
```

## 🐳 Docker Deployment

### Using Docker Compose (Recommended)

```bash
# Copy environment variables
cp env-example .env
# Edit .env with your credentials

# Run the transfer
docker-compose up
```

## 🔧 CLI Reference

```bash
# Transfer data
python -m snowpark_db_api.cli transfer [OPTIONS]

# Generate config template
python -m snowpark_db_api.cli config-template [OPTIONS]
```

## 🏗️ Architecture

The project follows FastAI principles with clean separation of concerns:

- **`config.py`**: Configuration management with validation
- **`connections.py`**: Database connection factories
- **`core.py`**: Main transfer logic using Snowpark DB-API
- **`utils.py`**: Logging, progress tracking, and utilities
- **`cli.py`**: Command-line interface with Rich formatting

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

Built with ❤️ using FastAI principles for simplicity and scalability.


<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->

This file will become your README and also the index of your
documentation.

## Developer Guide

If you are new to using `nbdev` here are some useful pointers to get you
started.

### Install snowpark_db_api in Development mode

``` sh
# make sure snowpark_db_api package is installed in development mode
$ pip install -e .

# make changes under nbs/ directory
# ...

# compile to have changes apply to snowpark_db_api
$ nbdev_prepare
```

## Usage

### Installation

Install latest from the GitHub
[repository](https://github.com/Jeremy-Demlow/snowpark-db-api):

``` sh
$ pip install git+https://github.com/Jeremy-Demlow/snowpark-db-api.git
```

or from [conda](https://anaconda.org/Jeremy-Demlow/snowpark-db-api)

``` sh
$ conda install -c Jeremy-Demlow snowpark_db_api
```

or from [pypi](https://pypi.org/project/snowpark-db-api/)

``` sh
$ pip install snowpark_db_api
```

### Documentation

Documentation can be found hosted on this GitHub
[repository](https://github.com/Jeremy-Demlow/snowpark-db-api)’s
[pages](https://Jeremy-Demlow.github.io/snowpark-db-api/). Additionally
you can find package manager specific guidelines on
[conda](https://anaconda.org/Jeremy-Demlow/snowpark-db-api) and
[pypi](https://pypi.org/project/snowpark-db-api/) respectively.

## How to use

Fill me in please! Don’t forget code examples:

``` python
1+1
```

    2
