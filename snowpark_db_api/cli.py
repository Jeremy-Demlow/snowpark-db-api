"""Command-line interface for Snowpark DB-API transfers.

This module provides a user-friendly CLI for database transfers using Typer and Rich.
"""

import typer
from typing import Optional, List
from pathlib import Path
import yaml
from rich.console import Console
from rich.table import Table
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TextColumn

from .config import config_manager, DatabaseType
from .core import transfer_data, DataTransfer
from .utils import setup_logging, print_banner

# Initialize CLI app
app = typer.Typer(
    name="snowpark-transfer",
    help="Professional data transfer tool using Snowpark DB-API",
    add_completion=False
)

console = Console()

@app.command()
def transfer(
    host: str = typer.Option(..., "--host", help="Source database host"),
    username: str = typer.Option(..., "--username", help="Source database username"),
    password: str = typer.Option(..., "--password", hide_input=True, help="Source database password"),
    database: str = typer.Option(..., "--database", help="Source database name"),
    source_table: str = typer.Option(..., "--source-table", help="Source table name"),
    sf_account: str = typer.Option(..., "--sf-account", help="Snowflake account"),
    sf_user: str = typer.Option(..., "--sf-user", help="Snowflake username"),
    sf_password: str = typer.Option(..., "--sf-password", hide_input=True, help="Snowflake password"),
    sf_role: str = typer.Option(..., "--sf-role", help="Snowflake role"),
    sf_warehouse: str = typer.Option(..., "--sf-warehouse", help="Snowflake warehouse"),
    sf_database: str = typer.Option(..., "--sf-database", help="Snowflake database"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file")
):
    """Transfer data from source database to Snowflake."""
    
    logger = setup_logging("INFO")
    
    try:
        # Load and override config
        config = config_manager.load_config(str(config_file) if config_file else None)
        
        # Override with CLI args
        config.source_db.host = host
        config.source_db.username = username  
        config.source_db.password = password
        config.source_db.database = database
        config.transfer.source_table = source_table
        config.snowflake.account = sf_account
        config.snowflake.user = sf_user
        config.snowflake.password = sf_password
        config.snowflake.role = sf_role
        config.snowflake.warehouse = sf_warehouse
        config.snowflake.database = sf_database
        
        console.print("[bold green]Starting data transfer...[/bold green]")
        success = transfer_data(config)
        
        if success:
            console.print("[bold green]✓ Transfer completed successfully![/bold green]")
        else:
            console.print("[bold red]✗ Transfer failed![/bold red]")
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise typer.Exit(1)

@app.command()
def config_template(
    output_file: Path = typer.Option("config.yaml", "--output", "-o")
):
    """Generate a configuration template."""
    
    template = {
        'database_type': 'sqlserver',
        'source_db': {
            'host': 'your-host',
            'username': 'your-username', 
            'password': 'your-password',
            'database': 'your-database'
        },
        'snowflake': {
            'account': 'your-account',
            'user': 'your-user',
            'password': 'your-password',
            'role': 'your-role',
            'warehouse': 'your-warehouse',
            'database': 'your-database'
        },
        'transfer': {
            'source_table': 'your-table'
        }
    }
    
    with open(output_file, 'w') as f:
        yaml.dump(template, f, default_flow_style=False)
    
    console.print(f"[green]Template saved to: {output_file}[/green]")

@app.command()
def list_tables(
    # Database configuration (same as transfer command but fewer options)
    db_type: DatabaseType = typer.Option(
        DatabaseType.SQLSERVER,
        "--db-type",
        help="Source database type"
    ),
    host: str = typer.Option(..., "--host", help="Source database host"),
    username: str = typer.Option(..., "--username", help="Source database username"),
    password: str = typer.Option(..., "--password", hide_input=True, help="Source database password"),
    database: str = typer.Option(..., "--database", help="Source database name"),
    port: Optional[int] = typer.Option(None, "--port", help="Source database port"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file")
):
    """List all tables in the source database."""
    
    try:
        # Build minimal config for connection
        config = _build_minimal_config(db_type, host, port, username, password, database, config_file)
        
        # Create transfer object and list tables
        transfer = DataTransfer(config)
        if not transfer.setup_connections():
            console.print("[red]Failed to connect to source database[/red]")
            raise typer.Exit(1)
        
        tables = transfer.list_tables()
        transfer.cleanup()
        
        if not tables:
            console.print("[yellow]No tables found[/yellow]")
            return
        
        # Display tables in a nice table format
        table = Table(title=f"Tables in {database}")
        table.add_column("Table Name", style="cyan")
        table.add_column("Index", style="magenta")
        
        for i, table_name in enumerate(tables, 1):
            table.add_row(table_name, str(i))
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def table_info(
    table_name: str = typer.Argument(..., help="Name of the table to inspect"),
    # Database configuration (same as list_tables)
    db_type: DatabaseType = typer.Option(DatabaseType.SQLSERVER, "--db-type"),
    host: str = typer.Option(..., "--host"),
    username: str = typer.Option(..., "--username"),
    password: str = typer.Option(..., "--password", hide_input=True),
    database: str = typer.Option(..., "--database"),
    port: Optional[int] = typer.Option(None, "--port"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c")
):
    """Get detailed information about a specific table."""
    
    try:
        config = _build_minimal_config(db_type, host, port, username, password, database, config_file)
        
        transfer = DataTransfer(config)
        if not transfer.setup_connections():
            console.print("[red]Failed to connect to source database[/red]")
            raise typer.Exit(1)
        
        info = transfer.get_table_info(table_name)
        transfer.cleanup()
        
        if not info:
            console.print(f"[red]Could not get information for table: {table_name}[/red]")
            return
        
        # Display table info
        console.print(f"\n[bold cyan]Table Information: {table_name}[/bold cyan]")
        console.print(f"Row Count: [green]{info.get('row_count', 'Unknown'):,}[/green]")
        
        if 'columns' in info and info['columns']:
            table = Table(title="Columns")
            table.add_column("Column Name", style="cyan")
            table.add_column("Data Type", style="green")
            table.add_column("Max Length", style="yellow")
            
            for col in info['columns']:
                max_len = str(col.get('max_length', '')) if col.get('max_length') else ''
                table.add_row(
                    col.get('name', ''),
                    col.get('type', ''),
                    max_len
                )
            
            console.print(table)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

def _build_config_from_args(*args, **kwargs):
    """Build configuration from command line arguments."""
    # This is a helper function to convert CLI args to config
    # Implementation would map all the CLI arguments to the config structure
    # For brevity, I'll create a simplified version
    
    # Extract arguments
    (db_type, host, port, username, password, database,
     sf_account, sf_user, sf_password, sf_role, sf_warehouse, sf_database, sf_schema,
     source_table, dest_table, query, mode,
     batch_size, max_workers, fetch_size,
     partition_column, lower_bound, upper_bound, num_partitions,
     config_file, log_level, log_file) = args[:25]
    
    # Use environment variables and config file if available
    config = config_manager.load_config(str(config_file) if config_file else None)
    
    # Override with CLI arguments
    if host:
        config.source_db.host = host
    if port:
        config.source_db.port = port
    if username:
        config.source_db.username = username
    if password:
        config.source_db.password = password
    if database:
        config.source_db.database = database
    
    # Snowflake config
    config.snowflake.account = sf_account
    config.snowflake.user = sf_user
    config.snowflake.password = sf_password
    config.snowflake.role = sf_role
    config.snowflake.warehouse = sf_warehouse
    config.snowflake.database = sf_database
    config.snowflake.schema = sf_schema
    
    # Transfer config
    config.transfer.source_table = source_table
    config.transfer.destination_table = dest_table or source_table
    config.transfer.mode = mode
    config.transfer.batch_size = batch_size
    config.transfer.max_workers = max_workers
    config.transfer.fetch_size = fetch_size
    
    if partition_column:
        config.transfer.partition_column = partition_column
        config.transfer.lower_bound = lower_bound
        config.transfer.upper_bound = upper_bound
        config.transfer.num_partitions = num_partitions
    
    config.log_level = log_level
    config.log_file = str(log_file) if log_file else None
    
    return config

def _build_minimal_config(db_type, host, port, username, password, database, config_file):
    """Build minimal configuration for connection testing."""
    from .config import AppConfig, DatabaseType, SqlServerConfig, SnowflakeConfig, TransferConfig
    
    # Create source config based on database type
    if db_type == DatabaseType.SQLSERVER:
        from .config import SqlServerConfig
        source_config = SqlServerConfig(
            host=host,
            port=port or 1433,
            username=username,
            password=password,
            database=database
        )
    else:
        from .config import DatabaseConfig
        source_config = DatabaseConfig(
            host=host,
            port=port or 5432,
            username=username,
            password=password,
            database=database
        )
    
    # Dummy Snowflake config (not used for listing tables)
    snowflake_config = SnowflakeConfig(
        account="dummy",
        user="dummy",
        password="dummy",
        role="dummy",
        warehouse="dummy",
        database="dummy"
    )
    
    transfer_config = TransferConfig(source_table="dummy")
    
    return AppConfig(
        database_type=db_type,
        source_db=source_config,
        snowflake=snowflake_config,
        transfer=transfer_config
    )

def _show_dry_run(config):
    """Show what would be transferred in a dry run."""
    console.print("\n[bold yellow]DRY RUN - No data will be transferred[/bold yellow]")
    
    table = Table(title="Transfer Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Database Type", config.database_type.value)
    table.add_row("Source Host", config.source_db.host)
    table.add_row("Source Database", config.source_db.database)
    table.add_row("Source Table", config.transfer.source_table)
    table.add_row("Snowflake Account", config.snowflake.account)
    table.add_row("Snowflake Database", config.snowflake.database)
    table.add_row("Snowflake Schema", config.snowflake.schema)
    table.add_row("Destination Table", config.transfer.destination_table)
    table.add_row("Transfer Mode", config.transfer.mode)
    table.add_row("Batch Size", str(config.transfer.batch_size))
    table.add_row("Max Workers", str(config.transfer.max_workers))
    
    console.print(table)

if __name__ == "__main__":
    app() 