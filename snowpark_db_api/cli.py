"""
Modern CLI for Snowpark DB API transfers.
Clean, simple commands
"""

import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
import yaml

from snowpark_db_api.config import Config, DatabaseType, TransferConfig
from snowpark_db_api.core import transfer_data, DataTransfer
from snowpark_db_api.connections import create_connection

# Initialize CLI app and console
app = typer.Typer(help="Snowpark DB-API Transfer Tool - Clean, simple data transfers")
console = Console()

@app.command()
def transfer(
    source_table: Optional[str] = typer.Option(None, "--source-table", envvar="SOURCE_TABLE", help="Source table name (optional when using --query)"),
    query: Optional[str] = typer.Option(None, "--query", help="Custom SQL query (overrides source-table)"),
    destination_table: Optional[str] = typer.Option(None, "--destination-table", help="Destination table name (auto-derived if not specified)"),
    save_metadata: bool = typer.Option(False, "--save-metadata", help="Save transfer metadata to JSON file"),
    config_file: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file")
):
    """Transfer data from source database to Snowflake."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Determine source and destination tables
        if query:
            # Extract destination from query alias if not explicitly provided
            if not destination_table:
                destination_table = _extract_destination_from_query(query)
            
            # Use a descriptive source name for logging when using query
            effective_source = source_table or f"<query: {query[:50]}...>"
        else:
            # Traditional table-to-table transfer
            if not source_table:
                console.print("[bold red]Error: --source-table is required when not using --query[/bold red]")
                raise typer.Exit(1)
            
            effective_source = source_table
            if not destination_table:
                clean_name = source_table.split('.')[-1] if '.' in source_table else source_table
                destination_table = clean_name.upper()
        
        # Create TransferConfig
        config.transfer = TransferConfig(
            source_table=effective_source,
            destination_table=destination_table,
            mode=config.transfer.mode,
            fetch_size=config.transfer.fetch_size,
            query_timeout=config.transfer.query_timeout,
            max_workers=config.transfer.max_workers,
            save_metadata=save_metadata
        )
        
        console.print(f"[bold green]Starting transfer...[/bold green]")
        if query:
            console.print(f"Query: {query[:100]}{'...' if len(query) > 100 else ''}")
        else:
            console.print(f"Source: {effective_source}")
        console.print(f"Destination: {config.snowflake.database}.{config.snowflake.db_schema}.{destination_table}")
        
        # Execute transfer
        success = transfer_data(config, query=query)
        
        if success:
            console.print("[bold green]✓ Transfer completed successfully![/bold green]")
        else:
            console.print("[bold red]✗ Transfer failed![/bold red]")
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise typer.Exit(1)

def _extract_destination_from_query(query: str) -> str:
    """Extract destination table name from query alias."""
    import re
    
    # Look for AS alias pattern at the end
    match = re.search(r'\)\s+AS\s+(\w+)$', query.strip(), re.IGNORECASE)
    if match:
        return match.group(1).upper()
    
    # Fallback to generic name
    return "QUERY_RESULT"

@app.command()
def test_connection():
    """Test database connections."""
    try:
        config = Config.from_env()
        
        console.print("Testing source database connection...")
        
        # Test source connection
        source_conn = create_connection(config.database_type, config.source)
        if source_conn:
            # Get connection info
            cursor = source_conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
            source_conn.close()
            
            console.print("✅ Connection successful!")
            
            # Display results
            table = Table(title="Connection Test Results")
            table.add_column("Property", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Database Type", str(config.database_type.value))
            table.add_row("Host", config.source.host)
            table.add_row("Database", config.source.database)
            table.add_row("Status", "✅ Connected")
            table.add_row("Version", version[:100] if version else "N/A")
            
            console.print(table)
        else:
            console.print("[bold red]❌ Connection failed[/bold red]")
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"[bold red]Connection error: {e}[/bold red]")
        raise typer.Exit(1)

@app.command()
def list_tables(
    schema_filter: Optional[str] = typer.Option(None, "--schema", help="Filter by schema name")
):
    """List available tables in the source database."""
    try:
        config = Config.from_env()
        
        # Create source database manager
        transfer = DataTransfer(config)
        if not transfer.setup_connections():
            console.print("[bold red]Failed to connect to database[/bold red]")
            raise typer.Exit(1)
        
        try:
            # Get table list - call the connection factory to get actual connection
            temp_connection = transfer.source_connection()
            cursor = temp_connection.cursor()
            
            if config.database_type == DatabaseType.SQLSERVER:
                query = """
                SELECT 
                    TABLE_SCHEMA as schema_name,
                    TABLE_NAME as table_name,
                    TABLE_TYPE as table_type
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                """
                if schema_filter:
                    query += f" AND TABLE_SCHEMA = '{schema_filter}'"
                query += " ORDER BY TABLE_SCHEMA, TABLE_NAME"
            else:
                # Generic query for other databases
                query = "SELECT table_schema, table_name, table_type FROM information_schema.tables"
                if schema_filter:
                    query += f" WHERE table_schema = '{schema_filter}'"
            
            cursor.execute(query)
            tables = cursor.fetchall()
            cursor.close()
            temp_connection.close()
            
            if not tables:
                console.print("[yellow]No tables found[/yellow]")
                return
            
            # Display results
            table = Table(title=f"Available Tables ({len(tables)} found)")
            table.add_column("Schema", style="cyan")
            table.add_column("Table Name", style="green")
            table.add_column("Type", style="blue")
            
            for row in tables:
                table.add_row(str(row[0]), str(row[1]), str(row[2]))
            
            console.print(table)
            
        finally:
            transfer.cleanup()
        
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise typer.Exit(1)

@app.command()
def preview(
    table_name: str = typer.Argument(..., help="Table name to preview"),
    rows: int = typer.Option(10, "--rows", help="Number of rows to preview")
):
    """Preview data from a table."""
    try:
        config = Config.from_env()
        
        transfer = DataTransfer(config)
        if not transfer.setup_connections():
            console.print("[bold red]Failed to connect to database[/bold red]")
            raise typer.Exit(1)
        
        try:
            # Execute preview query - call the connection factory to get actual connection
            temp_connection = transfer.source_connection()
            cursor = temp_connection.cursor()
            
            if config.database_type == DatabaseType.SQLSERVER:
                query = f"SELECT TOP {rows} * FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name} LIMIT {rows}"
            
            cursor.execute(query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            temp_connection.close()
            
            if not data:
                console.print("[yellow]No data found in table[/yellow]")
                return
            
            # Display results
            table = Table(title=f"Preview of {table_name} ({len(data)} rows)")
            
            for col in columns:
                table.add_column(col, style="cyan", max_width=30)
            
            for row in data:
                formatted_row = []
                for val in row:
                    if val is None:
                        formatted_row.append("[dim]NULL[/dim]")
                    else:
                        str_val = str(val)
                        if len(str_val) > 50:
                            str_val = str_val[:47] + "..."
                        formatted_row.append(str_val)
                
                table.add_row(*formatted_row)
            
            console.print(table)
                
        finally:
            transfer.cleanup()
            
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise typer.Exit(1)

@app.command()
def config_template(
    output_file: Path = typer.Option("config.yaml", "--output", "-o", help="Output file path")
):
    """Generate a configuration template."""
    template = {
        'database_type': 'sqlserver',
        'source': {
            'host': 'your-server.database.windows.net',
            'username': 'your-username', 
            'password': 'your-password',
            'database': 'your-database'
        },
        'snowflake': {
            'account': 'your-account',
            'user': 'your-user',
            'password': 'your-password',
            'role': 'ACCOUNTADMIN',
            'warehouse': 'COMPUTE_WH',
            'database': 'your-database'
        },
        'transfer': {
            'source_table': 'your-table',
            'mode': 'overwrite'
        }
    }
    
    with open(output_file, 'w') as f:
        yaml.dump(template, f, default_flow_style=False)
    
    console.print(f"[green]Template saved to: {output_file}[/green]")

@app.command()
def query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    limit: int = typer.Option(100, "--limit", help="Limit number of results")
):
    """Execute a SQL query on the source database."""
    try:
        config = Config.from_env()
        
        transfer = DataTransfer(config)
        if not transfer.setup_connections():
            console.print("[bold red]Failed to connect to database[/bold red]")
            raise typer.Exit(1)
        
        try:
            # Execute query - call the connection factory to get actual connection
            temp_connection = transfer.source_connection()
            cursor = temp_connection.cursor()
            
            # Add limit to query if not present
            if limit and "LIMIT" not in sql.upper() and "TOP" not in sql.upper():
                if config.database_type == DatabaseType.SQLSERVER:
                    # Add TOP clause for SQL Server
                    if sql.strip().upper().startswith("SELECT"):
                        sql = sql.replace("SELECT", f"SELECT TOP {limit}", 1)
                else:
                    sql += f" LIMIT {limit}"
            
            cursor.execute(sql)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            cursor.close()
            temp_connection.close()
            
            if not data:
                console.print("[yellow]No results returned[/yellow]")
            return
        
            # Display results
            table = Table(title=f"Query Results ({len(data)} rows)")
            
            for col in columns:
                table.add_column(col, style="cyan", max_width=30)
            
            for row in data:
                formatted_row = []
                for val in row:
                    if val is None:
                        formatted_row.append("[dim]NULL[/dim]")
                    else:
                        str_val = str(val)
                        if len(str_val) > 100:
                            str_val = str_val[:97] + "..."
                        formatted_row.append(str_val)
                
                table.add_row(*formatted_row)
            
            console.print(table)
            
        finally:
            transfer.cleanup()
        
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise typer.Exit(1)

if __name__ == "__main__":
    app() 