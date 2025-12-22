"""
Database tools for MCP server.
"""

import os
import tempfile
import webbrowser
from typing import List, Dict, Any, Optional

from mcp.server.fastmcp import FastMCP

# Create an MCP server instance
mcp = FastMCP("DatabaseTools")

# Placeholder for database manager and config manager instances
# These will be set by the server initialization
db_manager = None
config_manager = None

def set_managers(database_manager, configuration_manager):
    """Set the database and configuration managers for the tools."""
    global db_manager, config_manager
    db_manager = database_manager
    config_manager = configuration_manager


# =============================================================================
# 1. 连接管理工具 (Connection Management Tools)
# =============================================================================

@mcp.tool()
def add_database_connection(
    name: str,
    db_type: str,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    save_to_config: bool = False
) -> bool:
    """
    Add a new database connection.
    
    Args:
        name: Connection identifier (must be unique)
        db_type: Type of database (mysql, oracle, postgresql, sqlite, sqlserver)
        host: Database host
        port: Database port
        username: Database username
        password: Database password
        database: Database name/schema
        save_to_config: Whether to save connection to configuration file
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    # Add connection to database manager
    success = db_manager.add_connection(
        name=name,
        db_type=db_type,
        host=host,
        port=port,
        username=username,
        password=password,
        database=database
    )
    
    # Save to config if requested and connection was successful
    if success and save_to_config:
        config_manager.add_connection({
            "name": name,
            "type": db_type,
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database,
            "enabled": True
        })
    
    return success


@mcp.tool()
def remove_database_connection(name: str, remove_from_config: bool = False) -> bool:
    """
    Remove a database connection.
    
    Args:
        name: Connection identifier
        remove_from_config: Whether to also remove from configuration
        
    Returns:
        bool: True if removed, False if not found
    """
    # Remove from database manager
    success = db_manager.remove_connection(name)
    
    # Also remove from config if requested
    if success and remove_from_config:
        config_manager.remove_connection(name)
    
    return success


@mcp.tool()
def list_database_connections() -> Dict[str, str]:
    """
    List all database connections.
    
    Returns:
        Dict[str, str]: Mapping of connection names to database types
    """
    return db_manager.list_connections()


# =============================================================================
# 2. 配置管理工具 (Configuration Management Tools)
# =============================================================================

@mcp.tool()
def list_configured_connections() -> List[Dict[str, Any]]:
    """
    List all configured database connections from the configuration file.
    
    Returns:
        List of configured connections with their details
    """
    return config_manager.get_connections()


@mcp.tool()
def enable_configured_connection(name: str) -> bool:
    """
    Enable a configured connection so it will be automatically connected on startup.
    
    Args:
        name: Name of the connection to enable
        
    Returns:
        True if enabled successfully, False if connection not found
    """
    return config_manager.enable_connection(name)


@mcp.tool()
def disable_configured_connection(name: str) -> bool:
    """
    Disable a configured connection so it won't be automatically connected on startup.
    
    Args:
        name: Name of the connection to disable
        
    Returns:
        True if disabled successfully, False if connection not found
    """
    return config_manager.disable_connection(name)


@mcp.tool()
def reload_configurations() -> Dict[str, bool]:
    """
    Reload database connections from configuration file.
    
    Returns:
        Dict mapping connection names to connection success status
    """
    config_manager.load_config()
    return db_manager.initialize_from_config(config_manager.get_enabled_connections())


# =============================================================================
# 3. 表结构工具 (Table Structure Tools)
# =============================================================================

@mcp.tool()
def list_tables(connection_name: str, pattern: Optional[str] = None) -> Optional[List[str]]:
    """
    List all table names for the user in the specified database connection.
    
    Args:
        connection_name: Name of the database connection
        pattern: Optional pattern to filter table names (supports % as wildcard)
        
    Returns:
        List of table names or None if error occurred
    """
    return db_manager.list_tables(connection_name, pattern)


@mcp.tool()
def get_table_structure(connection_name: str, table_name: str, pattern: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    Get structure information for a specific table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        pattern: Optional pattern to filter column names (supports % as wildcard)
        
    Returns:
        List[Dict[str, Any]]: Column information or None if error occurred
    """
    return db_manager.get_table_structure(connection_name, table_name, pattern)


@mcp.tool()
def create_table(connection_name: str, table_name: str, 
                columns: List[Dict[str, Any]], table_comment: Optional[str] = None) -> bool:
    """
    Create a new table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table to create
        columns: List of dictionaries defining columns. Each dict should have:
                - name: column name
                - type: column type
                - nullable: whether column can be null (optional, defaults to True)
                - primary_key: whether column is primary key (optional)
                - default: default value (optional)
                - comment: column comment (optional)
        table_comment: Comment for the table (optional)
                
    Returns:
        bool: True if successful, False otherwise
    """
    return db_manager.create_table(connection_name, table_name, columns, table_comment)


@mcp.tool()
def drop_table(connection_name: str, table_name: str, 
              cascade: bool = False) -> bool:
    """
    Drop a table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table to drop
        cascade: Whether to drop dependent objects (CASCADE CONSTRAINTS in Oracle)
        
    Returns:
        bool: True if successful, False otherwise
    """
    return db_manager.drop_table(connection_name, table_name, cascade)


@mcp.tool()
def alter_table(connection_name: str, table_name: str,
                operations: List[Dict[str, Any]]) -> bool:
    """
    Alter table structure with various operations.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table to alter
        operations: List of operations to perform. Each operation is a dict with:
                   - operation: Type of operation ('add_column', 'drop_column', 'modify_column', 'rename_column')
                   - For add_column: name, type, [length], [nullable], [default], [comment]
                   - For drop_column: name
                   - For modify_column: name, type, [length], [nullable], [default], [comment]
                   - For rename_column: old_name, new_name

    Returns:
        bool: True if successful, False otherwise
    """
    return db_manager.alter_table(connection_name, table_name, operations)


# =============================================================================
# 4. 数据查询工具 (Data Query Tools)
# =============================================================================

@mcp.tool()
def execute_query(connection_name: str, query: str, commit: bool = False) -> Optional[List[Dict[str, Any]]]:
    """
    Execute a query on a specific database.
    
    Args:
        connection_name: Name of the database connection
        query: SQL query to execute
        commit: Whether to commit the transaction (useful for INSERT/UPDATE/DELETE)
        
    Returns:
        List[Dict[str, Any]]: Query results or None if error occurred
    """
    return db_manager.execute_query(connection_name, query, commit)


@mcp.tool()
def select_data(connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
               limit: Optional[int] = None) -> Optional[str]:
    """
    Select data from a specific table and format using smart table formatting.
    Automatically chooses the best format based on column count for optimal viewing experience.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        limit: Optional limit for number of rows returned
        
    Returns:
        str: Formatted table string in the most appropriate format
    """
    data = db_manager.select_data(connection_name, table_name, conditions, limit)
    if data is not None:
        # Use smart formatting that automatically selects the best display format
        # based on the number of columns in the result set
        return db_manager.format_as_smart_table(data, connection_name, table_name)
    return None


# =============================================================================
# 5. 数据操作工具 (Data Manipulation Tools)
# =============================================================================

@mcp.tool()
def insert_data(connection_name: str, table_name: str, data: Dict[str, Any]) -> bool:
    """
    Insert data into a specific table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        data: Dictionary of column-value pairs to insert
        
    Returns:
        bool: True if successful, False otherwise
    """
    return db_manager.insert_data(connection_name, table_name, data)


@mcp.tool()
def update_data(connection_name: str, table_name: str, data: Dict[str, Any], 
               conditions: Optional[Dict[str, Any]] = None) -> int:
    """
    Update data in a specific table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        data: Dictionary of column-value pairs to update
        conditions: Optional dictionary of column-value pairs for WHERE clause
        
    Returns:
        int: Number of rows affected, -1 if error occurred
    """
    return db_manager.update_data(connection_name, table_name, data, conditions)


@mcp.tool()
def delete_data(connection_name: str, table_name: str, 
               conditions: Optional[Dict[str, Any]] = None) -> int:
    """
    Delete data from a specific table.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        
    Returns:
        int: Number of rows affected, -1 if error occurred
    """
    return db_manager.delete_data(connection_name, table_name, conditions)


# =============================================================================
# 6. 高级查询工具 (Advanced Query Tools)
# =============================================================================

@mcp.tool()
def select_data_smart(connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None,
                      limit: Optional[int] = None, max_columns: int = 10) -> Optional[str]:
    """
    Select data from a specific table and format using smart table formatting.
    Automatically chooses the best format based on column count.

    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        limit: Optional limit for number of rows returned
        max_columns: Maximum number of columns to show in horizontal format

    Returns:
        str: Formatted table string in the most appropriate format
    """
    data = db_manager.select_data(connection_name, table_name, conditions, limit)
    if data is not None:
        return db_manager.format_as_smart_table(data, connection_name, table_name, max_columns)
    return None


@mcp.tool()
def select_data_paged(connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None,
                      limit: Optional[int] = None, columns_per_page: int = 8, rows_per_page: int = 20) -> Optional[str]:
    """
    Select data from a specific table and format as paged table.
    Shows only a subset of columns at a time, useful for tables with many columns.

    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        limit: Optional limit for number of rows returned
        columns_per_page: Number of columns to show per page
        rows_per_page: Number of rows to show per page

    Returns:
        str: Formatted paged table string
    """
    data = db_manager.select_data(connection_name, table_name, conditions, limit)
    if data is not None:
        return db_manager.format_as_paged_table(data, connection_name, table_name,
                                                columns_per_page, rows_per_page)
    return None


@mcp.tool()
def select_data_summary(connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None,
                        limit: Optional[int] = None, max_columns: int = 6, sample_rows: int = 5) -> Optional[str]:
    """
    Select data from a specific table and format as summary table.
    Shows only the most important columns and a sample of rows.

    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        limit: Optional limit for number of rows returned
        max_columns: Maximum number of columns to show
        sample_rows: Number of sample rows to show

    Returns:
        str: Formatted summary table string
    """
    data = db_manager.select_data(connection_name, table_name, conditions, limit)
    if data is not None:
        return db_manager.format_as_summary_table(data, connection_name, table_name,
                                                  max_columns, sample_rows)
    return None


@mcp.tool()
def select_data_html(connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None,
                     limit: Optional[int] = None) -> Optional[str]:
    """
    Select data from a specific table and format as HTML table for browser display.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        limit: Optional limit for number of rows returned
        
    Returns:
        str: Path to the HTML file with the table data
    """
    data = db_manager.select_data(connection_name, table_name, conditions, limit)
    if data is not None:
        html_content = db_manager.format_as_html_table(data, connection_name, table_name)
        # Create a temporary HTML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
            f.write(html_content)
            temp_filename = f.name

        # Open in browser
        webbrowser.open('file://' + os.path.abspath(temp_filename))

        # Return the path to the file
        return temp_filename
    return None