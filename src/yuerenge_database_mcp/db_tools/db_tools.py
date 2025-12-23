"""
Database tools for MCP server.

This module defines the MCP tools that provide database management functionality.
The tools are organized into several categories:
1. Connection Management Tools - for adding, removing, and listing database connections
2. Configuration Management Tools - for managing connection configurations
3. Table Structure Tools - for table operations like listing, creating, and dropping
4. Data Query Tools - for executing queries and selecting data
5. Data Manipulation Tools - for inserting, updating, and deleting data
6. Advanced Query Tools - for smart formatting and specialized queries

Each tool is decorated with @mcp.tool() to make it available through the MCP protocol.
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
    """Set the database and configuration managers for the tools.
    
    This function is called during server initialization to provide
    the tools with access to the database and configuration managers.
    
    Args:
        database_manager: Instance of DatabaseManager
        configuration_manager: Instance of DatabaseConfigManager
    """
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
    Add a new database connection and optionally save to configuration.
    
    This tool creates a new database connection and adds it to the connection pool.
    If save_to_config is True, the connection details are also saved to the configuration file.
    
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
    Remove a database connection and optionally from configuration.
    
    This tool removes a database connection from the connection pool.
    If remove_from_config is True, the connection is also removed from the configuration file.
    
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
    List all active database connections.
    
    Returns a mapping of connection names to their database types.
    Only connections that are currently active in the connection pool are listed.
    
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
    
    Returns detailed information about each connection defined in the configuration file,
    including connection parameters, enabled status, and pool settings.
    
    Returns:
        List of configured connections with their details
    """
    return config_manager.get_connections()


@mcp.tool()
def enable_configured_connection(name: str) -> bool:
    """
    Enable a configured connection so it will be automatically connected on startup.
    
    This tool updates the configuration to enable a connection, meaning it will
    be automatically connected when the server starts up or when reload_configurations is called.
    
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
    
    This tool updates the configuration to disable a connection, meaning it will
    not be automatically connected when the server starts up or when reload_configurations is called.
    
    Args:
        name: Name of the connection to disable
        
    Returns:
        True if disabled successfully, False if connection not found
    """
    return config_manager.disable_connection(name)


@mcp.tool()
def reload_configurations() -> Dict[str, bool]:
    """
    Reload database connections from configuration file and initialize them.
    
    This tool reloads the configuration file and initializes all enabled connections.
    It attempts to connect to each enabled database and returns the status of each connection attempt.
    This is useful for applying configuration changes without restarting the server.
    
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
    
    Optionally filter table names using a pattern where % acts as a wildcard.
    This allows for searching tables that match a specific naming convention.
    
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
    Get structure information for a specific table including column details.
    
    Returns detailed information about each column in the table such as name, type,
    nullability, default value, and any comments. Optionally filter columns using a pattern.
    
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
    Create a new table with specified columns and optional table comment.
    
    Each column is defined as a dictionary with properties like name, type, nullability, etc.
    The tool handles database-specific syntax differences automatically through adapters.
    
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
    Drop a table and optionally its dependent objects.
    
    Use the cascade parameter to drop dependent objects like foreign keys and views
    that reference the table. Behavior may vary depending on the database type.
    
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
    Alter table structure with various operations like adding, dropping, or modifying columns.
    
    Supports multiple types of operations in a single call. Each operation is defined as a
    dictionary specifying the operation type and required parameters.
    
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
    Execute a raw SQL query on a specific database connection.
    
    Use this tool for complex queries that are not covered by the specialized tools.
    Set commit=True for INSERT, UPDATE, or DELETE operations that need to be committed.
    The query is executed with proper parameterization to prevent SQL injection.
    
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
    Supports conditional selection and row limiting. The data is formatted for readability.
    
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
    Insert a single record into a specific table with proper parameterization.
    
    Handles database-specific data type conversions and date formatting automatically.
    The tool uses parameterized queries to prevent SQL injection attacks.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        data: Dictionary of column-value pairs to insert
        
    Returns:
        bool: True if successful, False otherwise
    """
    return db_manager.insert_data(connection_name, table_name, data)


@mcp.tool()
def batch_insert_data(connection_name: str, table_name: str, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Insert multiple records into a specific table efficiently in a batch operation.
    
    Processes multiple records in a single operation for better performance than
    individual inserts. Returns detailed results about successful and failed inserts.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        data_list: List of dictionaries containing column-value pairs to insert
        
    Returns:
        Dict containing success count, failure count, and details of failures
    """
    return db_manager.batch_insert_data(connection_name, table_name, data_list)


@mcp.tool()
def update_data(connection_name: str, table_name: str, data: Dict[str, Any], 
               conditions: Optional[Dict[str, Any]] = None) -> int:
    """
    Update records in a specific table based on specified conditions.
    
    Updates matching records with the provided data. If no conditions are provided,
    all records in the table will be updated (use with caution).
    Returns the number of affected rows.
    
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
def batch_update_data(connection_name: str, table_name: str, data_list: List[Dict[str, Any]], 
                     conditions_list: List[Optional[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Update multiple records in a specific table with different data and conditions for each update.
    
    Processes multiple update operations in a batch. Each update operation has its own
    data to update and conditions to match. The lists must have the same length.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        data_list: List of dictionaries containing column-value pairs to update
        conditions_list: List of dictionaries containing WHERE clause conditions for each update
        
    Returns:
        Dict containing success count, failure count, and details of failures
    """
    return db_manager.batch_update_data(connection_name, table_name, data_list, conditions_list)


@mcp.tool()
def delete_data(connection_name: str, table_name: str, 
               conditions: Optional[Dict[str, Any]] = None) -> int:
    """
    Delete records from a specific table based on specified conditions.
    
    Deletes matching records from the table. If no conditions are provided,
    all records in the table will be deleted (use with extreme caution).
    Returns the number of affected rows.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions: Optional dictionary of column-value pairs for WHERE clause
        
    Returns:
        int: Number of rows affected, -1 if error occurred
    """
    return db_manager.delete_data(connection_name, table_name, conditions)


@mcp.tool()
def batch_delete_data(connection_name: str, table_name: str, 
                     conditions_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Delete multiple sets of records from a specific table based on different conditions for each deletion.
    
    Processes multiple delete operations in a batch. Each operation has its own conditions
    to determine which records to delete. This is more efficient than individual deletions.
    
    Args:
        connection_name: Name of the database connection
        table_name: Name of the table
        conditions_list: List of dictionaries containing WHERE clause conditions for each delete
        
    Returns:
        Dict containing success count, failure count, and details of failures
    """
    return db_manager.batch_delete_data(connection_name, table_name, conditions_list)

# =============================================================================
# 6. 高级查询工具 (Advanced Query Tools)
# =============================================================================

@mcp.tool()
def select_data_smart(connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None,
                      limit: Optional[int] = None, max_columns: int = 10) -> Optional[str]:
    """
    Select data from a specific table and format using smart table formatting based on column count.
    
    Automatically chooses the best format (horizontal, vertical, or paged) based on the number of columns
    to ensure optimal readability. Particularly useful for tables with many columns.
    
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
    Select data from a specific table and format as paged table for tables with many columns or rows.
    
    Particularly useful for tables with many columns, showing only a subset at a time to improve readability.
    Also useful for large result sets that would be difficult to view in a single display.
    
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
    Select data from a specific table and format as summary table showing key information only.
    
    Displays only the most important columns and a sample of rows, useful for getting
    a quick overview of large tables without overwhelming the display with too much data.
    
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
    Select data from a specific table and format as HTML table for browser display with interactive features.
    
    Creates an HTML file with the table data and automatically opens it in the default browser.
    Provides better visualization for large datasets and includes search/filter capabilities in some cases.
    
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