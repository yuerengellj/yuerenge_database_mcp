"""
MCP Database Tools Server

This module implements a Model Context Protocol (MCP) server for database management.
It provides a comprehensive set of tools for managing database connections, executing
queries, manipulating data, and handling table structures across multiple database types.

The server supports:
- Multiple database types (MySQL, Oracle, PostgreSQL, SQLite, SQL Server)
- Connection pooling and management
- Configuration via JSON files with environment variable override
- Graceful shutdown handling
- Data formatting in multiple formats (table, HTML, paged, etc.)
- Batch operations for improved performance
- Smart data display based on content size

Configuration:
- The server can load configuration from a JSON file specified by the
  DATABASE_CONFIG_PATH environment variable
- If not specified, it defaults to config/database_config.json
- Configuration includes connection details, pooling settings, and enabled status

Usage:
- Run the server with `python -m yuerenge_database_mcp`
- Or use the installed CLI command `yuerenge-database-mcp`
- Connect using an MCP client to access the database tools
"""

import os
import sys
import threading
import time

from mcp.server.fastmcp import FastMCP

from .config.config_manager import DatabaseConfigManager
from .db_tools.database_manager import DatabaseManager
from .server_lifecycle import get_lifecycle_manager, add_cleanup_callback

# Create an MCP server with custom configuration
# You can specify the database config path like this:
# {
#   "mcpServers": {
#     "database-tools": {
#       "command": "python",
#       "args": ["db_server.py"],
#       "env": {
#       "DATABASE_CONFIG_PATH": "D:\\File\\Workspace\\mcp configs\\yuerenge-database-mcp\\database_config.json"
#       }
#     }
#   }
# }
mcp = FastMCP("DatabaseServer", json_response=True)

# Get config file path from environment variable DATABASE_CONFIG_PATH if set, otherwise use default
config_file_path = os.environ.get('DATABASE_CONFIG_PATH')

# Initialize database manager and config manager with specified config file
# If DATABASE_CONFIG_PATH is not set, DatabaseConfigManager will use default or DB_CONFIG_FILE env var
config_manager = DatabaseConfigManager(config_file=config_file_path)
db_manager = DatabaseManager()

# Initialize connections from config
init_results = db_manager.initialize_from_config(config_manager.get_enabled_connections())

# Import database tool functions
# 1. Connection Management Tools
from .db_tools.db_tools import (
    add_database_connection,
    remove_database_connection,
    list_database_connections,
    # 2. Configuration Management Tools
    list_configured_connections,
    enable_configured_connection,
    disable_configured_connection,
    reload_configurations,
    # 3. Table Structure Tools
    list_tables,
    get_table_structure,
    create_table,
    drop_table,
    alter_table,
    # 4. Data Query Tools
    execute_query,
    select_data,
    # 5. Data Manipulation Tools
    insert_data,
    batch_insert_data,
    update_data,
    batch_update_data,
    delete_data,
    batch_delete_data,
    # 6. Advanced Query Tools
    select_data_smart,
    select_data_paged,
    select_data_summary,
    select_data_html
)

# Set the managers in the db_tools module
from .db_tools.db_tools import set_managers
set_managers(db_manager, config_manager)

# Register all tools with the server
# 1. Connection Management Tools
mcp.add_tool(add_database_connection)
mcp.add_tool(remove_database_connection)
mcp.add_tool(list_database_connections)

# 2. Configuration Management Tools
mcp.add_tool(list_configured_connections)
mcp.add_tool(enable_configured_connection)
mcp.add_tool(disable_configured_connection)
mcp.add_tool(reload_configurations)

# 3. Table Structure Tools
mcp.add_tool(list_tables)
mcp.add_tool(get_table_structure)
mcp.add_tool(create_table)
mcp.add_tool(drop_table)
mcp.add_tool(alter_table)

# 4. Data Query Tools
mcp.add_tool(execute_query)
mcp.add_tool(select_data)

# 5. Data Manipulation Tools
mcp.add_tool(insert_data)
mcp.add_tool(batch_insert_data)
mcp.add_tool(update_data)
mcp.add_tool(batch_update_data)
mcp.add_tool(delete_data)
mcp.add_tool(batch_delete_data)

# 6. Advanced Query Tools
mcp.add_tool(select_data_smart)
mcp.add_tool(select_data_paged)
mcp.add_tool(select_data_summary)
mcp.add_tool(select_data_html)


def cleanup_database_connections():
    """Clean up all database connections.
    
    This function disposes of all active database connections to ensure
    proper resource cleanup during server shutdown. It should be called
    as part of the shutdown process to prevent resource leaks.
    """
    if db_manager:
        try:
            db_manager.dispose_all_connections()
        except Exception as e:
            print(f"Error disposing database connections: {e}", file=sys.stderr)


async def shutdown_handler():
    """Handle server shutdown gracefully.
    
    This async function performs the complete shutdown sequence:
    1. Executes all registered cleanup callbacks
    2. Cleans up database connections
    3. Logs shutdown completion
    """
    lifecycle_manager = get_lifecycle_manager()
    print("Shutting down database MCP server...", file=sys.stderr)
    await lifecycle_manager.cleanup()
    cleanup_database_connections()
    print("Database MCP server shutdown complete.", file=sys.stderr)


def main():
    """Main entry point for the database MCP server.
    
    This function initializes the server, sets up signal handlers for
    graceful shutdown, starts the MCP server, and manages the server lifecycle.
    It handles:
    - Configuration loading and validation
    - Lifecycle management setup
    - Signal handling for shutdown
    - Stdin monitoring for client disconnection
    - Thread management for server operation
    - Graceful cleanup on shutdown
    """
    print("Starting Database MCP Server...", file=sys.stderr)
    if config_file_path:
        print(f"Using config file: {config_file_path}", file=sys.stderr)
    else:
        print("Using default config location.", file=sys.stderr)

    # Setup lifecycle management
    lifecycle_manager = get_lifecycle_manager()
    add_cleanup_callback(cleanup_database_connections)
    lifecycle_manager.setup_signal_handlers()

    # Flag to indicate if server should stop
    should_stop = threading.Event()

    # Override the signal handlers to also set our flag
    def signal_handler(signum, frame):
        signame = signal.Signals(signum).name if 'signal' in globals() else signum
        print(f"Received signal {signame}, initiating shutdown...", file=sys.stderr)
        should_stop.set()

    # Set up signal handlers
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Thread function to monitor stdin
    def stdin_monitor():
        while not should_stop.is_set():
            try:
                if sys.stdin.closed:
                    print("Stdin closed, initiating shutdown...", file=sys.stderr)
                    should_stop.set()
                    break
                time.sleep(0.5)
            except Exception:
                print("Error reading stdin, assuming closed...", file=sys.stderr)
                should_stop.set()
                break

    # Start stdin monitoring thread
    stdin_thread = threading.Thread(target=stdin_monitor, daemon=True)
    stdin_thread.start()

    # Run the server in a separate thread
    def run_server():
        try:
            mcp.run(transport="stdio")
        except Exception as e:
            print(f"Error during server execution: {e}", file=sys.stderr)
        finally:
            should_stop.set()

    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    # Wait for shutdown signal
    try:
        while not should_stop.wait(0.1):
            pass
    except KeyboardInterrupt:
        print("Keyboard interrupt received, shutting down...", file=sys.stderr)
        should_stop.set()

    # Wait for threads to finish
    server_thread.join(timeout=2)
    stdin_thread.join(timeout=1)

    # Perform cleanup
    import asyncio
    try:
        asyncio.run(shutdown_handler())
    except RuntimeError:
        # If there's already an event loop, create a new task
        loop = asyncio.new_event_loop()
        loop.run_until_complete(shutdown_handler())

# Run with streamable HTTP transport
if __name__ == "__main__":
    main()