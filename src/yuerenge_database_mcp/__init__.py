"""
MCP Database Tools Server
"""

import sys
import os
from mcp.server.fastmcp import FastMCP
from .config.config_manager import DatabaseConfigManager
from .db_tools.database_manager import DatabaseManager

# Create an MCP server with custom configuration
# You can specify the database config path like this:
# {
#   "mcpServers": {
#     "database-tools": {
#       "command": "python",
#       "args": ["db_server.py"],
#       "env": {
#         "DATABASE_CONFIG_PATH": "D:\\File\\Workspace\\mcp configs\\yuerenge-database-mcp\\database_config.json"
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
    # 4. Data Query Tools
    execute_query,
    select_data,
    # 5. Data Manipulation Tools
    insert_data,
    update_data,
    delete_data,
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

# 4. Data Query Tools
mcp.add_tool(execute_query)
mcp.add_tool(select_data)

# 5. Data Manipulation Tools
mcp.add_tool(insert_data)
mcp.add_tool(update_data)
mcp.add_tool(delete_data)

# 6. Advanced Query Tools
mcp.add_tool(select_data_smart)
mcp.add_tool(select_data_paged)
mcp.add_tool(select_data_summary)
mcp.add_tool(select_data_html)

def main():
    """Main entry point for the database MCP server."""
    print("Starting Database MCP Server...", file=sys.stderr)
    if config_file_path:
        print(f"Using config file: {config_file_path}", file=sys.stderr)
    else:
        print("Using default config location.", file=sys.stderr)
    mcp.run(transport="stdio")

# Run with streamable HTTP transport
if __name__ == "__main__":
    main()