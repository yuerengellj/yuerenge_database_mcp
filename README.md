# Yuerenge Database MCP

A database management tool based on the Model Context Protocol (MCP).

## Features

- Support for multiple databases (MySQL, Oracle)
- Connection management
- Table structure operations
- Data querying and manipulation
- Configuration management

## Installation

```bash
pip install yuerenge-database-mcp
```

## Usage

After installation, you can run the database MCP server:

```bash
yuerenge-database-mcp
```

Or with a specific configuration file:

```bash
DATABASE_CONFIG_PATH=/path/to/your/config.json yuerenge-database-mcp
```

## Configuration

The tool uses a JSON configuration file to store database connection information. Example:

```json
{
  "connections": [
    {
      "name": "my_mysql_db",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "username": "user",
      "password": "password",
      "database": "mydb",
      "enabled": true
    }
  ]
}
```

## Tools Provided

This MCP server provides the following tools:

1. Connection Management Tools
   - add_database_connection
   - remove_database_connection
   - list_database_connections

2. Configuration Management Tools
   - list_configured_connections
   - enable_configured_connection
   - disable_configured_connection
   - reload_configurations

3. Table Structure Tools
   - list_tables
   - get_table_structure
   - create_table
   - drop_table

4. Data Query Tools
   - execute_query
   - select_data

5. Data Manipulation Tools
   - insert_data
   - update_data
   - delete_data

6. Advanced Query Tools
   - select_data_smart
   - select_data_paged
   - select_data_summary
   - select_data_html
