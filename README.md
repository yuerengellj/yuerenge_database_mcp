# Yuerenge Database MCP

A database management tool based on the Model Context Protocol (MCP).

## Features

- Support for multiple databases (MySQL, Oracle, PostgreSQL, SQLite, SQL Server)
- Connection management
- Table structure operations
- Data querying and manipulation
- Advanced configuration management with validation
- Database adapter pattern for easy extension

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

The tool uses a JSON configuration file to store database connection information. 

Configuration priority (highest to lowest):
1. Environment variable `DATABASE_CONFIG_PATH`
2. Default configuration file (`config/database_config.json`)

### Configuration Validation

The configuration manager validates all connection configurations to ensure:
- All required fields are present
- Port numbers are valid (1-65535)
- Database types are supported
- Enabled flags are boolean values

### Supported Database Types

- MySQL
- Oracle
- PostgreSQL
- SQLite
- SQL Server

### Connection Pool Settings

You can configure connection pool settings for each database connection:

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
      "enabled": true,
      "pool_size": 5,
      "max_overflow": 10,
      "pool_timeout": 30,
      "pool_recycle": 3600
    }
  ]
}
```

Available connection pool settings:
- `pool_size`: The number of connections to keep open inside the connection pool (default: 10)
- `max_overflow`: The number of connections to allow in connection pool "overflow" (default: 20)
- `pool_timeout`: The number of seconds to wait before giving up on getting a connection from the pool (default: 30)
- `pool_recycle`: Number of seconds after which to recreate idle connections (default: 3600)

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