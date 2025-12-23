

g# Yuerenge Database MCP

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

## Quick Start

1. **Install the package**:
   ```bash
   pip install yuerenge-database-mcp
   ```

2. **Create a configuration file** (optional, uses defaults if not provided):
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

3. **Run the server**:
   ```bash
   yuerenge-database-mcp
   ```

4. **Or run with a specific configuration**:
   ```bash
   DATABASE_CONFIG_PATH=/path/to/your/config.json yuerenge-database-mcp
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

## Environment Variables

The server supports the following environment variables:

- `DATABASE_CONFIG_PATH`: Specifies the path to the database configuration file
- `ERROR_LOG_PATH`: Specifies the directory where error logs will be stored (default: ./error_logs)

## Tools Provided

This MCP server provides the following tools:

1. Connection Management Tools
   - add_database_connection: Add a database connection
   - remove_database_connection: Remove a database connection
   - list_database_connections: List all active connections

2. Configuration Management Tools
   - list_configured_connections: List all connections from the configuration file
   - enable_configured_connection: Enable a connection in the configuration
   - disable_configured_connection: Disable a connection in the configuration
   - reload_configurations: Reload connections from the configuration file

3. Table Structure Tools
   - list_tables: List all tables in the database
   - get_table_structure: Get table structure information
   - create_table: Create a new table
   - drop_table: Drop a table
   - alter_table: Alter table structure

4. Data Query Tools
   - execute_query: Execute raw SQL queries
   - select_data: Select data from a table and format it intelligently

5. Data Manipulation Tools
   - insert_data: Insert data into a table
   - batch_insert_data: Batch insert data
   - update_data: Update data in a table
   - batch_update_data: Batch update data
   - delete_data: Delete data from a table
   - batch_delete_data: Batch delete data

6. Advanced Query Tools
   - select_data_smart: Format table data intelligently
   - select_data_paged: Display table data in pages
   - select_data_summary: Display summary of table data
   - select_data_html: Display table data as HTML and open in browser

## Usage Examples

### Adding a Database Connection
```python
# Using the MCP client to call the tool
add_database_connection(
    name="mysql_test",
    db_type="mysql",
    host="localhost",
    port=3306,
    username="root",
    password="password",
    database="testdb",
    save_to_config=True
)
```

### Querying Data
```python
# Select data from a table
select_data(connection_name="mysql_test", table_name="users", conditions={"age": 25}, limit=10)
```

### Inserting Data
```python
# Insert a single record
insert_data(connection_name="mysql_test", table_name="users", data={
    "name": "John Doe",
    "age": 30,
    "email": "john@example.com"
})
```

### Batch Operations
```python
# Batch insert multiple records
batch_insert_data(connection_name="mysql_test", table_name="users", data_list=[
    {"name": "Jane Smith", "age": 28, "email": "jane@example.com"},
    {"name": "Bob Johnson", "age": 32, "email": "bob@example.com"}
])
```

### Advanced Queries
```python
# Smart formatting of query results
select_data_smart(connection_name="mysql_test", table_name="users", max_columns=8)

# Paged display of large datasets
select_data_paged(connection_name="mysql_test", table_name="large_table", 
                  columns_per_page=5, rows_per_page=15)

# Display as HTML and open in browser
select_data_html(connection_name="mysql_test", table_name="users")
```

## Best Practices

1. **Connection Management**
   - Use configuration files to manage database connections, avoiding hardcoded sensitive information
   - Set appropriate connection pool parameters for performance optimization
   - Regularly use the `reload_configurations` tool to update connection settings

2. **Data Operations**
   - For bulk data insertion, prefer the `batch_insert_data` tool
   - Use the `conditions` parameter for precise queries to avoid full table scans
   - Before executing update or delete operations, first use `select_data` to verify conditions

3. **Data Presentation**
   - For tables with many columns, use `select_data_paged` or `select_data_summary` tools
   - For data analysis scenarios, use `select_data_html` for better visualization
   - Choose appropriate `limit` parameters based on data volume to prevent memory overflow