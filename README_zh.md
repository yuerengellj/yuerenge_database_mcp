# Yuerenge 数据库 MCP

一个基于模型上下文协议（MCP）的数据库管理工具。

## 功能特性

- 支持多种数据库（MySQL、Oracle）
- 连接管理
- 表结构操作
- 数据查询与操作
- 配置管理

## 安装

```bash
pip install yuerenge-database-mcp
```

## 使用方法

安装完成后，您可以运行数据库 MCP 服务器：

```bash
yuerenge-database-mcp
```

或者使用特定的配置文件：

```bash
DATABASE_CONFIG_PATH=/path/to/your/config.json yuerenge-database-mcp
```

## 配置

该工具使用 JSON 配置文件来存储数据库连接信息。示例：

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

## 提供的工具

此 MCP 服务器提供以下工具：

1. 连接管理工具
   - add_database_connection
   - remove_database_connection
   - list_database_connections

2. 配置管理工具
   - list_configured_connections
   - enable_configured_connection
   - disable_configured_connection
   - reload_configurations

3. 表结构工具
   - list_tables
   - get_table_structure
   - create_table
   - drop_table

4. 数据查询工具
   - execute_query
   - select_data

5. 数据操作工具
   - insert_data
   - update_data
   - delete_data

6. 高级查询工具
   - select_data_smart
   - select_data_paged
   - select_data_summary
   - select_data_html