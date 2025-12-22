# Yuerenge 数据库 MCP

基于模型上下文协议（MCP）的数据库管理工具。

## 功能特性

- 支持多种数据库（MySQL、Oracle、PostgreSQL、SQLite、SQL Server）
- 连接管理
- 表结构操作
- 数据查询与操作
- 带验证的高级配置管理
- 数据库适配器模式便于扩展

## 安装

```bash
pip install yuerenge-database-mcp
```

## 使用方法

安装后，您可以运行数据库 MCP 服务器：

```bash
yuerenge-database-mcp
```

或者使用特定的配置文件：

```bash
DATABASE_CONFIG_PATH=/path/to/your/config.json yuerenge-database-mcp
```

## 配置

该工具使用 JSON 配置文件来存储数据库连接信息。

配置优先级（从高到低）：
1. 环境变量 `DATABASE_CONFIG_PATH`
2. 默认配置文件（`config/database_config.json`）

### 配置验证

配置管理器会验证所有连接配置以确保：
- 所有必填字段都已填写
- 端口号有效（1-65535）
- 数据库类型受支持
- 启用标志是布尔值

### 支持的数据库类型

- MySQL
- Oracle
- PostgreSQL
- SQLite
- SQL Server

### 连接池设置

您可以为每个数据库连接配置连接池设置：

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

可用的连接池设置：
- `pool_size`：连接池内保持打开的连接数（默认：10）
- `max_overflow`：连接池中允许的"溢出"连接数（默认：20）
- `pool_timeout`：在放弃获取连接之前等待的秒数（默认：30）
- `pool_recycle`：重新创建空闲连接的秒数（默认：3600）

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