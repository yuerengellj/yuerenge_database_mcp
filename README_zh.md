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

## 快速开始

1. **安装包**:
   ```bash
   pip install yuerenge-database-mcp
   ```

2. **创建配置文件**（可选，不提供则使用默认值）:
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

3. **运行服务器**:
   ```bash
   yuerenge-database-mcp
   ```

4. **或使用特定配置运行**:
   ```bash
   DATABASE_CONFIG_PATH=/path/to/your/config.json yuerenge-database-mcp
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

## 环境变量

服务器支持以下环境变量：

- `DATABASE_CONFIG_PATH`：指定数据库配置文件的路径
- `ERROR_LOG_PATH`：指定错误日志存储的目录（默认：./error_logs）

## 提供的工具

此 MCP 服务器提供以下工具：

1. 连接管理工具
   - add_database_connection: 添加数据库连接
   - remove_database_connection: 移除数据库连接
   - list_database_connections: 列出所有活动连接

2. 配置管理工具
   - list_configured_connections: 列出配置文件中的所有连接
   - enable_configured_connection: 启用配置文件中的连接
   - disable_configured_connection: 禁用配置文件中的连接
   - reload_configurations: 从配置文件重新加载连接

3. 表结构工具
   - list_tables: 列出数据库中的所有表
   - get_table_structure: 获取表结构信息
   - create_table: 创建新表
   - drop_table: 删除表
   - alter_table: 修改表结构

4. 数据查询工具
   - execute_query: 执行原始SQL查询
   - select_data: 从表中选择数据并智能格式化

5. 数据操作工具
   - insert_data: 插入数据到表
   - batch_insert_data: 批量插入数据
   - update_data: 更新表中的数据
   - batch_update_data: 批量更新数据
   - delete_data: 从表中删除数据
   - batch_delete_data: 批量删除数据

6. 高级查询工具
   - select_data_smart: 智能格式化表格数据
   - select_data_paged: 分页显示表格数据
   - select_data_summary: 摘要显示表格数据
   - select_data_html: 以HTML格式显示表格数据并自动打开浏览器

## 使用示例

### 添加数据库连接
```python
# 使用MCP客户端调用工具
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

### 查询数据
```python
# 从表中选择数据
select_data(connection_name="mysql_test", table_name="users", conditions={"age": 25}, limit=10)
```

### 插入数据
```python
# 插入单条记录
insert_data(connection_name="mysql_test", table_name="users", data={
    "name": "张三",
    "age": 30,
    "email": "zhangsan@example.com"
})
```

### 批量操作
```python
# 批量插入多条记录
batch_insert_data(connection_name="mysql_test", table_name="users", data_list=[
    {"name": "李四", "age": 28, "email": "lisi@example.com"},
    {"name": "王五", "age": 32, "email": "wangwu@example.com"}
])
```

### 高级查询
```python
# 智能格式化查询结果
select_data_smart(connection_name="mysql_test", table_name="users", max_columns=8)

# 分页显示大量数据
select_data_paged(connection_name="mysql_test", table_name="large_table", 
                  columns_per_page=5, rows_per_page=15)

# 以HTML格式显示并在浏览器中打开
select_data_html(connection_name="mysql_test", table_name="users")
```

## 最佳实践

1. **连接管理**
   - 使用配置文件管理数据库连接，避免在代码中硬编码敏感信息
   - 合理设置连接池参数以优化性能
   - 定期使用`reload_configurations`工具更新连接配置

2. **数据操作**
   - 对于大量数据插入，优先使用`batch_insert_data`工具
   - 使用`conditions`参数进行精确查询，避免全表扫描
   - 在执行更新或删除操作前，先使用`select_data`验证条件

3. **数据展示**
   - 对于列数较多的表，使用`select_data_paged`或`select_data_summary`工具
   - 对于数据分析场景，使用`select_data_html`获取更好的可视化效果
   - 根据数据量选择合适的`limit`参数避免内存溢出