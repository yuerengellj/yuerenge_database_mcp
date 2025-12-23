# 配置文件模板说明 - Yuerenge Database MCP

## 配置文件结构

配置文件使用JSON格式，包含两个主要部分：
- `connections`: 数据库连接配置数组
- `default_settings`: 默认连接池设置（可选）

配置管理由 `config` 模块中的 `DatabaseConfigManager` 类处理，该模块专门负责配置的加载、验证和管理。

## 环境变量

该工具支持以下环境变量：

- `DATABASE_CONFIG_PATH`: 指定配置文件的路径
- `ERROR_LOG_PATH`: 指定错误日志存储的目录（默认：./error_logs）

## MCP服务器配置

作为MCP服务器运行时，需要在MCP客户端配置中添加以下配置：

```json
{
  "yuerenge-database-mcp": {
    "command": "uvx",
    "args": [
      "yuerenge-database-mcp"
    ],
    "env": {
      "DATABASE_CONFIG_PATH": "path/to/config.json",
      "ERROR_LOG_PATH": "path/to/log/directory"
    }
  }
}
```

配置优先级（从高到低）：
1. 环境变量 `DATABASE_CONFIG_PATH` 指定的文件
2. 默认路径 `config/database_config.json`

## 连接配置详解

### 通用字段

每个连接配置都包含以下字段：

- `name`: (必需) 连接的唯一标识符，用于在工具中引用此连接
- `type`: (必需) 数据库类型，支持: `mysql`, `oracle`, `postgresql`, `sqlite`, `sqlserver`
- `enabled`: (可选) 布尔值，指定是否在服务器启动时自动连接，默认为 `false`

### 非SQLite数据库的必需字段

对于MySQL、Oracle、PostgreSQL、SQL Server，需要以下字段：

- `host`: (必需) 数据库服务器地址
- `port`: (必需) 数据库服务器端口（1-65535）
- `username`: (必需) 数据库用户名
- `password`: (必需) 数据库密码
- `database`: (必需) 要连接的数据库名称

### SQLite数据库的必需字段

对于SQLite，只需要以下字段：

- `database`: (必需) SQLite数据库文件的路径

### 连接池设置（可选）

以下字段用于配置连接池参数，适用于所有数据库类型：

- `pool_size`: 连接池中保持打开的连接数，默认为10
- `max_overflow`: 连接池溢出时允许的最大额外连接数，默认为20
- `pool_timeout`: 获取连接时等待的秒数，默认为30
- `pool_recycle`: 连接被回收前的最大空闲时间（秒），默认为3600

## 配置示例

### 完整的MySQL配置

```json
{
  "name": "my_mysql_db",
  "type": "mysql",
  "host": "localhost",
  "port": 3306,
  "username": "myuser",
  "password": "mypassword",
  "database": "mydatabase",
  "enabled": true,
  "pool_size": 5,
  "max_overflow": 10,
  "pool_timeout": 30,
  "pool_recycle": 3600
}
```

### 简单的SQLite配置

```json
{
  "name": "local_sqlite",
  "type": "sqlite",
  "database": "/path/to/mydatabase.sqlite",
  "enabled": true
}
```

### PostgreSQL配置

```json
{
  "name": "my_postgres_db",
  "type": "postgresql",
  "host": "localhost",
  "port": 5432,
  "username": "myuser",
  "password": "mypassword",
  "database": "mydatabase",
  "enabled": false,
  "pool_size": 8,
  "max_overflow": 15,
  "pool_timeout": 25
}
```

## 默认设置

`default_settings` 部分定义了所有连接的默认连接池参数：

```json
{
  "default_settings": {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600
  }
}
```

## 完整配置文件示例

```json
{
  "connections": [
    {
      "name": "mysql_dev",
      "type": "mysql",
      "host": "localhost",
      "port": 3306,
      "username": "devuser",
      "password": "devpass",
      "database": "devdb",
      "enabled": true,
      "pool_size": 5,
      "max_overflow": 10
    },
    {
      "name": "sqlite_local",
      "type": "sqlite",
      "database": "./local.db",
      "enabled": true
    },
    {
      "name": "postgres_prod",
      "type": "postgresql",
      "host": "prod.example.com",
      "port": 5432,
      "username": "produser",
      "password": "prodpass",
      "database": "proddb",
      "enabled": false
    }
  ],
  "default_settings": {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 3600
  }
}
```

## 配置验证规则

配置文件在加载时会进行验证：

1. 所有必需字段必须存在
2. 端口号必须是1-65535之间的整数
3. 数据库类型必须是支持的类型之一
4. `enabled` 字段（如果存在）必须是布尔值
5. 连接名称在配置中必须唯一

## 最佳实践

1. **安全性**：不要在版本控制中提交包含真实凭证的配置文件
2. **连接命名**：使用描述性的连接名称以便识别
3. **连接池**：根据应用负载调整连接池参数
4. **启用设置**：在生产环境中谨慎启用连接，避免不必要的连接
5. **SQLite路径**：确保SQLite数据库文件路径存在且有适当的访问权限