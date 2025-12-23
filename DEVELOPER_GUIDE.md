# 开发者指南 - Yuerenge Database MCP

## 项目结构

```
yuerenge_database_mcp/
├── src/
│   ├── yuerenge_database_mcp/
│   │   ├── config/
│   │   │   ├── __init__.py
│   │   │   ├── config_manager.py      # 配置管理
│   │   │   ├── database_config.json   # 默认配置文件
│   │   │   └── sample_database_config.json # 配置示例
│   │   ├── db_tools/
│   │   │   ├── __init__.py
│   │   │   ├── async_data_manager.py  # 异步数据管理
│   │   │   ├── connection_manager.py  # 连接管理
│   │   │   ├── data_manager.py        # 数据操作管理
│   │   │   ├── database_adapters.py   # 数据库适配器
│   │   │   ├── database_manager.py    # 数据库管理主类
│   │   │   ├── db_tools.py            # MCP工具定义
│   │   │   ├── exceptions.py          # 自定义异常
│   │   │   ├── format_manager.py      # 数据格式化
│   │   │   ├── log_manager.py         # 日志管理
│   │   │   ├── oracle_utils.py        # Oracle特定工具
│   │   │   └── table_manager.py       # 表结构管理
│   │   ├── __init__.py
│   │   └── server_lifecycle.py        # 服务器生命周期管理
│   ├── README.md
│   ├── README_zh.md
│   ├── pyproject.toml
│   └── run_server.py
```

## 核心模块说明

### 1. 配置管理 (config/config_manager.py)

`DatabaseConfigManager` 类负责管理数据库连接配置：

- 从JSON文件加载配置
- 验证配置参数的有效性
- 提供添加、更新、删除和启用/禁用连接的API
- 支持环境变量 `DATABASE_CONFIG_PATH` 指定配置文件路径

### 2. 连接管理 (db_tools/connection_manager.py)

`ConnectionManager` 类负责数据库连接的创建和管理：

- 使用SQLAlchemy创建数据库引擎
- 支持多种数据库类型（MySQL、Oracle、PostgreSQL、SQLite、SQL Server）
- 管理连接池设置
- 提供连接测试功能

### 3. 数据操作 (db_tools/data_manager.py)

`DataManager` 类提供数据操作功能：

- 查询、插入、更新、删除操作
- 支持参数化查询防止SQL注入
- 批量操作支持
- 分页查询支持
- 日期时间处理（特别是Oracle数据库）

### 4. 表结构管理 (db_tools/table_manager.py)

提供表结构相关的操作：

- 列出表
- 获取表结构
- 创建、修改、删除表
- 支持不同数据库的语法差异

### 5. 格式化管理 (db_tools/format_manager.py)

提供多种数据格式化选项：

- 普通表格格式
- IDE风格表格
- 垂直表格
- HTML表格
- 智能表格（根据列数自动选择格式）
- 分页表格
- 摘要表格

### 6. 数据库适配器 (db_tools/database_adapters.py)

为不同数据库类型提供适配器模式：

- MySQLAdapter
- OracleAdapter
- PostgreSQLAdapter
- SQLiteAdapter
- SQLServerAdapter

每种适配器处理特定数据库的语法差异。

### 7. 日志管理 (db_tools/log_manager.py)

`LogManager` 类负责错误日志的记录和管理：

- 使用环境变量 `ERROR_LOG_PATH` 指定日志存储路径
- 若未设置，则使用默认路径 `./error_logs`
- 每条错误日志单独保存为JSON格式文件
- 文件名格式：操作类型+操作时间（YYYYMMDD_HHMMSS_随机数）

## 扩展开发

### 添加新的数据库支持

要添加新的数据库支持，需要：

1. 在 [database_adapters.py](file:///D:/File/Workspace/MCP/yuerenge_database_mcp/src/yuerenge_database_mcp/db_tools/database_adapters.py) 中创建新的适配器类
2. 实现基本的适配器方法（连接字符串、测试查询、查询生成等）
3. 在 `get_database_adapter` 函数中添加新的数据库类型映射
4. 在配置验证中添加新的数据库类型

### 添加新的工具函数

在 [db_tools/db_tools.py](file:///D:/File/Workspace/MCP/yuerenge_database_mcp/src/yuerenge_database_mcp/db_tools/db_tools.py) 中添加新的工具函数：

```python
@mcp.tool()
def new_tool_function(param1: str, param2: int) -> str:
    """
    新工具函数的详细文档说明。
    
    Args:
        param1: 参数1的描述
        param2: 参数2的描述
        
    Returns:
        返回值的描述
    """
    # 实现代码
    return result
```

### 添加新的格式化选项

在 [format_manager.py](file:///D:/File/Workspace/MCP/yuerenge_database_mcp/src/yuerenge_database_mcp/db_tools/format_manager.py) 中添加新的格式化方法：

```python
def format_as_new_style(self, data, connection_name, table_name):
    """
    新格式化方法的文档说明。
    
    Args:
        data: 要格式化的数据
        connection_name: 连接名称
        table_name: 表名
        
    Returns:
        格式化后的字符串
    """
    # 实现代码
    return formatted_data
```

## 最佳实践

### 1. 错误处理

所有操作都应包含适当的错误处理：

- 使用try-catch块捕获可能的异常
- 记录详细的错误信息
- 提供有意义的错误消息
- 在日志中记录请求ID以追踪问题

### 2. 配置验证

在使用配置前验证所有参数：

- 检查必需字段
- 验证数据类型
- 验证值范围（如端口号）
- 验证数据库类型支持

### 3. 连接管理

- 合理设置连接池参数以优化性能
- 及时释放数据库连接
- 在服务器关闭时正确关闭所有连接

### 4. 安全性

- 避免SQL注入，使用参数化查询
- 不要在代码中硬编码敏感信息
- 使用配置文件管理数据库凭证
- 验证和清理用户输入

### 5. 日志管理

- 使用环境变量 `ERROR_LOG_PATH` 指定日志存储路径
- 每条错误日志单独保存为JSON格式文件
- 文件名格式：操作类型+操作时间（YYYYMMDD_HHMMSS_随机数）
- 记录详细的错误上下文信息

## 测试策略

### 单元测试

为每个模块编写单元测试，覆盖：

- 正常操作路径
- 错误处理路径
- 边界条件
- 参数验证

### 集成测试

测试完整的数据库操作流程，包括：

- 配置加载和验证
- 连接创建和管理
- 数据操作的端到端流程
- 不同数据库类型的兼容性

## 部署和运维

### 环境变量

- `DATABASE_CONFIG_PATH`: 指定配置文件路径
- `ERROR_LOG_PATH`: 指定错误日志存储路径（默认：./error_logs）
- 通过环境变量管理敏感信息

### 日志管理

- 结构化日志记录，包含请求ID
- 记录操作详情和错误信息
- 保留错误日志以供故障排查
- 每条错误日志单独保存为JSON文件

### 性能优化

- 合理配置连接池大小
- 使用索引优化查询性能
- 避免不必要的数据传输
- 使用分页处理大量数据