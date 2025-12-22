"""
Database adapters for different database types.
Provides unified interface for database-specific operations.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from sqlalchemy import text


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""

    @abstractmethod
    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        """Generate database connection string."""
        pass

    @abstractmethod
    def get_test_query(self) -> str:
        """Get database-specific test query."""
        pass

    @abstractmethod
    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        """Get query to list tables."""
        pass

    @abstractmethod
    def get_table_structure_query(self, table_name: str) -> str:
        """Get query to retrieve table structure."""
        pass

    @abstractmethod
    def format_column_info(self, row: tuple) -> Dict[str, Any]:
        """Format column information from query result."""
        pass

    @abstractmethod
    def get_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]], 
                                 table_comment: Optional[str] = None) -> str:
        """Generate CREATE TABLE statement."""
        pass

    @abstractmethod
    def get_drop_table_statement(self, table_name: str, cascade: bool = False) -> str:
        """Generate DROP TABLE statement."""
        pass

    @abstractmethod
    def get_alter_table_statement(self, table_name: str, operation: Dict[str, Any]) -> str:
        """Generate ALTER TABLE statement."""
        pass

    @abstractmethod
    def get_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
                        limit: Optional[int] = None) -> str:
        """Generate SELECT query."""
        pass

    @abstractmethod
    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        """Generate INSERT query."""
        pass

    @abstractmethod
    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> str:
        """Generate UPDATE query."""
        pass

    @abstractmethod
    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> str:
        """Generate DELETE query."""
        pass


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        if pattern:
            # Convert LIKE pattern to MySQL pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            return f"SHOW TABLES LIKE '{pattern}'"
        return "SHOW TABLES"

    def get_table_structure_query(self, table_name: str) -> str:
        return f"DESCRIBE `{table_name}`"

    def format_column_info(self, row: tuple) -> Dict[str, Any]:
        # MySQL DESCRIBE returns: Field, Type, Null, Key, Default, Extra
        return {
            "name": row[0],
            "type": row[1],
            "nullable": row[2] == "YES",
            "key": row[3],
            "default": row[4],
            "extra": row[5]
        }

    def get_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]], 
                                 table_comment: Optional[str] = None) -> str:
        cols = []
        for col in columns:
            col_def = f"`{col['name']}` {col['type']}"
            if 'length' in col:
                col_def += f"({col['length']})"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in col:
                default_val = col['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if col.get('primary_key', False):
                col_def += " PRIMARY KEY"
            if 'comment' in col:
                col_def += f" COMMENT '{col['comment']}'"
            cols.append(col_def)
        
        sql = f"CREATE TABLE `{table_name}` (\n  " + ",\n  ".join(cols) + "\n)"
        if table_comment:
            sql += f" COMMENT='{table_comment}'"
        return sql

    def get_drop_table_statement(self, table_name: str, cascade: bool = False) -> str:
        sql = f"DROP TABLE `{table_name}`"
        if cascade:
            sql += " CASCADE"
        return sql

    def get_alter_table_statement(self, table_name: str, operation: Dict[str, Any]) -> str:
        op_type = operation["operation"]
        if op_type == "add_column":
            col_def = f"`{operation['name']}` {operation['type']}"
            if 'length' in operation:
                col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if 'comment' in operation:
                col_def += f" COMMENT '{operation['comment']}'"
            return f"ALTER TABLE `{table_name}` ADD COLUMN {col_def}"
        
        elif op_type == "drop_column":
            return f"ALTER TABLE `{table_name}` DROP COLUMN `{operation['name']}`"
        
        elif op_type == "modify_column":
            col_def = f"`{operation['name']}` {operation['type']}"
            if 'length' in operation:
                col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if 'comment' in operation:
                col_def += f" COMMENT '{operation['comment']}'"
            return f"ALTER TABLE `{table_name}` MODIFY COLUMN {col_def}"
        
        elif op_type == "rename_column":
            return f"ALTER TABLE `{table_name}` CHANGE COLUMN `{operation['old_name']}` `{operation['new_name']}` {operation['type']}"
        
        else:
            raise ValueError(f"Unsupported alter table operation: {op_type}")

    def get_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
                        limit: Optional[int] = None) -> str:
        query = f"SELECT * FROM `{table_name}`"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"`{key}` = '{value}'")
                else:
                    condition_strs.append(f"`{key}` = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" LIMIT {limit}"
        return query

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        columns = ", ".join([f"`{k}`" for k in data.keys()])
        values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        return f"INSERT INTO `{table_name}` ({columns}) VALUES ({values})"

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> str:
        set_strs = []
        for key, value in data.items():
            if isinstance(value, str):
                set_strs.append(f"`{key}` = '{value}'")
            else:
                set_strs.append(f"`{key}` = {value}")
        
        query = f"UPDATE `{table_name}` SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"`{key}` = '{value}'")
                else:
                    condition_strs.append(f"`{key}` = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> str:
        query = f"DELETE FROM `{table_name}`"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"`{key}` = '{value}'")
                else:
                    condition_strs.append(f"`{key}` = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query


class OracleAdapter(DatabaseAdapter):
    """Oracle database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={database}"

    def get_test_query(self) -> str:
        return "SELECT 1 FROM dual"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT TABLE_NAME FROM USER_TABLES"
        if pattern:
            # Convert LIKE pattern to Oracle pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" WHERE TABLE_NAME LIKE '{pattern}'"
        query += " ORDER BY TABLE_NAME"
        return query

    def get_table_structure_query(self, table_name: str) -> str:
        return f"""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE
        FROM USER_TAB_COLUMNS 
        WHERE TABLE_NAME = '{table_name.upper()}'
        ORDER BY COLUMN_ID
        """

    def format_column_info(self, row: tuple) -> Dict[str, Any]:
        # Oracle query returns: COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE
        return {
            "name": row[0],
            "type": row[1],
            "nullable": row[2] == "Y",
            "default": row[3],
            "char_length": row[4],
            "data_precision": row[5],
            "data_scale": row[6]
        }

    def get_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]], 
                                 table_comment: Optional[str] = None) -> str:
        cols = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if 'length' in col:
                if isinstance(col['length'], (list, tuple)) and len(col['length']) == 2:
                    # For NUMBER(p,s) type
                    col_def += f"({col['length'][0]},{col['length'][1]})"
                else:
                    col_def += f"({col['length']})"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in col:
                default_val = col['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if col.get('primary_key', False):
                col_def += " PRIMARY KEY"
            cols.append(col_def)
        
        sql = f"CREATE TABLE {table_name.upper()} (\n  " + ",\n  ".join(cols) + "\n)"
        return sql

    def get_drop_table_statement(self, table_name: str, cascade: bool = False) -> str:
        sql = f"DROP TABLE {table_name.upper()}"
        if cascade:
            sql += " CASCADE CONSTRAINTS"
        return sql

    def get_alter_table_statement(self, table_name: str, operation: Dict[str, Any]) -> str:
        op_type = operation["operation"]
        table_name = table_name.upper()
        
        if op_type == "add_column":
            col_def = f"{operation['name']} {operation['type']}"
            if 'length' in operation:
                if isinstance(operation['length'], (list, tuple)) and len(operation['length']) == 2:
                    col_def += f"({operation['length'][0]},{operation['length'][1]})"
                else:
                    col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            return f"ALTER TABLE {table_name} ADD ({col_def})"
        
        elif op_type == "drop_column":
            return f"ALTER TABLE {table_name} DROP COLUMN {operation['name']}"
        
        elif op_type == "modify_column":
            col_def = f"{operation['name']} {operation['type']}"
            if 'length' in operation:
                if isinstance(operation['length'], (list, tuple)) and len(operation['length']) == 2:
                    col_def += f"({operation['length'][0]},{operation['length'][1]})"
                else:
                    col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            return f"ALTER TABLE {table_name} MODIFY ({col_def})"
        
        elif op_type == "rename_column":
            return f"ALTER TABLE {table_name} RENAME COLUMN {operation['old_name']} TO {operation['new_name']}"
        
        else:
            raise ValueError(f"Unsupported alter table operation: {op_type}")

    def get_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
                        limit: Optional[int] = None) -> str:
        query = f"SELECT * FROM {table_name.upper()}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" FETCH FIRST {limit} ROWS ONLY"
        return query

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        columns = ", ".join([k for k in data.keys()])
        values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        return f"INSERT INTO {table_name.upper()} ({columns}) VALUES ({values})"

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> str:
        set_strs = []
        for key, value in data.items():
            if isinstance(value, str):
                set_strs.append(f"{key} = '{value}'")
            else:
                set_strs.append(f"{key} = {value}")
        
        query = f"UPDATE {table_name.upper()} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> str:
        query = f"DELETE FROM {table_name.upper()}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        if pattern:
            # Convert LIKE pattern to PostgreSQL pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" AND tablename LIKE '{pattern}'"
        query += " ORDER BY tablename"
        return query

    def get_table_structure_query(self, table_name: str) -> str:
        return f"""
        SELECT column_name, data_type, is_nullable, column_default, character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = '{table_name.lower()}' AND table_schema = 'public'
        ORDER BY ordinal_position
        """

    def format_column_info(self, row: tuple) -> Dict[str, Any]:
        # PostgreSQL query returns: column_name, data_type, is_nullable, column_default, character_maximum_length
        return {
            "name": row[0],
            "type": row[1],
            "nullable": row[2] == "YES",
            "default": row[3],
            "char_length": row[4]
        }

    def get_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]], 
                                 table_comment: Optional[str] = None) -> str:
        cols = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if 'length' in col:
                col_def += f"({col['length']})"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in col:
                default_val = col['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if col.get('primary_key', False):
                col_def += " PRIMARY KEY"
            cols.append(col_def)
        
        sql = f"CREATE TABLE {table_name.lower()} (\n  " + ",\n  ".join(cols) + "\n)"
        return sql

    def get_drop_table_statement(self, table_name: str, cascade: bool = False) -> str:
        sql = f"DROP TABLE {table_name.lower()}"
        if cascade:
            sql += " CASCADE"
        return sql

    def get_alter_table_statement(self, table_name: str, operation: Dict[str, Any]) -> str:
        op_type = operation["operation"]
        table_name = table_name.lower()
        
        if op_type == "add_column":
            col_def = f"{operation['name']} {operation['type']}"
            if 'length' in operation:
                col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            return f"ALTER TABLE {table_name} ADD COLUMN {col_def}"
        
        elif op_type == "drop_column":
            return f"ALTER TABLE {table_name} DROP COLUMN {operation['name']}"
        
        elif op_type == "modify_column":
            actions = []
            # PostgreSQL requires separate statements for different modifications
            if 'type' in operation:
                actions.append(f"ALTER COLUMN {operation['name']} TYPE {operation['type']}")
            if 'nullable' in operation:
                if not operation['nullable']:
                    actions.append(f"ALTER COLUMN {operation['name']} SET NOT NULL")
                else:
                    actions.append(f"ALTER COLUMN {operation['name']} DROP NOT NULL")
            if 'default' in operation:
                default_val = operation['default']
                if default_val is None:
                    actions.append(f"ALTER COLUMN {operation['name']} DROP DEFAULT")
                else:
                    if isinstance(default_val, str):
                        actions.append(f"ALTER COLUMN {operation['name']} SET DEFAULT '{default_val}'")
                    else:
                        actions.append(f"ALTER COLUMN {operation['name']} SET DEFAULT {default_val}")
            
            statements = [f"ALTER TABLE {table_name} {action}" for action in actions]
            return "; ".join(statements)
        
        elif op_type == "rename_column":
            return f"ALTER TABLE {table_name} RENAME COLUMN {operation['old_name']} TO {operation['new_name']}"
        
        else:
            raise ValueError(f"Unsupported alter table operation: {op_type}")

    def get_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
                        limit: Optional[int] = None) -> str:
        query = f"SELECT * FROM {table_name.lower()}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" LIMIT {limit}"
        return query

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        columns = ", ".join([k for k in data.keys()])
        values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        return f"INSERT INTO {table_name.lower()} ({columns}) VALUES ({values})"

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> str:
        set_strs = []
        for key, value in data.items():
            if isinstance(value, str):
                set_strs.append(f"{key} = '{value}'")
            else:
                set_strs.append(f"{key} = {value}")
        
        query = f"UPDATE {table_name.lower()} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> str:
        query = f"DELETE FROM {table_name.lower()}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query


class SQLiteAdapter(DatabaseAdapter):
    """SQLite database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        # For SQLite, the database parameter is the file path
        return f"sqlite:///{database}"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        if pattern:
            # Convert LIKE pattern to SQLite pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" AND name LIKE '{pattern}'"
        query += " ORDER BY name"
        return query

    def get_table_structure_query(self, table_name: str) -> str:
        return f"PRAGMA table_info({table_name})"

    def format_column_info(self, row: tuple) -> Dict[str, Any]:
        # SQLite PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        return {
            "name": row[1],
            "type": row[2],
            "nullable": row[3] == 0,
            "default": row[4],
            "primary_key": row[5] == 1
        }

    def get_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]], 
                                 table_comment: Optional[str] = None) -> str:
        cols = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if 'length' in col:
                col_def += f"({col['length']})"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in col:
                default_val = col['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if col.get('primary_key', False):
                col_def += " PRIMARY KEY"
            cols.append(col_def)
        
        sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols) + "\n)"
        return sql

    def get_drop_table_statement(self, table_name: str, cascade: bool = False) -> str:
        # SQLite doesn't support CASCADE
        return f"DROP TABLE {table_name}"

    def get_alter_table_statement(self, table_name: str, operation: Dict[str, Any]) -> str:
        # SQLite has limited ALTER TABLE support
        op_type = operation["operation"]
        
        if op_type == "add_column":
            col_def = f"{operation['name']} {operation['type']}"
            if 'length' in operation:
                col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            return f"ALTER TABLE {table_name} ADD COLUMN {col_def}"
        
        elif op_type in ["drop_column", "modify_column", "rename_column"]:
            # These operations are not supported in SQLite through simple ALTER TABLE statements
            raise ValueError(f"Operation '{op_type}' is not supported in SQLite. Consider using a workaround.")
        
        else:
            raise ValueError(f"Unsupported alter table operation: {op_type}")

    def get_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
                        limit: Optional[int] = None) -> str:
        query = f"SELECT * FROM {table_name}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" LIMIT {limit}"
        return query

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        columns = ", ".join([k for k in data.keys()])
        values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        return f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> str:
        set_strs = []
        for key, value in data.items():
            if isinstance(value, str):
                set_strs.append(f"{key} = '{value}'")
            else:
                set_strs.append(f"{key} = {value}")
        
        query = f"UPDATE {table_name} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> str:
        query = f"DELETE FROM {table_name}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query


class SQLServerAdapter(DatabaseAdapter):
    """SQL Server database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
        if pattern:
            # Convert LIKE pattern to SQL Server pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" AND TABLE_NAME LIKE '{pattern}'"
        query += " ORDER BY TABLE_NAME"
        return query

    def get_table_structure_query(self, table_name: str) -> str:
        return f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """

    def format_column_info(self, row: tuple) -> Dict[str, Any]:
        # SQL Server query returns: COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
        return {
            "name": row[0],
            "type": row[1],
            "nullable": row[2] == "YES",
            "default": row[3],
            "char_length": row[4]
        }

    def get_create_table_statement(self, table_name: str, columns: List[Dict[str, Any]], 
                                 table_comment: Optional[str] = None) -> str:
        cols = []
        for col in columns:
            col_def = f"{col['name']} {col['type']}"
            if 'length' in col:
                col_def += f"({col['length']})"
            if not col.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in col:
                default_val = col['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            if col.get('primary_key', False):
                col_def += " PRIMARY KEY"
            cols.append(col_def)
        
        sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols) + "\n)"
        return sql

    def get_drop_table_statement(self, table_name: str, cascade: bool = False) -> str:
        # SQL Server doesn't support CASCADE in DROP TABLE
        cascade_check = "IF EXISTS " if cascade else ""
        return f"DROP TABLE {cascade_check}{table_name}"

    def get_alter_table_statement(self, table_name: str, operation: Dict[str, Any]) -> str:
        op_type = operation["operation"]
        
        if op_type == "add_column":
            col_def = f"{operation['name']} {operation['type']}"
            if 'length' in operation:
                col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            if 'default' in operation:
                default_val = operation['default']
                if isinstance(default_val, str):
                    col_def += f" DEFAULT '{default_val}'"
                else:
                    col_def += f" DEFAULT {default_val}"
            return f"ALTER TABLE {table_name} ADD {col_def}"
        
        elif op_type == "drop_column":
            return f"ALTER TABLE {table_name} DROP COLUMN {operation['name']}"
        
        elif op_type == "modify_column":
            # In SQL Server, modifying a column is done with ALTER COLUMN
            col_def = f"{operation['name']} {operation['type']}"
            if 'length' in operation:
                col_def += f"({operation['length']})"
            if not operation.get('nullable', True):
                col_def += " NOT NULL"
            return f"ALTER TABLE {table_name} ALTER COLUMN {col_def}"
        
        elif op_type == "rename_column":
            # SQL Server renames columns with sp_rename
            return f"EXEC sp_rename '{table_name}.{operation['old_name']}', '{operation['new_name']}', 'COLUMN'"
        
        else:
            raise ValueError(f"Unsupported alter table operation: {op_type}")

    def get_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None, 
                        limit: Optional[int] = None) -> str:
        query = f"SELECT * FROM {table_name}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" TOP {limit}"
        return query

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> str:
        columns = ", ".join([k for k in data.keys()])
        values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        return f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> str:
        set_strs = []
        for key, value in data.items():
            if isinstance(value, str):
                set_strs.append(f"{key} = '{value}'")
            else:
                set_strs.append(f"{key} = {value}")
        
        query = f"UPDATE {table_name} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> str:
        query = f"DELETE FROM {table_name}"
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                if isinstance(value, str):
                    condition_strs.append(f"{key} = '{value}'")
                else:
                    condition_strs.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(condition_strs)
        return query


def get_database_adapter(db_type: str) -> DatabaseAdapter:
    """
    Factory function to get the appropriate database adapter.
    
    Args:
        db_type: Type of database (mysql, oracle, postgresql, sqlite, sqlserver)
        
    Returns:
        DatabaseAdapter: Appropriate database adapter instance
    """
    db_type = db_type.lower()
    if db_type == "mysql":
        return MySQLAdapter()
    elif db_type == "oracle":
        return OracleAdapter()
    elif db_type == "postgresql":
        return PostgreSQLAdapter()
    elif db_type == "sqlite":
        return SQLiteAdapter()
    elif db_type == "sqlserver":
        return SQLServerAdapter()
    else:
        raise ValueError(f"Unsupported database type: {db_type}")