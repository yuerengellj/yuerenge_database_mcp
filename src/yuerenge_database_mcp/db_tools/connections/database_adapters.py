"""
Database adapters for different database types.
Provides unified interface for database-specific operations.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
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
                        limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        """Generate SELECT query and parameters."""
        pass

    @abstractmethod
    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Generate INSERT query and parameters."""
        pass

    @abstractmethod
    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """Generate UPDATE query and parameters."""
        pass

    @abstractmethod
    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """Generate DELETE query and parameters."""
        pass

    @abstractmethod
    def get_count_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """Generate COUNT query and parameters for pagination."""
        pass

    @abstractmethod
    def get_paginated_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """Generate paginated SELECT query and parameters."""
        pass


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        # Use the current database as determined by the connection
        if pattern:
            # Convert LIKE pattern to MySQL pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            return f"SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '{pattern}' ORDER BY TABLE_NAME"
        return "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"

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
                        limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"SELECT * FROM `{table_name}`"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"`{key}` = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" LIMIT {limit}"
        return query, params

    def get_count_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate COUNT query and parameters for pagination.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT COUNT(*) FROM `{table_name}`"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"`{key}` = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_paginated_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate paginated SELECT query and parameters.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
                        May include 'limit' and 'offset' keys for pagination
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT * FROM `{table_name}`"
        params = {}
        
        limit = conditions.pop('limit', None) if conditions else None
        offset = conditions.pop('offset', None) if conditions else None
        
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"`{key}` = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
            
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
                
        return query, params

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        columns = ", ".join([f"`{k}`" for k in data.keys()])
        placeholders = ", ".join([f":{k}" for k in data.keys()])
        query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        return query, data

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        set_strs = []
        params = {}
        for key, value in data.items():
            set_strs.append(f"`{key}` = :{key}_{len(params)}")
            params[f"{key}_{len(params)}"] = value
        
        query = f"UPDATE `{table_name}` SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"`{key}` = :cond_{key}")
                params[f"cond_{key}"] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"DELETE FROM `{table_name}`"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"`{key}` = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params


class OracleAdapter(DatabaseAdapter):
    """Oracle database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={database}"

    def get_test_query(self) -> str:
        return "SELECT 1 FROM dual"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT t.TABLE_NAME, tc.COMMENTS FROM USER_TABLES t LEFT JOIN USER_TAB_COMMENTS tc ON t.TABLE_NAME = tc.TABLE_NAME"
        if pattern:
            # Convert LIKE pattern to Oracle pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" WHERE t.TABLE_NAME LIKE '{pattern}'"
        query += " ORDER BY t.TABLE_NAME"
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
                        limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"SELECT * FROM {table_name.upper()}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" FETCH FIRST {limit} ROWS ONLY"
        return query, params

    def get_count_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate COUNT query and parameters for pagination.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT COUNT(*) FROM {table_name.upper()}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_paginated_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate paginated SELECT query and parameters.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
                        May include 'limit' and 'offset' keys for pagination
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT * FROM {table_name.upper()}"
        params = {}
        
        limit = conditions.pop('limit', None) if conditions else None
        offset = conditions.pop('offset', None) if conditions else None
        
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
            
        if limit is not None:
            query += f" FETCH FIRST {limit} ROWS ONLY"
            if offset is not None:
                # Oracle uses ROWNUM or OFFSET/FETCH clauses depending on version
                # Using OFFSET/FETCH for newer versions
                query += f" OFFSET {offset} ROWS"
                
        return query, params

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        # For Oracle, we need to handle datetime values specially
        processed_data = {}
        datetime_columns = []
        
        for key, value in data.items():
            # Check if it's a datetime string
            if isinstance(value, str) and self._is_datetime_string(value):
                datetime_columns.append(key)
                # For Oracle, we'll handle datetime values in the query itself
                processed_data[key] = value
            else:
                processed_data[key] = value
        
        columns = ", ".join([k for k in processed_data.keys()])
        placeholders = ", ".join([f":{k}" for k in processed_data.keys()])
        query = f"INSERT INTO {table_name.upper()} ({columns}) VALUES ({placeholders})"
        return query, processed_data

    def _is_datetime_string(self, value: str) -> bool:
        """
        Check if a string represents a datetime in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS

        Args:
            value: String to check

        Returns:
            bool: True if string matches datetime format, False otherwise
        """
        import re
        if not isinstance(value, str):
            return False

        # Pattern for date only (YYYY-MM-DD) or date with time (YYYY-MM-DD HH:MM:SS)
        datetime_pattern = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
        return bool(re.match(datetime_pattern, value))

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        set_strs = []
        params = {}
        for key, value in data.items():
            set_strs.append(f"{key} = :{key}_{len(params)}")
            params[f"{key}_{len(params)}"] = value
        
        query = f"UPDATE {table_name.upper()} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :cond_{key}")
                params[f"cond_{key}"] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"DELETE FROM {table_name.upper()}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT t.tablename, d.description as table_comment FROM pg_tables t JOIN pg_class c ON t.tablename = c.relname JOIN pg_namespace n ON c.relnamespace = n.oid LEFT JOIN pg_description d ON c.oid = d.objoid WHERE n.nspname = 'public'"
        if pattern:
            # Convert LIKE pattern to PostgreSQL pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" AND t.tablename LIKE '{pattern}'"
        query += " ORDER BY t.tablename"
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
                        limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"SELECT * FROM {table_name.lower()}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" LIMIT {limit}"
        return query, params

    def get_count_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate COUNT query and parameters for pagination.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT COUNT(*) FROM {table_name.lower()}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_paginated_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate paginated SELECT query and parameters.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
                        May include 'limit' and 'offset' keys for pagination
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT * FROM {table_name.lower()}"
        params = {}
        
        limit = conditions.pop('limit', None) if conditions else None
        offset = conditions.pop('offset', None) if conditions else None
        
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
            
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
                
        return query, params

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        columns = ", ".join([k for k in data.keys()])
        placeholders = ", ".join([f":{k}" for k in data.keys()])
        query = f"INSERT INTO {table_name.lower()} ({columns}) VALUES ({placeholders})"
        return query, data

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        set_strs = []
        params = {}
        for key, value in data.items():
            set_strs.append(f"{key} = :{key}_{len(params)}")
            params[f"{key}_{len(params)}"] = value
        
        query = f"UPDATE {table_name.lower()} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :cond_{key}")
                params[f"cond_{key}"] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"DELETE FROM {table_name.lower()}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params


class SQLiteAdapter(DatabaseAdapter):
    """SQLite database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        # For SQLite, the database parameter is the file path
        return f"sqlite:///{database}"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        # SQLite doesn't support table comments, so we only return table names
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
                        limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"SELECT * FROM {table_name}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query += f" LIMIT {limit}"
        return query, params

    def get_count_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate COUNT query and parameters for pagination.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_paginated_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate paginated SELECT query and parameters.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
                        May include 'limit' and 'offset' keys for pagination
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT * FROM {table_name}"
        params = {}
        
        limit = conditions.pop('limit', None) if conditions else None
        offset = conditions.pop('offset', None) if conditions else None
        
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
            
        if limit is not None:
            query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
                
        return query, params

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        columns = ", ".join([k for k in data.keys()])
        placeholders = ", ".join([f":{k}" for k in data.keys()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        return query, data

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        set_strs = []
        params = {}
        for key, value in data.items():
            set_strs.append(f"{key} = :{key}_{len(params)}")
            params[f"{key}_{len(params)}"] = value
        
        query = f"UPDATE {table_name} SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :cond_{key}")
                params[f"cond_{key}"] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"DELETE FROM {table_name}"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params


class SQLServerAdapter(DatabaseAdapter):
    """SQL Server database adapter."""

    def get_connection_string(self, host: str, port: int, username: str, password: str, database: str) -> str:
        return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    def get_test_query(self) -> str:
        return "SELECT 1"

    def get_list_tables_query(self, pattern: Optional[str] = None) -> str:
        query = "SELECT t.TABLE_NAME, ex.value as table_comment FROM INFORMATION_SCHEMA.TABLES t LEFT JOIN sys.extended_properties ex ON t.TABLE_NAME = OBJECT_NAME(ex.major_id) AND ex.minor_id = 0 AND ex.name = 'MS_Description' WHERE t.TABLE_TYPE = 'BASE TABLE'"
        if pattern:
            # Convert LIKE pattern to SQL Server pattern
            pattern = pattern.replace('*', '%').replace('?', '_')
            query += f" AND t.TABLE_NAME LIKE '{pattern}'"
        query += " ORDER BY t.TABLE_NAME"
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
                        limit: Optional[int] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"SELECT * FROM [{table_name}]"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"[{key}] = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        if limit:
            query = f"SELECT TOP {limit} * FROM [{table_name}]"
            if conditions:
                condition_strs = []
                for key, value in conditions.items():
                    condition_strs.append(f"[{key}] = :{key}")
                    params[key] = value
                query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_count_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate COUNT query and parameters for pagination.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        query = f"SELECT COUNT(*) FROM [{table_name}]"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"[{key}] = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_paginated_select_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Generate paginated SELECT query and parameters.
        
        Args:
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
                        May include 'limit' and 'offset' keys for pagination
            
        Returns:
            Tuple of query string and parameters dictionary
        """
        # SQL Server uses a different approach for pagination
        query = f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM [{table_name}]"
        params = {}
        
        limit = conditions.pop('limit', None) if conditions else None
        offset = conditions.pop('offset', None) if conditions else None
        
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"[{key}] = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        
        query += ") AS numbered WHERE "
        
        if offset is not None:
            query += f"rn > {offset}"
            if limit is not None:
                query += f" AND rn <= {offset + limit}"
        elif limit is not None:
            query += f"rn <= {limit}"
        else:
            query = query.rstrip(" WHERE ")
            
        return query, params

    def get_insert_query(self, table_name: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        columns = ", ".join([k for k in data.keys()])
        placeholders = ", ".join([f":{k}" for k in data.keys()])
        query = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"
        return query, data

    def get_update_query(self, table_name: str, data: Dict[str, Any], 
                        conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        set_strs = []
        params = {}
        for key, value in data.items():
            set_strs.append(f"{key} = :{key}_{len(params)}")
            params[f"{key}_{len(params)}"] = value
        
        query = f"UPDATE [{table_name}] SET " + ", ".join(set_strs)
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :cond_{key}")
                params[f"cond_{key}"] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params

    def get_delete_query(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
        query = f"DELETE FROM [{table_name}]"
        params = {}
        if conditions:
            condition_strs = []
            for key, value in conditions.items():
                condition_strs.append(f"{key} = :{key}")
                params[key] = value
            query += " WHERE " + " AND ".join(condition_strs)
        return query, params


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