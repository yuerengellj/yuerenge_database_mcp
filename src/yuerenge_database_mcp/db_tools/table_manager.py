"""
Table Manager for handling table structure operations.
"""

import logging
import re
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


def is_datetime_string(value: str) -> bool:
    """
    Check if a string represents a datetime in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS

    Args:
        value: String to check

    Returns:
        bool: True if string matches datetime format, False otherwise
    """
    if not isinstance(value, str):
        return False

    # Pattern for date only (YYYY-MM-DD) or date with time (YYYY-MM-DD HH:MM:SS)
    datetime_pattern = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
    return bool(re.match(datetime_pattern, value))


def is_date_string(value: str) -> bool:
    """
    Check if a string represents a date in format YYYY-MM-DD

    Args:
        value: String to check

    Returns:
        bool: True if string matches date format, False otherwise
    """
    if not isinstance(value, str):
        return False

    # Pattern for date only (YYYY-MM-DD)
    date_pattern = r"^\d{4}-\d{2}-\d{2}$"
    return bool(re.match(date_pattern, value))


def format_datetime_for_oracle(value: Any) -> str:
    """
    Format a datetime value for Oracle database insertion.

    Args:
        value: Datetime value to format (datetime object or string)

    Returns:
        str: Formatted datetime string for Oracle
    """
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(value, date):
        return value.strftime('%Y-%m-%d')
    elif isinstance(value, str):
        # Check if it's a datetime string
        if is_datetime_string(value):
            # Ensure it has time component
            if ' ' not in value:
                return f"{value} 00:00:00"
            return value
        elif is_date_string(value):
            return f"{value} 00:00:00"
    return str(value)


def format_date_for_oracle(value: Any) -> str:
    """
    Format a date value for Oracle database insertion.

    Args:
        value: Date value to format (date object or string)

        Returns:
            str: Formatted date string for Oracle
    """
    if isinstance(value, (datetime, date)):
        return value.strftime('%Y-%m-%d')
    elif isinstance(value, str) and (is_datetime_string(value) or is_date_string(value)):
        # Extract date part
        if ' ' in value:
            return value.split(' ')[0]
        return value
    return str(value)


class TableManager:
    """Manages table structure operations."""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self.logger = logging.getLogger(__name__)

    def list_tables(self, connection_name: str, pattern: Optional[str] = None) -> Optional[List[str]]:
        """
        List all table names for the user in the specified database connection.
        If table has comments, return "table_name(table_comment)" format, otherwise return table_name only.

        Args:
            connection_name: Name of the database connection
            pattern: Optional pattern to filter table names (supports % as wildcard)

        Returns:
            List of table names (with comments if available) or None if connection not found/error occurred
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return None

        try:
            db_type = engine.url.drivername.split('+')[0]

            if db_type == "mysql":
                if pattern:
                    # For MySQL with pattern matching and comments
                    query = """
                    SELECT t.TABLE_NAME, t.TABLE_COMMENT
                    FROM information_schema.TABLES t
                    WHERE t.TABLE_SCHEMA = :schema 
                    AND t.TABLE_NAME LIKE :pattern
                    ORDER BY t.TABLE_NAME
                    """
                    database_name = engine.url.database
                    with engine.connect() as conn:
                        result = conn.execute(text(query), {"schema": database_name, "pattern": pattern})
                        tables = []
                        for row in result.fetchall():
                            table_name = row[0]
                            table_comment = row[1]
                            if table_comment:
                                tables.append(f"{table_name}({table_comment})")
                            else:
                                tables.append(table_name)
                        return tables
                else:
                    # For MySQL without pattern matching but with comments
                    query = """
                    SELECT t.TABLE_NAME, t.TABLE_COMMENT
                    FROM information_schema.TABLES t
                    WHERE t.TABLE_SCHEMA = %s
                    ORDER BY t.TABLE_NAME
                    """
                    database_name = engine.url.database
                    with engine.connect() as conn:
                        result = conn.execute(text(query), (database_name,))
                        tables = []
                        for row in result.fetchall():
                            table_name = row[0]
                            table_comment = row[1]
                            if table_comment:
                                tables.append(f"{table_name}({table_comment})")
                            else:
                                tables.append(table_name)
                        return tables

            elif db_type == "oracle":
                # For Oracle, we need to get the actual username, not the connection username
                with engine.connect() as conn:
                    # Get the actual user/owner for this connection
                    user_result = conn.execute(text("SELECT USER FROM dual"))
                    actual_username = user_result.fetchone()[0]

                    if pattern:
                        # For Oracle with pattern matching and comments
                        query = """
                        SELECT t.TABLE_NAME, tc.COMMENTS
                        FROM all_tables t
                        LEFT JOIN all_tab_comments tc ON t.OWNER = tc.OWNER AND t.TABLE_NAME = tc.TABLE_NAME
                        WHERE t.OWNER = :owner 
                        AND t.TABLE_NAME LIKE UPPER(:pattern)
                        ORDER BY t.TABLE_NAME
                        """
                        result = conn.execute(text(query), {"owner": actual_username, "pattern": pattern})
                        tables = []
                        for row in result.fetchall():
                            table_name = row[0]
                            table_comment = row[1]
                            if table_comment:
                                tables.append(f"{table_name}({table_comment})")
                            else:
                                tables.append(table_name)
                        return tables
                    else:
                        # For Oracle without pattern matching but with comments
                        query = """
                        SELECT t.TABLE_NAME, tc.COMMENTS
                        FROM all_tables t
                        LEFT JOIN all_tab_comments tc ON t.OWNER = tc.OWNER AND t.TABLE_NAME = tc.TABLE_NAME
                        WHERE t.OWNER = :owner
                        ORDER BY t.TABLE_NAME
                        """
                        result = conn.execute(text(query), {"owner": actual_username})
                        tables = []
                        for row in result.fetchall():
                            table_name = row[0]
                            table_comment = row[1]
                            if table_comment:
                                tables.append(f"{table_name}({table_comment})")
                            else:
                                tables.append(table_name)
                        return tables

            else:
                self.logger.error(f"Unsupported database type for listing tables: {db_type}")
                return None

        except SQLAlchemyError as e:
            self.logger.error(f"Failed to list tables for '{connection_name}': {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error listing tables for '{connection_name}': {str(e)}")
            return None

    def get_table_structure(self, connection_name: str, table_name: str, pattern: Optional[str] = None) -> Optional[
        List[Dict[str, Any]]]:
        """
        Get structure information for a specific table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            pattern: Optional pattern to filter column names (supports % as wildcard)

        Returns:
            List of dictionaries containing column information or None if error occurred
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return None

        try:
            db_type = engine.url.drivername.split('+')[0]

            if db_type == "mysql":
                if pattern:
                    # For MySQL with column pattern matching
                    query = """
                    SELECT 
                        COLUMN_NAME as column_name,
                        DATA_TYPE as data_type,
                        IS_NULLABLE as is_nullable,
                        COLUMN_DEFAULT as column_default,
                        CHARACTER_MAXIMUM_LENGTH as char_max_length,
                        NUMERIC_PRECISION as numeric_precision,
                        NUMERIC_SCALE as numeric_scale,
                        COLUMN_KEY as column_key,
                        EXTRA as extra,
                        COLUMN_COMMENT as column_comment
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = :schema 
                    AND TABLE_NAME = :table_name
                    AND COLUMN_NAME LIKE :pattern
                    ORDER BY ORDINAL_POSITION
                    """
                    database_name = engine.url.database
                    with engine.connect() as conn:
                        result = conn.execute(text(query), {
                            "schema": database_name,
                            "table_name": table_name,
                            "pattern": pattern
                        })
                        columns = result.keys()
                        rows = result.fetchall()
                else:
                    # For MySQL without column pattern matching
                    query = """
                    SELECT 
                        COLUMN_NAME as column_name,
                        DATA_TYPE as data_type,
                        IS_NULLABLE as is_nullable,
                        COLUMN_DEFAULT as column_default,
                        CHARACTER_MAXIMUM_LENGTH as char_max_length,
                        NUMERIC_PRECISION as numeric_precision,
                        NUMERIC_SCALE as numeric_scale,
                        COLUMN_KEY as column_key,
                        EXTRA as extra,
                        COLUMN_COMMENT as column_comment
                    FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                    """
                    database_name = engine.url.database
                    with engine.connect() as conn:
                        result = conn.execute(text(query), (database_name, table_name))
                        columns = result.keys()
                        rows = result.fetchall()

            elif db_type == "oracle":
                # For Oracle, we need to get the actual username
                with engine.connect() as conn:
                    # Get the actual user/owner for this connection
                    user_result = conn.execute(text("SELECT USER FROM dual"))
                    actual_username = user_result.fetchone()[0]

                    if pattern:
                        # For Oracle with column pattern matching
                        query = """
                        SELECT 
                            c.COLUMN_NAME as column_name,
                            c.DATA_TYPE as data_type,
                            CASE WHEN c.NULLABLE = 'Y' THEN 'YES' ELSE 'NO' END as is_nullable,
                            c.DATA_DEFAULT as column_default,
                            c.CHAR_LENGTH as char_max_length,
                            c.DATA_PRECISION as numeric_precision,
                            c.DATA_SCALE as numeric_scale,
                            c.COLUMN_ID as column_key,
                            NULL as extra,
                            acc.COMMENTS as column_comment
                        FROM all_tab_columns c
                        LEFT JOIN all_col_comments acc 
                            ON c.OWNER = acc.OWNER 
                            AND c.TABLE_NAME = acc.TABLE_NAME 
                            AND c.COLUMN_NAME = acc.COLUMN_NAME
                        WHERE c.OWNER = :owner 
                        AND c.TABLE_NAME = :table_name
                        AND c.COLUMN_NAME LIKE UPPER(:pattern)
                        ORDER BY c.COLUMN_ID
                        """
                        result = conn.execute(text(query), {
                            "owner": actual_username,
                            "table_name": table_name.upper(),
                            "pattern": pattern
                        })
                        columns = result.keys()
                        rows = result.fetchall()
                    else:
                        # For Oracle without column pattern matching
                        query = """
                        SELECT 
                            c.COLUMN_NAME as column_name,
                            c.DATA_TYPE as data_type,
                            CASE WHEN c.NULLABLE = 'Y' THEN 'YES' ELSE 'NO' END as is_nullable,
                            c.DATA_DEFAULT as column_default,
                            c.CHAR_LENGTH as char_max_length,
                            c.DATA_PRECISION as numeric_precision,
                            c.DATA_SCALE as numeric_scale,
                            c.COLUMN_ID as column_key,
                            NULL as extra,
                            acc.COMMENTS as column_comment
                        FROM all_tab_columns c
                        LEFT JOIN all_col_comments acc 
                            ON c.OWNER = acc.OWNER 
                            AND c.TABLE_NAME = acc.TABLE_NAME 
                            AND c.COLUMN_NAME = acc.COLUMN_NAME
                        WHERE c.OWNER = :owner 
                        AND c.TABLE_NAME = :table_name
                        ORDER BY c.COLUMN_ID
                        """
                        result = conn.execute(text(query), {
                            "owner": actual_username,
                            "table_name": table_name.upper()
                        })
                        columns = result.keys()
                        rows = result.fetchall()

            else:
                self.logger.error(f"Unsupported database type for table structure: {db_type}")
                return None

            # Convert to list of dictionaries
            return [dict(zip(columns, row)) for row in rows]

        except SQLAlchemyError as e:
            self.logger.error(f"Failed to get table structure for '{connection_name}.{table_name}': {str(e)}")
            return None
        except Exception as e:
            self.logger.error(
                f"Unexpected error getting table structure for '{connection_name}.{table_name}': {str(e)}")
            return None

    def create_table(self, connection_name: str, table_name: str,
                     columns: List[Dict[str, Any]], table_comment: Optional[str] = None) -> bool:
        """
        Create a new table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table to create
            columns: List of dictionaries defining columns. Each dict should have:
                    - name: column name
                    - type: column type
                    - nullable: whether column can be null (optional, defaults to True)
                    - primary_key: whether column is primary key (optional)
                    - default: default value (optional)
                    - comment: column comment (optional)
            table_comment: Comment for the table (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return False

        try:
            db_type = engine.url.drivername.split('+')[0]

            # Build CREATE TABLE query
            column_defs = []
            primary_keys = []
            column_comments = []

            for col in columns:
                col_def = f"{col['name']} {col['type']}"

                # Handle length specification
                if "length" in col:
                    if isinstance(col["length"], (list, tuple)):
                        col_def += f"({col['length'][0]},{col['length'][1]})"  # precision,scale
                    else:
                        col_def += f"({col['length']})"

                # Handle nullable constraint
                if "nullable" in col and not col["nullable"]:
                    col_def += " NOT NULL"

                # Handle default value
                if "default" in col:
                    default_val = col["default"]
                    if isinstance(default_val, str):
                        col_def += f" DEFAULT '{default_val}'"
                    else:
                        col_def += f" DEFAULT {default_val}"

                # Collect primary keys
                if "primary_key" in col and col["primary_key"]:
                    primary_keys.append(col["name"])

                # Collect column comments
                if "comment" in col and col["comment"]:
                    if db_type == "mysql":
                        col_def += f" COMMENT '{col['comment']}'"
                    elif db_type == "oracle":
                        column_comments.append((col['name'], col['comment']))

                column_defs.append(col_def)

            # Add primary key constraint
            if primary_keys:
                pk_def = f"CONSTRAINT pk_{table_name} PRIMARY KEY ({', '.join(primary_keys)})"
                column_defs.append(pk_def)

            query = f"CREATE TABLE {table_name} ({', '.join(column_defs)})"

            # Add table comment for MySQL
            if table_comment and db_type == "mysql":
                query += f" COMMENT='{table_comment}'"

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query))

                    # Add table comment for Oracle
                    if table_comment and db_type == "oracle":
                        comment_query = f"COMMENT ON TABLE {table_name} IS '{table_comment}'"
                        conn.execute(text(comment_query))

                    # Add column comments for Oracle
                    if db_type == "oracle":
                        for col_name, col_comment in column_comments:
                            comment_query = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{col_comment}'"
                            conn.execute(text(comment_query))

                    trans.commit()
                    self.logger.info(f"Successfully created table '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Create table failed on '{connection_name}.{table_name}': {str(e)}")
            return False

    def drop_table(self, connection_name: str, table_name: str,
                   cascade: bool = False) -> bool:
        """
        Drop a table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table to drop
            cascade: Whether to drop dependent objects (CASCADE CONSTRAINTS in Oracle)

        Returns:
            bool: True if successful, False otherwise
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return False

        try:
            db_type = engine.url.drivername.split('+')[0]

            # Build DROP TABLE query
            query = f"DROP TABLE {table_name}"
            if cascade:
                if db_type == "oracle":
                    query += " CASCADE CONSTRAINTS"
                elif db_type == "mysql":
                    query += " CASCADE"

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query))
                    trans.commit()
                    self.logger.info(f"Successfully dropped table '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Drop table failed on '{connection_name}.{table_name}': {str(e)}")
            return False

    def alter_table(self, connection_name: str, table_name: str,
                    operations: List[Dict[str, Any]]) -> bool:
        """
        Alter table structure with various operations.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table to alter
            operations: List of operations to perform. Each operation is a dict with:
                       - operation: Type of operation ('add_column', 'drop_column', 'modify_column', 'rename_column')
                       - For add_column: name, type, [length], [nullable], [default], [comment]
                       - For drop_column: name
                       - For modify_column: name, type, [length], [nullable], [default], [comment]
                       - For rename_column: old_name, new_name

        Returns:
            bool: True if successful, False otherwise
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return False

        try:
            db_type = engine.url.drivername.split('+')[0]

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for op in operations:
                        operation = op.get('operation')

                        if operation == 'add_column':
                            # Add a new column
                            column_def = op['name'] + ' ' + op['type']

                            # Handle length specification
                            if 'length' in op:
                                if isinstance(op['length'], (list, tuple)):
                                    column_def += f"({op['length'][0]},{op['length'][1]})"  # precision,scale
                                else:
                                    column_def += f"({op['length']})"

                            # Handle nullable constraint
                            if 'nullable' in op and not op['nullable']:
                                column_def += ' NOT NULL'

                            # Handle default value
                            if 'default' in op:
                                default_val = op['default']
                                if isinstance(default_val, str):
                                    column_def += f" DEFAULT '{default_val}'"
                                else:
                                    column_def += f" DEFAULT {default_val}"

                            query = f"ALTER TABLE {table_name} ADD COLUMN {column_def}"
                            if db_type == "oracle":
                                # Oracle doesn't use "COLUMN" keyword
                                query = f"ALTER TABLE {table_name} ADD ({column_def})"

                            conn.execute(text(query))

                            # Add column comment if provided
                            if 'comment' in op and op['comment']:
                                if db_type == "mysql":
                                    comment_query = f"ALTER TABLE {table_name} MODIFY COLUMN {op['name']} {op['type']} COMMENT '{op['comment']}'"
                                    conn.execute(text(comment_query))
                                elif db_type == "oracle":
                                    comment_query = f"COMMENT ON COLUMN {table_name}.{op['name']} IS '{op['comment']}'"
                                    conn.execute(text(comment_query))

                        elif operation == 'drop_column':
                            # Drop a column
                            query = f"ALTER TABLE {table_name} DROP COLUMN {op['name']}"
                            if db_type == "oracle":
                                # Oracle doesn't use "COLUMN" keyword
                                query = f"ALTER TABLE {table_name} DROP ({op['name']})"

                            conn.execute(text(query))

                        elif operation == 'modify_column':
                            # Modify a column
                            if db_type == "mysql":
                                column_def = op['name'] + ' ' + op['type']

                                # Handle length specification
                                if 'length' in op:
                                    if isinstance(op['length'], (list, tuple)):
                                        column_def += f"({op['length'][0]},{op['length'][1]})"  # precision,scale
                                    else:
                                        column_def += f"({op['length']})"

                                # Handle nullable constraint
                                if 'nullable' in op and not op['nullable']:
                                    column_def += ' NOT NULL'
                                else:
                                    column_def += ' NULL'

                                # Handle default value
                                if 'default' in op:
                                    default_val = op['default']
                                    if isinstance(default_val, str):
                                        column_def += f" DEFAULT '{default_val}'"
                                    else:
                                        column_def += f" DEFAULT {default_val}"

                                query = f"ALTER TABLE {table_name} MODIFY COLUMN {column_def}"
                                conn.execute(text(query))

                                # Add column comment if provided
                                if 'comment' in op and op['comment']:
                                    comment_query = f"ALTER TABLE {table_name} MODIFY COLUMN {op['name']} {op['type']} COMMENT '{op['comment']}'"
                                    conn.execute(text(comment_query))

                            elif db_type == "oracle":
                                # For Oracle, we may need multiple operations
                                # Change data type if specified
                                if 'type' in op:
                                    column_def = op['name'] + ' ' + op['type']

                                    # Handle length specification
                                    if 'length' in op:
                                        if isinstance(op['length'], (list, tuple)):
                                            column_def += f"({op['length'][0]},{op['length'][1]})"  # precision,scale
                                        else:
                                            column_def += f"({op['length']})"

                                    query = f"ALTER TABLE {table_name} MODIFY ({column_def})"
                                    conn.execute(text(query))

                                # Change nullability if specified
                                if 'nullable' in op:
                                    if not op['nullable']:
                                        query = f"ALTER TABLE {table_name} MODIFY ({op['name']} NOT NULL)"
                                    else:
                                        query = f"ALTER TABLE {table_name} MODIFY ({op['name']} NULL)"
                                    conn.execute(text(query))

                                # Change default value if specified
                                if 'default' in op:
                                    if op['default'] is None:
                                        query = f"ALTER TABLE {table_name} MODIFY ({op['name']} DEFAULT NULL)"
                                    else:
                                        default_val = op['default']
                                        if isinstance(default_val, str):
                                            query = f"ALTER TABLE {table_name} MODIFY ({op['name']} DEFAULT '{default_val}')"
                                        else:
                                            query = f"ALTER TABLE {table_name} MODIFY ({op['name']} DEFAULT {default_val})"
                                    conn.execute(text(query))

                                # Add column comment if provided
                                if 'comment' in op and op['comment']:
                                    comment_query = f"COMMENT ON COLUMN {table_name}.{op['name']} IS '{op['comment']}'"
                                    conn.execute(text(comment_query))

                        elif operation == 'rename_column':
                            # Rename a column
                            if db_type == "mysql":
                                # Need column definition for rename in MySQL
                                # This is a simplified version - in practice you would need the full column definition
                                query = f"ALTER TABLE {table_name} CHANGE COLUMN {op['old_name']} {op['new_name']} {op.get('definition', '')}"
                                conn.execute(text(query))
                            elif db_type == "oracle":
                                query = f"ALTER TABLE {table_name} RENAME COLUMN {op['old_name']} TO {op['new_name']}"
                                conn.execute(text(query))

                    trans.commit()
                    self.logger.info(f"Successfully altered table '{connection_name}.{table_name}'")
                    return True

                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Alter table failed on '{connection_name}.{table_name}': {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during alter table on '{connection_name}.{table_name}': {str(e)}")
            return False