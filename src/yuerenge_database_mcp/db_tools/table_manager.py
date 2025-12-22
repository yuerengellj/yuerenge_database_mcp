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

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return None

        try:
            # Use adapter to get the list tables query
            query = adapter.get_list_tables_query(pattern)
            
            with engine.connect() as conn:
                if pattern:
                    result = conn.execute(text(query))
                else:
                    result = conn.execute(text(query))
                    
                if "mysql" in engine.url.drivername:
                    tables = []
                    for row in result.fetchall():
                        table_name = row[0]
                        table_comment = row[1] if len(row) > 1 else None
                        if table_comment:
                            tables.append(f"{table_name}({table_comment})")
                        else:
                            tables.append(table_name)
                    return tables
                elif "oracle" in engine.url.drivername:
                    tables = []
                    for row in result.fetchall():
                        table_name = row[0]
                        table_comment = row[1] if len(row) > 1 else None
                        if table_comment:
                            tables.append(f"{table_name}({table_comment})")
                        else:
                            tables.append(table_name)
                    return tables
                else:
                    # For other databases, just return table names
                    return [row[0] for row in result.fetchall()]

        except SQLAlchemyError as e:
            self.logger.error(f"List tables failed on '{connection_name}': {str(e)}")
            return None

    def get_table_structure(self, connection_name: str, table_name: str, pattern: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
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

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return None

        try:
            # Use adapter to get the table structure query
            query = adapter.get_table_structure_query(table_name)
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                
                # Use adapter to format column info
                columns = []
                for row in rows:
                    column_info = adapter.format_column_info(row)
                    columns.append(column_info)
                
                return columns

        except SQLAlchemyError as e:
            self.logger.error(f"Get table structure failed on '{connection_name}.{table_name}': {str(e)}")
            return None

    def create_table(self, connection_name: str, table_name: str, 
                     columns: List[Dict[str, Any]], table_comment: Optional[str] = None) -> bool:
        """
        Create a new table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table to create
            columns: List of dictionaries defining columns
            table_comment: Comment for the table (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return False

        try:
            # Use adapter to generate CREATE TABLE statement
            query = adapter.get_create_table_statement(table_name, columns, table_comment)
            
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query))
                    trans.commit()
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Create table failed on '{connection_name}.{table_name}': {str(e)}")
            return False

    def drop_table(self, connection_name: str, table_name: str, cascade: bool = False) -> bool:
        """
        Drop a table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table to drop
            cascade: Whether to drop dependent objects

        Returns:
            bool: True if successful, False otherwise
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return False

        try:
            # Use adapter to generate DROP TABLE statement
            query = adapter.get_drop_table_statement(table_name, cascade)
            
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query))
                    trans.commit()
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
            operations: List of operations to perform

        Returns:
            bool: True if successful, False otherwise
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return False

        try:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for operation in operations:
                        # Use adapter to generate ALTER TABLE statement
                        query = adapter.get_alter_table_statement(table_name, operation)
                        conn.execute(text(query))
                    trans.commit()
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Alter table failed on '{connection_name}.{table_name}': {str(e)}")
            return False
