"""
Data Manager for handling data operations.
"""

import logging
import re
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Import the functions we need for Oracle datetime handling
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


class DataManager:
    """Manages data operations."""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self.logger = logging.getLogger(__name__)

    def execute_query(self, connection_name: str, query: str, params: Optional[Dict[str, Any]] = None, commit: bool = False) -> Optional[list]:
        """
        Execute a query on a specific database.

        Args:
            connection_name: Name of the database connection
            query: SQL query to execute
            params: Parameters for the SQL query
            commit: Whether to commit the transaction (useful for INSERT/UPDATE/DELETE)

        Returns:
            list: Query results or None if connection not found/error occurred
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return None

        try:
            with engine.connect() as conn:
                trans = conn.begin() if commit else None
                try:
                    if params:
                        result = conn.execute(text(query), params)
                    else:
                        result = conn.execute(text(query))
                    if commit:
                        trans.commit()
                    # Convert to list of dictionaries
                    columns = result.keys()
                    return [dict(zip(columns, row)) for row in result.fetchall()]
                except Exception as e:
                    if commit and trans:
                        trans.rollback()
                    raise e
        except SQLAlchemyError as e:
            self.logger.error(f"Query execution failed on '{connection_name}': {str(e)}")
            return None

    def select_data(self, connection_name: str, table_name: str, conditions: Optional[Dict[str, Any]] = None,
                    limit: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Select data from a specific table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            limit: Optional limit for number of rows returned

        Returns:
            List of dictionaries containing row data or None if error occurred
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
            # Use adapter to generate SELECT query and parameters
            query, params = adapter.get_select_query(table_name, conditions, limit)

            with engine.connect() as conn:
                result = conn.execute(text(query), params)
                columns = result.keys()
                rows = result.fetchall()

                # Process rows to handle datetime objects
                processed_rows = []
                for row in rows:
                    processed_row = {}
                    for i, column in enumerate(columns):
                        value = row[i]
                        # Handle datetime objects by converting to string
                        if hasattr(value, 'strftime'):
                            processed_row[column] = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            processed_row[column] = value
                    processed_rows.append(processed_row)

                return processed_rows

        except SQLAlchemyError as e:
            self.logger.error(f"Select data failed on '{connection_name}.{table_name}': {str(e)}")
            return None

    def insert_data(self, connection_name: str, table_name: str, data: Dict[str, Any]) -> bool:
        """
        Insert data into a specific table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            data: Dictionary of column-value pairs to insert

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
            # Process data for datetime objects
            processed_data = {}
            for key, value in data.items():
                # Handle datetime objects and datetime strings
                if isinstance(value, datetime):
                    processed_data[key] = value
                elif isinstance(value, str):
                    # For Oracle, check if string looks like a datetime
                    if "oracle" in engine.url.drivername and is_datetime_string(value):
                        # Treat as datetime string for Oracle
                        processed_data[key] = value
                    else:
                        processed_data[key] = value
                else:
                    processed_data[key] = value

            # Use adapter to generate INSERT query and parameters
            query, params = adapter.get_insert_query(table_name, processed_data)

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # For Oracle, we may need special handling
                    if "oracle" in engine.url.drivername:
                        # Handle Oracle datetime values
                        oracle_params = {}
                        for key, value in params.items():
                            if isinstance(value, datetime) or (isinstance(value, str) and is_datetime_string(value)):
                                oracle_params[key] = format_datetime_for_oracle(value)
                            else:
                                oracle_params[key] = value
                        conn.execute(text(query), oracle_params)
                    else:
                        conn.execute(text(query), params)
                    trans.commit()
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Insert data failed on '{connection_name}.{table_name}': {str(e)}")
            return False

    def update_data(self, connection_name: str, table_name: str, data: Dict[str, Any],
                    conditions: Optional[Dict[str, Any]] = None) -> int:
        """
        Update data in a specific table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            data: Dictionary of column-value pairs to update
            conditions: Optional dictionary of column-value pairs for WHERE clause

        Returns:
            int: Number of rows affected, -1 if error occurred
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return -1

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return -1

        try:
            # Use adapter to generate UPDATE query and parameters
            query, params = adapter.get_update_query(table_name, data, conditions)

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(text(query), params)
                    trans.commit()
                    return result.rowcount
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Update data failed on '{connection_name}.{table_name}': {str(e)}")
            return -1

    def delete_data(self, connection_name: str, table_name: str,
                    conditions: Optional[Dict[str, Any]] = None) -> int:
        """
        Delete data from a specific table.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause

        Returns:
            int: Number of rows affected, -1 if error occurred
        """
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            self.logger.error(f"Connection '{connection_name}' not found")
            return -1

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            self.logger.error(f"Adapter for connection '{connection_name}' not found")
            return -1

        try:
            # Use adapter to generate DELETE query and parameters
            query, params = adapter.get_delete_query(table_name, conditions)

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(text(query), params)
                    trans.commit()
                    return result.rowcount
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Delete data failed on '{connection_name}.{table_name}': {str(e)}")
            return -1