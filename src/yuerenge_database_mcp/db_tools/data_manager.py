"""
Data Manager for handling data operations.
"""

import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class DataManager:
    """Manages data operations."""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self.logger = logging.getLogger(__name__)

    def execute_query(self, connection_name: str, query: str, commit: bool = False) -> Optional[list]:
        """
        Execute a query on a specific database.

        Args:
            connection_name: Name of the database connection
            query: SQL query to execute
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

        try:
            # Build query
            query = f"SELECT * FROM {table_name}"
            params = {}

            # Add WHERE conditions if provided
            if conditions:
                where_clauses = []
                for i, (column, value) in enumerate(conditions.items()):
                    param_name = f"param_{i}"
                    where_clauses.append(f"{column} = :{param_name}")
                    params[param_name] = value
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

            # Add LIMIT if provided
            if limit:
                db_type = engine.url.drivername.split('+')[0]
                if db_type == "mysql":
                    query += f" LIMIT {limit}"
                elif db_type == "oracle":
                    # For Oracle, we need to wrap the query
                    query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"

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

        try:
            db_type = engine.url.drivername.split('+')[0]

            # Process data for datetime objects
            processed_data = {}
            for key, value in data.items():
                # Handle datetime objects and datetime strings
                if isinstance(value, datetime):
                    if db_type == "oracle":
                        # For Oracle, we need to use TO_DATE function
                        processed_data[key] = value
                    else:
                        # For MySQL, keep as datetime object
                        processed_data[key] = value
                elif isinstance(value, str) and db_type == "oracle":
                    # For Oracle, check if string looks like a datetime
                    if is_datetime_string(value):
                        # Treat as datetime string for Oracle
                        processed_data[key] = value
                    else:
                        processed_data[key] = value
                else:
                    processed_data[key] = value

            # Build INSERT query
            columns = list(processed_data.keys())

            if db_type == "oracle":
                # For Oracle, we need special handling for datetime
                placeholders = []
                params = {}
                for i, col in enumerate(columns):
                    # Check if value is datetime object or datetime string
                    if isinstance(processed_data[col], datetime) or (
                            isinstance(processed_data[col], str) and is_datetime_string(processed_data[col])):
                        # Handle datetime values with TO_DATE function
                        placeholders.append(f"TO_DATE(:{col}, 'YYYY-MM-DD HH24:MI:SS')")
                        params[col] = format_datetime_for_oracle(processed_data[col])
                    else:
                        # Handle non-datetime values normally
                        placeholders.append(f":{col}")
                        params[col] = processed_data[col]
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                final_params = params
            else:
                # For MySQL and others
                placeholders = [f":{col}" for col in columns]
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                final_params = processed_data

            with engine.connect() as conn:
                # For Oracle, use direct SQL approach first
                if db_type == "oracle":
                    try:
                        # Build a direct SQL statement for Oracle
                        direct_values = []
                        for col in columns:
                            val = processed_data[col]
                            if isinstance(val, datetime) or (isinstance(val, str) and is_datetime_string(val)):
                                # Format datetime for direct inclusion in SQL
                                formatted_dt = format_datetime_for_oracle(val)
                                direct_values.append(f"TO_DATE('{formatted_dt}', 'YYYY-MM-DD HH24:MI:SS')")
                            elif isinstance(val, str):
                                # Escape single quotes in string values
                                escaped_val = val.replace("'", "''")
                                direct_values.append(f"'{escaped_val}'")
                            elif val is None:
                                direct_values.append("NULL")
                            else:
                                direct_values.append(str(val))

                        direct_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(direct_values)})"
                        conn.execute(text(direct_query))
                        conn.execute(text("COMMIT"))
                        self.logger.info(f"Successfully inserted data using direct SQL: {direct_query}")
                        return True
                    except Exception as e1:
                        self.logger.error(f"Direct SQL approach failed: {str(e1)}")
                        self.logger.error(f"Direct query: {direct_query}")

                # For other databases or if direct SQL failed for Oracle
                trans = conn.begin()
                try:
                    result = conn.execute(text(query), final_params)
                    trans.commit()
                    self.logger.info(f"Successfully inserted data into '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    # For Oracle, try alternative approach with explicit COMMIT
                    if db_type == "oracle":
                        try:
                            # Try with manual commit
                            conn.execute(text(query), final_params)
                            conn.execute(text("COMMIT"))
                            self.logger.info(
                                f"Successfully inserted data into '{connection_name}.{table_name}' using manual commit")
                            return True
                        except Exception as e2:
                            self.logger.error(f"Manual commit approach also failed: {str(e2)}")
                    self.logger.error(f"Error inserting data: {str(e)}")
                    self.logger.error(f"Query: {query}")
                    self.logger.error(f"Params: {final_params}")
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Insert data failed on '{connection_name}.{table_name}': {str(e)}")
            self.logger.error(f"Query: {query}")
            self.logger.error(f"Parameters: {final_params}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during insert on '{connection_name}.{table_name}': {str(e)}")
            self.logger.error(f"Query: {query}")
            self.logger.error(f"Parameters: {final_params}")
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

        try:
            db_type = engine.url.drivername.split('+')[0]

            if db_type == "oracle":
                # For Oracle, build query directly with TO_DATE functions for datetime strings
                set_clauses = []
                params = {}

                # Process each data item
                for i, (column, value) in enumerate(data.items()):
                    param_name = f"set_{i}"
                    if isinstance(value, datetime) or (isinstance(value, str) and is_datetime_string(value)):
                        # Handle datetime values with TO_DATE function
                        set_clauses.append(f"{column} = TO_DATE(:{param_name}, 'YYYY-MM-DD HH24:MI:SS')")
                        params[param_name] = format_datetime_for_oracle(value)
                    else:
                        # Handle non-datetime values normally
                        set_clauses.append(f"{column} = :{param_name}")
                        params[param_name] = value

                # Build the query
                query = f"UPDATE {table_name} SET {', '.join(set_clauses)}"

                # Add WHERE conditions if provided
                if conditions:
                    where_clauses = []
                    for i, (column, value) in enumerate(conditions.items()):
                        param_name = f"where_{i}"
                        if isinstance(value, datetime) or (isinstance(value, str) and is_datetime_string(value)):
                            # Handle datetime values with TO_DATE function
                            where_clauses.append(f"{column} = TO_DATE(:{param_name}, 'YYYY-MM-DD HH24:MI:SS')")
                            params[param_name] = format_datetime_for_oracle(value)
                        else:
                            # Handle non-datetime values normally
                            where_clauses.append(f"{column} = :{param_name}")
                            params[param_name] = value
                    if where_clauses:
                        query += " WHERE " + " AND ".join(where_clauses)

            else:
                # For MySQL and others, use parameterized queries
                set_clauses = []
                params = {}

                # Process each data item
                for i, (column, value) in enumerate(data.items()):
                    param_name = f"set_{i}"
                    set_clauses.append(f"{column} = :{param_name}")
                    params[param_name] = value

                # Build the query
                query = f"UPDATE {table_name} SET {', '.join(set_clauses)}"

                # Add WHERE conditions if provided
                if conditions:
                    where_clauses = []
                    for i, (column, value) in enumerate(conditions.items()):
                        param_name = f"where_{i}"
                        where_clauses.append(f"{column} = :{param_name}")
                        params[param_name] = value
                    if where_clauses:
                        query += " WHERE " + " AND ".join(where_clauses)

            # Execute the query
            with engine.connect() as conn:
                try:
                    result = conn.execute(text(query), params)
                    # For Oracle, we need to explicitly commit
                    if db_type == "oracle":
                        conn.execute(text("COMMIT"))
                    row_count = result.rowcount
                    self.logger.info(f"Successfully updated {row_count} rows in '{connection_name}.{table_name}'")
                    return row_count
                except Exception as e:
                    self.logger.error(f"Exception during update: {str(e)}")
                    self.logger.error(f"Query: {query}")
                    self.logger.error(f"Params: {params}")
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Update data failed on '{connection_name}.{table_name}': {str(e)}")
            self.logger.error(f"Query: {query}")
            self.logger.error(f"Parameters: {params}")
            return -1
        except Exception as e:
            self.logger.error(f"Unexpected error during update on '{connection_name}.{table_name}': {str(e)}")
            self.logger.error(f"Query: {query}")
            self.logger.error(f"Parameters: {params}")
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

        try:
            # Build DELETE query
            query = f"DELETE FROM {table_name}"
            params = {}

            # Add WHERE conditions if provided
            if conditions:
                where_clauses = []
                for i, (column, value) in enumerate(conditions.items()):
                    param_name = f"where_{i}"
                    where_clauses.append(f"{column} = :{param_name}")
                    params[param_name] = value
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(text(query), params)
                    trans.commit()
                    row_count = result.rowcount
                    self.logger.info(f"Successfully deleted {row_count} rows from '{connection_name}.{table_name}'")
                    return row_count
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            self.logger.error(f"Delete data failed on '{connection_name}.{table_name}': {str(e)}")
            return -1