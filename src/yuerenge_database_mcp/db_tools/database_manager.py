"""
Database Manager for handling multiple database connections.
"""
import logging
import re
from datetime import datetime, date
from typing import Dict, Any, Optional, List

from sqlalchemy import create_engine, text
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


class DatabaseManager:
    """Manages multiple database connections."""

    def __init__(self):
        self.connections: Dict[str, Engine] = {}
        self.logger = logging.getLogger(__name__)

    def add_connection(
            self,
            name: str,
            db_type: str,
            host: str,
            port: int,
            username: str,
            password: str,
            database: str,
            **kwargs
    ) -> bool:
        """
        Add a new database connection.

        Args:
            name: Connection identifier
            db_type: Type of database (mysql, oracle, etc.)
            host: Database host
            port: Database port
            username: Database username
            password: Database password
            database: Database name
            **kwargs: Additional connection parameters

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Create connection string based on database type
            if db_type.lower() == "mysql":
                connection_string = (
                    f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
                )
            elif db_type.lower() == "oracle":
                # Try service_name format first (works for Oracle 19c)
                connection_string = (
                    f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={database}"
                )
            elif db_type.lower() == "postgresql":
                connection_string = (
                    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
                )
            else:
                raise ValueError(f"Unsupported database type: {db_type}")

            # Create engine
            engine = create_engine(connection_string, **kwargs)

            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1 FROM dual" if db_type.lower() == "oracle" else "SELECT 1"))

            # Store connection
            self.connections[name] = engine
            self.logger.info(f"Successfully connected to {db_type} database '{name}'")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to database '{name}': {str(e)}")
            return False

    def remove_connection(self, name: str) -> bool:
        """
        Remove a database connection.

        Args:
            name: Connection identifier

        Returns:
            bool: True if removed, False if not found
        """
        if name in self.connections:
            try:
                self.connections[name].dispose()
                del self.connections[name]
                self.logger.info(f"Removed database connection '{name}'")
                return True
            except Exception as e:
                self.logger.error(f"Error disposing connection '{name}': {str(e)}")
                return False
        return False

    def initialize_from_config(self, config_connections: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Initialize database connections from configuration.

        Args:
            config_connections: List of connection configurations

        Returns:
            Dict mapping connection names to connection success status
        """
        results = {}
        for conn_config in config_connections:
            # Skip disabled connections
            if not conn_config.get("enabled", False):
                continue

            name = conn_config["name"]
            try:
                success = self.add_connection(
                    name=name,
                    db_type=conn_config["type"],
                    host=conn_config["host"],
                    port=conn_config["port"],
                    username=conn_config["username"],
                    password=conn_config["password"],
                    database=conn_config["database"]
                )
                results[name] = success
            except Exception as e:
                self.logger.error(f"Failed to initialize connection '{name}': {str(e)}")
                results[name] = False
        return results

    def get_connection(self, name: str) -> Optional[Engine]:
        """
        Get a database engine by name.

        Args:
            name: Connection identifier

        Returns:
            Engine: Database engine or None if not found
        """
        return self.connections.get(name)

    def list_connections(self) -> Dict[str, str]:
        """
        List all connection names and their database types.

        Returns:
            Dict[str, str]: Mapping of connection names to database types
        """
        return {
            name: str(engine.url.drivername.split('+')[0])
            for name, engine in self.connections.items()
        }

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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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
        engine = self.get_connection(connection_name)
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

    def format_as_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as a table string with column comments.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Calculate column widths with a maximum limit to prevent overly wide columns
        MAX_COL_WIDTH = 50  # Maximum column width
        MIN_COL_WIDTH = 8   # Minimum column width
        col_widths = {}
        for i, header in enumerate(headers):
            col_widths[i] = min(max(len(str(header)), MIN_COL_WIDTH), MAX_COL_WIDTH)
            for row in data:
                col_value = str(row.get(columns[i], ''))
                # Limit the width of the content when calculating column width
                col_widths[i] = min(max(col_widths[i], len(col_value)), MAX_COL_WIDTH)

        # Create format string
        format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"

        # Create separator
        separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

        # Build table
        lines = [separator]
        lines.append(format_str.format(*headers))
        lines.append(separator)

        for row in data:
            formatted_row = []
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                # Truncate long values for display
                if len(col_value) > MAX_COL_WIDTH:
                    col_value = col_value[:MAX_COL_WIDTH - 3] + "..."
                formatted_row.append(col_value)
            lines.append(format_str.format(*formatted_row))

        lines.append(separator)

        return "\n".join(lines)

    def format_as_ide_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as a table string with adaptive column widths for IDE display.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted table string with adaptive column widths
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Calculate adaptive column widths based on content
        col_widths = {}
        # First calculate width based on headers
        for i, header in enumerate(headers):
            col_widths[i] = len(str(header))

        # Then adjust based on data content
        for row in data:
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                # Replace newlines with spaces for width calculation
                col_value_single_line = col_value.replace('\n', ' ')
                col_widths[i] = max(col_widths[i], len(col_value_single_line))

        # Apply reasonable maximum width to prevent overly wide columns
        MAX_COL_WIDTH = 100
        for i in col_widths:
            col_widths[i] = min(col_widths[i], MAX_COL_WIDTH)

        # Apply minimum width to ensure proper visibility
        MIN_COL_WIDTH = 8
        for i in col_widths:
            col_widths[i] = max(col_widths[i], MIN_COL_WIDTH)

        # Create format string
        format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"

        # Create separator
        separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

        # Build table
        lines = [separator]
        lines.append(format_str.format(*headers))
        lines.append(separator)

        for row in data:
            formatted_row = []
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                # Replace newlines with spaces for display
                col_value = col_value.replace('\n', ' ')
                formatted_row.append(col_value)
            lines.append(format_str.format(*formatted_row))

        lines.append(separator)

        return "\n".join(lines)

    def format_as_scrollable_html_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as an HTML table string with horizontal scrolling support.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted HTML table string with horizontal scrolling
        """
        if not data:
            return "<!DOCTYPE html><html><head><title>No Data</title></head><body><p>No data to display</p></body></html>"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Start building HTML with horizontal scrolling support
        html_lines = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "    <meta charset=\"UTF-8\">",
            "    <title>Table Data: {}</title>".format(table_name),
            "    <style>",
            "        body { font-family: Arial, sans-serif; margin: 20px; }",
            "        .table-container { overflow-x: auto; white-space: nowrap; }",
            "        table { border-collapse: collapse; width: 100%; }",
            "        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }",
            "        th { background-color: #f2f2f2; font-weight: bold; }",
            "        tr:nth-child(even) { background-color: #f9f9f9; }",
            "        pre { white-space: pre-wrap; word-wrap: break-word; margin: 0; }",
            "    </style>",
            "</head>",
            "<body>",
            "    <h2>Table: {}</h2>".format(table_name),
            "    <div class=\"table-container\">",
            "        <table>",
            "            <thead>",
            "                <tr>"
        ]

        # Add headers
        for header in headers:
            html_lines.append(
                "                    <th>{}</th>".format(header.replace('<', '&lt;').replace('>', '&gt;')))

        html_lines.extend([
            "                </tr>",
            "            </thead>",
            "            <tbody>"
        ])

        # Add data rows
        for row in data:
            html_lines.append("                <tr>")
            for col in columns:
                col_value = str(row.get(col, ''))
                # Escape HTML special characters
                col_value = col_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                # Preserve line breaks
                col_value = col_value.replace('\n', '<br>')
                html_lines.append("                    <td><pre>{}</pre></td>".format(col_value))
            html_lines.append("                </tr>")

        # Close HTML tags
        html_lines.extend([
            "            </tbody>",
            "        </table>",
            "    </div>",
            "</body>",
            "</html>"
        ])

        return "\n".join(html_lines)

    def format_as_html_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as an HTML table string with column comments.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted HTML table string
        """
        if not data:
            return "<!DOCTYPE html><html><head><title>No Data</title></head><body><p>No data to display</p></body></html>"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get column names
        columns = list(data[0].keys())

        # Create header with column names and comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Start building HTML
        html_lines = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "    <meta charset=\"UTF-8\">",
            "    <title>Table Data: {}</title>".format(table_name),
            "    <style>",
            "        body { font-family: Arial, sans-serif; margin: 20px; }",
            "        table { border-collapse: collapse; width: 100%; }",
            "        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; white-space: pre-wrap; }",
            "        th { background-color: #f2f2f2; font-weight: bold; }",
            "        tr:nth-child(even) { background-color: #f9f9f9; }",
            "        .container { max-width: 100%; overflow-x: auto; }",
            "    </style>",
            "</head>",
            "<body>",
            "    <h2>Table: {}</h2>".format(table_name),
            "    <div class=\"container\">",
            "        <table>",
            "            <thead>",
            "                <tr>"
        ]

        # Add headers
        for header in headers:
            html_lines.append(
                "                    <th>{}</th>".format(header.replace('<', '&lt;').replace('>', '&gt;')))

        html_lines.extend([
            "                </tr>",
            "            </thead>",
            "            <tbody>"
        ])

        # Add data rows
        for row in data:
            html_lines.append("                <tr>")
            for col in columns:
                col_value = str(row.get(col, ''))
                # Escape HTML special characters
                col_value = col_value.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('\n',
                                                                                                              '<br>')
                html_lines.append("                    <td>{}</td>".format(col_value))
            html_lines.append("                </tr>")

        # Close HTML tags
        html_lines.extend([
            "            </tbody>",
            "        </table>",
            "    </div>",
            "</body>",
            "</html>"
        ])

        return "\n".join(html_lines)

    def format_as_vertical_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str) -> str:
        """
        Format data as a vertical table string, displaying each row as key-value pairs.
        This format is especially useful when there are many columns that don't fit horizontally.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table

        Returns:
            str: Formatted vertical table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Calculate maximum key width for alignment
        max_key_width = 0
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())

        for key in all_keys:
            display_key = f"{key}({column_comments[key]})" if key in column_comments else key
            max_key_width = max(max_key_width, len(display_key))

        # Build vertical table
        lines = []
        separator = "-" * (max_key_width + 25)  # Adjust spacing as needed

        for i, row in enumerate(data):
            lines.append(separator)
            lines.append(f"Row {i + 1}:")
            lines.append(separator)

            for key in sorted(row.keys()):  # Sort keys for consistent display
                display_key = f"{key}({column_comments[key]})" if key in column_comments else key
                value = str(row.get(key, ''))

                # Truncate long values for display
                if len(value) > 100:
                    value = value[:97] + "..."

                lines.append(f"{display_key.ljust(max_key_width)} : {value}")
            lines.append("")  # Empty line between rows

        if lines and lines[-1] == "":  # Remove trailing empty line
            lines.pop()

        return "\n".join(lines)

    def format_as_smart_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str, max_columns: int = 10) -> str:
        """
        Smart table formatting that automatically chooses the best format based on column count.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table
            max_columns: Maximum number of columns to show in horizontal format

        Returns:
            str: Formatted table string in the most appropriate format
        """
        if not data:
            return "No data to display"

        # Get column count
        columns = list(data[0].keys())
        column_count = len(columns)

        # Choose format based on column count
        if column_count <= max_columns:
            # Use horizontal format for tables with few columns
            return self.format_as_ide_table(data, connection_name, table_name)
        else:
            # Use vertical format for tables with many columns
            return self.format_as_vertical_table(data, connection_name, table_name)

    def format_as_paged_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str,
                              columns_per_page: int = 8, rows_per_page: int = 20) -> str:
        """
        Format data as a paged table, showing only a subset of columns at a time.
        This is useful for tables with many columns.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table
            columns_per_page: Number of columns to show per page
            rows_per_page: Number of rows to show per page

        Returns:
            str: Formatted paged table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get all columns
        all_columns = list(data[0].keys())
        total_columns = len(all_columns)
        total_rows = len(data)

        # Calculate number of pages
        column_pages = (total_columns + columns_per_page - 1) // columns_per_page
        row_pages = (total_rows + rows_per_page - 1) // rows_per_page

        # Build paged output
        lines = []
        lines.append(f"Table: {table_name}")
        lines.append(f"Total columns: {total_columns}, Total rows: {total_rows}")
        lines.append(f"Showing {columns_per_page} columns per page, {rows_per_page} rows per page")
        lines.append("=" * 80)

        # Process each column page
        for col_page in range(column_pages):
            start_col = col_page * columns_per_page
            end_col = min(start_col + columns_per_page, total_columns)
            page_columns = all_columns[start_col:end_col]

            lines.append(f"\nColumns {start_col + 1}-{end_col} of {total_columns}:")
            lines.append("-" * 40)

            # Process each row page
            for row_page in range(row_pages):
                start_row = row_page * rows_per_page
                end_row = min(start_row + rows_per_page, total_rows)
                page_data = data[start_row:end_row]

                if row_pages > 1:
                    lines.append(f"\nRows {start_row + 1}-{end_row} of {total_rows}:")

                # Create headers with comments
                headers = []
                for col in page_columns:
                    if col in column_comments:
                        headers.append(f"{col}({column_comments[col]})")
                    else:
                        headers.append(col)

                # Calculate column widths
                col_widths = {}
                for i, header in enumerate(headers):
                    col_widths[i] = len(str(header))
                    for row in page_data:
                        col_value = str(row.get(page_columns[i], ''))
                        col_widths[i] = max(col_widths[i], min(len(col_value), 50))

                # Apply reasonable limits
                MAX_COL_WIDTH = 50
                MIN_COL_WIDTH = 8
                for i in col_widths:
                    col_widths[i] = min(max(col_widths[i], MIN_COL_WIDTH), MAX_COL_WIDTH)

                # Create format string
                format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"
                separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

                # Build table for this page
                lines.append(separator)
                lines.append(format_str.format(*headers))
                lines.append(separator)

                for row in page_data:
                    formatted_row = []
                    for i, col in enumerate(page_columns):
                        col_value = str(row.get(col, ''))
                        if len(col_value) > MAX_COL_WIDTH:
                            col_value = col_value[:MAX_COL_WIDTH - 3] + "..."
                        formatted_row.append(col_value)
                    lines.append(format_str.format(*formatted_row))

                lines.append(separator)

        return "\n".join(lines)

    def format_as_summary_table(self, data: List[Dict[str, Any]], connection_name: str, table_name: str,
                                max_columns: int = 6, sample_rows: int = 5) -> str:
        """
        Format data as a summary table, showing only the most important columns and a sample of rows.
        This is useful for getting a quick overview of large datasets.

        Args:
            data: List of dictionaries containing row data
            connection_name: Name of the database connection
            table_name: Name of the table
            max_columns: Maximum number of columns to show
            sample_rows: Number of sample rows to show

        Returns:
            str: Formatted summary table string
        """
        if not data:
            return "No data to display"

        # Get column comments
        column_info = self.get_table_structure(connection_name, table_name)
        column_comments = {}
        if column_info:
            for col in column_info:
                if col.get('column_comment'):
                    column_comments[col['column_name']] = col['column_comment']

        # Get all columns and limit to max_columns
        all_columns = list(data[0].keys())
        if len(all_columns) > max_columns:
            columns = all_columns[:max_columns]
            has_more_columns = True
        else:
            columns = all_columns
            has_more_columns = False

        # Limit rows to sample_rows
        if len(data) > sample_rows:
            sample_data = data[:sample_rows]
            has_more_rows = True
        else:
            sample_data = data
            has_more_rows = False

        # Create headers with comments
        headers = []
        for col in columns:
            if col in column_comments:
                headers.append(f"{col}({column_comments[col]})")
            else:
                headers.append(col)

        # Calculate column widths
        col_widths = {}
        for i, header in enumerate(headers):
            col_widths[i] = len(str(header))
            for row in sample_data:
                col_value = str(row.get(columns[i], ''))
                col_widths[i] = max(col_widths[i], min(len(col_value), 50))

        # Apply reasonable limits
        MAX_COL_WIDTH = 50
        MIN_COL_WIDTH = 8
        for i in col_widths:
            col_widths[i] = min(max(col_widths[i], MIN_COL_WIDTH), MAX_COL_WIDTH)

        # Create format string
        format_str = "| " + " | ".join([f"{{:{col_widths[i]}}}" for i in range(len(headers))]) + " |"
        separator = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(len(headers))]) + "+"

        # Build table
        lines = []
        lines.append(f"Table: {table_name}")
        if has_more_columns:
            lines.append(f"[Showing {len(columns)} of {len(all_columns)} columns]")
        if has_more_rows:
            lines.append(f"[Showing {len(sample_data)} of {len(data)} rows]")
        lines.append(separator)
        lines.append(format_str.format(*headers))
        lines.append(separator)

        for row in sample_data:
            formatted_row = []
            for i, col in enumerate(columns):
                col_value = str(row.get(col, ''))
                if len(col_value) > MAX_COL_WIDTH:
                    col_value = col_value[:MAX_COL_WIDTH - 3] + "..."
                formatted_row.append(col_value)
            lines.append(format_str.format(*formatted_row))

        lines.append(separator)

        return "\n".join(lines)

    def dispose_all_connections(self):
        """
        Dispose all database connections.
        This should be called during server shutdown for graceful cleanup.
        """
        for name, engine in self.connections.items():
            try:
                engine.dispose()
            except Exception as e:
                self.logger.error(f"Error disposing connection '{name}': {str(e)}")
        self.connections.clear()
