"""
Data Manager for handling data operations.
"""

import logging
import re
import traceback
import uuid
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .exceptions import DataOperationError
from .log_manager import get_log_manager


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
        self.request_id = str(uuid.uuid4())
        self.log_manager = get_log_manager()

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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Executing query on connection '{connection_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("execute_query_missing_connection", {
                "connection_name": connection_name,
                "query": query,
                "error_message": error_msg
            })
            
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
                    rows = result.fetchall()
                    self.logger.info(f"[Request ID: {request_id}] Query executed successfully, fetched {len(rows)} rows")
                    return [dict(zip(columns, row)) for row in rows]
                except Exception as e:
                    if commit and trans:
                        trans.rollback()
                    raise e
        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "query": query,
                "params": params,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error executing query on '{connection_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("execute_query_sqlalchemy_error", {
                "connection_name": connection_name,
                "query": query,
                "params": params,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "query": query,
                "params": params,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error executing query on '{connection_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("execute_query_unexpected_error", {
                "connection_name": connection_name,
                "query": query,
                "params": params,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Selecting data from '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("select_data_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return None

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("select_data_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return None

        try:
            # Use adapter to generate SELECT query and parameters
            query, params = adapter.get_select_query(table_name, conditions, limit)
            self.logger.debug(f"[Request ID: {request_id}] Generated query: {query}")

            with engine.connect() as conn:
                result = conn.execute(text(query), params)
                columns = result.keys()
                rows = result.fetchall()
                
                self.logger.info(f"[Request ID: {request_id}] Selected {len(rows)} rows from '{connection_name}.{table_name}'")

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
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "limit": limit,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error selecting data from '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("select_data_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "limit": limit,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "limit": limit,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error selecting data from '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("select_data_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "limit": limit,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None

    def select_data_with_pagination(self, connection_name: str, table_name: str, 
                                   page: int = 1, page_size: int = 100,
                                   conditions: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Select data from a specific table with pagination support.

        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            page: Page number (starting from 1)
            page_size: Number of records per page
            conditions: Optional dictionary of column-value pairs for WHERE clause

        Returns:
            Dictionary containing 'data', 'page', 'page_size', 'total_pages', 'total_records' or None if error occurred
        """
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Selecting data with pagination from '{connection_name}.{table_name}', page {page}, size {page_size}")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("select_data_pagination_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "error_message": error_msg
            })
            
            return None

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("select_data_pagination_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "error_message": error_msg
            })
            
            return None

        try:
            # First get total count
            count_query, count_params = adapter.get_count_query(table_name, conditions)
            self.logger.debug(f"[Request ID: {request_id}] Count query: {count_query}")
            
            with engine.connect() as conn:
                count_result = conn.execute(text(count_query), count_params)
                total_records = count_result.scalar()
                
                # Calculate pagination info
                total_pages = (total_records + page_size - 1) // page_size
                offset = (page - 1) * page_size
                
                # Add pagination to conditions
                paginated_conditions = {
                    "limit": page_size,
                    "offset": offset
                }
                if conditions:
                    paginated_conditions.update(conditions)
                
                # Use adapter to generate paginated SELECT query and parameters
                query, params = adapter.get_paginated_select_query(table_name, paginated_conditions)
                self.logger.debug(f"[Request ID: {request_id}] Paginated query: {query}")

                result = conn.execute(text(query), params)
                columns = result.keys()
                rows = result.fetchall()
                
                self.logger.info(f"[Request ID: {request_id}] Selected {len(rows)} rows (page {page}/{total_pages}) from '{connection_name}.{table_name}'")

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

                return {
                    "data": processed_rows,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "total_records": total_records
                }

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "conditions": conditions,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error selecting data with pagination from '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("select_data_pagination_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "conditions": conditions,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "conditions": conditions,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error selecting data with pagination from '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("select_data_pagination_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "page": page,
                "page_size": page_size,
                "conditions": conditions,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None