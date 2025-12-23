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

from ..core.exceptions import DataOperationError
from ..utils.log_manager import get_log_manager


# Import the functions we need for Oracle datetime handling
from ..utils.oracle_utils import is_datetime_string, is_date_string


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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Inserting data into '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("insert_data_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "error_message": error_msg
            })
            
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("insert_data_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "error_message": error_msg
            })
            
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
            self.logger.debug(f"[Request ID: {request_id}] Insert query: {query}")

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    # For Oracle, we may need special handling
                    if "oracle" in engine.url.drivername:
                        # Handle Oracle datetime values by modifying the query
                        if any(is_datetime_string(str(v)) for v in params.values()):
                            # Reconstruct query with TO_DATE functions for datetime strings
                            columns = list(params.keys())
                            formatted_values = []
                            new_params = {}
                            
                            for i, (key, value) in enumerate(params.items()):
                                if isinstance(value, str) and is_datetime_string(value):
                                    if ' ' in value:
                                        # DateTime format
                                        formatted_values.append(f"TO_DATE(:{key}, 'YYYY-MM-DD HH24:MI:SS')")
                                    else:
                                        # Date only format
                                        formatted_values.append(f"TO_DATE(:{key}, 'YYYY-MM-DD')")
                                    new_params[key] = value
                                else:
                                    formatted_values.append(f":{key}")
                                    new_params[key] = value
                                    
                            columns_str = ", ".join(columns)
                            values_str = ", ".join(formatted_values)
                            query = f"INSERT INTO {table_name.upper()} ({columns_str}) VALUES ({values_str})"
                            conn.execute(text(query), new_params)
                        else:
                            conn.execute(text(query), params)
                    else:
                        conn.execute(text(query), params)
                    trans.commit()
                    self.logger.info(f"[Request ID: {request_id}] Successfully inserted data into '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error inserting data into '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("insert_data_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error inserting data into '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("insert_data_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Updating data in '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("update_data_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "conditions": conditions,
                "error_message": error_msg
            })
            
            return -1

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("update_data_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "conditions": conditions,
                "error_message": error_msg
            })
            
            return -1

        try:
            # Use adapter to generate UPDATE query and parameters
            query, params = adapter.get_update_query(table_name, data, conditions)
            self.logger.debug(f"[Request ID: {request_id}] Update query: {query}")

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(text(query), params)
                    trans.commit()
                    rows_affected = result.rowcount
                    self.logger.info(f"[Request ID: {request_id}] Successfully updated {rows_affected} rows in '{connection_name}.{table_name}'")
                    return rows_affected
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "conditions": conditions,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error updating data in '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("update_data_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "conditions": conditions,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return -1
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "conditions": conditions,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error updating data in '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("update_data_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "data": data,
                "conditions": conditions,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Deleting data from '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("delete_data_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "error_message": error_msg
            })
            
            return -1

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("delete_data_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "error_message": error_msg
            })
            
            return -1

        try:
            # Use adapter to generate DELETE query and parameters
            query, params = adapter.get_delete_query(table_name, conditions)
            self.logger.debug(f"[Request ID: {request_id}] Delete query: {query}")

            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    result = conn.execute(text(query), params)
                    trans.commit()
                    rows_affected = result.rowcount
                    self.logger.info(f"[Request ID: {request_id}] Successfully deleted {rows_affected} rows from '{connection_name}.{table_name}'")
                    return rows_affected
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error deleting data from '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("delete_data_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return -1
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error deleting data from '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("delete_data_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "conditions": conditions,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return -1

    def batch_insert_data(self, connection_name: str, table_name: str, 
                        data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Insert multiple records into a specific table.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            data_list: List of dictionaries containing column-value pairs to insert
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Batch inserting {len(data_list)} records into '{connection_name}.{table_name}'")
        
        successes = 0
        failures = 0
        failed_records = []
        
        for i, data in enumerate(data_list):
            try:
                result = self.insert_data(connection_name, table_name, data)
                if result:
                    successes += 1
                else:
                    failures += 1
                    failed_records.append({
                        "index": i,
                        "data": data,
                        "error": "Insert operation returned False"
                    })
            except Exception as e:
                failures += 1
                failed_records.append({
                    "index": i,
                    "data": data,
                    "error": str(e)
                })
                
        self.logger.info(f"[Request ID: {request_id}] Batch insert completed: {successes} succeeded, {failures} failed")
        
        return {
            "success_count": successes,
            "failure_count": failures,
            "failed_records": failed_records
        }

    def batch_update_data(self, connection_name: str, table_name: str,
                         data_list: List[Dict[str, Any]], 
                         conditions_list: List[Optional[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Update multiple records in a specific table.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            data_list: List of dictionaries containing column-value pairs to update
            conditions_list: List of dictionaries containing WHERE clause conditions for each update
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Batch updating {len(data_list)} records in '{connection_name}.{table_name}'")
        
        if len(data_list) != len(conditions_list):
            error_msg = "Data list and conditions list must have the same length"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            raise ValueError(error_msg)
        
        successes = 0
        failures = 0
        failed_records = []
        total_affected_rows = 0
        
        for i, (data, conditions) in enumerate(zip(data_list, conditions_list)):
            try:
                affected_rows = self.update_data(connection_name, table_name, data, conditions)
                if affected_rows >= 0:
                    successes += 1
                    total_affected_rows += affected_rows
                else:
                    failures += 1
                    failed_records.append({
                        "index": i,
                        "data": data,
                        "conditions": conditions,
                        "error": "Update operation returned negative value"
                    })
            except Exception as e:
                failures += 1
                failed_records.append({
                    "index": i,
                    "data": data,
                    "conditions": conditions,
                    "error": str(e)
                })
                
        self.logger.info(f"[Request ID: {request_id}] Batch update completed: {successes} succeeded, {failures} failed, {total_affected_rows} total rows affected")
        
        return {
            "success_count": successes,
            "failure_count": failures,
            "total_affected_rows": total_affected_rows,
            "failed_records": failed_records
        }

    def batch_delete_data(self, connection_name: str, table_name: str,
                         conditions_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Delete multiple records from a specific table.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            conditions_list: List of dictionaries containing WHERE clause conditions for each delete
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Batch deleting records from '{connection_name}.{table_name}'")
        
        successes = 0
        failures = 0
        failed_records = []
        total_affected_rows = 0
        
        for i, conditions in enumerate(conditions_list):
            try:
                affected_rows = self.delete_data(connection_name, table_name, conditions)
                if affected_rows >= 0:
                    successes += 1
                    total_affected_rows += affected_rows
                else:
                    failures += 1
                    failed_records.append({
                        "index": i,
                        "conditions": conditions,
                        "error": "Delete operation returned negative value"
                    })
            except Exception as e:
                failures += 1
                failed_records.append({
                    "index": i,
                    "conditions": conditions,
                    "error": str(e)
                })
                
        self.logger.info(f"[Request ID: {request_id}] Batch delete completed: {successes} succeeded, {failures} failed, {total_affected_rows} total rows affected")
        
        return {
            "success_count": successes,
            "failure_count": failures,
            "total_affected_rows": total_affected_rows,
            "failed_records": failed_records
        }