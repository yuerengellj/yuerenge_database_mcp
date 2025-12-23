"""
Table Manager for handling table structure operations.
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

from ..core.exceptions import TableOperationError
from ..utils.log_manager import get_log_manager


from ..utils.oracle_utils import is_datetime_string, is_date_string, format_datetime_for_oracle, format_date_for_oracle


class TableManager:
    """Manages table structure operations."""

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self.logger = logging.getLogger(__name__)
        self.request_id = str(uuid.uuid4())
        self.log_manager = get_log_manager()

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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Listing tables for connection '{connection_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("list_tables_missing_connection", {
                "connection_name": connection_name,
                "error_message": error_msg
            })
            
            return None

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("list_tables_missing_adapter", {
                "connection_name": connection_name,
                "error_message": error_msg
            })
            
            return None

        try:
            # Use adapter to get the list tables query
            query = adapter.get_list_tables_query(pattern)
            self.logger.debug(f"[Request ID: {request_id}] List tables query: {query}")
            
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
                    self.logger.info(f"[Request ID: {request_id}] Found {len(tables)} tables in MySQL database")
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
                    self.logger.info(f"[Request ID: {request_id}] Found {len(tables)} tables in Oracle database")
                    return tables
                else:
                    # For other databases, just return table names
                    tables = [row[0] for row in result.fetchall()]
                    self.logger.info(f"[Request ID: {request_id}] Found {len(tables)} tables")
                    return tables

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "pattern": pattern,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error listing tables on '{connection_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("list_tables_sqlalchemy_error", {
                "connection_name": connection_name,
                "pattern": pattern,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "pattern": pattern,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error listing tables on '{connection_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("list_tables_unexpected_error", {
                "connection_name": connection_name,
                "pattern": pattern,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Getting table structure for '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("get_table_structure_missing_connection", {
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
            self.log_manager.save_error_log("get_table_structure_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return None

        try:
            # Use adapter to get the table structure query
            query = adapter.get_table_structure_query(table_name)
            self.logger.debug(f"[Request ID: {request_id}] Table structure query: {query}")
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                rows = result.fetchall()
                
                # Use adapter to format column info
                columns = []
                for row in rows:
                    column_info = adapter.format_column_info(row)
                    columns.append(column_info)
                
                self.logger.info(f"[Request ID: {request_id}] Retrieved structure for {len(columns)} columns")
                return columns

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "pattern": pattern,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error getting table structure for '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("get_table_structure_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "pattern": pattern,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return None
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "pattern": pattern,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error getting table structure for '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("get_table_structure_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "pattern": pattern,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Creating table '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("create_table_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("create_table_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return False

        try:
            # Use adapter to generate CREATE TABLE statement
            query = adapter.get_create_table_statement(table_name, columns, table_comment)
            self.logger.debug(f"[Request ID: {request_id}] Create table query: {query}")
            
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query))
                    trans.commit()
                    self.logger.info(f"[Request ID: {request_id}] Successfully created table '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "columns": columns,
                "table_comment": table_comment,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error creating table '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("create_table_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "columns": columns,
                "table_comment": table_comment,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "columns": columns,
                "table_comment": table_comment,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error creating table '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("create_table_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "columns": columns,
                "table_comment": table_comment,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Dropping table '{connection_name}.{table_name}'")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("drop_table_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("drop_table_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "error_message": error_msg
            })
            
            return False

        try:
            # Use adapter to generate DROP TABLE statement
            query = adapter.get_drop_table_statement(table_name, cascade)
            self.logger.debug(f"[Request ID: {request_id}] Drop table query: {query}")
            
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(query))
                    trans.commit()
                    self.logger.info(f"[Request ID: {request_id}] Successfully dropped table '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "cascade": cascade,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error dropping table '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("drop_table_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "cascade": cascade,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "cascade": cascade,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error dropping table '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("drop_table_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "cascade": cascade,
                "error_message": str(e),
                "error_details": error_details
            })
            
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
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Altering table '{connection_name}.{table_name}' with {len(operations)} operations")
        
        engine = self.connection_manager.get_connection(connection_name)
        if not engine:
            error_msg = f"Connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("alter_table_missing_connection", {
                "connection_name": connection_name,
                "table_name": table_name,
                "operations_count": len(operations),
                "error_message": error_msg
            })
            
            return False

        # Get the appropriate adapter
        adapter = self.connection_manager.get_adapter(connection_name)
        if not adapter:
            error_msg = f"Adapter for connection '{connection_name}' not found"
            self.logger.error(f"[Request ID: {request_id}] {error_msg}")
            
            # Save error log
            self.log_manager.save_error_log("alter_table_missing_adapter", {
                "connection_name": connection_name,
                "table_name": table_name,
                "operations_count": len(operations),
                "error_message": error_msg
            })
            
            return False

        try:
            with engine.connect() as conn:
                trans = conn.begin()
                try:
                    for i, operation in enumerate(operations):
                        # Use adapter to generate ALTER TABLE statement
                        query = adapter.get_alter_table_statement(table_name, operation)
                        self.logger.debug(f"[Request ID: {request_id}] Alter table query {i+1}: {query}")
                        conn.execute(text(query))
                        
                    trans.commit()
                    self.logger.info(f"[Request ID: {request_id}] Successfully altered table '{connection_name}.{table_name}'")
                    return True
                except Exception as e:
                    trans.rollback()
                    raise e

        except SQLAlchemyError as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "operations": operations,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] SQLAlchemy error altering table '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("alter_table_sqlalchemy_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "operations": operations,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False
        except Exception as e:
            error_details = {
                "connection_name": connection_name,
                "table_name": table_name,
                "operations": operations,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error altering table '{connection_name}.{table_name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("alter_table_unexpected_error", {
                "connection_name": connection_name,
                "table_name": table_name,
                "operations": operations,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False