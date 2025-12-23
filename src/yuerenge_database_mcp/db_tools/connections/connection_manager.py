"""
Connection Manager for handling database connections.
"""

import logging
import traceback
import uuid
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .database_adapters import get_database_adapter
from ..core.exceptions import DatabaseConnectionError, ConfigurationError
from ..utils.log_manager import get_log_manager


class ConnectionManager:
    """Manages database connections."""

    def __init__(self):
        self.connections: Dict[str, Engine] = {}
        self.adapters: Dict[str, Any] = {}  # Store adapters for each connection
        self.logger = logging.getLogger(__name__)
        self.request_id = str(uuid.uuid4())
        self.log_manager = get_log_manager()

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
            **kwargs: Additional connection parameters including pool settings

        Returns:
            bool: True if connection successful, False otherwise
        """
        request_id = kwargs.get('request_id', self.request_id)
        # Remove request_id from kwargs as it shouldn't be passed to create_engine
        kwargs.pop('request_id', None)
        
        try:
            # Extract connection pool settings from kwargs or use defaults
            pool_settings = {
                'pool_size': kwargs.pop('pool_size', 10),
                'max_overflow': kwargs.pop('max_overflow', 20),
                'pool_timeout': kwargs.pop('pool_timeout', 30),
                'pool_recycle': kwargs.pop('pool_recycle', 3600)
            }
            
            # Add any remaining kwargs as additional engine options
            engine_options = {**pool_settings, **kwargs}
            
            # Get the appropriate adapter
            adapter = get_database_adapter(db_type)
            
            # Create connection string using adapter
            connection_string = adapter.get_connection_string(
                host, port, username, password, database
            )

            # Log connection attempt
            self.logger.info(f"[Request ID: {request_id}] Attempting to connect to {db_type} database '{name}' at {host}:{port}")
            
            # Create engine with additional options
            engine = create_engine(connection_string, **engine_options)

            # Test connection using adapter-specific test query
            test_query = adapter.get_test_query()
            with engine.connect() as conn:
                conn.execute(text(test_query))

            # Store connection and adapter
            self.connections[name] = engine
            self.adapters[name] = adapter
            self.logger.info(f"[Request ID: {request_id}] Successfully connected to {db_type} database '{name}'")
            return True

        except SQLAlchemyError as e:
            error_details = {
                "db_type": db_type,
                "host": host,
                "port": port,
                "database": database,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Database connection error for '{name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("connection_error", {
                "connection_name": name,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False
        except Exception as e:
            error_details = {
                "db_type": db_type,
                "host": host,
                "port": port,
                "database": database,
                "error": str(e),
                "traceback": traceback.format_exc()
            }
            self.logger.error(f"[Request ID: {request_id}] Unexpected error connecting to database '{name}': {str(e)}", extra=error_details)
            
            # Save error log
            self.log_manager.save_error_log("connection_unexpected_error", {
                "connection_name": name,
                "error_message": str(e),
                "error_details": error_details
            })
            
            return False

    def remove_connection(self, name: str) -> bool:
        """
        Remove a database connection.

        Args:
            name: Connection identifier

        Returns:
            bool: True if removed, False if not found
        """
        request_id = self.request_id
        if name in self.connections:
            try:
                self.connections[name].dispose()
                del self.connections[name]
                if name in self.adapters:
                    del self.adapters[name]
                self.logger.info(f"[Request ID: {request_id}] Removed database connection '{name}'")
                return True
            except Exception as e:
                error_details = {
                    "connection_name": name,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
                self.logger.error(f"[Request ID: {request_id}] Error disposing connection '{name}': {str(e)}", extra=error_details)
                
                # Save error log
                self.log_manager.save_error_log("remove_connection_error", {
                    "connection_name": name,
                    "error_message": str(e),
                    "error_details": error_details
                })
                
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
        request_id = self.request_id
        results = {}
        for conn_config in config_connections:
            # Skip disabled connections
            if not conn_config.get("enabled", False):
                continue

            name = conn_config["name"]
            try:
                self.logger.info(f"[Request ID: {request_id}] Initializing connection '{name}' from config")
                
                # Extract connection parameters
                connection_params = {
                    "name": name,
                    "db_type": conn_config["type"],
                    "host": conn_config["host"],
                    "port": conn_config["port"],
                    "username": conn_config["username"],
                    "password": conn_config["password"],
                    "database": conn_config["database"],
                    "request_id": request_id
                }
                
                # Add optional connection pool parameters
                optional_params = ["pool_size", "max_overflow", "pool_timeout", "pool_recycle"]
                for param in optional_params:
                    if param in conn_config:
                        connection_params[param] = conn_config[param]
                
                success = self.add_connection(**connection_params)
                results[name] = success
                
                if success:
                    self.logger.info(f"[Request ID: {request_id}] Successfully initialized connection '{name}'")
                else:
                    self.logger.error(f"[Request ID: {request_id}] Failed to initialize connection '{name}'")
                    
            except Exception as e:
                error_details = {
                    "connection_name": name,
                    "config": conn_config,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
                self.logger.error(f"[Request ID: {request_id}] Failed to initialize connection '{name}': {str(e)}", extra=error_details)
                
                # Save error log
                self.log_manager.save_error_log("initialize_connection_error", {
                    "connection_name": name,
                    "error_message": str(e),
                    "error_details": error_details
                })
                
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

    def get_adapter(self, name: str) -> Optional[Any]:
        """
        Get a database adapter by name.

        Args:
            name: Connection identifier

        Returns:
            DatabaseAdapter: Database adapter or None if not found
        """
        return self.adapters.get(name)

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

    def dispose_all_connections(self):
        """
        Dispose all database connections.
        This should be called during server shutdown for graceful cleanup.
        """
        request_id = self.request_id
        self.logger.info(f"[Request ID: {request_id}] Disposing all database connections")
        for name, engine in self.connections.items():
            try:
                engine.dispose()
                self.logger.info(f"[Request ID: {request_id}] Disposed connection '{name}'")
            except Exception as e:
                error_details = {
                    "connection_name": name,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }
                self.logger.error(f"[Request ID: {request_id}] Error disposing connection '{name}': {str(e)}", extra=error_details)
                
                # Save error log
                self.log_manager.save_error_log("dispose_connection_error", {
                    "connection_name": name,
                    "error_message": str(e),
                    "error_details": error_details
                })
                
        self.connections.clear()
        self.adapters.clear()
        self.logger.info(f"[Request ID: {request_id}] All database connections disposed")