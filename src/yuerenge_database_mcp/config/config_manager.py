"""
Configuration manager for database connections.

This module provides functionality to load, validate, save, and manage database connection configurations.
It supports multiple database types and validates configuration parameters to ensure proper setup.

Configuration files are stored in JSON format and can be loaded from:
1. The path specified in the DATABASE_CONFIG_PATH environment variable
2. A default path provided during initialization
3. The default location: config/database_config.json

The module includes validation for:
- Required fields for each database type
- Valid port numbers (1-65535)
- Supported database types
- Boolean values for enabled flag
"""

import json
import os
from typing import Dict, List, Any


class ConfigValidationError(Exception):
    """Exception raised for configuration validation errors."""
    pass


class DatabaseConfigManager:
    """Manages database connection configurations.

    This class handles loading, validating, saving, and managing database connection configurations
    from JSON files. It supports multiple database types and validates configuration parameters
    to ensure proper setup before attempting connections.
    
    Configuration files follow this structure:
    {
      "connections": [
        {
          "name": "connection_name",
          "type": "database_type",
          "host": "hostname",  # Required for non-SQLite databases
          "port": 3306,        # Required for non-SQLite databases
          "username": "user",  # Required for non-SQLite databases
          "password": "pass",  # Required for non-SQLite databases
          "database": "dbname",# Required for all database types
          "enabled": true,     # Optional, defaults to False
          "pool_size": 5,      # Optional connection pool settings
          "max_overflow": 10,  # Optional connection pool settings
          "pool_timeout": 30,  # Optional connection pool settings
          "pool_recycle": 3600 # Optional connection pool settings
        }
      ]
    }
    """

    def __init__(self, config_file: str = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to the configuration file. If not provided, 
                        will try to read from DATABASE_CONFIG_PATH environment variable,
                        otherwise defaults to "config/database_config.json"
        """
        if config_file is None:
            # Try to get config file path from environment variable
            # Use DATABASE_CONFIG_PATH as per user preference
            self.config_file = os.environ.get('DATABASE_CONFIG_PATH', 
                                            os.path.join(os.path.dirname(__file__), 'database_config.json'))
        else:
            self.config_file = config_file
        self.config_data = {"connections": []}
        self.load_config()

    def load_config(self) -> None:
        """Load configuration from file.
        
        This method loads the configuration from the specified file path.
        If the file doesn't exist, it creates a default configuration.
        The loaded configuration is validated before being stored.
        
        Raises:
            ConfigValidationError: If the configuration file contains invalid data
        """
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    self.validate_config(loaded_data)
                    self.config_data = loaded_data
            except ConfigValidationError:
                raise
            except Exception as e:
                print(f"Error loading config file: {e}")
                # Initialize with default structure
                self.config_data = {"connections": []}
        else:
            # Create default config file if it doesn't exist
            self.save_config()

    def validate_config(self, config_data: Dict[str, Any]) -> None:
        """
        Validate configuration data.
        
        This method validates the overall configuration structure including:
        - Ensuring the data is a dictionary
        - Checking for required 'connections' key
        - Validating each connection configuration
        
        Args:
            config_data: Configuration data to validate
            
        Raises:
            ConfigValidationError: If configuration is invalid
        """
        if not isinstance(config_data, dict):
            raise ConfigValidationError("Configuration must be a dictionary")
            
        if "connections" not in config_data:
            raise ConfigValidationError("Missing 'connections' key in configuration")
            
        if not isinstance(config_data["connections"], list):
            raise ConfigValidationError("'connections' must be a list")
            
        for i, conn in enumerate(config_data.get("connections", [])):
            self._validate_connection_config(conn, i)
    
    def _validate_connection_config(self, conn: Dict[str, Any], index: int) -> None:
        """
        Validate a single connection configuration.
        
        Validates required fields based on database type:
        - For non-SQLite databases: name, type, host, port, username, password, database
        - For SQLite databases: name, type, database
        - Validates port number range (1-65535)
        - Validates database type is supported
        - Validates enabled flag is boolean if present
        
        Args:
            conn: Connection configuration to validate
            index: Index of the connection in the list
            
        Raises:
            ConfigValidationError: If connection configuration is invalid
        """
        required_fields = ["name", "type"]
        for field in required_fields:
            if field not in conn:
                raise ConfigValidationError(f"Connection {index}: Missing required field '{field}'")
        
        # Validate type
        valid_types = ["mysql", "oracle", "postgresql", "sqlite", "sqlserver"]
        if conn["type"].lower() not in valid_types:
            raise ConfigValidationError(f"Connection '{conn['name']}': Invalid database type '{conn['type']}'. "
                                      f"Valid types: {valid_types}")
        
        # For non-SQLite databases, we need more fields
        if conn["type"].lower() != "sqlite":
            extra_required_fields = ["host", "port", "username", "password", "database"]
            for field in extra_required_fields:
                if field not in conn:
                    raise ConfigValidationError(f"Connection '{conn['name']}': Missing required field '{field}' for {conn['type']} database")
            
            # Validate port for non-SQLite databases
            if not isinstance(conn["port"], int) or not (1 <= conn["port"] <= 65535):
                raise ConfigValidationError(f"Connection '{conn['name']}': Port must be an integer between 1 and 65535")
        else:
            # For SQLite, we only need the database file path
            if "database" not in conn:
                raise ConfigValidationError(f"Connection '{conn['name']}': Missing required field 'database' for SQLite")
        
        # Validate enabled field if present
        if "enabled" in conn and not isinstance(conn["enabled"], bool):
            raise ConfigValidationError(f"Connection '{conn['name']}': 'enabled' must be a boolean")

    def save_config(self) -> None:
        """Save configuration to file.
        
        This method saves the current configuration to the specified file.
        It ensures the directory exists before writing the file.
        The configuration is saved in a human-readable JSON format with indentation.
        """
        # Ensure directory exists
        config_dir = os.path.dirname(self.config_file)
        if config_dir:  # Only create directory if there is a path
            os.makedirs(config_dir, exist_ok=True)
        
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving config file: {e}")

    def get_connections(self) -> List[Dict[str, Any]]:
        """
        Get all connection configurations.
        
        Returns:
            List of connection configurations
        """
        return self.config_data.get("connections", [])

    def get_enabled_connections(self) -> List[Dict[str, Any]]:
        """
        Get enabled connection configurations.
        
        Returns:
            List of enabled connection configurations
        """
        connections = self.get_connections()
        return [conn for conn in connections if conn.get("enabled", False)]

    def add_connection(self, connection_config: Dict[str, Any]) -> bool:
        """
        Add a new connection configuration.
        
        Validates the connection configuration before adding it.
        Checks for duplicate connection names.
        Saves the updated configuration to file.
        
        Args:
            connection_config: Connection configuration dictionary
            
        Returns:
            True if added successfully, False otherwise
        """
        # Validate the connection config
        try:
            self._validate_connection_config(connection_config, len(self.get_connections()))
        except ConfigValidationError as e:
            print(f"Invalid connection configuration: {e}")
            return False
            
        # Check if connection with same name already exists
        connections = self.get_connections()
        for conn in connections:
            if conn.get("name") == connection_config.get("name"):
                return False  # Connection with this name already exists
        
        connections.append(connection_config)
        self.config_data["connections"] = connections
        self.save_config()
        return True

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> bool:
        """
        Update an existing connection configuration.
        
        Validates the new connection configuration before updating.
        Replaces the entire connection configuration with the new one.
        Saves the updated configuration to file.
        
        Args:
            name: Name of the connection to update
            connection_config: New connection configuration
            
        Returns:
            True if updated successfully, False if connection not found
        """
        # Validate the connection config
        try:
            self._validate_connection_config(connection_config, -1)  # -1 since we don't know the index
        except ConfigValidationError as e:
            print(f"Invalid connection configuration: {e}")
            return False
            
        connections = self.get_connections()
        for i, conn in enumerate(connections):
            if conn.get("name") == name:
                connections[i] = connection_config
                self.config_data["connections"] = connections
                self.save_config()
                return True
        return False

    def remove_connection(self, name: str) -> bool:
        """
        Remove a connection configuration.
        
        Removes the connection from the configuration and saves the file.
        
        Args:
            name: Name of the connection to remove
            
        Returns:
            True if removed successfully, False if connection not found
        """
        connections = self.get_connections()
        for i, conn in enumerate(connections):
            if conn.get("name") == name:
                connections.pop(i)
                self.config_data["connections"] = connections
                self.save_config()
                return True
        return False

    def enable_connection(self, name: str) -> bool:
        """
        Enable a connection.
        
        Sets the 'enabled' flag to True for the specified connection.
        Saves the updated configuration to file.
        
        Args:
            name: Name of the connection to enable
            
        Returns:
            True if enabled successfully, False if connection not found
        """
        connections = self.get_connections()
        for conn in connections:
            if conn.get("name") == name:
                conn["enabled"] = True
                self.save_config()
                return True
        return False

    def disable_connection(self, name: str) -> bool:
        """
        Disable a connection.
        
        Sets the 'enabled' flag to False for the specified connection.
        Saves the updated configuration to file.
        
        Args:
            name: Name of the connection to disable
            
        Returns:
            True if disabled successfully, False if connection not found
        """
        connections = self.get_connections()
        for conn in connections:
            if conn.get("name") == name:
                conn["enabled"] = False
                self.save_config()
                return True
        return False