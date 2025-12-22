"""
Configuration manager for database connections.
"""

import json
import os
from typing import Dict, List, Any


class ConfigValidationError(Exception):
    """Exception raised for configuration validation errors."""
    pass


class DatabaseConfigManager:
    """Manages database connection configurations."""

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
        """Load configuration from file."""
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
        
        Args:
            conn: Connection configuration to validate
            index: Index of the connection in the list
            
        Raises:
            ConfigValidationError: If connection configuration is invalid
        """
        required_fields = ["name", "type", "host", "port", "username", "password", "database"]
        for field in required_fields:
            if field not in conn:
                raise ConfigValidationError(f"Connection {index}: Missing required field '{field}'")
        
        # Validate port
        if not isinstance(conn["port"], int) or not (1 <= conn["port"] <= 65535):
            raise ConfigValidationError(f"Connection '{conn['name']}': Port must be an integer between 1 and 65535")
            
        # Validate type
        valid_types = ["mysql", "oracle", "postgresql"]
        if conn["type"].lower() not in valid_types:
            raise ConfigValidationError(f"Connection '{conn['name']}': Invalid database type '{conn['type']}'. "
                                      f"Valid types: {valid_types}")
        
        # Validate enabled field if present
        if "enabled" in conn and not isinstance(conn["enabled"], bool):
            raise ConfigValidationError(f"Connection '{conn['name']}': 'enabled' must be a boolean")

    def save_config(self) -> None:
        """Save configuration to file."""
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