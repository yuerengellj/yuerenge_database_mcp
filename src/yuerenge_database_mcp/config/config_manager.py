"""
Configuration manager for database connections.
"""

import json
import os
from typing import Dict, List, Any


class DatabaseConfigManager:
    """Manages database connection configurations."""

    def __init__(self, config_file: str = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Path to the configuration file. If not provided, 
                        will try to read from DB_CONFIG_FILE environment variable,
                        otherwise defaults to "config/database_config.json"
        """
        if config_file is None:
            # Try to get config file path from environment variable
            self.config_file = os.environ.get('DB_CONFIG_FILE', os.path.join(os.path.dirname(__file__), 'database_config.json'))
        else:
            self.config_file = config_file
        self.config_data = {"connections": []}
        self.load_config()

    def load_config(self) -> None:
        """Load configuration from file."""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config_data = json.load(f)
            except Exception as e:
                print(f"Error loading config file: {e}")
                # Initialize with default structure
                self.config_data = {"connections": []}
        else:
            # Create default config file if it doesn't exist
            self.save_config()

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