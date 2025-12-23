"""
Database Manager for handling multiple database connections.

This module provides a unified interface for managing multiple database connections
and performing various database operations. It acts as a facade that delegates
operations to specialized managers for connection management, table operations,
data operations, and formatting.
"""

from ..connections.connection_manager import ConnectionManager
from ..operations.table_manager import TableManager
from ..operations.data_manager import DataManager
from ..formatting.format_manager import FormatManager


class DatabaseManager:
    """Manages multiple database connections and operations.

    This class serves as the main entry point for all database operations.
    It coordinates between different specialized managers:
    - ConnectionManager: Handles database connections
    - TableManager: Handles table structure operations
    - DataManager: Handles data manipulation operations
    - FormatManager: Handles data formatting operations
    
    The DatabaseManager provides a unified API that delegates operations
    to the appropriate specialized manager.
    """

    def __init__(self):
        """Initialize the DatabaseManager with specialized managers."""
        self.connection_manager = ConnectionManager()
        self.table_manager = TableManager(self.connection_manager)
        self.data_manager = DataManager(self.connection_manager)
        self.format_manager = FormatManager(self.table_manager)

    # Connection management methods (delegated to ConnectionManager)
    def add_connection(self, *args, **kwargs):
        """Add a new database connection.
        
        Args:
            *args: Positional arguments passed to ConnectionManager.add_connection
            **kwargs: Keyword arguments passed to ConnectionManager.add_connection
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        return self.connection_manager.add_connection(*args, **kwargs)

    def remove_connection(self, *args, **kwargs):
        """Remove a database connection.
        
        Args:
            *args: Positional arguments passed to ConnectionManager.remove_connection
            **kwargs: Keyword arguments passed to ConnectionManager.remove_connection
            
        Returns:
            bool: True if removed, False if not found
        """
        return self.connection_manager.remove_connection(*args, **kwargs)

    def initialize_from_config(self, *args, **kwargs):
        """Initialize database connections from configuration.
        
        Args:
            *args: Positional arguments passed to ConnectionManager.initialize_from_config
            **kwargs: Keyword arguments passed to ConnectionManager.initialize_from_config
            
        Returns:
            Dict mapping connection names to connection success status
        """
        return self.connection_manager.initialize_from_config(*args, **kwargs)

    def get_connection(self, *args, **kwargs):
        """Get a database engine by name.
        
        Args:
            *args: Positional arguments passed to ConnectionManager.get_connection
            **kwargs: Keyword arguments passed to ConnectionManager.get_connection
            
        Returns:
            Engine: Database engine or None if not found
        """
        return self.connection_manager.get_connection(*args, **kwargs)

    def list_connections(self, *args, **kwargs):
        """List all connection names and their database types.
        
        Args:
            *args: Positional arguments passed to ConnectionManager.list_connections
            **kwargs: Keyword arguments passed to ConnectionManager.list_connections
            
        Returns:
            Dict[str, str]: Mapping of connection names to database types
        """
        return self.connection_manager.list_connections(*args, **kwargs)

    def dispose_all_connections(self, *args, **kwargs):
        """Dispose all database connections.
        
        Args:
            *args: Positional arguments passed to ConnectionManager.dispose_all_connections
            **kwargs: Keyword arguments passed to ConnectionManager.dispose_all_connections
        """
        return self.connection_manager.dispose_all_connections(*args, **kwargs)

    # Table structure methods (delegated to TableManager)
    def list_tables(self, *args, **kwargs):
        """List all table names for the user in the specified database connection.
        
        Args:
            *args: Positional arguments passed to TableManager.list_tables
            **kwargs: Keyword arguments passed to TableManager.list_tables
            
        Returns:
            List[str]: List of table names or None if error occurred
        """
        return self.table_manager.list_tables(*args, **kwargs)

    def get_table_structure(self, *args, **kwargs):
        """Get structure information for a specific table.
        
        Args:
            *args: Positional arguments passed to TableManager.get_table_structure
            **kwargs: Keyword arguments passed to TableManager.get_table_structure
            
        Returns:
            List[Dict[str, Any]]: Column information or None if error occurred
        """
        return self.table_manager.get_table_structure(*args, **kwargs)

    def create_table(self, *args, **kwargs):
        """Create a new table.
        
        Args:
            *args: Positional arguments passed to TableManager.create_table
            **kwargs: Keyword arguments passed to TableManager.create_table
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.table_manager.create_table(*args, **kwargs)

    def drop_table(self, *args, **kwargs):
        """Drop a table.
        
        Args:
            *args: Positional arguments passed to TableManager.drop_table
            **kwargs: Keyword arguments passed to TableManager.drop_table
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.table_manager.drop_table(*args, **kwargs)

    def alter_table(self, *args, **kwargs):
        """Alter table structure with various operations.
        
        Args:
            *args: Positional arguments passed to TableManager.alter_table
            **kwargs: Keyword arguments passed to TableManager.alter_table
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.table_manager.alter_table(*args, **kwargs)

    # Data operation methods (delegated to DataManager)
    def execute_query(self, *args, **kwargs):
        """Execute a query on a specific database.
        
        Args:
            *args: Positional arguments passed to DataManager.execute_query
            **kwargs: Keyword arguments passed to DataManager.execute_query
            
        Returns:
            List[Dict[str, Any]]: Query results or None if error occurred
        """
        return self.data_manager.execute_query(*args, **kwargs)

    def select_data(self, *args, **kwargs):
        """Select data from a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.select_data
            **kwargs: Keyword arguments passed to DataManager.select_data
            
        Returns:
            List[Dict[str, Any]]: Query results or None if error occurred
        """
        return self.data_manager.select_data(*args, **kwargs)

    def insert_data(self, *args, **kwargs):
        """Insert data into a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.insert_data
            **kwargs: Keyword arguments passed to DataManager.insert_data
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.data_manager.insert_data(*args, **kwargs)

    def update_data(self, *args, **kwargs):
        """Update data in a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.update_data
            **kwargs: Keyword arguments passed to DataManager.update_data
            
        Returns:
            int: Number of rows affected, -1 if error occurred
        """
        return self.data_manager.update_data(*args, **kwargs)

    def delete_data(self, *args, **kwargs):
        """Delete data from a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.delete_data
            **kwargs: Keyword arguments passed to DataManager.delete_data
            
        Returns:
            int: Number of rows affected, -1 if error occurred
        """
        return self.data_manager.delete_data(*args, **kwargs)

    def batch_insert_data(self, *args, **kwargs):
        """Insert multiple records into a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.batch_insert_data
            **kwargs: Keyword arguments passed to DataManager.batch_insert_data
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        return self.data_manager.batch_insert_data(*args, **kwargs)

    def batch_update_data(self, *args, **kwargs):
        """Update multiple records in a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.batch_update_data
            **kwargs: Keyword arguments passed to DataManager.batch_update_data
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        return self.data_manager.batch_update_data(*args, **kwargs)

    def batch_delete_data(self, *args, **kwargs):
        """Delete multiple records from a specific table.
        
        Args:
            *args: Positional arguments passed to DataManager.batch_delete_data
            **kwargs: Keyword arguments passed to DataManager.batch_delete_data
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        return self.data_manager.batch_delete_data(*args, **kwargs)

    # Formatting methods (delegated to FormatManager)
    def format_as_table(self, *args, **kwargs):
        """Format data as a simple table string.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_table
            
        Returns:
            str: Formatted table string
        """
        return self.format_manager.format_as_table(*args, **kwargs)

    def format_as_ide_table(self, *args, **kwargs):
        """Format data as an IDE-style table string.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_ide_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_ide_table
            
        Returns:
            str: Formatted IDE-style table string
        """
        return self.format_manager.format_as_ide_table(*args, **kwargs)

    def format_as_scrollable_html_table(self, *args, **kwargs):
        """Format data as a scrollable HTML table.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_scrollable_html_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_scrollable_html_table
            
        Returns:
            str: HTML string with scrollable table
        """
        return self.format_manager.format_as_scrollable_html_table(*args, **kwargs)

    def format_as_html_table(self, *args, **kwargs):
        """Format data as an HTML table.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_html_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_html_table
            
        Returns:
            str: HTML string with table
        """
        return self.format_manager.format_as_html_table(*args, **kwargs)

    def format_as_vertical_table(self, *args, **kwargs):
        """Format data as a vertical table string.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_vertical_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_vertical_table
            
        Returns:
            str: Formatted vertical table string
        """
        return self.format_manager.format_as_vertical_table(*args, **kwargs)

    def format_as_smart_table(self, *args, **kwargs):
        """Format data using smart table formatting based on column count.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_smart_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_smart_table
            
        Returns:
            str: Formatted table string in the most appropriate format
        """
        return self.format_manager.format_as_smart_table(*args, **kwargs)

    def format_as_paged_table(self, *args, **kwargs):
        """Format data as a paged table for tables with many columns.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_paged_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_paged_table
            
        Returns:
            str: Formatted paged table string
        """
        return self.format_manager.format_as_paged_table(*args, **kwargs)

    def format_as_summary_table(self, *args, **kwargs):
        """Format data as a summary table showing key columns and sample rows.
        
        Args:
            *args: Positional arguments passed to FormatManager.format_as_summary_table
            **kwargs: Keyword arguments passed to FormatManager.format_as_summary_table
            
        Returns:
            str: Formatted summary table string
        """
        return self.format_manager.format_as_summary_table(*args, **kwargs)