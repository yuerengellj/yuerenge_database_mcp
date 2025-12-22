"""
Database Manager for handling multiple database connections.
"""

from yuerenge_database_mcp.db_tools.connection_manager import ConnectionManager
from yuerenge_database_mcp.db_tools.table_manager import TableManager
from yuerenge_database_mcp.db_tools.data_manager import DataManager
from yuerenge_database_mcp.db_tools.format_manager import FormatManager


class DatabaseManager:
    """Manages multiple database connections and operations."""

    def __init__(self):
        self.connection_manager = ConnectionManager()
        self.table_manager = TableManager(self.connection_manager)
        self.data_manager = DataManager(self.connection_manager)
        self.format_manager = FormatManager(self.table_manager)

    # Connection management methods (delegated to ConnectionManager)
    def add_connection(self, *args, **kwargs):
        return self.connection_manager.add_connection(*args, **kwargs)

    def remove_connection(self, *args, **kwargs):
        return self.connection_manager.remove_connection(*args, **kwargs)

    def initialize_from_config(self, *args, **kwargs):
        return self.connection_manager.initialize_from_config(*args, **kwargs)

    def get_connection(self, *args, **kwargs):
        return self.connection_manager.get_connection(*args, **kwargs)

    def list_connections(self, *args, **kwargs):
        return self.connection_manager.list_connections(*args, **kwargs)

    def dispose_all_connections(self, *args, **kwargs):
        return self.connection_manager.dispose_all_connections(*args, **kwargs)

    # Table structure methods (delegated to TableManager)
    def list_tables(self, *args, **kwargs):
        return self.table_manager.list_tables(*args, **kwargs)

    def get_table_structure(self, *args, **kwargs):
        return self.table_manager.get_table_structure(*args, **kwargs)

    def create_table(self, *args, **kwargs):
        return self.table_manager.create_table(*args, **kwargs)

    def drop_table(self, *args, **kwargs):
        return self.table_manager.drop_table(*args, **kwargs)

    def alter_table(self, *args, **kwargs):
        return self.table_manager.alter_table(*args, **kwargs)

    # Data operation methods (delegated to DataManager)
    def execute_query(self, *args, **kwargs):
        return self.data_manager.execute_query(*args, **kwargs)

    def select_data(self, *args, **kwargs):
        return self.data_manager.select_data(*args, **kwargs)

    def insert_data(self, *args, **kwargs):
        return self.data_manager.insert_data(*args, **kwargs)

    def update_data(self, *args, **kwargs):
        return self.data_manager.update_data(*args, **kwargs)

    def delete_data(self, *args, **kwargs):
        return self.data_manager.delete_data(*args, **kwargs)

    # Formatting methods (delegated to FormatManager)
    def format_as_table(self, *args, **kwargs):
        return self.format_manager.format_as_table(*args, **kwargs)

    def format_as_ide_table(self, *args, **kwargs):
        return self.format_manager.format_as_ide_table(*args, **kwargs)

    def format_as_scrollable_html_table(self, *args, **kwargs):
        return self.format_manager.format_as_scrollable_html_table(*args, **kwargs)

    def format_as_html_table(self, *args, **kwargs):
        return self.format_manager.format_as_html_table(*args, **kwargs)

    def format_as_vertical_table(self, *args, **kwargs):
        return self.format_manager.format_as_vertical_table(*args, **kwargs)

    def format_as_smart_table(self, *args, **kwargs):
        return self.format_manager.format_as_smart_table(*args, **kwargs)

    def format_as_paged_table(self, *args, **kwargs):
        return self.format_manager.format_as_paged_table(*args, **kwargs)

    def format_as_summary_table(self, *args, **kwargs):
        return self.format_manager.format_as_summary_table(*args, **kwargs)
