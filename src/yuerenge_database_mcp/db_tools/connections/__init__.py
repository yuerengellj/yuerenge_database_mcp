"""
Connections module for database tools.

This module contains connection management and database adapters.
"""

from .connection_manager import ConnectionManager
from .database_adapters import (
    DatabaseAdapter,
    MySQLAdapter,
    OracleAdapter,
    PostgreSQLAdapter,
    SQLiteAdapter,
    SQLServerAdapter,
    get_database_adapter
)

__all__ = [
    'ConnectionManager',
    'DatabaseAdapter',
    'MySQLAdapter',
    'OracleAdapter',
    'PostgreSQLAdapter',
    'SQLiteAdapter',
    'SQLServerAdapter',
    'get_database_adapter'
]