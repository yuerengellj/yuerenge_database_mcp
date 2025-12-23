"""
Core module for database tools.

This module contains the main DatabaseManager and core exceptions.
"""

from .database_manager import DatabaseManager
from .exceptions import (
    DatabaseToolError,
    DatabaseConnectionError,
    DatabaseOperationError,
    TableOperationError,
    DataOperationError,
    ConfigurationError
)

__all__ = [
    'DatabaseManager',
    'DatabaseToolError',
    'DatabaseConnectionError',
    'DatabaseOperationError',
    'TableOperationError',
    'DataOperationError',
    'ConfigurationError'
]