"""
Custom exceptions for the database tools module.
"""


class DatabaseConnectionError(Exception):
    """Raised when there is an error connecting to a database."""
    pass


class DatabaseOperationError(Exception):
    """Raised when there is an error executing a database operation."""
    pass


class TableOperationError(Exception):
    """Raised when there is an error performing a table structure operation."""
    pass


class DataOperationError(Exception):
    """Raised when there is an error performing a data operation."""
    pass


class ConfigurationError(Exception):
    """Raised when there is an error with database configuration."""
    pass