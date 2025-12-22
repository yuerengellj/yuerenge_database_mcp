"""
Oracle utilities for handling Oracle-specific operations.
"""

import re
from datetime import datetime
from typing import Any, Dict


def is_oracle_datetime_string(value: str) -> bool:
    """
    Check if a string represents a datetime in format YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
    Specifically for Oracle database operations.

    Args:
        value: String to check

    Returns:
        bool: True if string matches datetime format, False otherwise
    """
    if not isinstance(value, str):
        return False

    # Pattern for date only (YYYY-MM-DD) or date with time (YYYY-MM-DD HH:MM:SS)
    datetime_pattern = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
    return bool(re.match(datetime_pattern, value))


def format_datetime_for_oracle_insert(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format datetime values in a data dictionary for Oracle database insertion.
    
    Args:
        data: Dictionary of column-value pairs
        
    Returns:
        Dict[str, Any]: Dictionary with formatted datetime values
    """
    formatted_data = {}
    for key, value in data.items():
        # Handle datetime objects and datetime strings
        if isinstance(value, datetime):
            formatted_data[key] = value.strftime('TO_DATE(\'%Y-%m-%d %H:%M:%S\', \'YYYY-MM-DD HH24:MI:SS\')')
        elif isinstance(value, str) and is_oracle_datetime_string(value):
            # For string datetime values, format them for Oracle
            if ' ' in value:
                # DateTime format
                formatted_data[key] = f"TO_DATE('{value}', 'YYYY-MM-DD HH24:MI:SS')"
            else:
                # Date only format
                formatted_data[key] = f"TO_DATE('{value}', 'YYYY-MM-DD')"
        else:
            formatted_data[key] = value
    return formatted_data