"""
Custom exceptions for the database tools module.
"""

import traceback
from typing import Optional, Any, Dict
import uuid
import os
import json
import logging
from datetime import datetime


class DatabaseToolError(Exception):
    """Base exception class for database tools with enhanced error information."""
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.request_id = str(uuid.uuid4())
        self._save_error_log()
        
    def __str__(self):
        base_msg = f"[Request ID: {self.request_id}] {self.message}"
        if self.error_code:
            base_msg = f"[{self.error_code}] {base_msg}"
        return base_msg
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for serialization."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "request_id": self.request_id,
            "details": self.details
        }
        
    def _save_error_log(self):
        """Save error log to file if ERROR_LOG_PATH environment variable is set."""
        try:
            error_log_path = os.environ.get('ERROR_LOG_PATH')
            if error_log_path:
                # Ensure directory exists
                if not os.path.exists(error_log_path):
                    os.makedirs(error_log_path)
                
                # Generate filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                filename = f"exception_{timestamp}.json"
                filepath = os.path.join(error_log_path, filename)
                
                # Prepare error data
                error_data = {
                    "timestamp": datetime.now().isoformat(),
                    "error_type": self.__class__.__name__,
                    "message": self.message,
                    "error_code": self.error_code,
                    "request_id": self.request_id,
                    "details": self.details
                }
                
                # Save to file
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(error_data, f, ensure_ascii=False, indent=2)
                    
        except Exception as e:
            # We can't log this error as it would cause infinite recursion
            pass


class DatabaseConnectionError(DatabaseToolError):
    """Raised when there is an error connecting to a database."""
    def __init__(self, message: str, error_code: Optional[str] = "DB_CONN_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, details)


class DatabaseOperationError(DatabaseToolError):
    """Raised when there is an error executing a database operation."""
    def __init__(self, message: str, error_code: Optional[str] = "DB_OP_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, details)


class TableOperationError(DatabaseToolError):
    """Raised when there is an error performing a table structure operation."""
    def __init__(self, message: str, error_code: Optional[str] = "TABLE_OP_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, details)


class DataOperationError(DatabaseToolError):
    """Raised when there is an error performing a data operation."""
    def __init__(self, message: str, error_code: Optional[str] = "DATA_OP_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, details)


class ConfigurationError(DatabaseToolError):
    """Raised when there is an error with database configuration."""
    def __init__(self, message: str, error_code: Optional[str] = "CONFIG_ERROR", details: Optional[Dict[str, Any]] = None):
        super().__init__(message, error_code, details)