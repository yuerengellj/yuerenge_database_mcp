"""
Log Manager for handling error log storage.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional


class LogManager:
    """Manages error log storage in JSON format files."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.log_path = os.environ.get('ERROR_LOG_PATH', './error_logs')
        self._ensure_log_directory()
        
    def _ensure_log_directory(self):
        """Ensure the log directory exists."""
        try:
            if not os.path.exists(self.log_path):
                os.makedirs(self.log_path)
                self.logger.info(f"Created error log directory: {self.log_path}")
        except Exception as e:
            self.logger.error(f"Failed to create error log directory {self.log_path}: {e}")
            
    def save_error_log(self, operation_type: str, error_info: Dict[str, Any]) -> bool:
        """
        Save error information as a JSON file.
        
        Args:
            operation_type: Type of operation that caused the error
            error_info: Dictionary containing error information
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        try:
            # Generate filename with operation type and timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{operation_type}_{timestamp}.json"
            filepath = os.path.join(self.log_path, filename)
            
            # Add timestamp to error info
            error_info_with_timestamp = {
                "logged_at": datetime.now().isoformat(),
                "operation_type": operation_type,
                **error_info
            }
            
            # Save to JSON file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(error_info_with_timestamp, f, ensure_ascii=False, indent=2)
                
            self.logger.info(f"Error log saved to: {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save error log: {e}")
            return False


# Global instance
log_manager = LogManager()


def get_log_manager() -> LogManager:
    """Get the global log manager instance."""
    return log_manager