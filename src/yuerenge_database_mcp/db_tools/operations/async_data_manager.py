"""
Async Data Manager for handling asynchronous data operations.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text

from .data_manager import DataManager
from ..core.exceptions import DataOperationError


class AsyncDataManager:
    """Manages asynchronous data operations."""
    
    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
        self.logger = logging.getLogger(__name__)

    async def select_data(self, connection_name: str, table_name: str, 
                         conditions: Optional[Dict[str, Any]] = None,
                         limit: Optional[int] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Asynchronously select data from a specific table.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            conditions: Optional dictionary of column-value pairs for WHERE clause
            limit: Optional limit for number of rows returned
            
        Returns:
            List of dictionaries containing row data or None if error occurred
        """
        try:
            # Run synchronous select_data in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            data_manager = DataManager(self.connection_manager)
            result = await loop.run_in_executor(
                None, 
                data_manager.select_data, 
                connection_name, 
                table_name, 
                conditions, 
                limit
            )
            return result
        except Exception as e:
            self.logger.error(f"Error in async select_data: {str(e)}")
            return None

    async def insert_data(self, connection_name: str, table_name: str, 
                         data: Dict[str, Any]) -> bool:
        """
        Asynchronously insert data into a specific table.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            data: Dictionary of column-value pairs to insert
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Run synchronous insert_data in a thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            data_manager = DataManager(self.connection_manager)
            result = await loop.run_in_executor(
                None, 
                data_manager.insert_data, 
                connection_name, 
                table_name, 
                data
            )
            return result
        except Exception as e:
            self.logger.error(f"Error in async insert_data: {str(e)}")
            return False

    async def batch_insert_data(self, connection_name: str, table_name: str, 
                               data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Asynchronously insert multiple records into a specific table.
        
        Args:
            connection_name: Name of the database connection
            table_name: Name of the table
            data_list: List of dictionaries containing column-value pairs to insert
            
        Returns:
            Dict containing success count, failure count, and details of failures
        """
        successes = 0
        failures = 0
        failed_records = []
        
        # Process inserts concurrently
        tasks = [
            self.insert_data(connection_name, table_name, data) 
            for data in data_list
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception) or not result:
                failures += 1
                failed_records.append({
                    "index": i,
                    "data": data_list[i],
                    "error": str(result) if isinstance(result, Exception) else "Insert failed"
                })
            else:
                successes += 1
                
        return {
            "success_count": successes,
            "failure_count": failures,
            "failed_records": failed_records
        }