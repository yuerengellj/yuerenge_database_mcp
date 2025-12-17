"""
Server lifecycle management for graceful shutdown.
Handles signals like SIGINT, SIGTERM and stdin closure for clean exit.
"""

import asyncio
import logging
import signal
import sys
import time
from typing import Callable

logger = logging.getLogger(__name__)


class ServerLifecycleManager:
    """Manages server lifecycle for graceful startup and shutdown."""

    def __init__(self):
        """Initialize the lifecycle manager."""
        self.shutdown_event = None
        self.cleanup_callbacks: list[Callable] = []
        self.is_shutting_down = False
        self.loop = None

    def add_cleanup_callback(self, callback: Callable):
        """
        Add a callback to be executed during shutdown.
        
        Args:
            callback: A callable to execute during shutdown
        """
        self.cleanup_callbacks.append(callback)

    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()
        await self.shutdown_event.wait()

    def _signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown."""
        signame = signal.Signals(signum).name
        logger.info(f"Received signal {signame} ({signum}), initiating shutdown...")
        self._initiate_shutdown()

    def _stdin_monitor(self):
        """Monitor stdin for closure."""
        while not self.is_shutting_down:
            try:
                # Check if stdin is closed by trying to read
                if sys.stdin.closed:
                    logger.info("Stdin closed, initiating shutdown...")
                    self._initiate_shutdown()
                    break

                time.sleep(1)
            except ValueError:
                # ValueError occurs when trying to read from closed stdin
                logger.info("Stdin closed, initiating shutdown...")
                self._initiate_shutdown()
                break
            except Exception as e:
                # If we can't read from stdin, it might be closed
                logger.debug(f"Error reading from stdin: {e}")
                # Don't shut down on minor read errors
                time.sleep(1)

    def _initiate_shutdown(self):
        """Initiate the shutdown process."""
        if not self.is_shutting_down:
            self.is_shutting_down = True
            # Set the event in a thread-safe way
            if self.shutdown_event:
                try:
                    # Try to get the event loop
                    loop = asyncio.get_running_loop()
                    loop.call_soon_threadsafe(self.shutdown_event.set)
                except RuntimeError:
                    # No event loop running, set it directly
                    self.shutdown_event.set()

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()

        # Signal handling is now done in main(), just prepare the event

    async def cleanup(self):
        """Execute all cleanup callbacks."""
        logger.info("Executing cleanup callbacks...")
        for callback in self.cleanup_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                logger.error(f"Error during cleanup callback: {e}")
        logger.info("Cleanup completed.")


# Global instance
lifecycle_manager = ServerLifecycleManager()


def get_lifecycle_manager() -> ServerLifecycleManager:
    """Get the global lifecycle manager instance."""
    return lifecycle_manager


def add_cleanup_callback(callback: Callable):
    """
    Add a callback to be executed during shutdown.
    
    Args:
        callback: A callable to execute during shutdown
    """
    lifecycle_manager.add_cleanup_callback(callback)
