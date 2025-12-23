"""
Server lifecycle management for graceful shutdown.

This module provides functionality to handle server startup and shutdown gracefully.
It manages system signals like SIGINT and SIGTERM, monitors stdin closure, and 
executes cleanup callbacks during shutdown. This ensures that database connections
and other resources are properly released when the server stops.

The ServerLifecycleManager handles:
- Signal handling for clean shutdown
- Stdin monitoring to detect client disconnections
- Execution of cleanup callbacks
- Thread-safe event signaling
"""

import asyncio
import logging
import signal
import sys
import time
from typing import Callable

logger = logging.getLogger(__name__)


class ServerLifecycleManager:
    """Manages server lifecycle for graceful startup and shutdown.

    This class provides functionality for managing the server lifecycle,
    including handling shutdown signals, monitoring stdin, and executing
    cleanup callbacks. It ensures that resources like database connections
    are properly released during server shutdown.
    """

    def __init__(self):
        """Initialize the lifecycle manager.

        Sets up the internal state for tracking shutdown status,
        cleanup callbacks, and the asyncio event for shutdown signaling.
        """
        self.shutdown_event = None
        self.cleanup_callbacks: list[Callable] = []
        self.is_shutting_down = False
        self.loop = None

    def add_cleanup_callback(self, callback: Callable):
        """
        Add a callback to be executed during shutdown.
        
        Use this method to register cleanup functions that should be
        executed when the server shuts down. These might include closing
        database connections, saving state, or other cleanup operations.
        
        Args:
            callback: A callable to execute during shutdown
        """
        self.cleanup_callbacks.append(callback)

    async def wait_for_shutdown(self):
        """Wait for shutdown signal.

        This method blocks until a shutdown signal is received.
        It can be used in an asyncio task to keep the server running
        until a shutdown signal is received.

        Returns:
            None when shutdown is initiated
        """
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()
        await self.shutdown_event.wait()

    def _signal_handler(self, signum, frame):
        """Handle system signals for graceful shutdown.
        
        This internal method is called when the process receives a signal
        like SIGINT (Ctrl+C) or SIGTERM. It logs the received signal and
        initiates the shutdown process.
        
        Args:
            signum: Signal number received
            frame: Current stack frame (unused)
        """
        signame = signal.Signals(signum).name
        logger.info(f"Received signal {signame} ({signum}), initiating shutdown...")
        self._initiate_shutdown()

    def _stdin_monitor(self):
        """Monitor stdin for closure and initiate shutdown if closed.
        
        This internal method continuously monitors stdin for closure,
        which can indicate that the client has disconnected. When stdin
        is closed, it initiates the shutdown process.
        """
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
        """Initiate the shutdown process.
        
        This internal method sets the shutdown flag and signals the
        shutdown event in a thread-safe manner. It ensures that the
        shutdown process is properly initiated regardless of the
        context in which it's called.
        """
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
        """Setup signal handlers for graceful shutdown.
        
        This method prepares the shutdown event and sets up signal handlers
        for clean shutdown when receiving system signals like SIGINT or SIGTERM.
        """
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()

        # Signal handling is now done in main(), just prepare the event

    async def cleanup(self):
        """Execute all registered cleanup callbacks.
        
        This method runs all the cleanup callbacks that were registered
        using add_cleanup_callback. It handles both synchronous and
        asynchronous callbacks, ensuring that all resources are properly
        cleaned up during shutdown.
        """
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
    """Get the global lifecycle manager instance.
    
    Returns:
        The global ServerLifecycleManager instance
    """
    return lifecycle_manager


def add_cleanup_callback(callback: Callable):
    """
    Add a callback to be executed during shutdown.
    
    This is a convenience function that adds a cleanup callback to the
    global lifecycle manager instance. Use this function to register
    cleanup operations that should run when the server shuts down.
    
    Args:
        callback: A callable to execute during shutdown
    """
    lifecycle_manager.add_cleanup_callback(callback)