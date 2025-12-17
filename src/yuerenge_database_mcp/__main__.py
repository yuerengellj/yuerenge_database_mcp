"""
Entry point for running the Database MCP Server as a module.
"""

import os
import sys

# Add the src directory to the path so we can import the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Now we can import and run the main function
from yuerenge_database_mcp import main

if __name__ == "__main__":
    main()
