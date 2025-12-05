#!/usr/bin/env python3
"""
Startup script for Spark EventLog MCP Server with FastAPI integration

This script starts the MCP server with FastAPI integration,
providing both MCP tools and RESTful API endpoints.
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from spark_eventlog_mcp.server import main

if __name__ == "__main__":
    main()
