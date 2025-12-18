"""
Spark Event Log Analysis MCP Server with FastAPI Integration

A FastMCP 2.0 based MCP server integrated with FastAPI for comprehensive
Spark event log analysis, providing both MCP tools and HTTP API endpoints.
"""
from typing import Dict, Any, Optional
import os
import sys
from datetime import datetime
from pathlib import Path
from contextlib import asynccontextmanager

# Import FastAPI and FastMCP
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastmcp import FastMCP

# Import our modules
from .utils.helpers import setup_logging, load_config_from_env
from .utils.middleware import log_requests_middleware
from .utils.uvicorn_config import get_uvicorn_log_config
from .tools.mcp_tools import (
    generate_report_tool, get_analysis_status_tool, clear_session_tool, set_server_config
)

# Load configuration
config = load_config_from_env()
logger = setup_logging(config["log_level"])

# Define report data directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
REPORT_DATA_DIR = PROJECT_ROOT / "report_data"

# Ensure report data directory exists
REPORT_DATA_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"Report data directory: {REPORT_DATA_DIR}")

# Create MCP server
mcp = FastMCP(
    name=config["server_name"],
    version=config["server_version"]
)

# ==================== MCP Tools ====================

@mcp.tool()
async def generate_report(path: str) -> Dict[str, Any]:
    """
    Generate comprehensive Spark event log analysis reports from S3 or URL paths

    This tool provides complete end-to-end processing:
    1. Automatically detects path type (S3 or URL)
    2. Loads and validates Spark event log data
    3. Performs comprehensive performance analysis
    4. Generates interactive HTML report with visualizations
    5. Extracts optimization suggestions and recommendations

    Args:
        path: S3 path (s3://bucket/path/) or HTTP URL (https://example.com/logs.zip)
              Examples:
              - "s3://my-bucket/spark-logs/application-123/"
              - "https://example.com/spark-eventlogs.zip"

    Returns:
        Complete analysis report with metadata, visualization URL, and optimization suggestions
    """
    return await generate_report_tool(path, config["html_report_host_address"])


@mcp.tool()
async def get_analysis_status()-> Dict[str, Any]:
    """
    Get current analysis session status and summary information

    Returns information about the current analysis session, including
    data source, analysis configuration, and key metrics.

    Returns:
        Current session status and summary
    """
    return await get_analysis_status_tool()


@mcp.tool()
async def clear_session()-> Dict[str, Any]:
    """
    Clear current analysis session and cached data

    This tool resets the server state, clearing all cached analysis results
    and data sources. Use this to start a fresh analysis session.

    Returns:
        Confirmation of session clearing
    """
    return await clear_session_tool()

# ==================== MCP Resources ====================

@mcp.resource("server://info")
async def server_info():
    """Provide server information and capabilities"""
    return {
        "uri": "server://info",
        "name": "Spark EventLog MCP Server Info",
        "content": {
            "name": config["server_name"],
            "version": config["server_version"],
            "description": "End-to-end MCP Server for comprehensive Spark event log analysis",
            "primary_capability": "Single-command end-to-end Spark event log processing and report generation",
            "supported_data_sources": ["s3", "url"],
            "default_source_type": config["default_source_type"],
            "supported_report_formats": ["html"],
            "configuration": {
                "cache_enabled": config["cache_enabled"],
                "cache_ttl": config["cache_ttl"],
                "end_to_end_processing": True,
                "automatic_optimization_suggestions": True,
                "interactive_reports": True
            }
        },
        "mimeType": "application/json"
    }

@mcp.resource("health://components")
async def check_components():
    """Check health of server components"""
    try:
        health_data = {
            "status": "healthy",
            "components": {
                "data_loader": "operational",
                "analyzer": "operational",
                "report_generator": "operational"
            },
            "configuration": {
                "cache_enabled": config["cache_enabled"],
                "aws_configured": bool(config.get("aws_access_key_id"))
            }
        }

        return {
            "uri": "health://components",
            "name": "MCP Server Health Check",
            "content": health_data,
            "mimeType": "application/json"
        }
    except Exception as e:
        return {
            "uri": "health://components",
            "name": "MCP Server Health Check",
            "content": {
                "status": "unhealthy",
                "error": str(e)
            },
            "mimeType": "application/json"
        }

# ==================== FastAPI Integration ====================

# Create MCP ASGI app
mcp_app = mcp.http_app(path='/mcp')

# Define FastAPI lifespan
@asynccontextmanager
async def fastapi_lifespan(app: FastAPI):
    """FastAPI lifespan for initialization and cleanup"""
    logger.info("FastAPI app starting up...")
    yield
    logger.info("FastAPI app shutting down...")

# Combine lifespans
@asynccontextmanager
async def combined_lifespan(app: FastAPI):
    """Combined lifespan for both FastAPI and MCP"""
    async with fastapi_lifespan(app):
        async with mcp_app.lifespan(app):
            yield

# Create FastAPI app with combined lifespan
fastapi_app = FastAPI(
    title="Spark EventLog Analysis API",
    version=config["server_version"],
    description="RESTful API for Spark event log analysis with MCP integration",
    lifespan=combined_lifespan
)

# Add request logging middleware
@fastapi_app.middleware("http")
async def log_requests(request, call_next):
    """Request logging middleware"""
    return await log_requests_middleware(request, call_next)

# ==================== FastAPI HTTP Endpoints ====================

@fastapi_app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Spark EventLog Analysis API",
        "version": config["server_version"],
        "description": "RESTful API for Spark event log analysis with MCP integration",
        "endpoints": {
            "health": "/health",
            "api_docs": "/docs",
            "mcp_endpoint": "/mcp",
            "reports": {
                "list_reports": "GET /api/reports - 列出所有报告(JSON)",
                "view_report_1": "GET /reports/{filename} - 在浏览器中查看报告(HTML)",
                "view_report_2": "GET /api/reports/{filename} - 在浏览器中查看报告(HTML)",
                "delete_report": "DELETE /api/reports/{filename} - 删除报告"
            }
        }
    }

@fastapi_app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Spark EventLog Analysis API",
        "version": config["server_version"],
        "timestamp": datetime.now().isoformat()
    }

# ==================== Report API Endpoints ====================

@fastapi_app.get("/api/reports")
async def list_reports():
    """列出所有可用的报告文件"""
    try:
        reports = []

        # 扫描 report_data 目录
        if REPORT_DATA_DIR.exists():
            for file_path in REPORT_DATA_DIR.glob("*.html"):
                file_stat = file_path.stat()
                reports.append({
                    "filename": file_path.name,
                    "size": file_stat.st_size,
                    "created": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
                    "modified": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                    "url": f"/reports/{file_path.name}"
                })

        # 按修改时间降序排序(最新的在前面)
        reports.sort(key=lambda x: x["modified"], reverse=True)

        return {
            "total": len(reports),
            "reports": reports,
            "report_directory": str(REPORT_DATA_DIR)
        }
    except Exception as e:
        logger.error(f"Failed to list reports: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {str(e)}")

@fastapi_app.get("/api/reports/{filename}")
async def get_report_html(filename: str):
    """直接返回 HTML 报告文件,在浏览器中显示"""
    try:
        file_path = REPORT_DATA_DIR / filename

        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Report not found: {filename}")

        if not file_path.is_file():
            raise HTTPException(status_code=400, detail=f"Not a file: {filename}")

        # 检查文件扩展名
        if file_path.suffix.lower() not in [".html", ".htm"]:
            raise HTTPException(status_code=400, detail=f"Not an HTML file: {filename}")

        # 读取 HTML 文件内容并直接返回
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        return HTMLResponse(content=html_content, status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get report: {str(e)}")

@fastapi_app.delete("/api/reports/{filename}")
async def delete_report(filename: str):
    """删除指定的报告文件"""
    try:
        file_path = REPORT_DATA_DIR / filename

        if not file_path.exists():
            raise HTTPException(status_code=404, detail=f"Report not found: {filename}")

        if not file_path.is_file():
            raise HTTPException(status_code=400, detail=f"Not a file: {filename}")

        # 删除文件
        file_path.unlink()
        logger.info(f"Deleted report: {filename}")

        return {
            "success": True,
            "message": f"Report deleted: {filename}",
            "filename": filename
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete report: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete report: {str(e)}")

# ==================== Static Files ====================

# Mount static files for reports
fastapi_app.mount("/reports", StaticFiles(directory=str(REPORT_DATA_DIR)), name="reports")

# Mount MCP server at root
fastapi_app.mount("/", mcp_app)


def main():
    """Main entry point for the integrated server"""
    logger.info(f"Starting {config['server_name']} v{config['server_version']} with FastAPI integration")
    logger.info(f"Configuration: Cache={'ON' if config['cache_enabled'] else 'OFF'}, "
               f"Log Level={config['log_level']}")

    # Get transport mode configuration
    transport_mode = os.getenv("MCP_TRANSPORT", "streamable-http")
    mcp_host = os.getenv("MCP_HOST", "localhost")
    mcp_port = int(os.getenv("MCP_PORT", "7799"))

    # Set server configuration for MCP tools
    set_server_config(mcp_host, mcp_port, transport_mode)

    try:
        if transport_mode.lower() == "streamable-http":
            # Use HTTP transport mode with FastAPI
            logger.info(f"Starting HTTP server with FastAPI on {mcp_host}:{mcp_port}")
            logger.info(f"API Documentation available at: http://{mcp_host}:{mcp_port}/docs")
            logger.info(f"MCP endpoint available at: http://{mcp_host}:{mcp_port}/mcp")
            logger.info(f"Reports directory: {REPORT_DATA_DIR}")
            logger.info(f"View reports at: http://{mcp_host}:{mcp_port}/reports/<filename>")
            logger.info(f"List reports at: http://{mcp_host}:{mcp_port}/api/reports")

            # Run FastAPI app with uvicorn
            import uvicorn

            # Get custom uvicorn log config
            log_config = get_uvicorn_log_config(config["log_level"])

            uvicorn.run(
                fastapi_app,
                host=mcp_host,
                port=mcp_port,
                log_level=config["log_level"].lower(),
                log_config=log_config,
                access_log=True
            )
        else:
            # Default to stdio transport mode (original MCP only)
            mcp.run(transport="stdio")
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()