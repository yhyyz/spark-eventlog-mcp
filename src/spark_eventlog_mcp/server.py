"""
Spark Event Log Analysis MCP Server with FastAPI Integration

A FastMCP 2.0 based MCP server integrated with FastAPI for comprehensive
Spark event log analysis, providing both MCP tools and HTTP API endpoints.
"""

import os
import sys
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
from contextlib import asynccontextmanager

# Import FastAPI and FastMCP
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastmcp import FastMCP



# Import our modules
from .models.schemas import (
    ParseEventLogInput, AnalyzePerformanceInput, GenerateReportInput,
    GetOptimizationSuggestionsInput, DataSource, AnalysisConfig, ReportConfig,
    AnalysisResult, GeneratedReport, OptimizationSuggestion
)
from .core.mature_data_loader import MatureDataLoader
from .tools.mature_analyzer import MatureSparkEventLogAnalyzer
from .tools.mature_report_generator import HTMLReportGenerator
from .models.mature_models import MatureAnalysisResult
from .utils.helpers import setup_logging, load_config_from_env, create_error_response, create_success_response

# Load configuration
config = load_config_from_env()
logger = setup_logging(config["log_level"])

# Initialize components
mature_data_loader = MatureDataLoader(config)
analyzer = MatureSparkEventLogAnalyzer()
report_generator = HTMLReportGenerator()

# Server state
_current_analysis: Optional[MatureAnalysisResult] = None
_current_data_source: Optional[DataSource] = None
_server_host: str = "localhost"
_server_port: int = 7799
_transport_mode: str = "streamable-http"

# Define report data directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
REPORT_DATA_DIR = PROJECT_ROOT / "report_data"

# Ensure report data directory exists
REPORT_DATA_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"Report data directory: {REPORT_DATA_DIR}")

# Create MCP server with all original tools
mcp = FastMCP(
    name=config["server_name"],
    version=config["server_version"]
)

# ==================== MCP Tools (保留所有原有工具) ====================

async def parse_eventlog(input_data: ParseEventLogInput) -> Dict[str, Any]:
    """
    Parse Spark event logs from various data sources (S3, URL, local files)

    This tool loads and validates Spark event log data, making it available
    for subsequent analysis operations.

    Args:
        input_data: Configuration specifying the data source

    Returns:
        Parsing results with summary statistics
    """
    global _current_data_source

    try:
        logger.info(f"Parsing event logs from {input_data.data_source.source_type}: {input_data.data_source.path}")

        # Validate data source first
        validation_result = await mature_data_loader.validate_data_source(input_data.data_source)

        if not validation_result["is_valid"]:
            return create_error_response(
                "ValidationError",
                f"Invalid data source: {validation_result['error_message']}",
                validation_result
            )

        # Store current data source
        _current_data_source = input_data.data_source

        # Generate summary
        summary = {
            "source_type": input_data.data_source.source_type,
            "source_path": input_data.data_source.path,
            "validation_info": validation_result["info"],
            "status": "Data source validated successfully"
        }

        if validation_result["warnings"]:
            summary["warnings"] = validation_result["warnings"]

        logger.info(f"Successfully validated data source: {input_data.data_source.path}")

        return create_success_response(
            {
                "parsed": True,
                "summary": summary
            },
            {
                "parse_timestamp": "completed",
                "data_cached": True
            }
        )

    except Exception as e:
        logger.error(f"Failed to parse event logs: {str(e)}")
        return create_error_response(
            "ParseError",
            f"Failed to parse event logs: {str(e)}"
        )

async def analyze_performance(input_data: AnalyzePerformanceInput) -> Dict[str, Any]:
    """
    Perform comprehensive performance analysis of Spark event logs

    This tool analyzes parsed event logs to extract performance metrics,
    resource utilization, shuffle statistics, and task execution patterns.

    Args:
        input_data: Analysis configuration and optional data source

    Returns:
        Complete analysis results with metrics and insights
    """
    global _current_analysis, _current_data_source

    try:
        # Use provided data source or current one
        data_source = input_data.data_source or _current_data_source

        if not data_source:
            return create_error_response(
                "ConfigurationError",
                "No data source available. Please run parse_eventlog first or provide a data source."
            )

        logger.info(f"Starting performance analysis with config: {input_data.analysis_config.analysis_depth}")

        # Load event logs using mature data loader
        if input_data.data_source:
            if data_source.source_type == "s3":
                event_logs_data = await mature_data_loader.load_from_s3(data_source.path)
            elif data_source.source_type == "url":
                event_logs_data = await mature_data_loader.load_from_url(data_source.path)
            elif data_source.source_type == "local":
                event_logs_data = await mature_data_loader.load_from_upload(data_source.path)
            else:
                return create_error_response(
                    "DataError",
                    f"Unsupported data source type: {data_source.source_type}"
                )
        else:
            return create_error_response(
                "DataError",
                "Please provide a data_source parameter for analysis."
            )

        # Perform analysis using mature analyzer
        analysis_result = analyzer.analyze(event_logs_data)

        # Store current analysis
        _current_analysis = analysis_result

        # Create response with summary
        summary = analyzer.get_analysis_summary()

        logger.info(f"Analysis completed for application: {analysis_result.application_id}")

        return create_success_response(
            {
                "analysis_complete": True,
                "analysis_result": analysis_result.dict(),
                "summary": summary
            },
            {
                "analysis_timestamp": analysis_result.analysis_timestamp.isoformat(),
                "config_used": input_data.analysis_config.dict()
            }
        )

    except Exception as e:
        logger.error(f"Performance analysis failed: {str(e)}")
        return create_error_response(
            "AnalysisError",
            f"Performance analysis failed: {str(e)}"
        )

@mcp.tool()
async def generate_report(input_data: GenerateReportInput) -> Dict[str, Any]:
    """
    Generate comprehensive reports with complete end-to-end processing
    
    This tool provides complete end-to-end Spark event log processing and report generation:
    1. Data parsing and validation from various sources (S3, URL, local files)
    2. Comprehensive performance analysis with configurable depth and metrics
    3. Optimization suggestions extraction and categorization
    4. Formatted report generation with visualizations and recommendations
    
    The tool automatically handles the complete pipeline and can work in multiple modes:
    - End-to-end: Provide data_source for complete processing from raw data
    - Analysis + Report: Use current session data for analysis and reporting
    - Report-only: Provide existing analysis_result to skip analysis phase

    Args:
        input_data: Complete configuration including:
            - report_config: Report formatting and content configuration (format, visualizations, etc.)
            - data_source: Optional data source for end-to-end processing (S3/URL/local file)
            - analysis_config: Optional analysis configuration (depth, metrics, etc.)
            - analysis_result: Optional existing analysis result (skips parsing and analysis if provided)

    Returns:
        Comprehensive report with content, metadata, analysis results, and optimization suggestions
        including parsing status, analysis metadata, and suggestion summaries
    """
    global _current_analysis, _current_data_source

    try:
        parsing_performed = False
        analysis_performed = False
        analysis_result = input_data.analysis_result

        # Phase 1: Data Parsing (if needed)
        if not analysis_result:
            data_source_to_use = input_data.data_source or _current_data_source
            
            # If we have a new data source, perform parsing
            if input_data.data_source and input_data.data_source != _current_data_source:
                logger.info(f"Starting end-to-end processing with new data source: {input_data.data_source.source_type}://{input_data.data_source.path}")
                
                # Create parse input and call parse_eventlog internally
                parse_input = ParseEventLogInput(data_source=input_data.data_source)
                parse_response = await parse_eventlog(parse_input)
                
                # Check parsing success
                if not parse_response.get("success", False):
                    return create_error_response(
                        "ParseError",
                        f"Data parsing failed: {parse_response.get('message', 'Unknown error')}"
                    )
                
                parsing_performed = True
                data_source_to_use = input_data.data_source
                logger.info("Data parsing completed successfully")
            
            # Verify we have a data source for analysis
            if not data_source_to_use:
                return create_error_response(
                    "ConfigurationError",
                    "No data source available for processing. "
                    "Please provide data_source parameter or run parse_eventlog first, or provide existing analysis_result."
                )

        # Phase 2: Performance Analysis (if needed)
        if not analysis_result:
            logger.info("No existing analysis found, performing new analysis")
            
            # Use provided analysis config or default
            analysis_config = input_data.analysis_config or AnalysisConfig()
            
            # Create analysis input and call analyze_performance internally
            analysis_input = AnalyzePerformanceInput(
                analysis_config=analysis_config,
                data_source=data_source_to_use
            )
            
            analysis_response = await analyze_performance(analysis_input)
            
            # Check analysis success
            if not analysis_response.get("success", False):
                return create_error_response(
                    "AnalysisError",
                    f"Performance analysis failed: {analysis_response.get('message', 'Unknown error')}"
                )
            
            # Get analysis result from current session
            analysis_result = _current_analysis
            analysis_performed = True
            
            if not analysis_result:
                return create_error_response(
                    "AnalysisError", 
                    "Analysis completed but no result available"
                )
            
            logger.info(f"Performance analysis completed for application: {analysis_result.application_id}")

        # Phase 3: Optimization Suggestions Extraction
        logger.info("Extracting optimization suggestions")
        
        suggestions_input = GetOptimizationSuggestionsInput(
            focus_areas=[],  # Get all suggestions
            priority_filter=None  # No priority filter
        )
        
        suggestions_response = await get_optimization_suggestions(suggestions_input)
        optimization_suggestions = []
        
        if suggestions_response.get("success", False):
            optimization_suggestions = suggestions_response.get("data", {}).get("suggestions", [])
            logger.info(f"Retrieved {len(optimization_suggestions)} optimization suggestions")
        else:
            logger.warning(f"Failed to get optimization suggestions: {suggestions_response.get('message', 'Unknown error')}")

        # Phase 4: Report Generation
        logger.info(f"Generating {input_data.report_config.report_format} report")

        if input_data.report_config.report_format == "html":
            # Generate HTML report with server configuration
            report_address = await report_generator.generate_html_report(
                analysis_result,
                server_host=_server_host,
                server_port=_server_port,
                transport_mode=_transport_mode
            )

            if _transport_mode.lower() == "streamable-http":
                # HTTP mode: return HTTP URL for browser access
                response_data = {
                    "report_generated": True,
                    "report_format": "html",
                    "title": f"Spark Analysis Report - {analysis_result.application_name}",
                    "report_address": report_address,
                }
            else:
                # stdio mode: return file path
                response_data = {
                    "report_generated": True,
                    "report_format": "html",
                    "title": f"Spark Analysis Report - {analysis_result.application_name}",
                    "report_address": report_address,
                }
                
        elif input_data.report_config.report_format == "json":
            # Return JSON format with full analysis data and suggestions
            response_data = {
                "report_generated": True,
                "report_format": "json",
                "report_data": analysis_result.dict(),
                "optimization_suggestions": optimization_suggestions
            }
        else:
            return create_error_response(
                "ReportError",
                f"Unsupported report format: {input_data.report_config.report_format}"
            )

        # Add comprehensive processing metadata
        response_data["processing_metadata"] = {
            "end_to_end_processing": parsing_performed or analysis_performed,
            "phases_completed": {
                "data_parsing": parsing_performed,
                "performance_analysis": analysis_performed,
                "optimization_extraction": len(optimization_suggestions) > 0,
                "report_generation": True
            },
            "data_source": {
                "type": _current_data_source.source_type if _current_data_source else None,
                "path": _current_data_source.path if _current_data_source else None
            } if _current_data_source else None
        }

        # Add analysis metadata
        response_data["analysis_metadata"] = {
            "application_id": analysis_result.application_id,
            "application_name": analysis_result.application_name,
            "analysis_timestamp": analysis_result.analysis_timestamp.isoformat(),
            "total_optimization_suggestions": len(optimization_suggestions),
            "analysis_summary": {
                "total_jobs": len(analysis_result.jobs),
                "total_tasks": sum(job.num_tasks for job in analysis_result.jobs),
                "total_duration_ms": analysis_result.duration_ms,
                "successful_jobs": analysis_result.successful_jobs,
                "failed_jobs": analysis_result.failed_jobs,
                "total_executors": analysis_result.total_executors
            }
        }
        
        # Add optimization suggestions summary
        if optimization_suggestions:
            priority_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
            category_counts = {}
            
            for suggestion in optimization_suggestions:
                priority = suggestion.get('priority', 'LOW')
                category = suggestion.get('category', 'OTHER')
                
                priority_counts[priority] = priority_counts.get(priority, 0) + 1
                category_counts[category] = category_counts.get(category, 0) + 1
            
            response_data["optimization_summary"] = {
                "total_suggestions": len(optimization_suggestions),
                "by_priority": priority_counts,
                "by_category": category_counts,
                "high_priority_suggestions": [s for s in optimization_suggestions if s.get('priority') == 'HIGH']
            }

        logger.info(f"End-to-end report generation completed successfully")

        return create_success_response(
            response_data,
            {
                "generation_timestamp": datetime.now().isoformat(),
                "config_used": {
                    "report_config": input_data.report_config.dict(),
                    "analysis_config": input_data.analysis_config.dict() if input_data.analysis_config else None
                },
                "processing_summary": {
                    "parsing_performed": parsing_performed,
                    "analysis_performed": analysis_performed,
                    "suggestions_included": len(optimization_suggestions),
                    "total_processing_phases": sum([parsing_performed, analysis_performed, True, True])  # parse, analyze, suggest, report
                }
            }
        )

    except Exception as e:
        logger.error(f"End-to-end report generation failed: {str(e)}")
        return create_error_response(
            "ReportError",
            f"End-to-end report generation failed: {str(e)}"
        )

async def get_optimization_suggestions(
    input_data: GetOptimizationSuggestionsInput
) -> Dict[str, Any]:
    """
    Get targeted optimization suggestions based on analysis results

    This tool extracts and filters optimization recommendations from the
    current analysis, allowing focus on specific areas or priority levels.

    Args:
        input_data: Filter configuration for suggestions

    Returns:
        Filtered optimization suggestions with implementation details
    """
    global _current_analysis

    try:
        if not _current_analysis:
            return create_error_response(
                "ConfigurationError",
                "No analysis result available. Please run analyze_performance first."
            )

        logger.info(f"Retrieving optimization suggestions with filters: {input_data.focus_areas or 'all'}")

        # Get filtered suggestions
        suggestions = analyzer.get_optimization_suggestions(
            focus_areas=input_data.focus_areas,
            priority_filter=input_data.priority_filter
        )

        # Group suggestions by category and priority
        categorized_suggestions = {}
        priority_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}

        for suggestion in suggestions:
            category = suggestion['category']
            if category not in categorized_suggestions:
                categorized_suggestions[category] = []
            categorized_suggestions[category].append(suggestion)
            priority_counts[suggestion['priority']] += 1

        response_data = {
            "suggestions_found": len(suggestions),
            "suggestions": suggestions,
            "categorized_suggestions": categorized_suggestions,
            "priority_breakdown": priority_counts
        }

        # Add configuration recommendations summary
        if suggestions:
            config_params = {}
            for suggestion in suggestions:
                config_params.update(suggestion['config_parameters'])

            response_data["recommended_spark_config"] = config_params

        logger.info(f"Retrieved {len(suggestions)} optimization suggestions")

        return create_success_response(
            response_data,
            {
                "filters_applied": {
                    "focus_areas": input_data.focus_areas,
                    "priority_filter": input_data.priority_filter
                },
                "total_available": len(_current_analysis.optimization_recommendations)
            }
        )

    except Exception as e:
        logger.error(f"Failed to get optimization suggestions: {str(e)}")
        return create_error_response(
            "SuggestionError",
            f"Failed to get optimization suggestions: {str(e)}"
        )

@mcp.tool()
async def get_analysis_status() -> Dict[str, Any]:
    """
    Get current analysis session status and summary information

    Returns information about the current analysis session, including
    data source, analysis configuration, and key metrics.

    Returns:
        Current session status and summary
    """
    global _current_analysis, _current_data_source

    try:
        status = {
            "session_active": _current_analysis is not None,
            "data_source_loaded": _current_data_source is not None,
        }

        if _current_data_source:
            status["data_source"] = {
                "type": _current_data_source.source_type,
                "path": _current_data_source.path
            }

            status["data_info"] = {
                "source_type": _current_data_source.source_type,
                "path": _current_data_source.path
            }

        if _current_analysis:
            status["analysis_summary"] = analyzer.get_analysis_summary()
            status["optimization_suggestions_available"] = len(_current_analysis.optimization_recommendations)

        return create_success_response(status)

    except Exception as e:
        logger.error(f"Failed to get analysis status: {str(e)}")
        return create_error_response(
            "StatusError",
            f"Failed to get analysis status: {str(e)}"
        )

@mcp.tool()
async def clear_session() -> Dict[str, Any]:
    """
    Clear current analysis session and cached data

    This tool resets the server state, clearing all cached analysis results
    and data sources. Use this to start a fresh analysis session.

    Returns:
        Confirmation of session clearing
    """
    global _current_analysis, _current_data_source

    try:
        # Clear global state
        _current_analysis = None
        _current_data_source = None

        logger.info("Session cleared successfully")

        return create_success_response({
            "session_cleared": True,
            "message": "All cached data and analysis results have been cleared"
        })

    except Exception as e:
        logger.error(f"Failed to clear session: {str(e)}")
        return create_error_response(
            "ClearError",
            f"Failed to clear session: {str(e)}"
        )

# ==================== MCP Resources (保留所有原有资源) ====================

@mcp.resource("server://info")
async def server_info():
    """Provide server information and capabilities"""
    return {
        "uri": "server://info",
        "name": "Spark EventLog MCP Server Info",
        "content": {
            "name": config["server_name"],
            "version": config["server_version"],
            "description": "End-to-end MCP Server for comprehensive Spark event log analysis with integrated processing pipeline",
            "primary_capability": "Single-command end-to-end Spark event log processing and report generation",
            "processing_pipeline": [
                "Data parsing and validation from multiple sources (S3, URL, local files)",
                "Comprehensive performance analysis with configurable metrics",
                "Intelligent optimization suggestions with priority categorization", 
                "Professional report generation with interactive visualizations"
            ],
            "key_features": [
                "End-to-end processing in single tool call",
                "Automatic data source detection and validation",
                "Configurable analysis depth and focus areas",
                "Multi-format report generation (HTML/JSON)",
                "Real-time processing metadata and phase tracking",
                "Comprehensive optimization recommendations",
                "Interactive HTML reports with FastAPI integration",
                "Session management and data caching"
            ],
            "supported_data_sources": ["s3", "url", "local"],
            "default_source_type": config["default_source_type"],
            "supported_report_formats": ["html", "json"],
            "analysis_capabilities": [
                "Application-level metrics and timing analysis",
                "Stage-by-stage execution breakdown",
                "Task distribution and execution patterns",
                "Shuffle performance and data locality analysis",
                "Resource utilization (CPU, memory, storage)",
                "Garbage collection impact assessment",
                "Performance bottleneck identification",
                "Optimization opportunity detection"
            ],
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

@mcp.resource("docs://tools")
async def tools_documentation():
    """Provide comprehensive tool documentation"""
    return {
        "uri": "docs://tools",
        "name": "Spark EventLog MCP Server Tools Documentation",
        "content": {
            "tools": {
                "generate_report": {
                    "description": "Complete end-to-end Spark event log processing and report generation",
                    "input": "GenerateReportInput with comprehensive configuration options",
                    "output": "Generated report with complete processing metadata, analysis results, and optimization suggestions",
                    "processing_modes": {
                        "end_to_end": "Provide data_source for complete processing from raw data to final report",
                        "analysis_and_report": "Use current session data for analysis and report generation", 
                        "report_only": "Provide existing analysis_result to generate report from existing analysis"
                    },
                    "processing_phases": [
                        "1. Data Parsing - Load and validate Spark event logs from various sources",
                        "2. Performance Analysis - Extract metrics, resource utilization, and execution patterns", 
                        "3. Optimization Suggestions - Generate categorized recommendations for performance improvements",
                        "4. Report Generation - Create formatted reports with visualizations and insights"
                    ],
                    "example_end_to_end": {
                        "data_source": {
                            "source_type": "s3",
                            "path": "s3://my-bucket/spark-logs/application_123/"
                        },
                        "analysis_config": {
                            "analysis_depth": "detailed",
                            "include_shuffle_analysis": True,
                            "include_resource_analysis": True,
                            "include_task_analysis": True,
                            "include_optimization_suggestions": True
                        },
                        "report_config": {
                            "report_format": "html",
                            "include_visualizations": True,
                            "include_raw_metrics": False
                        }
                    },
                    "example_report_only": {
                        "analysis_result": {
                            "application_id": "app-20231201-123456",
                            "jobs": "...",
                            "optimization_recommendations": "..."
                        },
                        "report_config": {
                            "report_format": "json",
                            "include_visualizations": False,
                            "include_raw_metrics": True
                        }
                    },
                    "supported_formats": ["html", "json"],
                    "supported_sources": ["s3", "url", "local"],
                    "features": [
                        "Automatic data source validation and parsing",
                        "Configurable performance analysis depth and metrics",
                        "Intelligent optimization suggestions with priority categorization",
                        "Interactive HTML reports with visualizations",
                        "Comprehensive JSON reports with raw data",
                        "Processing metadata and phase tracking",
                        "Error handling and detailed logging throughout pipeline"
                    ]
                },
                "get_analysis_status": {
                    "description": "Get current session status and processing summary",
                    "input": "No parameters required",
                    "output": "Current session information including data source status, analysis availability, and processing history"
                },
                "clear_session": {
                    "description": "Clear current analysis session and reset all cached data", 
                    "input": "No parameters required",
                    "output": "Session clearing confirmation and reset status"
                }
            },
            "workflow_guide": {
                "simple_usage": [
                    "Single call: generate_report with data_source for complete end-to-end processing",
                    "Result: Comprehensive report with all analysis and optimization insights"
                ],
                "advanced_usage": [
                    "1. Configure analysis_config for specific analysis requirements",
                    "2. Use report_config to customize report format and content",
                    "3. Leverage processing_metadata to track pipeline execution",
                    "4. Use optimization_summary for focused performance improvements"
                ],
                "internal_functions": {
                    "parse_eventlog": "Internal data parsing and validation (called automatically)",
                    "analyze_performance": "Internal performance analysis engine (called automatically)", 
                    "get_optimization_suggestions": "Internal optimization recommendation engine (called automatically)"
                }
            },
            "data_sources": {
                "s3": {
                    "format": {"source_type": "s3", "path": "s3://bucket-name/path/to/logs/"},
                    "requirements": "AWS credentials configured or IAM role permissions"
                },
                "url": {
                    "format": {"source_type": "url", "path": "https://example.com/spark-logs.zip"},
                    "requirements": "Publicly accessible URL or authenticated endpoint"
                },
                "local": {
                    "format": {"source_type": "local", "path": "/path/to/local/spark-logs/"},
                    "requirements": "Local file system access to log files"
                }
            }
        },
        "mimeType": "application/json"
    }

@mcp.resource("health://components")
async def check_components():
    """Check health of server components"""
    try:
        # Test basic functionality
        test_config = AnalysisConfig()
        test_report_config = ReportConfig()

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

# Add CORS middleware (if needed)
# Note: Only add if your FastAPI endpoints need CORS, not for MCP routes
# fastapi_app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

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
    """
    列出所有可用的报告文件

    Returns:
        List of available HTML report files with metadata
    """
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
    """
    直接返回 HTML 报告文件,在浏览器中显示

    Args:
        filename: Report filename

    Returns:
        HTML file content
    """
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
    """
    删除指定的报告文件

    Args:
        filename: Report filename to delete

    Returns:
        Deletion confirmation
    """
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
# 注意: StaticFiles 必须在所有其他路由之后挂载,因为它会匹配所有路径
# 移除 html=True 参数,让浏览器根据 content-type 自动处理
fastapi_app.mount("/reports", StaticFiles(directory=str(REPORT_DATA_DIR)), name="reports")

# Mount MCP server at root
fastapi_app.mount("/", mcp_app)


def main():
    """Main entry point for the integrated server"""
    global _server_host, _server_port, _transport_mode

    logger.info(f"Starting {config['server_name']} v{config['server_version']} with FastAPI integration")
    logger.info(f"Configuration: Cache={'ON' if config['cache_enabled'] else 'OFF'}, "
               f"Log Level={config['log_level']}")

    # Get transport mode configuration
    transport_mode = os.getenv("MCP_TRANSPORT", "streamable-http")
    mcp_host = os.getenv("MCP_HOST", "localhost")
    mcp_port = int(os.getenv("MCP_PORT", "7799"))

    # Update global state
    _server_host = mcp_host
    _server_port = mcp_port
    _transport_mode = transport_mode

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
            uvicorn.run(
                fastapi_app,
                host=mcp_host,
                port=mcp_port,
                log_level=config["log_level"].lower()
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
