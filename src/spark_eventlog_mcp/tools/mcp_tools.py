"""
MCP Tools 实现模块
包含所有 MCP 工具的具体实现逻辑
"""

from typing import Dict, Any, Optional
from datetime import datetime

from ..models.schemas import (
    ParseEventLogInput, AnalyzePerformanceInput,
    GetOptimizationSuggestionsInput, DataSource, AnalysisConfig
)
from ..core.mature_data_loader import MatureDataLoader
from ..tools.mature_analyzer import MatureSparkEventLogAnalyzer
from ..tools.mature_report_generator import HTMLReportGenerator
from ..models.mature_models import MatureAnalysisResult
from ..utils.helpers import setup_logging, load_config_from_env, create_error_response, create_success_response

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


def set_server_config(host: str, port: int, transport_mode: str):
    """设置服务器配置"""
    global _server_host, _server_port, _transport_mode
    _server_host = host
    _server_port = port
    _transport_mode = transport_mode


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
                "analysis_result": analysis_result.model_dump(),
                "summary": summary
            },
            {
                "analysis_timestamp": analysis_result.analysis_timestamp.isoformat(),
                "config_used": input_data.analysis_config.model_dump()
            }
        )

    except Exception as e:
        logger.error(f"Performance analysis failed: {str(e)}")
        return create_error_response(
            "AnalysisError",
            f"Performance analysis failed: {str(e)}"
        )


async def generate_report_tool(path: str, html_report_host_address="http://localhost:7799") -> Dict[str, Any]:
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
    global _current_analysis, _current_data_source

    try:
        logger.info(f"Starting end-to-end report generation for path: {path}")

        # Phase 1: Auto-detect path type and create data source
        if path.startswith('s3://'):
            data_source = DataSource(source_type="s3", path=path)
            logger.info(f"Detected S3 path: {path}")
        elif path.startswith(('http://', 'https://')):
            data_source = DataSource(source_type="url", path=path)
            logger.info(f"Detected URL path: {path}")
        else:
            return create_error_response(
                "InvalidPath",
                f"Unsupported path format. Path must start with 's3://' or 'http(s)://'. Got: {path}"
            )

        # Phase 2: Data Parsing
        logger.info(f"Starting data parsing from {data_source.source_type}")
        parse_input = ParseEventLogInput(data_source=data_source)
        parse_response = await parse_eventlog(parse_input)

        if not parse_response.get("success", False):
            return create_error_response(
                "ParseError",
                f"Data parsing failed: {parse_response.get('message', 'Unknown error')}"
            )

        # Phase 3: Performance Analysis
        logger.info("Starting performance analysis")
        analysis_config = AnalysisConfig(
            analysis_depth="detailed",
            include_shuffle_analysis=True,
            include_resource_analysis=True,
            include_task_analysis=True,
            include_optimization_suggestions=True
        )

        analysis_input = AnalyzePerformanceInput(
            analysis_config=analysis_config,
            data_source=data_source
        )

        analysis_response = await analyze_performance(analysis_input)

        if not analysis_response.get("success", False):
            return create_error_response(
                "AnalysisError",
                f"Performance analysis failed: {analysis_response.get('message', 'Unknown error')}"
            )

        # Get analysis result from current session
        analysis_result = _current_analysis

        if not analysis_result:
            return create_error_response(
                "AnalysisError",
                "Analysis completed but no result available"
            )

        logger.info(f"Performance analysis completed for application: {analysis_result.application_id}")

        # Phase 4: Optimization Suggestions Extraction
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

        # Phase 5: HTML Report Generation
        logger.info("Generating HTML report")

        report_address = await report_generator.generate_html_report(
            analysis_result,
            html_report_host_address=html_report_host_address,
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

        # Add comprehensive processing metadata
        response_data["processing_metadata"] = {
            "end_to_end_processing": True,
            "phases_completed": {
                "data_parsing": True,
                "performance_analysis": True,
                "optimization_extraction": len(optimization_suggestions) > 0,
                "report_generation": True
            },
            "data_source": {
                "type": data_source.source_type,
                "path": data_source.path
            },
            "path_detection": f"Auto-detected as {data_source.source_type.upper()}"
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

        logger.info(f"End-to-end report generation completed successfully from {data_source.source_type}: {data_source.path}")

        return create_success_response(
            response_data,
            {
                "generation_timestamp": datetime.now().isoformat(),
                "input_path": path,
                "detected_source_type": data_source.source_type,
                "processing_summary": {
                    "phases_completed": 4,  # parse, analyze, suggest, report
                    "suggestions_included": len(optimization_suggestions),
                    "report_format": "html"
                }
            }
        )

    except Exception as e:
        logger.error(f"End-to-end report generation failed: {str(e)}")
        return create_error_response(
            "ReportError",
            f"End-to-end report generation failed: {str(e)}"
        )


async def get_optimization_suggestions(input_data: GetOptimizationSuggestionsInput) -> Dict[str, Any]:
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


async def get_analysis_status_tool() -> Dict[str, Any]:
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


async def clear_session_tool() -> Dict[str, Any]:
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