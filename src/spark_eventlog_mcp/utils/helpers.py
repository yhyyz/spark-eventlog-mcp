"""
Helper utilities for Spark Event Log Analysis MCP Server
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Setup logging configuration for the MCP server

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger instance
    """
    # 详细的日志格式,包含时间、日志级别、文件名、行号、函数名和消息
    log_format = (
        '%(asctime)s - %(levelname)-8s - '
        '[%(filename)s:%(lineno)d:%(funcName)s] - '
        '%(name)s - %(message)s'
    )

    # 时间格式
    date_format = '%Y-%m-%d %H:%M:%S'

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt=date_format
    )

    logger = logging.getLogger("spark-eventlog-mcp")
    return logger

def load_config_from_env() -> Dict[str, Any]:
    """
    Load configuration from environment variables with optimized parsing

    Returns:
        Configuration dictionary with properly typed values
    """
    # Helper function for boolean conversion
    def get_bool(key: str, default: str = "false") -> bool:
        return os.getenv(key, default).lower() in ("true", "1", "yes", "on")

    # Helper function for integer conversion with validation
    def get_int(key: str, default: int) -> int:
        try:
            return int(os.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    # Server settings
    server_config = {
        "server_name": os.getenv("MCP_SERVER_NAME", "Spark EventLog Analyzer"),
        "server_version": os.getenv("MCP_SERVER_VERSION", "1.0.0"),
        "log_level": os.getenv("LOG_LEVEL", "INFO").upper(),
        "mcp_host": os.getenv("MCP_HOST"),
        "mcp_port": os.getenv("MCP_PORT"),
        "html_report_host_address": os.getenv("HTML_REPORT_HOST_ADDRESS","http://localhost:7799"),
    }

    # AWS settings with S3 as default
    aws_config = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        "default_source_type": os.getenv("DEFAULT_SOURCE_TYPE", "s3"),  # 默认使用 S3
    }

    # Cache settings
    cache_config = {
        "cache_enabled": get_bool("CACHE_ENABLED", "true"),
        "cache_ttl": get_int("CACHE_TTL", 300),
    }

    # Analysis settings (暂无实际使用的分析配置)
    analysis_config = {
        # 所有分析配置都已移除，因为它们在业务逻辑中未被使用
        # 分析深度由用户请求参数控制，不使用环境变量默认值
    }

    # Performance settings (未实现的功能，保留配置结构)
    performance_config = {
        "enable_metrics": get_bool("ENABLE_METRICS", "false"),
        "metrics_port": get_int("METRICS_PORT", 9090),
    }

    # Merge all configurations
    config = {
        **server_config,
        **aws_config,
        **cache_config,
        **analysis_config,
        **performance_config,
    }

    return config

def validate_file_path(file_path: str) -> bool:
    """
    Validate if a file path exists and is accessible

    Args:
        file_path: Path to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        path = Path(file_path)
        return path.exists() and path.is_file()
    except Exception:
        return False

def validate_s3_path(s3_path: str) -> bool:
    """
    Validate S3 path format

    Args:
        s3_path: S3 path to validate

    Returns:
        True if valid format, False otherwise
    """
    return s3_path.startswith("s3://") and len(s3_path.split("/")) >= 4

def validate_url(url: str) -> bool:
    """
    Validate URL format

    Args:
        url: URL to validate

    Returns:
        True if valid format, False otherwise
    """
    return url.startswith(("http://", "https://"))

def format_bytes(bytes_value: int) -> str:
    """
    Format bytes to human readable format

    Args:
        bytes_value: Number of bytes

    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    if bytes_value == 0:
        return "0 B"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024

    return f"{bytes_value:.2f} PB"

def format_duration_ms(duration_ms: int) -> str:
    """
    Format duration in milliseconds to human readable format

    Args:
        duration_ms: Duration in milliseconds

    Returns:
        Formatted string (e.g., "2m 30s")
    """
    if duration_ms == 0:
        return "0ms"

    seconds = duration_ms / 1000

    if seconds < 60:
        return f"{seconds:.2f}s"

    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    if minutes < 60:
        if remaining_seconds > 0:
            return f"{minutes}m {remaining_seconds:.0f}s"
        else:
            return f"{minutes}m"

    hours = int(minutes // 60)
    remaining_minutes = minutes % 60

    if remaining_minutes > 0:
        return f"{hours}h {remaining_minutes}m"
    else:
        return f"{hours}h"

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero

    Args:
        numerator: Numerator value
        denominator: Denominator value
        default: Default value to return if division by zero

    Returns:
        Result of division or default
    """
    if denominator == 0:
        return default
    return numerator / denominator

def extract_application_id_from_path(path: str) -> Optional[str]:
    """
    Extract Spark application ID from file path

    Args:
        path: File path that might contain application ID

    Returns:
        Application ID if found, None otherwise
    """
    # Common patterns for Spark application IDs
    import re

    # Pattern: application_1234567890123_0001
    pattern = r'application_\d+_\d+'
    match = re.search(pattern, path)

    if match:
        return match.group(0)

    return None

def create_error_response(error_type: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create standardized error response

    Args:
        error_type: Type of error (e.g., "ValidationError", "ProcessingError")
        message: Error message
        details: Optional additional error details

    Returns:
        Standardized error response dictionary
    """
    response = {
        "success": False,
        "error": {
            "type": error_type,
            "message": message
        }
    }

    if details:
        response["error"]["details"] = details

    return response

def create_success_response(data: Any, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create standardized success response

    Args:
        data: Response data
        metadata: Optional metadata

    Returns:
        Standardized success response dictionary
    """
    response = {
        "success": True,
        "data": data
    }

    if metadata:
        response["metadata"] = metadata

    return response

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by removing/replacing invalid characters

    Args:
        filename: Original filename

    Returns:
        Sanitized filename
    """
    import re

    # Remove or replace invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)

    # Remove multiple underscores
    sanitized = re.sub(r'_{2,}', '_', sanitized)

    # Remove leading/trailing underscores and dots
    sanitized = sanitized.strip('_.')

    return sanitized or "unnamed_file"

def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes

    Args:
        file_path: Path to file

    Returns:
        File size in MB, 0 if file doesn't exist
    """
    try:
        size_bytes = Path(file_path).stat().st_size
        return size_bytes / (1024 * 1024)
    except Exception:
        return 0.0