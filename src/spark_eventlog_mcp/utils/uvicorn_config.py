"""
Uvicorn 日志配置模块
"""


def get_uvicorn_log_config(log_level: str) -> dict:
    """
    获取 uvicorn 的日志配置

    Args:
        log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR)

    Returns:
        uvicorn 日志配置字典
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)-8s - [%(name)s:%(lineno)d:%(funcName)s] - uvicorn - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "access": {
                "format": "%(asctime)s - %(levelname)-8s - [uvicorn.access] - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": log_level,
                "propagate": False
            },
            "uvicorn.access": {
                "handlers": ["access"],
                "level": "INFO",
                "propagate": False
            },
        },
    }