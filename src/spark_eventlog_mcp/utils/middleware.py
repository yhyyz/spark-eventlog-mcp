"""
FastAPI 中间件模块
"""

import time
from fastapi import Request
from ..utils.helpers import setup_logging, load_config_from_env

config = load_config_from_env()
logger = setup_logging(config["log_level"])


async def log_requests_middleware(request: Request, call_next):
    """
    记录所有HTTP请求的详细信息，包括请求头和请求体
    """
    start_time = time.time()

    # 记录请求开始信息
    client_host = request.client.host if request.client else "unknown"
    user_agent = request.headers.get("user-agent", "unknown")
    content_type = request.headers.get("content-type", "none")
    accept = request.headers.get("accept", "none")
    content_length = request.headers.get("content-length", "0")

    # 记录关键请求头信息
    logger.info(f"Request started - {request.method} {request.url.path}")
    logger.info(f"Client: {client_host} | User-Agent: {user_agent}")
    logger.info(f"Content-Type: {content_type} | Accept: {accept} | Content-Length: {content_length}")

    # 读取请求体（如果有）
    request_body = None
    if request.method in ["POST", "PUT", "PATCH"]:
        try:
            body_bytes = await request.body()
            if body_bytes:
                request_body = body_bytes.decode('utf-8')
                # 限制请求体日志长度，避免过长
                if len(request_body) > 1000:
                    logger.info(f"Request body (truncated): {request_body[:1000]}...")
                else:
                    logger.info(f"Request body: {request_body}")
            else:
                logger.info("Request body: (empty)")
        except Exception as e:
            logger.warning(f"Failed to read request body: {e}")

    # 记录所有请求头（可选，用于调试）
    if logger.level <= 10:  # DEBUG level
        logger.debug("All request headers:")
        for name, value in request.headers.items():
            logger.debug(f"  {name}: {value}")

    # 记录查询参数（如果有）
    if request.query_params:
        logger.info(f"Query params: {dict(request.query_params)}")

    # 重新构造请求对象，因为 body() 只能读取一次
    async def receive():
        return {"type": "http.request", "body": request_body.encode('utf-8') if request_body else b""}

    if request_body is not None:
        # 创建新的 scope，保持请求体可用
        scope = request.scope.copy()
        new_request = Request(scope, receive=receive)
        # 复制原始请求的属性
        for attr in ['method', 'url', 'headers', 'query_params', 'path_params']:
            setattr(new_request, f'_{attr}', getattr(request, f'_{attr}', getattr(request, attr, None)))
        request = new_request

    # 执行请求
    response = await call_next(request)

    # 计算处理时间
    process_time = time.time() - start_time

    # 记录响应信息
    logger.info(f"Request completed - Status: {response.status_code} | Duration: {process_time:.3f}s")

    # 如果是错误状态码，记录更多信息
    if response.status_code >= 400:
        logger.warning(f"HTTP Error {response.status_code} - {request.method} {request.url.path} from {client_host}")

    # 添加自定义响应头（可选）
    response.headers["X-Process-Time"] = str(process_time)

    return response