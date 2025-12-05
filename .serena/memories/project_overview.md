# Spark Event Log Analysis MCP Server - 项目概述

## 项目目的
这是一个用于分析 Apache Spark 事件日志的 MCP (Model Context Protocol) 服务器。该项目为 AI 助手提供了专业的 Spark 性能分析能力，能够解析事件日志、生成分析报告并提供优化建议。

## 核心功能
- 📊 **事件日志解析**: 支持多种数据源（本地文件、S3、URL）
- 🔍 **性能分析**: 深度分析 Spark 应用程序性能瓶颈
- 📈 **报告生成**: 生成详细的 HTML 分析报告，包含可视化图表
- ☁️ **多源支持**: 支持本地文件、Amazon S3 和 HTTP URL 数据源
- 🎯 **优化建议**: 基于分析结果提供具体的配置优化建议

## 技术栈
- **核心框架**: FastMCP 2.0 (Model Context Protocol server)
- **语言**: Python 3.12+
- **数据处理**: pandas, numpy
- **云存储**: boto3 (AWS S3 支持)
- **可视化**: plotly
- **模板引擎**: jinja2
- **HTTP**: httpx, aiohttp, fastapi
- **异步**: asyncio, aiofiles

## 架构组成
- **Server**: MCP 服务器主控制器 (server.py)
- **Tools**: 分析工具模块
  - mature_analyzer.py: 核心分析引擎
  - mature_report_generator.py: 报告生成器
- **Core**: 核心组件
  - mature_data_loader.py: 数据加载器
- **Models**: 数据模型定义
  - mature_models.py: Pydantic 数据模型
- **Utils**: 辅助工具 (helpers.py)