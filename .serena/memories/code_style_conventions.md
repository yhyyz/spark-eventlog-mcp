# 代码风格和约定

## 命名约定
- **类名**: PascalCase (如: MatureSparkEventLogAnalyzer, DataSource)
- **函数名**: snake_case (如: parse_eventlog, analyze_performance)
- **变量名**: snake_case (如: _current_analysis, data_source)
- **常量名**: UPPER_CASE (如: BOTO3_AVAILABLE)
- **私有方法**: 以单下划线开头 (如: _parse_event_log, _analyze_jobs_correctly)

## 文档字符串风格
- 使用标准的 Python docstring 格式
- 多语言支持：中文注释与英文文档字符串并存
- 函数文档包含 Args 和 Returns 说明
- 类文档描述类的用途和主要功能

## 类型提示
- 严格使用 Python 类型提示
- 使用 Pydantic 进行数据验证
- 导入类型：`from typing import Dict, Any, List, Optional`

## 代码组织
- 模块化设计：每个功能独立成模块
- 清晰的文件夹结构：tools/, core/, models/, utils/
- 依赖注入：全局变量用于组件共享

## 异步编程
- 大量使用 async/await 模式
- 所有 MCP 工具函数都是异步的
- 文件I/O操作使用 aiofiles

## 错误处理
- 统一的错误响应格式
- 详细的日志记录
- 优雅的异常处理和回退机制