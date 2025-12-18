# Spark EventLog MCP Server

基于 FastMCP 2.0 和 FastAPI 的 Spark 事件日志分析 MCP 服务器,提供全面的性能分析、资源监控和优化建议。

## 特性

- 🌐 **FastMCP & FastAPI 集成**: FastMCP 2.0协议支持和FastAPI分析报告API
- 📊 **性能分析**: Shuffle 分析、资源利用率监控、任务执行分析
- 📈 **可视化报告**: 自动生成交互式 HTML 报告,支持浏览器直接访问
- ☁️ **云数据源**: 支持 S3 桶和 HTTP URL,自动路径检测
- 💡 **智能优化**: 基于分析结果的自动优化建议
- 🔧 **模块化架构**: 清晰的关注点分离,工具、中间件和配置专用模块
- 📝 **增强日志**: 全面的请求/响应日志记录,包含详细调试信息

## 快速开始

### MCP 客户端集成

#### uvx 模式 (推荐 - 直接从 GitHub 运行)

```json
{
  "mcpServers": {
    "spark-eventlog": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/yhyyz/spark-eventlog-mcp",
        "spark-eventlog-mcp"
      ],
      "env": {
        "MCP_TRANSPORT": "stdio"
      }
    }
  }
}
```

#### stdio 模式 (本地开发)

```json
{
  "mcpServers": {
    "spark-eventlog": {
      "command": "uv run python",
      "args": ["/path/to/spark-eventlog-mcp/start.py"],
      "env": {
        "MCP_TRANSPORT": "stdio"
      }
    }
  }
}
```

#### HTTP 模式

**1. 启动 HTTP 服务器:**

```bash
export MCP_TRANSPORT=streamable-http
export MCP_HOST=localhost
export MCP_PORT=7799

uv run python start.py
```

**2. 配置 Remote MCP:**

```json
{
  "mcpServers": {
    "spark-eventlog": {
      "url": "http://localhost:7799/mcp",
      "type": "http"
    }
  }
}
```

**3. 服务启动后可访问:**

- API 文档: http://localhost:7799/docs
- 健康检查: http://localhost:7799/health
- 报告列表: http://localhost:7799/api/reports
- MCP 端点: http://localhost:7799/mcp

## 分析样例

![emr-serverless-small-job](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/Screenshot%202025-12-06%20at%2015-12-46%20Spark%20Event%20Log%20Analysis%20Report.png)

![emr-eks-big-job-1](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202512121551514.png)

![emr-eks-big-job-2](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/Screenshot%202025-12-12%20at%2015-53-47%20Spark%20Event%20Log%20Analysis%20Report.png)

![emr-eks-big-job-sub-01](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202512121521204.png)

![emr-eks-big-job-sub-02](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202512061601391.png)

## 项目结构

```
spark-eventlog-mcp/
├── src/spark_eventlog_mcp/
│   ├── server.py              # 主要 FastAPI + MCP 集成服务器 (重构后)
│   ├── core/
│   │   └── mature_data_loader.py    # 数据加载器 (S3/URL)
│   ├── tools/
│   │   ├── mcp_tools.py      # MCP 工具实现 (新增)
│   │   ├── mature_analyzer.py       # 事件日志分析器
│   │   └── mature_report_generator.py  # HTML 报告生成器
│   ├── models/
│   │   ├── schemas.py        # Pydantic 数据模型
│   │   └── mature_models.py  # 分析结果模型
│   └── utils/
│       ├── helpers.py         # 工具函数和日志配置
│       ├── middleware.py      # FastAPI 请求日志中间件 (新增)
│       └── uvicorn_config.py  # Uvicorn 日志配置 (新增)
├── report_data/               # 生成的报告存储目录
├── start.py                   # 启动脚本
├── README.md                 # 英文文档
└── README_zh.md              # 中文文档
```

## MCP 工具

| 工具名称 | 功能描述 | 参数 |
|---------|---------|-----|
| `generate_report` | **端到端报告生成** - 自动检测S3/URL,分析数据,生成HTML报告 | `path: str` (S3 或 HTTP URL) |
| `get_analysis_status` | 查询当前分析会话状态和指标 | 无 |
| `clear_session` | 清除会话缓存并重置服务器状态 | 无 |

### 简化的工具使用方式

重构后的 MCP 工具专注于简单性和自动化:

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "generate_report",
    "arguments": {
      "path": "s3://my-bucket/spark-logs/"
    }
  },
  "id": 1
}
```

## RESTful API 端点

### 基础端点

- `GET /` - 服务信息
- `GET /health` - 健康检查
- `GET /docs` - API 文档 (Swagger UI)

### 报告管理

- `GET /api/reports` - 列出所有报告
- `GET /api/reports/{filename}` - 查看 HTML 报告
- `GET /reports/{filename}` - 直接访问报告文件
- `DELETE /api/reports/{filename}` - 删除报告

### MCP 工具调用

- `POST /mcp` - MCP 协议端点

## 配置说明

### 环境变量

```bash
# 服务器配置
MCP_TRANSPORT=http          # stdio 或 streamable-http
MCP_HOST=0.0.0.0           # HTTP 模式监听地址
MCP_PORT=7799              # HTTP 模式端口
LOG_LEVEL=INFO             # 日志级别

# AWS S3 配置 (可选)，如果机器已经配置好aws cli 或者在ec2上已经有role且有s3权限，就不需要配置
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_DEFAULT_REGION=us-east-1

# 缓存配置
CACHE_ENABLED=true
CACHE_TTL=300

# 默认数据源
DEFAULT_SOURCE_TYPE=s3  # s3, url, 或 local
```

### 日志格式

日志包含详细的调试信息:

```
2025-12-05 10:30:45 - INFO     - [server.py:243:generate_report] - spark-eventlog-mcp - Generating html report
```

格式: `时间戳 - 级别 - [文件名:行号:函数名] - Logger名 - 消息`

## 数据源支持

### S3

```python
{
    "source_type": "s3",
    "path": "s3://bucket-name/path/to/eventlogs/"
}
```

### HTTP URL

```python
{
    "source_type": "url",
    "path": "https://example.com/eventlog.zip"
}
```

### 本地文件

```python
{
    "source_type": "local",
    "path": "/path/to/local/eventlog.zip"
}
```

## 报告示例

生成的 HTML 报告包含:

- 📊 应用概览 (任务数、成功率、持续时间)
- 💻 Executor 资源使用分布
- 🔄 Shuffle 性能分析
- ⚖️ 数据倾斜检测
- 💡 智能优化建议
- 📈 交互式可视化图表

## 故障排查

### 端口被占用

```bash
# 更改端口
MCP_PORT=9090 python start.py
```

### 依赖包未安装

```bash
# 重新安装依赖
uv pip install -e .
```

### AWS 凭证问题

```bash
# 检查 AWS 配置
aws configure list

# 或在 .env 中配置
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
```

### 日志调试

```bash
# 启用 DEBUG 日志
LOG_LEVEL=DEBUG uv run python start.py
```

## 最近改进 (2025-12-18)

### 主要代码重构

- **🎯 简化MCP工具**: `generate_report` 现在只需要一个字符串参数 (S3 或 URL 路径)
- **📦 模块化架构**: 从主服务器提取 MCP 工具实现到专用模块
- **📝 增强日志**: 添加全面的请求/响应日志记录，包含客户端信息、请求头和请求体
- **🔧 集中配置**: 将 uvicorn 和中间件配置移到独立的工具模块
- **📉 降低复杂度**: 主 server.py 从约1150行减少到370行 (减少70%)

### 架构变更

- **新模块**: `tools/mcp_tools.py` - 包含所有 MCP 工具实现
- **新模块**: `utils/middleware.py` - FastAPI 请求日志中间件
- **新模块**: `utils/uvicorn_config.py` - 集中的 uvicorn 日志配置
- **自动检测**: 在 `generate_report` 工具中自动路径类型检测 (S3 vs URL)
- **简化接口**: 单参数 MCP 工具，内部逻辑处理复杂性

### HTTP 传输修复

- **MCP 协议兼容性**: 通过确保正确的 Accept 请求头修复 HTTP 406 错误
- **请求跟踪**: 添加详细的请求/响应日志以便更好地调试
- **错误处理**: 改进错误信息和状态码处理

## 技术栈

- **FastMCP 2.0**: MCP 协议支持
- **FastAPI**: RESTful API 框架
- **Pydantic**: 数据验证和序列化
- **Plotly**: 交互式图表
- **boto3**: AWS S3 集成
- **aiofiles**: 异步文件操作

## 开发

```bash
# 克隆项目
git clone <repository-url>
cd spark-eventlog-mcp

# 安装开发依赖
uv pip install -e .

# MCP Inspector - stdio 模式
MCP_TRANSPORT="stdio" npx @modelcontextprotocol/inspector uv run python start.py

# MCP Inspector - HTTP 模式
MCP_TRANSPORT="streamable-http" uv run python start.py
npx @modelcontextprotocol/inspector --cli http://localhost:7799 --transport http --method tools/list
```


## 支持

- 文档: 查看 `/docs` API 文档
- 问题: 提交 GitHub Issue
- 参考: [FastMCP 文档](https://gofastmcp.com/)
