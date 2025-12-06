# Spark EventLog MCP Server

[中文版本](README_zh.md) | English

A comprehensive Spark event log analysis MCP server built on FastMCP 2.0 and FastAPI, providing in-depth performance analysis, resource monitoring, and optimization recommendations.

## Features

- 🌐 **FastMCP & FastAPI Integration**: MCP protocol support and analysis report APIs powered by FastAPI & FastMCP
- 📊 **Performance Analysis**: Shuffle analysis, resource utilization monitoring, task execution analysis
- 📈 **Visual Reports**: Auto-generated interactive HTML reports with direct browser access
- ☁️ **Multiple Data Sources**: Support for S3, HTTP URLs, and local files
- 💡 **Intelligent Optimization**: Automated optimization recommendations based on analysis results

## Quick Start

### MCP Client Integration

#### uvx Mode (Recommended - Direct from GitHub)

```json
{
  "mcpServers": {
    "spark-eventlog-mcp": {
      "type": "stdio",
      "command": "uvx",
      "args": ["--from", "https://github.com/yhyyz/spark-eventlog-mcp", "spark-eventlog-mcp"],
      "env": {
        "MCP_TRANSPORT": "stdio"
      }
    }
  }
}
```

#### stdio Mode (Local Development)

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

#### HTTP Mode

**1. Start HTTP Server:**

```bash
export MCP_TRANSPORT=streamable-http
export MCP_HOST=localhost
export MCP_PORT=7799

uv run python start.py
```

**2. Configure Remote MCP:**

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

**3. Access Services:**

- API Documentation: http://localhost:7799/docs
- Health Check: http://localhost:7799/health
- Reports List: http://localhost:7799/api/reports
- MCP Endpoint: http://localhost:7799/mcp

## Analysis Examples

![emr-serverless-small-job](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/Screenshot%202025-12-06%20at%2015-12-46%20Spark%20Event%20Log%20Analysis%20Report.png)

![emr-eks-big-job](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/Screenshot%202025-12-06%20at%2015-10-42%20Spark%20Event%20Log%20Analysis%20Report.png)

![emr-eks-big-job-sub-01](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202512061601158.png)

![emr-eks-big-job-sub-02](https://pcmyp.oss-cn-beijing.aliyuncs.com/markdown/202512061601391.png)

## Project Structure

```
spark-eventlog-mcp/
├── src/spark_eventlog_mcp/
│   ├── server.py              # FastAPI + MCP integrated server
│   ├── core/
│   │   └── mature_data_loader.py    # Data loader (S3/URL/Local)
│   ├── tools/
│   │   ├── mature_analyzer.py       # Event log analyzer
│   │   └── mature_report_generator.py  # HTML report generator
│   ├── models/
│   │   ├── schemas.py        # Pydantic data models
│   │   └── mature_models.py  # Analysis result models
│   └── utils/
│       └── helpers.py         # Utility functions and logging config
├── report_data/               # Generated reports storage
├── start.py                   # Launch script
├── README.md                 # This file (English)
└── README_zh.md              # Chinese version
```

## MCP Tools

| Tool Name | Description |
|-----------|-------------|
| `parse_eventlog` | Parse event logs (S3/URL/Local) |
| `analyze_performance` | Execute performance analysis |
| `generate_report` | Generate visual reports |
| `get_optimization_suggestions` | Get optimization recommendations |
| `get_analysis_status` | Query current analysis status |
| `clear_session` | Clear session cache |

## RESTful API Endpoints

### Basic Endpoints

- `GET /` - Service information
- `GET /health` - Health check
- `GET /docs` - API documentation (Swagger UI)

### Report Management

- `GET /api/reports` - List all reports
- `GET /api/reports/{filename}` - View HTML report
- `GET /reports/{filename}` - Direct access to report files
- `DELETE /api/reports/{filename}` - Delete report

### MCP Tool Calls

- `POST /mcp` - MCP protocol endpoint

## Configuration

### Environment Variables

```bash
# Server Configuration
MCP_TRANSPORT=http          # stdio or streamable-http
MCP_HOST=0.0.0.0           # HTTP mode listen address
MCP_PORT=7799              # HTTP mode port
LOG_LEVEL=INFO             # Log level

# AWS S3 Configuration (Optional)
# Not needed if AWS CLI is configured or running on EC2 with appropriate IAM role
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
AWS_DEFAULT_REGION=us-east-1

# Cache Configuration
CACHE_ENABLED=true
CACHE_TTL=300

# Default Data Source
DEFAULT_SOURCE_TYPE=s3  # s3, url, or local
```

### Log Format

Logs contain detailed debugging information:

```
2025-12-05 10:30:45 - INFO     - [server.py:243:generate_report] - spark-eventlog-mcp - Generating html report
```

Format: `Timestamp - Level - [Filename:Line:Function] - Logger Name - Message`

## Data Source Support

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

### Local File

```python
{
    "source_type": "local",
    "path": "/path/to/local/eventlog.zip"
}
```

## Report Features

Generated HTML reports include:

- 📊 Application Overview (task counts, success rate, duration)
- 💻 Executor Resource Usage Distribution
- 🔄 Shuffle Performance Analysis
- ⚖️ Data Skew Detection
- 💡 Intelligent Optimization Recommendations
- 📈 Interactive Visualizations

## Troubleshooting

### Port Already in Use

```bash
# Change port
MCP_PORT=9090 python start.py
```

### Missing Dependencies

```bash
# Reinstall dependencies
uv pip install -e .
```

### AWS Credentials Issues

```bash
# Check AWS configuration
aws configure list

# Or configure in .env
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
```

### Debug Logging

```bash
# Enable DEBUG logs
LOG_LEVEL=DEBUG uv run python start.py
```

## Tech Stack

- **FastMCP 2.0**: MCP protocol support
- **FastAPI**: RESTful API framework
- **Pydantic**: Data validation and serialization
- **Plotly**: Interactive charts
- **boto3**: AWS S3 integration
- **aiofiles**: Async file operations

## Development

```bash
# Clone repository
git clone <repository-url>
cd spark-eventlog-mcp

# Install development dependencies
uv pip install -e .

# MCP Inspector - stdio mode
MCP_TRANSPORT="stdio" npx @modelcontextprotocol/inspector uv run python start.py

# MCP Inspector - HTTP mode
MCP_TRANSPORT="streamable-http" uv run python start.py
npx @modelcontextprotocol/inspector --cli http://localhost:7799 --transport http --method tools/list
```

## Support

- Documentation: Check `/docs` API documentation
- Issues: Submit GitHub Issues
- Reference: [FastMCP Documentation](https://gofastmcp.com/)