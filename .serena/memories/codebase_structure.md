# 代码库结构详解

## 目录结构
```
spark-eventlog-mcp/
├── .env                    # 环境变量配置
├── .env.example           # 环境变量配置模板
├── .gitignore             # Git 忽略文件
├── README.md              # 项目说明文档
├── requirements.txt       # Python 依赖列表
├── uv.lock               # UV 锁定文件
├── pyproject.toml        # 项目配置和依赖
├── start.py              # 启动脚本
└── src/spark_eventlog_mcp/
    ├── __init__.py       # 包初始化
    ├── server.py         # MCP 服务器主文件
    ├── core/             # 核心组件
    │   ├── __init__.py
    │   └── mature_data_loader.py  # 数据加载器
    ├── models/           # 数据模型
    │   ├── __init__.py
    │   ├── mature_models.py      # 主要数据模型
    │   └── schemas.py           # 模式定义
    ├── tools/            # 分析工具
    │   ├── __init__.py
    │   ├── mature_analyzer.py        # 核心分析引擎
    │   └── mature_report_generator.py # 报告生成器
    └── utils/            # 辅助工具
        ├── __init__.py
        └── helpers.py    # 辅助函数
```

## 核心组件功能

### server.py
- MCP 服务器主控制器
- 定义所有 MCP 工具接口
- 管理全局状态和配置
- 提供以下工具：
  - `parse_eventlog`: 解析事件日志
  - `analyze_performance`: 性能分析
  - `generate_report`: 生成报告
  - `get_optimization_suggestions`: 获取优化建议
  - `get_analysis_status`: 获取分析状态
  - `clear_session`: 清除会话

### core/mature_data_loader.py
- 数据源验证和加载
- 支持本地文件、S3、HTTP URL
- 异步文件操作
- 数据源类型检测

### tools/mature_analyzer.py
- Spark 事件日志核心分析引擎
- 解析 JSON 格式的事件日志
- 分析作业、阶段、任务、执行器性能
- 生成优化建议
- 主要类：`MatureSparkEventLogAnalyzer`

### tools/mature_report_generator.py
- HTML 报告生成器
- 使用 Jinja2 模板引擎
- 集成 Plotly 可视化图表
- 支持多种报告格式

### models/mature_models.py
- Pydantic 数据模型定义
- 类型安全和数据验证
- 包含所有分析结果的数据结构

### utils/helpers.py
- 通用辅助函数
- 文件处理、数据转换等工具函数