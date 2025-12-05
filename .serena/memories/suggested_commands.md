# 推荐的开发命令

## 环境管理
```bash
# 使用 uv 管理依赖 (推荐)
uv sync                 # 安装/同步依赖
uv add package_name     # 添加新依赖
uv remove package_name  # 移除依赖

# 传统方式
pip install -r requirements.txt
```

## 启动服务器
```bash
# 主要启动方式
python start.py

# 直接启动 (开发调试)
python -m spark_eventlog_mcp.server

# 使用 script 入口点
spark-eventlog-mcp
```

## 环境配置
```bash
# 复制环境配置模板
cp .env.example .env

# 编辑环境变量
nano .env  # 或使用你喜欢的编辑器
```

## 开发工具
```bash
# 代码格式化
python -m black src/
python -m isort src/

# 类型检查
python -m mypy src/

# 代码质量检查
python -m flake8 src/
```

## 测试相关
```bash
# 运行测试
python -m pytest

# 测试覆盖率
python -m pytest --cov=src/

# 特定测试
python -m pytest tests/test_analyzer.py
```

## Git 操作
```bash
# 常用 git 命令
git status
git add .
git commit -m "feat: 添加新功能"
git push origin main

# 分支操作
git checkout -b feature/new-feature
git merge main
```

## 系统工具 (Darwin/macOS)
```bash
# 文件操作
ls -la          # 详细列表
find . -name "*.py"  # 查找文件
grep -r "pattern" src/  # 搜索内容

# 进程管理
ps aux | grep python  # 查看 Python 进程
kill -9 PID          # 强制结束进程

# 网络
lsof -i :8080       # 查看端口占用
curl http://localhost:8080/health  # 测试服务器
```