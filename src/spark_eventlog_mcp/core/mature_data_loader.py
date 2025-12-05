"""
成熟的数据加载器 - 从现有项目复制
"""

import asyncio
import json
import zipfile
import tempfile
import shutil
import logging
from pathlib import Path
from typing import List, Dict, Any
import aiofiles
import httpx

# 获取 logger
logger = logging.getLogger("spark-eventlog-mcp")

# boto3 导入处理
try:
    import boto3
    from botocore.exceptions import NoCredentialsError, ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    boto3 = None
    NoCredentialsError = Exception
    ClientError = Exception

from ..models.mature_models import EventLogData
from ..models.schemas import DataSource

class MatureDataLoader:
    """成熟的数据加载器"""

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.s3_client = None
        if BOTO3_AVAILABLE:
            self._init_s3_client()

    def _init_s3_client(self):
        """初始化 S3 客户端 - 智能选择凭证来源"""
        try:
            # 检查是否配置了环境变量中的 AWS 凭证
            aws_access_key = self.config.get("aws_access_key_id")
            aws_secret_key = self.config.get("aws_secret_access_key")
            aws_region = self.config.get("aws_region")

            if aws_access_key and aws_secret_key:
                # 使用环境变量配置的凭证
                logger.info(f"使用环境变量配置的 AWS 凭证，区域: {aws_region}")
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
            else:
                # 使用系统默认配置 (环境变量、~/.aws/credentials、IAM角色等)
                logger.info("使用系统默认 AWS 配置")
                self.s3_client = boto3.client('s3')

            # 测试连接
            self.s3_client.list_buckets()
            logger.info("✅ S3 客户端初始化成功")

        except (NoCredentialsError, ClientError) as e:
            logger.warning(f"⚠️  警告：无法初始化 S3 客户端，S3 功能将不可用。错误: {str(e)}")
            self.s3_client = None

    async def load_from_s3(self, s3_path: str) -> List[EventLogData]:
        """
        从 S3 路径加载 Event Log 数据

        Args:
            s3_path: S3 路径，如 s3://bucket/path/to/logs/

        Returns:
            List[EventLogData]: 解析后的事件日志数据列表
        """
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 未安装，无法使用S3功能")

        if not self.s3_client:
            raise RuntimeError("S3 客户端未初始化")

        # 解析 S3 路径
        s3_path = s3_path.replace("s3://", "")
        parts = s3_path.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        try:
            # 列出所有文件
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )

            if 'Contents' not in response:
                raise RuntimeError("未找到任何文件")

            # 按作业组织文件
            job_files = self._organize_s3_files(response['Contents'], bucket, prefix)

            # 下载并解析每个作业的文件
            event_logs = []
            for job_id, files in job_files.items():
                job_data = await self._download_s3_job_files(bucket, files)
                event_logs.append(EventLogData(job_id, job_data))

            return event_logs

        except ClientError as e:
            raise RuntimeError(f"S3 错误: {str(e)}")

    def _organize_s3_files(self, contents: List[Dict], bucket: str, prefix: str) -> Dict[str, Dict[str, List[str]]]:
        """组织 S3 文件按作业分组 - 灵活处理任意目录结构"""
        job_files = {}

        for obj in contents:
            key = obj['Key']
            if not key.endswith('/'):  # 跳过目录
                path_parts = key.split('/')
                filename = path_parts[-1]

                # 跳过非Spark日志文件
                if not (filename.startswith('events_') or filename.startswith('appstatus_') or
                       'events' in filename or 'appstatus' in filename):
                    continue

                job_id = None

                # 策略1: 查找 'jobs' 目录结构
                for i, part in enumerate(path_parts):
                    if part.lower() == 'jobs' and i + 1 < len(path_parts):
                        job_id = path_parts[i + 1]
                        break

                # 策略2: 如果没找到标准jobs目录，从文件名提取job_id
                if not job_id and '_' in filename:
                    # 文件名格式：events_0_job_id 或 appstatus_job_id
                    parts = filename.split('_')
                    if len(parts) >= 2:
                        job_id = parts[-1]  # 取最后一个部分作为job_id

                # 策略3: 如果还是没找到，尝试从路径的倒数第二或第三个部分提取
                if not job_id and len(path_parts) >= 3:
                    # 遍历路径的后几个部分，寻找可能的job_id
                    for i in range(-3, -1):  # 检查倒数第3个和第2个路径部分
                        if abs(i) <= len(path_parts):
                            potential_job_id = path_parts[i]
                            # 简单验证：job_id通常是字母数字组合，长度合理
                            if potential_job_id and len(potential_job_id) > 5 and potential_job_id.replace('-', '').replace('_', '').isalnum():
                                job_id = potential_job_id
                                break

                # 策略4: 最后的兜底方案，使用包含该文件的直接父目录作为job_id
                if not job_id and len(path_parts) >= 2:
                    job_id = path_parts[-2]

                if job_id:
                    if job_id not in job_files:
                        job_files[job_id] = {'events': [], 'appstatus': []}

                    # 根据文件名分类 - 更灵活的匹配
                    if any(keyword in filename.lower() for keyword in ['events', 'event']):
                        job_files[job_id]['events'].append(key)
                    elif any(keyword in filename.lower() for keyword in ['appstatus', 'status', 'app']):
                        job_files[job_id]['appstatus'].append(key)
                    else:
                        # 默认归类到events
                        job_files[job_id]['events'].append(key)

        return job_files

    async def _download_s3_job_files(self, bucket: str, files: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """下载单个作业的所有文件"""
        job_data = {'events': [], 'appstatus': []}

        # 下载事件文件
        for event_file in files['events']:
            try:
                response = self.s3_client.get_object(Bucket=bucket, Key=event_file)
                content = response['Body'].read().decode('utf-8')
                job_data['events'].append(content)
            except Exception as e:
                logger.error(f"下载事件文件失败 {event_file}: {e}")

        # 下载状态文件
        for status_file in files['appstatus']:
            try:
                response = self.s3_client.get_object(Bucket=bucket, Key=status_file)
                content = response['Body'].read().decode('utf-8')
                job_data['appstatus'].append(content)
            except Exception as e:
                logger.error(f"下载状态文件失败 {status_file}: {e}")

        return job_data

    async def load_from_url(self, url: str) -> List[EventLogData]:
        """
        从 HTTP URL 下载 ZIP 文件并解析

        Args:
            url: HTTP URL 指向 ZIP 文件

        Returns:
            List[EventLogData]: 解析后的事件日志数据列表
        """
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                response.raise_for_status()

                # 创建临时文件
                with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as temp_file:
                    temp_file.write(response.content)
                    temp_path = temp_file.name

                # 解析 ZIP 文件
                event_logs = await self._extract_and_parse_zip(temp_path)

                # 清理临时文件
                Path(temp_path).unlink()

                return event_logs

        except httpx.RequestError as e:
            raise RuntimeError(f"下载失败: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"处理失败: {str(e)}")

    async def load_from_upload(self, file_path: str) -> List[EventLogData]:
        """
        从文件路径解析（支持ZIP文件和直接文件）

        Args:
            file_path: 文件路径

        Returns:
            List[EventLogData]: 解析后的事件日志数据列表
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise RuntimeError(f"文件不存在: {file_path}")

        try:
            if file_path.suffix.lower() == '.zip':
                # 解析 ZIP 文件
                return await self._extract_and_parse_zip(str(file_path))
            else:
                # 直接处理单个文件
                async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                    if content.strip():
                        job_data = {'events': [content], 'appstatus': []}
                        return [EventLogData('single_job', job_data)]
                    else:
                        raise RuntimeError("文件内容为空")

        except Exception as e:
            raise RuntimeError(f"文件处理失败: {str(e)}")

    async def _extract_and_parse_zip(self, zip_path: str) -> List[EventLogData]:
        """解压并解析 ZIP 文件"""
        event_logs = []

        with tempfile.TemporaryDirectory() as temp_dir:
            # 解压 ZIP 文件
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # 扫描解压后的目录结构
            temp_path = Path(temp_dir)
            job_files = self._organize_extracted_files(temp_path)

            # 解析每个作业的文件
            for job_id, files in job_files.items():
                job_data = await self._read_local_job_files(files)
                event_logs.append(EventLogData(job_id, job_data))

        return event_logs

    def _organize_extracted_files(self, base_path: Path) -> Dict[str, Dict[str, List[Path]]]:
        """组织解压后的文件按作业分组 - 灵活处理任意目录结构"""
        job_files = {}

        # 递归扫描所有文件
        for file_path in base_path.rglob('*'):
            if file_path.is_file():
                parts = file_path.parts
                filename = file_path.name

                # 跳过非Spark日志文件
                if not (filename.startswith('events_') or filename.startswith('appstatus_') or
                       'events' in filename or 'appstatus' in filename or
                       not filename or len(filename) > 20):  # 包含可能的无扩展名事件文件
                    continue

                job_id = None

                # 策略1: 查找 'jobs' 目录结构
                for i, part in enumerate(parts):
                    if part.lower() == 'jobs' and i + 1 < len(parts):
                        job_id = parts[i + 1]
                        break

                # 策略2: 从文件名提取job_id
                if not job_id and '_' in filename:
                    # 文件名格式：events_0_job_id 或 appstatus_job_id
                    name_parts = filename.split('_')
                    if len(name_parts) >= 2:
                        job_id = name_parts[-1]

                # 策略3: 从路径的后几个部分查找可能的job_id
                if not job_id and len(parts) >= 3:
                    # 遍历路径的后几个部分，寻找可能的job_id
                    for i in range(-3, -1):  # 检查倒数第3个和第2个路径部分
                        if abs(i) <= len(parts):
                            potential_job_id = parts[i]
                            # 简单验证：job_id通常是字母数字组合，长度合理
                            if potential_job_id and len(potential_job_id) > 5 and potential_job_id.replace('-', '').replace('_', '').isalnum():
                                job_id = potential_job_id
                                break

                # 策略4: 使用文件的父目录作为job_id
                if not job_id and len(parts) >= 2:
                    job_id = parts[-2]

                # 策略5: 最后兜底，使用一个默认job_id
                if not job_id:
                    job_id = "default_job"

                if job_id:
                    if job_id not in job_files:
                        job_files[job_id] = {'events': [], 'appstatus': []}

                    # 根据文件名分类 - 更灵活的匹配
                    if any(keyword in filename.lower() for keyword in ['events', 'event']) or not filename:
                        # 处理没有扩展名的事件文件或明确的event文件
                        job_files[job_id]['events'].append(file_path)
                    elif any(keyword in filename.lower() for keyword in ['appstatus', 'status', 'app']):
                        job_files[job_id]['appstatus'].append(file_path)
                    else:
                        # 默认当作事件文件处理
                        job_files[job_id]['events'].append(file_path)

        return job_files

    async def _read_local_job_files(self, files: Dict[str, List[Path]]) -> Dict[str, List[str]]:
        """读取本地作业文件"""
        job_data = {'events': [], 'appstatus': []}

        # 读取事件文件
        for event_file in files['events']:
            try:
                async with aiofiles.open(event_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                    if content.strip():  # 只添加非空内容
                        job_data['events'].append(content)
            except Exception as e:
                logger.error(f"读取事件文件失败 {event_file}: {e}")

        # 读取状态文件
        for status_file in files['appstatus']:
            try:
                async with aiofiles.open(status_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                    if content.strip():  # 只添加非空内容
                        job_data['appstatus'].append(content)
            except Exception as e:
                logger.error(f"读取状态文件失败 {status_file}: {e}")

        return job_data

    async def validate_data_source(self, data_source: DataSource) -> Dict[str, Any]:
        """
        验证数据源的有效性

        Args:
            data_source: 数据源配置

        Returns:
            Dict: 验证结果
        """
        validation_result = {
            "is_valid": False,
            "error_message": "",
            "warnings": [],
            "info": {}
        }

        try:
            if data_source.source_type == "local":
                path = Path(data_source.path)
                if not path.exists():
                    validation_result["error_message"] = f"本地文件不存在: {data_source.path}"
                    return validation_result

                if not path.is_file():
                    validation_result["error_message"] = f"路径不是文件: {data_source.path}"
                    return validation_result

                # 检查文件大小
                file_size = path.stat().st_size
                validation_result["info"]["file_size"] = file_size
                validation_result["info"]["file_size_mb"] = round(file_size / 1024 / 1024, 2)

                # 检查文件扩展名
                if path.suffix.lower() not in ['.log', '.eventlog', '.json', '.txt', '.zip', '']:
                    validation_result["warnings"].append(f"文件扩展名 '{path.suffix}' 可能不是有效的事件日志格式")

            elif data_source.source_type == "s3":
                # S3路径格式验证
                if not data_source.path.startswith("s3://"):
                    validation_result["error_message"] = "S3路径必须以 's3://' 开头"
                    return validation_result

                # 基础路径解析
                parts = data_source.path[5:].split("/", 1)
                if len(parts) < 2:
                    validation_result["error_message"] = "S3路径格式不正确，需要包含bucket和key"
                    return validation_result

                validation_result["info"]["bucket"] = parts[0]
                validation_result["info"]["key"] = parts[1]

            elif data_source.source_type == "url":
                # URL格式验证
                if not data_source.path.startswith(("http://", "https://")):
                    validation_result["error_message"] = "URL必须以 'http://' 或 'https://' 开头"
                    return validation_result

            validation_result["is_valid"] = True
            validation_result["info"]["source_type"] = data_source.source_type
            validation_result["info"]["path"] = data_source.path

        except Exception as e:
            validation_result["error_message"] = f"验证数据源时发生错误: {str(e)}"

        return validation_result