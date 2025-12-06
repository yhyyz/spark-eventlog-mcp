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
        """
        组织 S3 文件按 application 分组 - 支持两种场景：
        1. 直接文件：给定目录下直接是事件日志文件，同前缀的是同一个application
        2. 子目录：给定目录下有子目录，每个子目录包含事件日志文件
        """
        job_files = {}

        # 分析文件结构，确定是直接文件场景还是子目录场景
        files_in_root = []  # 根目录下的文件
        subdirs = set()     # 子目录

        for obj in contents:
            key = obj['Key']
            if key.endswith('/'):  # 跳过目录本身
                continue

            # 移除 prefix 前缀以获取相对路径
            relative_key = key[len(prefix):] if key.startswith(prefix) else key
            if relative_key.startswith('/'):
                relative_key = relative_key[1:]

            path_parts = relative_key.split('/')
            filename = path_parts[-1]

            # 跳过 appstatus 开头的文件
            if filename.startswith('appstatus'):
                continue

            # 跳过明显不是事件日志的文件（如 .txt, .log 等小文件，但保留无扩展名文件）
            if '.' in filename and filename.split('.')[-1].lower() in ['txt', 'log', 'md', 'json']:
                # 进一步检查文件大小，小文件通常不是事件日志
                if obj.get('Size', 0) < 1024:  # 小于1KB的文件跳过
                    continue

            if len(path_parts) == 1:
                # 文件直接在根目录下
                files_in_root.append((key, filename))
            else:
                # 文件在子目录中
                subdirs.add(path_parts[0])

        # 场景判断：如果根目录下有文件，且子目录不多，优先按直接文件处理
        if files_in_root and (len(subdirs) <= 1 or len(files_in_root) > len(subdirs)):
            # 直接文件场景：按文件名前缀分组
            logger.info(f"检测到直接文件场景：根目录下有 {len(files_in_root)} 个文件，{len(subdirs)} 个子目录")
            self._organize_files_by_prefix(files_in_root, job_files)
        else:
            # 子目录场景：按子目录分组
            logger.info(f"检测到子目录场景：根目录下有 {len(files_in_root)} 个文件，{len(subdirs)} 个子目录")
            for obj in contents:
                key = obj['Key']
                if key.endswith('/'):
                    continue

                relative_key = key[len(prefix):] if key.startswith(prefix) else key
                if relative_key.startswith('/'):
                    relative_key = relative_key[1:]

                path_parts = relative_key.split('/')
                filename = path_parts[-1]

                # 跳过 appstatus 开头的文件
                if filename.startswith('appstatus'):
                    continue

                # 跳过小文件
                if '.' in filename and filename.split('.')[-1].lower() in ['txt', 'log', 'md', 'json']:
                    if obj.get('Size', 0) < 1024:
                        continue

                if len(path_parts) > 1:
                    # 使用第一级子目录作为 application_id
                    application_id = path_parts[0]

                    if application_id not in job_files:
                        job_files[application_id] = {'events': []}

                    job_files[application_id]['events'].append(key)

        return job_files

    def _organize_files_by_prefix(self, files_list: List[tuple], job_files: Dict[str, Dict[str, List[str]]]):
        """根据文件名前缀组织文件（直接文件场景）"""
        prefix_groups = {}

        for key, filename in files_list:
            # 提取文件名前缀（到第一个下划线或点之前，或者取文件名的前半部分）
            prefix = self._extract_file_prefix(filename)

            if prefix not in prefix_groups:
                prefix_groups[prefix] = []
            prefix_groups[prefix].append(key)

        # 将每个前缀组作为一个 application
        for prefix, file_keys in prefix_groups.items():
            if prefix not in job_files:
                job_files[prefix] = {'events': []}
            job_files[prefix]['events'].extend(file_keys)
            logger.info(f"应用 '{prefix}' 包含 {len(file_keys)} 个事件文件")

    def _extract_file_prefix(self, filename: str) -> str:
        """提取文件名前缀来识别同一个 application 的文件"""
        # 移除文件扩展名
        name_without_ext = filename.split('.')[0] if '.' in filename else filename

        # 策略1: 如果文件名包含下划线，取第一部分作为前缀
        if '_' in name_without_ext:
            parts = name_without_ext.split('_')
            # 对于 application_xxx_1, application_xxx_2 这种格式
            if len(parts) >= 3:
                return '_'.join(parts[:-1])  # 去掉最后一个数字部分
            else:
                return parts[0]

        # 策略2: 如果文件名包含连字符，取第一部分
        elif '-' in name_without_ext:
            parts = name_without_ext.split('-')
            if len(parts) >= 2:
                return parts[0]

        # 策略3: 对于纯字母数字文件名，取前面的字母部分
        else:
            # 尝试找到最后一个非数字字符的位置
            for i in range(len(name_without_ext) - 1, -1, -1):
                if not name_without_ext[i].isdigit():
                    return name_without_ext[:i+1]

        # 兜底：返回完整文件名
        return name_without_ext

    async def _download_s3_job_files(self, bucket: str, files: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """下载单个 application 的所有事件文件"""
        job_data = {'events': []}

        # 下载所有事件文件
        for event_file in files['events']:
            try:
                response = self.s3_client.get_object(Bucket=bucket, Key=event_file)
                content = response['Body'].read().decode('utf-8')
                if content.strip():  # 只添加非空内容
                    job_data['events'].append(content)
            except Exception as e:
                logger.error(f"下载事件文件失败 {event_file}: {e}")

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
        从文件/目录路径解析（支持ZIP文件、直接文件和目录）

        Args:
            file_path: 文件或目录路径

        Returns:
            List[EventLogData]: 解析后的事件日志数据列表
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise RuntimeError(f"路径不存在: {file_path}")

        try:
            if file_path.is_dir():
                # 处理目录
                return await self._load_from_directory(file_path)
            elif file_path.suffix.lower() == '.zip':
                # 解析 ZIP 文件
                return await self._extract_and_parse_zip(str(file_path))
            else:
                # 直接处理单个文件
                async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                    if content.strip():
                        job_data = {'events': [content]}
                        return [EventLogData('single_job', job_data)]
                    else:
                        raise RuntimeError("文件内容为空")

        except Exception as e:
            raise RuntimeError(f"文件处理失败: {str(e)}")

    async def _load_from_directory(self, dir_path: Path) -> List[EventLogData]:
        """
        从目录加载事件日志文件 - 支持两种场景：
        1. 直接文件：目录下直接是事件日志文件，同前缀的是同一个application
        2. 子目录：目录下有子目录，每个子目录包含事件日志文件
        """
        # 使用与解压文件相同的组织逻辑
        job_files = self._organize_extracted_files(dir_path)

        if not job_files:
            raise RuntimeError("目录中没有找到有效的事件日志文件")

        # 解析每个 application 的文件
        event_logs = []
        for application_id, files in job_files.items():
            job_data = await self._read_local_job_files(files)
            if job_data['events']:  # 只有当有事件数据时才添加
                event_logs.append(EventLogData(application_id, job_data))

        return event_logs

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
        """
        组织解压后的文件按 application 分组 - 支持两种场景：
        1. 直接文件：解压目录下直接是事件日志文件，同前缀的是同一个application
        2. 子目录：解压目录下有子目录，每个子目录包含事件日志文件
        """
        job_files = {}

        # 收集所有文件，分析目录结构
        files_in_root = []  # 根目录下的文件
        subdirs = set()     # 子目录

        for file_path in base_path.rglob('*'):
            if not file_path.is_file():
                continue

            filename = file_path.name

            # 跳过 appstatus 开头的文件
            if filename.startswith('appstatus'):
                continue

            # 跳过明显不是事件日志的文件
            if '.' in filename and filename.split('.')[-1].lower() in ['txt', 'log', 'md', 'json']:
                # 检查文件大小，小文件通常不是事件日志
                try:
                    if file_path.stat().st_size < 1024:  # 小于1KB的文件跳过
                        continue
                except:
                    continue

            # 计算相对于基础路径的相对路径
            try:
                relative_path = file_path.relative_to(base_path)
                path_parts = relative_path.parts
            except ValueError:
                continue

            if len(path_parts) == 1:
                # 文件直接在根目录下
                files_in_root.append((file_path, filename))
            else:
                # 文件在子目录中
                subdirs.add(path_parts[0])

        # 场景判断：如果根目录下有文件，且子目录不多，优先按直接文件处理
        if files_in_root and (len(subdirs) <= 1 or len(files_in_root) > len(subdirs)):
            # 直接文件场景：按文件名前缀分组
            logger.info(f"本地文件检测到直接文件场景：根目录下有 {len(files_in_root)} 个文件，{len(subdirs)} 个子目录")
            self._organize_local_files_by_prefix(files_in_root, job_files)
        else:
            # 子目录场景：按子目录分组
            logger.info(f"本地文件检测到子目录场景：根目录下有 {len(files_in_root)} 个文件，{len(subdirs)} 个子目录")
            for file_path in base_path.rglob('*'):
                if not file_path.is_file():
                    continue

                filename = file_path.name

                # 跳过 appstatus 开头的文件
                if filename.startswith('appstatus'):
                    continue

                # 跳过小文件
                if '.' in filename and filename.split('.')[-1].lower() in ['txt', 'log', 'md', 'json']:
                    try:
                        if file_path.stat().st_size < 1024:
                            continue
                    except:
                        continue

                try:
                    relative_path = file_path.relative_to(base_path)
                    path_parts = relative_path.parts
                except ValueError:
                    continue

                if len(path_parts) > 1:
                    # 使用第一级子目录作为 application_id
                    application_id = path_parts[0]

                    if application_id not in job_files:
                        job_files[application_id] = {'events': []}

                    job_files[application_id]['events'].append(file_path)

        return job_files

    def _organize_local_files_by_prefix(self, files_list: List[tuple], job_files: Dict[str, Dict[str, List[Path]]]):
        """根据文件名前缀组织本地文件（直接文件场景）"""
        prefix_groups = {}

        for file_path, filename in files_list:
            # 提取文件名前缀
            prefix = self._extract_file_prefix(filename)

            if prefix not in prefix_groups:
                prefix_groups[prefix] = []
            prefix_groups[prefix].append(file_path)

        # 将每个前缀组作为一个 application
        for prefix, file_paths in prefix_groups.items():
            if prefix not in job_files:
                job_files[prefix] = {'events': []}
            job_files[prefix]['events'].extend(file_paths)
            logger.info(f"本地应用 '{prefix}' 包含 {len(file_paths)} 个事件文件")

    async def _read_local_job_files(self, files: Dict[str, List[Path]]) -> Dict[str, List[str]]:
        """读取本地 application 的所有事件文件"""
        job_data = {'events': []}

        # 读取所有事件文件
        for event_file in files['events']:
            try:
                async with aiofiles.open(event_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                    if content.strip():  # 只添加非空内容
                        job_data['events'].append(content)
            except Exception as e:
                logger.error(f"读取事件文件失败 {event_file}: {e}")

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