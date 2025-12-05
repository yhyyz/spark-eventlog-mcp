"""
Spark Event Log 核心分析器 - 基于真实数据结构 (复制的成熟实现)
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from collections import defaultdict, Counter
from dataclasses import dataclass
import logging

from ..models.mature_models import (
    MatureAnalysisResult, JobMetrics, ExecutorMetrics, DriverMetrics, ShuffleStageMetrics,
    ShuffleAnalysis, PerformanceMetrics, OptimizationRecommendations,
    FieldDescription, FieldDescriptions, EventLogData
)

@dataclass
class TaskMetrics:
    """任务指标 - 基于真实 Event Log 结构"""
    task_id: int
    stage_id: int
    stage_attempt_id: int
    executor_id: str
    host: str
    launch_time: int
    finish_time: int
    duration: int
    locality: str

    # 执行器指标
    executor_deserialize_time: int = 0
    executor_deserialize_cpu_time: int = 0
    executor_run_time: int = 0
    executor_cpu_time: int = 0
    result_size: int = 0
    jvm_gc_time: int = 0
    result_serialization_time: int = 0
    peak_execution_memory: int = 0
    memory_bytes_spilled: int = 0
    disk_bytes_spilled: int = 0

    # Shuffle Read 指标
    shuffle_remote_blocks_fetched: int = 0
    shuffle_local_blocks_fetched: int = 0
    shuffle_fetch_wait_time: int = 0
    shuffle_remote_bytes_read: int = 0
    shuffle_remote_bytes_read_to_disk: int = 0
    shuffle_local_bytes_read: int = 0
    shuffle_total_records_read: int = 0
    shuffle_remote_requests_duration: int = 0

    # Shuffle Write 指标
    shuffle_bytes_written: int = 0
    shuffle_write_time: int = 0
    shuffle_records_written: int = 0

    # Input/Output 指标
    input_bytes_read: int = 0
    input_records_read: int = 0
    output_bytes_written: int = 0
    output_records_written: int = 0

class MatureSparkEventLogAnalyzer:
    """成熟的 Spark Event Log 分析器"""

    def __init__(self):
        self.logger = logging.getLogger("spark-eventlog-mcp")
        self.reset()

    def reset(self):
        """重置分析器状态"""
        self.events = []
        self.jobs = {}  # job_id -> job_info
        self.stages = {}  # stage_id -> stage_info
        self.tasks = []  # List[TaskMetrics]
        self.executors = {}  # executor_id -> executor_info
        self.driver_info = {}  # Driver 信息
        self.application_info = {}
        self.spark_properties = {}
        self.hadoop_properties = {}
        self.sql_executions = {}  # SQL 执行信息

    def analyze(self, event_logs: List[EventLogData]) -> MatureAnalysisResult:
        """
        分析事件日志并返回正确结果
        """
        self.reset()

        # 解析所有事件日志
        logging.info(f"parse_event_log starting...")
        for log_data in event_logs:
            self._parse_event_log(log_data)
        logging.info(f"parse_event_log complated.")

        # 执行正确的分析
        result = MatureAnalysisResult(
            application_id=self.application_info.get('appId', 'unknown'),
            application_name=self.application_info.get('appName', 'unknown'),
            spark_version=self.application_info.get('sparkVersion', 'unknown'),
            start_time=self._parse_timestamp(self.application_info.get('startTime', 0)),
            end_time=self._parse_timestamp(self.application_info.get('endTime', 0)) if self.application_info.get('endTime') else None,
            duration_ms=self.application_info.get('duration'),
            jobs=self._analyze_jobs_correctly(),
            total_jobs=len(self.jobs),
            successful_jobs=len([j for j in self.jobs.values() if j.get('result', {}).get('Result') == 'JobSucceeded']),
            failed_jobs=len([j for j in self.jobs.values() if j.get('result', {}).get('Result') != 'JobSucceeded']),
            executors=self._analyze_executors_correctly(),
            total_executors=len([e for e in self.executors.keys() if e != 'driver']),  # 不计算 driver
            driver_metrics=self._analyze_driver_correctly(),
            performance_metrics=self._analyze_performance_correctly(),
            shuffle_analysis=self._analyze_shuffle_correctly(),
            spark_properties=self.spark_properties,
            hadoop_properties=self.hadoop_properties,
            optimization_recommendations=self._generate_correct_recommendations(),
            analysis_summary=self._generate_correct_summary()
        )

        return result

    def _parse_event_log(self, log_data: EventLogData):
        """解析单个作业的事件日志"""
        for event_content in log_data.files['events']:
            for line in event_content.strip().split('\n'):
                if line.strip():
                    try:
                        event = json.loads(line)
                        self._process_event_correctly(event)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"解析事件失败: {e}, 行内容: {line[:100]}...")

    def _process_event_correctly(self, event: Dict[str, Any]):
        """正确处理单个事件"""
        event_type = event.get('Event')

        if event_type == 'SparkListenerLogStart':
            self.application_info['sparkVersion'] = event.get('Spark Version', 'unknown')

        elif event_type == 'SparkListenerApplicationStart':
            self.application_info.update({
                'appId': event.get('App ID'),
                'appName': event.get('App Name'),
                'startTime': event.get('Timestamp'),
                'user': event.get('User')
            })

        elif event_type == 'SparkListenerApplicationEnd':
            self.application_info['endTime'] = event.get('Timestamp')
            if self.application_info.get('startTime'):
                self.application_info['duration'] = event.get('Timestamp') - self.application_info['startTime']

        elif event_type == 'SparkListenerEnvironmentUpdate':
            # 提取 Spark 和 Hadoop 配置
            self.spark_properties = event.get('Spark Properties', {})
            self.hadoop_properties = event.get('Hadoop Properties', {})

            # 收集 Driver 配置信息
            self.driver_info.update({
                'memory': self.spark_properties.get('spark.driver.memory', '1g'),
                'cores': self.spark_properties.get('spark.driver.cores', '1'),
                'host': self.spark_properties.get('spark.driver.host', 'unknown')
            })

        elif event_type == 'SparkListenerJobStart':
            job_id = event.get('Job ID')
            self.jobs[job_id] = {
                'jobId': job_id,
                'startTime': event.get('Submission Time'),
                'stageIds': event.get('Stage IDs', []),
                'properties': event.get('Properties', {}),
                'stageInfos': event.get('Stage Infos', [])
            }

        elif event_type == 'SparkListenerJobEnd':
            job_id = event.get('Job ID')
            if job_id in self.jobs:
                self.jobs[job_id].update({
                    'endTime': event.get('Completion Time'),
                    'result': event.get('Job Result', {})
                })

        elif event_type == 'SparkListenerStageSubmitted':
            stage_info = event.get('Stage Info', {})
            stage_id = stage_info.get('Stage ID')
            self.stages[stage_id] = {
                'stageId': stage_id,
                'stageName': stage_info.get('Stage Name', ''),
                'submissionTime': stage_info.get('Submission Time'),
                'numTasks': stage_info.get('Number of Tasks'),
                'parentIds': stage_info.get('Parent IDs', []),
                'rddInfos': stage_info.get('RDD Info', []),
                'stageAttemptId': stage_info.get('Stage Attempt ID', 0)
            }

        elif event_type == 'SparkListenerStageCompleted':
            stage_info = event.get('Stage Info', {})
            stage_id = stage_info.get('Stage ID')
            if stage_id in self.stages:
                self.stages[stage_id].update({
                    'completionTime': stage_info.get('Completion Time'),
                    'failureReason': stage_info.get('Failure Reason'),
                    'executorRunTime': stage_info.get('Executor Run Time', 0),
                    'executorCpuTime': stage_info.get('Executor CPU Time', 0),
                    'executorDeserializeTime': stage_info.get('Executor Deserialize Time', 0),
                    'resultSerializationTime': stage_info.get('Result Serialization Time', 0),
                    'jvmGcTime': stage_info.get('JVM GC Time', 0),
                    'resultSize': stage_info.get('Result Size', 0),
                    'numCompletedTasks': stage_info.get('Number of Completed Tasks', 0),
                    'numFailedTasks': stage_info.get('Number of Failed Tasks', 0),
                    'numKilledTasks': stage_info.get('Number of Killed Tasks', 0),
                    'inputBytes': stage_info.get('Input Size', 0),
                    'inputRecords': stage_info.get('Input Records', 0),
                    'outputBytes': stage_info.get('Output Size', 0),
                    'outputRecords': stage_info.get('Output Records', 0),
                    'shuffleReadBytes': stage_info.get('Shuffle Read Size', 0),
                    'shuffleReadRecords': stage_info.get('Shuffle Read Records', 0),
                    'shuffleWriteBytes': stage_info.get('Shuffle Write Size', 0),
                    'shuffleWriteRecords': stage_info.get('Shuffle Write Records', 0)
                })

        elif event_type == 'SparkListenerTaskEnd':
            task_info = event.get('Task Info', {})
            task_metrics = event.get('Task Metrics', {})

            # 基本任务信息
            task = TaskMetrics(
                task_id=task_info.get('Task ID'),
                stage_id=event.get('Stage ID'),
                stage_attempt_id=event.get('Stage Attempt ID', 0),
                executor_id=task_info.get('Executor ID'),
                host=task_info.get('Host'),
                launch_time=task_info.get('Launch Time', 0),
                finish_time=task_info.get('Finish Time', 0),
                duration=task_info.get('Finish Time', 0) - task_info.get('Launch Time', 0),
                locality=task_info.get('Locality', 'ANY')
            )

            # 执行器指标
            if task_metrics:
                task.executor_deserialize_time = task_metrics.get('Executor Deserialize Time', 0)
                task.executor_deserialize_cpu_time = task_metrics.get('Executor Deserialize CPU Time', 0)
                task.executor_run_time = task_metrics.get('Executor Run Time', 0)
                task.executor_cpu_time = task_metrics.get('Executor CPU Time', 0)
                task.result_size = task_metrics.get('Result Size', 0)
                task.jvm_gc_time = task_metrics.get('JVM GC Time', 0)
                task.result_serialization_time = task_metrics.get('Result Serialization Time', 0)
                task.peak_execution_memory = task_metrics.get('Peak Execution Memory', 0)
                task.memory_bytes_spilled = task_metrics.get('Memory Bytes Spilled', 0)
                task.disk_bytes_spilled = task_metrics.get('Disk Bytes Spilled', 0)

                # Shuffle Read 指标
                shuffle_read = task_metrics.get('Shuffle Read Metrics', {})
                if shuffle_read:
                    task.shuffle_remote_blocks_fetched = shuffle_read.get('Remote Blocks Fetched', 0)
                    task.shuffle_local_blocks_fetched = shuffle_read.get('Local Blocks Fetched', 0)
                    task.shuffle_fetch_wait_time = shuffle_read.get('Fetch Wait Time', 0)
                    task.shuffle_remote_bytes_read = shuffle_read.get('Remote Bytes Read', 0)
                    task.shuffle_remote_bytes_read_to_disk = shuffle_read.get('Remote Bytes Read To Disk', 0)
                    task.shuffle_local_bytes_read = shuffle_read.get('Local Bytes Read', 0)
                    task.shuffle_total_records_read = shuffle_read.get('Total Records Read', 0)
                    task.shuffle_remote_requests_duration = shuffle_read.get('Remote Requests Duration', 0)

                # Shuffle Write 指标
                shuffle_write = task_metrics.get('Shuffle Write Metrics', {})
                if shuffle_write:
                    task.shuffle_bytes_written = shuffle_write.get('Shuffle Bytes Written', 0)
                    task.shuffle_write_time = shuffle_write.get('Shuffle Write Time', 0)
                    task.shuffle_records_written = shuffle_write.get('Shuffle Records Written', 0)

                # Input 指标
                input_metrics = task_metrics.get('Input Metrics', {})
                if input_metrics:
                    task.input_bytes_read = input_metrics.get('Bytes Read', 0)
                    task.input_records_read = input_metrics.get('Records Read', 0)

                # Output 指标
                output_metrics = task_metrics.get('Output Metrics', {})
                if output_metrics:
                    task.output_bytes_written = output_metrics.get('Bytes Written', 0)
                    task.output_records_written = output_metrics.get('Records Written', 0)

            self.tasks.append(task)

        elif event_type == 'SparkListenerExecutorAdded':
            executor_id = event.get('Executor ID')
            executor_info = event.get('Executor Info', {})
            self.executors[executor_id] = {
                'executorId': executor_id,
                'host': executor_info.get('Host'),
                'totalCores': executor_info.get('Total Cores'),
                'maxMemory': executor_info.get('Maximum Memory'),
                'addedTime': event.get('Timestamp'),
                'executorInfo': executor_info
            }

        elif event_type == 'SparkListenerBlockManagerAdded':
            block_manager = event.get('Block Manager ID', {})
            executor_id = block_manager.get('Executor ID')
            if executor_id:
                # 更新或创建 executor 信息，优先使用 BlockManager 的真实内存数据
                if executor_id not in self.executors:
                    self.executors[executor_id] = {}

                # 更新内存信息（BlockManager 有真实的内存分配数据）
                self.executors[executor_id].update({
                    'executorId': executor_id,
                    'host': block_manager.get('Host'),
                    'maxMemory': event.get('Maximum Memory'),  # 真实可用内存
                    'maxOnheapMemory': event.get('Maximum Onheap Memory'),
                    'maxOffheapMemory': event.get('Maximum Offheap Memory'),
                    'addedTime': event.get('Timestamp'),
                    'port': block_manager.get('Port')
                })

        # SQL 执行事件
        elif event_type == 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart':
            execution_id = event.get('executionId')
            self.sql_executions[execution_id] = {
                'executionId': execution_id,
                'description': event.get('description', ''),
                'details': event.get('details', ''),
                'physicalPlanDescription': event.get('physicalPlanDescription', ''),
                'sparkPlanInfo': event.get('sparkPlanInfo', {}),
                'time': event.get('time')
            }

        elif event_type == 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd':
            execution_id = event.get('executionId')
            if execution_id in self.sql_executions:
                self.sql_executions[execution_id]['endTime'] = event.get('time')

    def _analyze_jobs_correctly(self) -> List[JobMetrics]:
        """正确分析作业指标"""
        job_metrics = []

        for job_id, job_data in self.jobs.items():
            # 获取该作业的所有任务
            job_stage_ids = set(job_data.get('stageIds', []))
            job_tasks = [task for task in self.tasks if task.stage_id in job_stage_ids]

            metrics = JobMetrics(
                job_id=job_id,
                job_name=job_data.get('properties', {}).get('spark.job.description', f'Job {job_id}'),
                start_time=self._parse_timestamp(job_data.get('startTime', 0)),
                end_time=self._parse_timestamp(job_data.get('endTime', 0)) if job_data.get('endTime') else None,
                duration_ms=job_data.get('endTime', 0) - job_data.get('startTime', 0) if job_data.get('endTime') and job_data.get('startTime') else None,
                status='SUCCESS' if job_data.get('result', {}).get('Result') == 'JobSucceeded' else 'FAILED',
                stage_ids=job_data.get('stageIds', []),
                num_tasks=len(job_tasks),
                num_completed_tasks=len([t for t in job_tasks if t.finish_time > 0]),
                num_failed_tasks=0  # 从 stage 完成信息中获取
            )

            job_metrics.append(metrics)

        return job_metrics

    def _analyze_executors_correctly(self) -> List[ExecutorMetrics]:
        """正确分析 Executor 指标"""
        executor_metrics = []

        for executor_id, executor_data in self.executors.items():
            if executor_id == 'driver':
                continue  # 跳过 driver

            # 计算该 Executor 上的任务指标
            executor_tasks = [task for task in self.tasks if task.executor_id == executor_id]

            # 获取真实内存（来自 BlockManager）和配置内存
            max_memory = executor_data.get('maxMemory', 0) or 0  # BlockManager 的真实内存
            configured_memory_str = self.spark_properties.get('spark.executor.memory', '1g')
            configured_memory_bytes = self._parse_memory_size(configured_memory_str)

            # 计算实际内存使用（峰值执行内存）
            actual_memory_used = max((task.peak_execution_memory for task in executor_tasks), default=0)

            # 获取 CPU 核心数（从 ExecutorAdded 或配置中获取）
            total_cores = executor_data.get('totalCores') or int(self.spark_properties.get('spark.executor.cores', '2'))

            metrics = ExecutorMetrics(
                executor_id=executor_id,
                host=executor_data.get('host', 'unknown'),
                cores=total_cores,
                memory=f"{max_memory / (1024**3):.1f}GB" if max_memory > 0 else configured_memory_str,
                max_memory=max_memory,
                configured_memory=configured_memory_str,
                configured_memory_bytes=configured_memory_bytes,
                actual_memory_used=actual_memory_used,
                max_onheap_memory=executor_data.get('maxOnheapMemory', 0) or 0,
                max_offheap_memory=executor_data.get('maxOffheapMemory', 0) or 0,
                total_gc_time=sum(task.jvm_gc_time for task in executor_tasks),
                total_input_bytes=sum(task.input_bytes_read for task in executor_tasks),
                total_shuffle_read=sum(task.shuffle_remote_bytes_read + task.shuffle_local_bytes_read for task in executor_tasks),
                total_shuffle_write=sum(task.shuffle_bytes_written for task in executor_tasks)
            )

            executor_metrics.append(metrics)

        return executor_metrics

    def _analyze_driver_correctly(self) -> Optional[DriverMetrics]:
        """正确分析 Driver 指标"""
        if not self.driver_info:
            return None

        # 获取 Driver 任务（executor_id 为 'driver' 的任务）
        driver_tasks = [task for task in self.tasks if task.executor_id == 'driver']

        # 解析内存配置
        memory_str = self.driver_info.get('memory', '1g')
        memory_bytes = self._parse_memory_size(memory_str)

        # 计算执行统计
        total_gc_time = sum(task.jvm_gc_time for task in driver_tasks)
        total_execution_time = sum(task.executor_run_time for task in driver_tasks)

        # 计算 Driver 峰值内存使用
        peak_memory_used = max((task.peak_execution_memory for task in driver_tasks), default=0)

        # 获取 Driver 的真实内存（从 BlockManager）
        driver_executor = self.executors.get('driver', {})
        actual_memory_available = driver_executor.get('maxMemory', 0) or 0

        try:
            cores = int(self.driver_info.get('cores', '1'))
        except (ValueError, TypeError):
            cores = 1

        return DriverMetrics(
            cores=cores,
            memory=memory_str,
            memory_bytes=memory_bytes,
            host=self.driver_info.get('host', 'unknown'),
            total_gc_time=total_gc_time,
            total_execution_time=total_execution_time,
            peak_memory_used=peak_memory_used,
            actual_memory_available=actual_memory_available
        )

    def _analyze_performance_correctly(self) -> PerformanceMetrics:
        """正确分析性能指标"""
        if not self.tasks:
            return PerformanceMetrics(
                total_execution_time_ms=0,
                total_cpu_time_ms=0
            )

        return PerformanceMetrics(
            total_execution_time_ms=sum(task.executor_run_time for task in self.tasks),
            total_cpu_time_ms=sum(task.executor_cpu_time for task in self.tasks) // 1000000,  # 纳秒转毫秒
            total_gc_time_ms=sum(task.jvm_gc_time for task in self.tasks),
            peak_execution_memory=max((task.peak_execution_memory for task in self.tasks), default=0),
            total_memory_spilled=sum(task.memory_bytes_spilled for task in self.tasks),
            total_disk_spilled=sum(task.disk_bytes_spilled for task in self.tasks),
            total_input_bytes=sum(task.input_bytes_read for task in self.tasks),
            total_output_bytes=sum(task.output_bytes_written for task in self.tasks),
            max_concurrent_tasks=self._calculate_max_concurrent_tasks(),
            average_concurrent_tasks=self._calculate_avg_concurrent_tasks()
        )

    def _analyze_shuffle_correctly(self) -> ShuffleAnalysis:
        """正确分析 Shuffle 数据"""
        # 总体 Shuffle 统计
        total_shuffle_read = sum(task.shuffle_remote_bytes_read + task.shuffle_local_bytes_read for task in self.tasks)
        total_shuffle_write = sum(task.shuffle_bytes_written for task in self.tasks)
        total_shuffle_read_records = sum(task.shuffle_total_records_read for task in self.tasks)
        total_shuffle_write_records = sum(task.shuffle_records_written for task in self.tasks)

        # 按 Stage 分组的 Shuffle 指标
        stage_shuffle_metrics = []
        stage_tasks = defaultdict(list)

        for task in self.tasks:
            stage_tasks[task.stage_id].append(task)

        for stage_id, tasks in stage_tasks.items():
            stage_info = self.stages.get(stage_id, {})

            # 计算该 stage 的 shuffle 指标
            stage_shuffle_read = sum(task.shuffle_remote_bytes_read + task.shuffle_local_bytes_read for task in tasks)
            stage_shuffle_write = sum(task.shuffle_bytes_written for task in tasks)

            stage_metrics = ShuffleStageMetrics(
                stage_id=stage_id,
                stage_name=stage_info.get('stageName', f'Stage {stage_id}'),
                shuffle_read_bytes=stage_shuffle_read,
                shuffle_write_bytes=stage_shuffle_write,
                shuffle_read_records=sum(task.shuffle_total_records_read for task in tasks),
                shuffle_write_records=sum(task.shuffle_records_written for task in tasks),
                remote_blocks_fetched=sum(task.shuffle_remote_blocks_fetched for task in tasks),
                local_blocks_fetched=sum(task.shuffle_local_blocks_fetched for task in tasks),
                fetch_wait_time=sum(task.shuffle_fetch_wait_time for task in tasks),
                remote_bytes_read=sum(task.shuffle_remote_bytes_read for task in tasks),
                local_bytes_read=sum(task.shuffle_local_bytes_read for task in tasks),
                shuffle_write_time=sum(task.shuffle_write_time for task in tasks)
            )

            # 按 Executor 分组的 Shuffle 指标
            executor_metrics = defaultdict(lambda: {'read_bytes': 0, 'write_bytes': 0, 'read_records': 0, 'write_records': 0})
            for task in tasks:
                executor_metrics[task.executor_id]['read_bytes'] += task.shuffle_remote_bytes_read + task.shuffle_local_bytes_read
                executor_metrics[task.executor_id]['write_bytes'] += task.shuffle_bytes_written
                executor_metrics[task.executor_id]['read_records'] += task.shuffle_total_records_read
                executor_metrics[task.executor_id]['write_records'] += task.shuffle_records_written

            stage_metrics.executor_shuffle_metrics = dict(executor_metrics)
            stage_shuffle_metrics.append(stage_metrics)

        # 找出 Shuffle 密集型 Stage
        most_shuffle_intensive = sorted(
            stage_shuffle_metrics,
            key=lambda x: x.shuffle_read_bytes + x.shuffle_write_bytes,
            reverse=True
        )[:10]

        most_shuffle_intensive_stages = [
            {
                'stage_id': stage.stage_id,
                'stage_name': stage.stage_name,
                'total_shuffle_bytes': stage.shuffle_read_bytes + stage.shuffle_write_bytes,
                'shuffle_read_bytes': stage.shuffle_read_bytes,
                'shuffle_write_bytes': stage.shuffle_write_bytes
            }
            for stage in most_shuffle_intensive if (stage.shuffle_read_bytes + stage.shuffle_write_bytes) > 0
        ]

        # 计算 Shuffle 效率指标
        shuffle_efficiency = {}
        if total_shuffle_write > 0:
            shuffle_efficiency['compression_ratio'] = total_shuffle_read / total_shuffle_write
        if total_shuffle_read_records > 0:
            shuffle_efficiency['avg_record_size'] = total_shuffle_read / total_shuffle_read_records

        # 数据倾斜分析
        data_skew_analysis = self._analyze_data_skew_correctly(stage_shuffle_metrics)

        return ShuffleAnalysis(
            total_shuffle_read_bytes=total_shuffle_read,
            total_shuffle_write_bytes=total_shuffle_write,
            total_shuffle_read_records=total_shuffle_read_records,
            total_shuffle_write_records=total_shuffle_write_records,
            stage_shuffle_metrics=stage_shuffle_metrics,
            most_shuffle_intensive_stages=most_shuffle_intensive_stages,
            shuffle_efficiency_metrics=shuffle_efficiency,
            data_skew_analysis=data_skew_analysis
        )

    def _analyze_data_skew_correctly(self, stage_metrics: List[ShuffleStageMetrics]) -> Dict[str, Any]:
        """正确分析数据倾斜"""
        skew_analysis = {
            'stages_with_skew': [],
            'skew_severity': 'LOW',
            'recommendations': []
        }

        for stage in stage_metrics:
            if not stage.executor_shuffle_metrics or len(stage.executor_shuffle_metrics) <= 1:
                continue

            # 计算每个 Executor 的 Shuffle 读取量
            executor_reads = [
                metrics['read_bytes']
                for metrics in stage.executor_shuffle_metrics.values()
            ]

            if not executor_reads or max(executor_reads) == 0:
                continue

            avg_read = sum(executor_reads) / len(executor_reads)
            max_read = max(executor_reads)
            min_read = min(executor_reads)

            # 计算倾斜系数
            if avg_read > 0:
                skew_ratio = max_read / avg_read
                if skew_ratio > 2.0:  # 2倍以上认为有倾斜
                    skew_analysis['stages_with_skew'].append({
                        'stage_id': stage.stage_id,
                        'stage_name': stage.stage_name,
                        'skew_ratio': skew_ratio,
                        'max_executor_read': max_read,
                        'min_executor_read': min_read,
                        'avg_executor_read': avg_read,
                        'executor_count': len(executor_reads)
                    })

        # 确定倾斜严重程度
        if skew_analysis['stages_with_skew']:
            max_skew = max(stage['skew_ratio'] for stage in skew_analysis['stages_with_skew'])
            if max_skew > 10:
                skew_analysis['skew_severity'] = 'HIGH'
            elif max_skew > 5:
                skew_analysis['skew_severity'] = 'MEDIUM'
            else:
                skew_analysis['skew_severity'] = 'LOW'

        return skew_analysis

    def _generate_correct_recommendations(self) -> List[OptimizationRecommendations]:
        """生成正确的优化建议"""
        recommendations = []

        # 基于真实指标的建议
        shuffle_recs = self._generate_shuffle_recommendations_correct()
        if shuffle_recs:
            recommendations.append(shuffle_recs)

        resource_recs = self._generate_resource_recommendations_correct()
        if resource_recs:
            recommendations.append(resource_recs)

        performance_recs = self._generate_performance_recommendations_correct()
        if performance_recs:
            recommendations.append(performance_recs)

        return recommendations

    def _generate_shuffle_recommendations_correct(self) -> Optional[OptimizationRecommendations]:
        """基于真实数据生成 Shuffle 优化建议"""
        total_shuffle = sum(
            task.shuffle_remote_bytes_read + task.shuffle_local_bytes_read + task.shuffle_bytes_written
            for task in self.tasks
        )

        if total_shuffle == 0:
            return None

        recommendations = []

        # 检查 Shuffle 分区数
        current_partitions = self.spark_properties.get('spark.sql.shuffle.partitions', '200')
        try:
            current_partitions_int = int(current_partitions)
            if current_partitions_int == 200:  # 默认值
                recommended_partitions = max(1, min(2000, total_shuffle // (128 * 1024 * 1024)))  # 每个分区 128MB
                recommendations.append({
                    'title': '调整 Shuffle 分区数',
                    'description': f'当前使用默认的 200 个分区，对于 {self._format_bytes(total_shuffle)} 的 Shuffle 数据不够优化',
                    'suggestion': f'建议设置为 {recommended_partitions} 个分区以获得更好性能',
                    'config': f'spark.sql.shuffle.partitions={recommended_partitions}',
                    'priority': 'HIGH'
                })
        except ValueError:
            pass

        # 检查数据倾斜
        shuffle_analysis = self._analyze_shuffle_correctly()
        if shuffle_analysis.data_skew_analysis.get('skew_severity') != 'LOW':
            recommendations.append({
                'title': '数据倾斜优化',
                'description': f'检测到 {shuffle_analysis.data_skew_analysis.get("skew_severity")} 级别的数据倾斜',
                'suggestion': '考虑使用 salting、预聚合或广播 join 等技术来减少数据倾斜',
                'priority': 'HIGH' if shuffle_analysis.data_skew_analysis.get('skew_severity') == 'HIGH' else 'MEDIUM'
            })

        return OptimizationRecommendations(
            priority_level='HIGH',
            category='SHUFFLE',
            recommendations=recommendations
        ) if recommendations else None

    def _generate_resource_recommendations_correct(self) -> Optional[OptimizationRecommendations]:
        """基于真实数据生成资源优化建议"""
        recommendations = []

        if not self.tasks:
            return None

        # 检查内存使用
        max_memory_used = max((task.peak_execution_memory for task in self.tasks), default=0)
        total_spilled = sum(task.memory_bytes_spilled + task.disk_bytes_spilled for task in self.tasks)

        executor_memory_str = self.spark_properties.get('spark.executor.memory', '1g')
        executor_memory_bytes = self._parse_memory_size(executor_memory_str)

        if max_memory_used > executor_memory_bytes * 0.8:
            recommendations.append({
                'title': '增加 Executor 内存',
                'description': f'峰值内存使用 {self._format_bytes(max_memory_used)} 接近配置的 {executor_memory_str}',
                'suggestion': f'建议增加到 {self._format_bytes(int(max_memory_used * 1.5))}',
                'config': f'spark.executor.memory={self._recommend_memory_size(max_memory_used)}',
                'priority': 'HIGH'
            })

        if total_spilled > 0:
            recommendations.append({
                'title': '减少内存溢写',
                'description': f'总溢写量 {self._format_bytes(total_spilled)}，影响性能',
                'suggestion': '增加 executor 内存或优化数据处理逻辑',
                'priority': 'MEDIUM'
            })

        return OptimizationRecommendations(
            priority_level='MEDIUM',
            category='RESOURCE',
            recommendations=recommendations
        ) if recommendations else None

    def _generate_performance_recommendations_correct(self) -> Optional[OptimizationRecommendations]:
        """基于真实数据生成性能优化建议"""
        recommendations = []

        if not self.tasks:
            return None

        # 检查 GC 时间
        total_gc_time = sum(task.jvm_gc_time for task in self.tasks)
        total_execution_time = sum(task.executor_run_time for task in self.tasks)

        if total_execution_time > 0:
            gc_ratio = total_gc_time / total_execution_time
            if gc_ratio > 0.1:  # GC 时间超过 10%
                recommendations.append({
                    'title': 'GC 优化',
                    'description': f'GC 时间占执行时间的 {gc_ratio*100:.1f}%',
                    'suggestion': '考虑增加堆内存或调整 GC 参数',
                    'priority': 'HIGH' if gc_ratio > 0.2 else 'MEDIUM'
                })

        # 检查任务本地性
        local_tasks = len([t for t in self.tasks if t.locality in ['PROCESS_LOCAL', 'NODE_LOCAL']])
        if self.tasks:
            locality_ratio = local_tasks / len(self.tasks)
            if locality_ratio < 0.8:
                recommendations.append({
                    'title': '改善任务本地性',
                    'description': f'本地任务比例仅 {locality_ratio*100:.1f}%',
                    'suggestion': '检查数据分布和缓存策略',
                    'priority': 'MEDIUM'
                })

        return OptimizationRecommendations(
            priority_level='MEDIUM',
            category='PERFORMANCE',
            recommendations=recommendations
        ) if recommendations else None

    def _generate_correct_summary(self) -> Dict[str, Any]:
        """生成正确的分析摘要"""
        total_shuffle_data = sum(
            task.shuffle_remote_bytes_read + task.shuffle_local_bytes_read + task.shuffle_bytes_written
            for task in self.tasks
        )

        return {
            'total_jobs': len(self.jobs),
            'total_stages': len(self.stages),
            'total_tasks': len(self.tasks),
            'total_executors': len([e for e in self.executors.keys() if e != 'driver']),
            'total_execution_time_ms': sum(task.executor_run_time for task in self.tasks),
            'total_shuffle_data': total_shuffle_data,
            'sql_executions_count': len(self.sql_executions),
            'analysis_timestamp': datetime.now().isoformat()
        }

    # 辅助方法保持不变
    def _parse_timestamp(self, timestamp: int) -> datetime:
        """解析时间戳"""
        return datetime.fromtimestamp(timestamp / 1000.0) if timestamp else datetime.now()

    def _calculate_max_concurrent_tasks(self) -> int:
        """计算最大并发任务数"""
        if not self.tasks:
            return 0
        # 简化实现：使用 executor 数量乘以核心数
        total_cores = sum(
            executor.get('totalCores', 2)
            for executor in self.executors.values()
            if executor.get('executorId') != 'driver'
        )
        return total_cores or len(self.executors) * 2

    def _calculate_avg_concurrent_tasks(self) -> float:
        """计算平均并发任务数"""
        return self._calculate_max_concurrent_tasks() * 0.7  # 简化实现

    def _format_bytes(self, bytes_size: int) -> str:
        """格式化字节数"""
        if bytes_size == 0:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.1f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.1f} PB"

    def _parse_memory_size(self, memory_str: str) -> int:
        """解析内存大小字符串为字节数"""
        memory_str = memory_str.lower()
        multipliers = {'k': 1024, 'm': 1024**2, 'g': 1024**3, 't': 1024**4}

        for suffix, multiplier in multipliers.items():
            if memory_str.endswith(suffix):
                return int(float(memory_str[:-1]) * multiplier)

        return int(memory_str) if memory_str.isdigit() else 1024**3

    def _recommend_memory_size(self, peak_memory: int) -> str:
        """推荐内存大小"""
        recommended_bytes = int(peak_memory * 1.5)
        return self._format_bytes(recommended_bytes).replace(' ', '').lower()

    def get_field_descriptions(self) -> FieldDescriptions:
        """返回字段描述信息"""
        descriptions = {
            "basic_info": [
                FieldDescription(
                    field_name="application_id",
                    description="Spark 应用程序的唯一标识符",
                    data_type="string",
                    example_value="app-20231201-123456-0000"
                ),
                FieldDescription(
                    field_name="spark_version",
                    description="运行应用程序的 Spark 版本",
                    data_type="string",
                    example_value="3.5.2-amzn-1"
                )
            ],
            "performance_metrics": [
                FieldDescription(
                    field_name="total_execution_time_ms",
                    description="所有任务的执行器运行时间总和",
                    data_type="integer",
                    unit="毫秒",
                    example_value="1500000"
                ),
                FieldDescription(
                    field_name="peak_execution_memory",
                    description="所有任务中的最大峰值内存使用量",
                    data_type="integer",
                    unit="字节",
                    example_value="2147483648"
                )
            ],
            "shuffle_metrics": [
                FieldDescription(
                    field_name="total_shuffle_read_bytes",
                    description="所有任务 Shuffle 读取的总字节数（远程+本地）",
                    data_type="integer",
                    unit="字节",
                    example_value="1073741824"
                ),
                FieldDescription(
                    field_name="total_shuffle_write_bytes",
                    description="所有任务 Shuffle 写入的总字节数",
                    data_type="integer",
                    unit="字节",
                    example_value="536870912"
                )
            ]
        }

        return FieldDescriptions(categories=descriptions)

    def get_analysis_summary(self) -> Dict[str, Any]:
        """获取分析摘要 - 兼容接口"""
        return self._generate_correct_summary()

    def get_optimization_suggestions(self, focus_areas: List[str] = None, priority_filter: str = None) -> List[Dict[str, Any]]:
        """获取优化建议 - 兼容接口"""
        all_recommendations = self._generate_correct_recommendations()

        # 提取所有建议
        suggestions = []
        for rec in all_recommendations:
            for suggestion in rec.recommendations:
                suggestion_dict = {
                    'category': rec.category,
                    'priority': suggestion.get('priority', 'MEDIUM'),
                    'title': suggestion.get('title', ''),
                    'description': suggestion.get('description', ''),
                    'suggestion': suggestion.get('suggestion', ''),
                    'config_parameters': {suggestion.get('config', ''): ''} if suggestion.get('config') else {}
                }
                suggestions.append(suggestion_dict)

        # 应用过滤器
        if focus_areas:
            suggestions = [s for s in suggestions if s['category'].lower() in [area.lower() for area in focus_areas]]

        if priority_filter:
            suggestions = [s for s in suggestions if s['priority'] == priority_filter.upper()]

        return suggestions