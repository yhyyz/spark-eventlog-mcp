"""
成熟的数据模型定义 - 从现有项目复制
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class DataSource(BaseModel):
    """数据源配置"""
    path: str = Field(..., description="数据路径：S3 路径或 HTTP URL")

class ExecutorMetrics(BaseModel):
    """Executor 指标"""
    executor_id: str = Field(..., description="Executor ID")
    host: str = Field(..., description="主机地址")
    cores: int = Field(..., description="核心数")
    memory: str = Field(..., description="内存大小")
    max_memory: int = Field(..., description="最大内存字节数")
    configured_memory: str = Field(..., description="配置的内存大小")
    configured_memory_bytes: int = Field(..., description="配置的内存字节数")
    actual_memory_used: int = Field(default=0, description="实际JVM内存使用")
    disk_used: int = Field(default=0, description="磁盘使用量")
    max_onheap_memory: int = Field(..., description="最大堆内存")
    max_offheap_memory: int = Field(..., description="最大堆外内存")
    overhead_memory: int = Field(default=0, description="Overhead 内存字节数")
    total_gc_time: int = Field(default=0, description="总 GC 时间（毫秒）")
    total_input_bytes: int = Field(default=0, description="总输入字节数")
    total_shuffle_read: int = Field(default=0, description="总 Shuffle 读取字节数")
    total_shuffle_write: int = Field(default=0, description="总 Shuffle 写入字节数")

class DriverMetrics(BaseModel):
    """Driver 指标"""
    cores: int = Field(..., description="Driver 核心数")
    memory: str = Field(..., description="Driver 内存大小")
    memory_bytes: int = Field(..., description="Driver 内存字节数")
    host: str = Field(..., description="Driver 主机")
    overhead_memory: int = Field(default=0, description="Driver Overhead 内存字节数")
    total_gc_time: int = Field(default=0, description="Driver GC 时间")
    total_execution_time: int = Field(default=0, description="Driver 执行时间")
    peak_memory_used: int = Field(default=0, description="Driver 峰值内存使用")
    actual_memory_available: int = Field(default=0, description="Driver 实际可用内存")

class ShuffleStageMetrics(BaseModel):
    """Stage 级别的 Shuffle 指标"""
    stage_id: int = Field(..., description="Stage ID")
    stage_name: str = Field(..., description="Stage 名称")

    # Shuffle 读取指标
    shuffle_read_bytes: int = Field(default=0, description="Shuffle 读取字节数")
    shuffle_read_records: int = Field(default=0, description="Shuffle 读取记录数")
    remote_blocks_fetched: int = Field(default=0, description="远程块获取数量")
    local_blocks_fetched: int = Field(default=0, description="本地块获取数量")
    fetch_wait_time: int = Field(default=0, description="获取等待时间（毫秒）")
    remote_bytes_read: int = Field(default=0, description="远程读取字节数")
    local_bytes_read: int = Field(default=0, description="本地读取字节数")

    # Shuffle 写入指标
    shuffle_write_bytes: int = Field(default=0, description="Shuffle 写入字节数")
    shuffle_write_records: int = Field(default=0, description="Shuffle 写入记录数")
    shuffle_write_time: int = Field(default=0, description="Shuffle 写入时间（毫秒）")

    # 关联的 Executor 指标
    executor_shuffle_metrics: Dict[str, Dict[str, int]] = Field(
        default_factory=dict,
        description="每个 Executor 的 Shuffle 指标"
    )

class JobMetrics(BaseModel):
    """作业级别指标"""
    job_id: int = Field(..., description="Job ID")
    job_name: str = Field(..., description="Job 名称")
    start_time: datetime = Field(..., description="开始时间")
    end_time: Optional[datetime] = Field(None, description="结束时间")
    duration_ms: Optional[int] = Field(None, description="持续时间（毫秒）")
    status: str = Field(..., description="作业状态")

    # Stage 信息
    stage_ids: List[int] = Field(default_factory=list, description="包含的 Stage ID 列表")
    num_tasks: int = Field(default=0, description="任务总数")
    num_active_tasks: int = Field(default=0, description="活跃任务数")
    num_completed_tasks: int = Field(default=0, description="完成任务数")
    num_skipped_tasks: int = Field(default=0, description="跳过任务数")
    num_failed_tasks: int = Field(default=0, description="失败任务数")

class ShuffleAnalysis(BaseModel):
    """深入的 Shuffle 分析结果"""
    total_shuffle_read_bytes: int = Field(..., description="总 Shuffle 读取字节数")
    total_shuffle_write_bytes: int = Field(..., description="总 Shuffle 写入字节数")
    total_shuffle_read_records: int = Field(..., description="总 Shuffle 读取记录数")
    total_shuffle_write_records: int = Field(..., description="总 Shuffle 写入记录数")

    # Stage 级别的 Shuffle 分析
    stage_shuffle_metrics: List[ShuffleStageMetrics] = Field(
        default_factory=list,
        description="每个 Stage 的 Shuffle 指标"
    )

    # 热点分析
    most_shuffle_intensive_stages: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Shuffle 密集型 Stage 排行"
    )

    # 效率分析
    shuffle_efficiency_metrics: Dict[str, float] = Field(
        default_factory=dict,
        description="Shuffle 效率指标"
    )

    # 数据倾斜检测
    data_skew_analysis: Dict[str, Any] = Field(
        default_factory=dict,
        description="数据倾斜分析结果"
    )

class PerformanceMetrics(BaseModel):
    """性能指标"""
    total_execution_time_ms: int = Field(..., description="总执行时间（毫秒）")
    total_cpu_time_ms: int = Field(..., description="总 CPU 时间（毫秒）")
    total_gc_time_ms: int = Field(default=0, description="总 GC 时间（毫秒）")

    # 内存使用
    peak_execution_memory: int = Field(default=0, description="峰值执行内存")
    total_memory_spilled: int = Field(default=0, description="总溢写内存")
    total_disk_spilled: int = Field(default=0, description="总溢写磁盘")

    # I/O 指标
    total_input_bytes: int = Field(default=0, description="总输入字节数")
    total_output_bytes: int = Field(default=0, description="总输出字节数")

    # 并发度
    max_concurrent_tasks: int = Field(default=0, description="最大并发任务数")
    average_concurrent_tasks: float = Field(default=0.0, description="平均并发任务数")

class OptimizationRecommendations(BaseModel):
    """优化建议"""
    priority_level: str = Field(..., description="优先级：HIGH/MEDIUM/LOW")
    category: str = Field(..., description="类别：RESOURCE/CONFIGURATION/DATA/CODE")
    recommendations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="具体建议列表"
    )

class MatureAnalysisResult(BaseModel):
    """完整的分析结果 - 成熟版本"""

    # 基础信息
    application_id: str = Field(..., description="应用程序 ID")
    application_name: str = Field(..., description="应用程序名称")
    spark_version: str = Field(..., description="Spark 版本")
    start_time: datetime = Field(..., description="应用开始时间")
    end_time: Optional[datetime] = Field(None, description="应用结束时间")
    duration_ms: Optional[int] = Field(None, description="总运行时间（毫秒）")

    # 作业信息
    jobs: List[JobMetrics] = Field(default_factory=list, description="作业列表")
    total_jobs: int = Field(default=0, description="总作业数")
    successful_jobs: int = Field(default=0, description="成功作业数")
    failed_jobs: int = Field(default=0, description="失败作业数")

    # Executor 信息
    executors: List[ExecutorMetrics] = Field(default_factory=list, description="Executor 列表")
    total_executors: int = Field(default=0, description="总 Executor 数")

    # Driver 信息
    driver_metrics: Optional[DriverMetrics] = Field(None, description="Driver 指标")

    # 性能指标
    performance_metrics: PerformanceMetrics = Field(..., description="性能指标")

    # Shuffle 深入分析
    shuffle_analysis: ShuffleAnalysis = Field(..., description="Shuffle 深入分析")

    # 环境配置
    spark_properties: Dict[str, str] = Field(default_factory=dict, description="Spark 配置")
    hadoop_properties: Dict[str, str] = Field(default_factory=dict, description="Hadoop 配置")

    # 优化建议
    optimization_recommendations: List[OptimizationRecommendations] = Field(
        default_factory=list,
        description="优化建议"
    )

    # 分析摘要
    analysis_summary: Dict[str, Any] = Field(
        default_factory=dict,
        description="分析摘要"
    )

    # 分析时间戳
    analysis_timestamp: datetime = Field(default_factory=datetime.now, description="分析时间戳")

class FieldDescription(BaseModel):
    """字段描述"""
    field_name: str = Field(..., description="字段名称")
    description: str = Field(..., description="字段描述")
    data_type: str = Field(..., description="数据类型")
    unit: Optional[str] = Field(None, description="单位")
    example_value: Optional[str] = Field(None, description="示例值")

class FieldDescriptions(BaseModel):
    """所有字段的描述信息"""
    categories: Dict[str, List[FieldDescription]] = Field(
        default_factory=dict,
        description="按类别组织的字段描述"
    )

# Event Log 数据容器
class EventLogData:
    """Event Log 数据容器"""
    def __init__(self, job_id: str, files: Dict[str, List[str]]):
        self.job_id = job_id
        self.files = files  # {'events': [...], 'appstatus': [...]}