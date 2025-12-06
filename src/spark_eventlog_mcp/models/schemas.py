"""
Pydantic schemas for Spark Event Log Analysis MCP Server
"""

from typing import Dict, List, Optional, Any, Union, Literal
from pydantic import BaseModel, Field
from datetime import datetime

# Input Models
class DataSource(BaseModel):
    """Data source configuration"""
    source_type: Literal["s3", "url", "local"] = Field(
        "s3", description="Type of data source: s3, url, or local (defaults to s3)"
    )
    path: str = Field(..., description="Path to event log (S3 path, URL, or local file path)")
    credentials: Optional[Dict[str, str]] = Field(
        None, description="Optional credentials for accessing the data source"
    )

    @classmethod
    def create_s3_source(cls, s3_path: str, credentials: Optional[Dict[str, str]] = None) -> 'DataSource':
        """Create a DataSource for S3 with convenience method"""
        return cls(source_type="s3", path=s3_path, credentials=credentials)

    @classmethod
    def create_default_source(cls, path: str, source_type: str = "s3") -> 'DataSource':
        """Create a DataSource with default source type (s3)"""
        return cls(source_type=source_type, path=path)

class AnalysisConfig(BaseModel):
    """Configuration for analysis parameters"""
    include_shuffle_analysis: bool = Field(True, description="Include shuffle performance analysis")
    include_resource_analysis: bool = Field(True, description="Include resource usage analysis")
    include_task_analysis: bool = Field(True, description="Include task execution analysis")
    include_optimization_suggestions: bool = Field(True, description="Generate optimization suggestions")
    analysis_depth: Literal["basic", "detailed", "comprehensive"] = Field(
        "detailed", description="Depth of analysis to perform"
    )

class ReportConfig(BaseModel):
    """Configuration for report generation"""
    report_format: Literal["html"] = Field("html", description="Output format for the report")
    include_visualizations: bool = Field(True, description="Include charts and visualizations")
    include_raw_metrics: bool = Field(False, description="Include raw metric data")
    custom_title: Optional[str] = Field(None, description="Custom title for the report")

# Output Models
class ShuffleMetrics(BaseModel):
    """Shuffle performance metrics"""
    total_shuffle_read_bytes: int = Field(0, description="Total bytes read during shuffle operations")
    total_shuffle_write_bytes: int = Field(0, description="Total bytes written during shuffle operations")
    shuffle_read_time_ms: int = Field(0, description="Total time spent reading shuffle data (ms)")
    shuffle_write_time_ms: int = Field(0, description="Total time spent writing shuffle data (ms)")
    shuffle_stages_count: int = Field(0, description="Number of stages with shuffle operations")
    avg_shuffle_partition_size: float = Field(0.0, description="Average shuffle partition size (bytes)")
    max_shuffle_partition_size: int = Field(0, description="Maximum shuffle partition size (bytes)")
    shuffle_spill_memory: int = Field(0, description="Memory spilled to disk during shuffle")
    shuffle_spill_disk: int = Field(0, description="Data spilled to disk during shuffle")

class ResourceMetrics(BaseModel):
    """Resource utilization metrics"""
    driver_memory_used: int = Field(0, description="Driver memory used (bytes)")
    driver_memory_max: int = Field(0, description="Driver memory maximum (bytes)")
    executor_memory_used: int = Field(0, description="Total executor memory used (bytes)")
    executor_memory_max: int = Field(0, description="Total executor memory maximum (bytes)")
    executor_cores_total: int = Field(0, description="Total executor cores")
    executor_count: int = Field(0, description="Number of executors")
    peak_executor_memory: int = Field(0, description="Peak executor memory usage (bytes)")
    gc_time_ms: int = Field(0, description="Total garbage collection time (ms)")
    cpu_time_ms: int = Field(0, description="Total CPU time (ms)")

class TaskMetrics(BaseModel):
    """Task execution metrics"""
    total_tasks: int = Field(0, description="Total number of tasks")
    successful_tasks: int = Field(0, description="Number of successful tasks")
    failed_tasks: int = Field(0, description="Number of failed tasks")
    avg_task_duration_ms: float = Field(0.0, description="Average task duration (ms)")
    max_task_duration_ms: int = Field(0, description="Maximum task duration (ms)")
    task_locality_stats: Dict[str, int] = Field(
        default_factory=dict, description="Task locality statistics"
    )
    skewed_tasks_count: int = Field(0, description="Number of tasks with execution skew")

class StageMetrics(BaseModel):
    """Stage execution metrics"""
    stage_id: int = Field(..., description="Stage ID")
    stage_name: str = Field("", description="Stage name")
    num_tasks: int = Field(0, description="Number of tasks in this stage")
    duration_ms: int = Field(0, description="Stage duration (ms)")
    shuffle_read_bytes: int = Field(0, description="Shuffle read bytes for this stage")
    shuffle_write_bytes: int = Field(0, description="Shuffle write bytes for this stage")
    input_bytes: int = Field(0, description="Input bytes for this stage")
    output_bytes: int = Field(0, description="Output bytes for this stage")
    is_shuffle_intensive: bool = Field(False, description="Whether this stage is shuffle-intensive")

class OptimizationSuggestion(BaseModel):
    """Performance optimization suggestion"""
    category: Literal["SHUFFLE", "RESOURCE", "PERFORMANCE", "CONFIGURATION"] = Field(
        ..., description="Category of the optimization"
    )
    priority: Literal["HIGH", "MEDIUM", "LOW"] = Field(..., description="Priority level")
    title: str = Field(..., description="Short title of the suggestion")
    description: str = Field(..., description="Detailed description of the issue")
    suggestion: str = Field(..., description="Recommended action to take")
    config_parameters: Dict[str, str] = Field(
        default_factory=dict, description="Specific Spark configuration parameters"
    )
    expected_impact: str = Field("", description="Expected performance impact")

class ApplicationInfo(BaseModel):
    """Spark application information"""
    application_id: str = Field("", description="Spark application ID")
    application_name: str = Field("", description="Spark application name")
    spark_version: str = Field("", description="Spark version used")
    start_time: Optional[datetime] = Field(None, description="Application start time")
    end_time: Optional[datetime] = Field(None, description="Application end time")
    duration_ms: int = Field(0, description="Total application duration (ms)")
    total_jobs: int = Field(0, description="Total number of jobs")
    total_stages: int = Field(0, description="Total number of stages")
    user: str = Field("", description="User who submitted the application")

class AnalysisResult(BaseModel):
    """Complete analysis result"""
    application_info: ApplicationInfo = Field(
        default_factory=ApplicationInfo, description="Application metadata"
    )
    shuffle_metrics: ShuffleMetrics = Field(
        default_factory=ShuffleMetrics, description="Shuffle performance metrics"
    )
    resource_metrics: ResourceMetrics = Field(
        default_factory=ResourceMetrics, description="Resource utilization metrics"
    )
    task_metrics: TaskMetrics = Field(
        default_factory=TaskMetrics, description="Task execution metrics"
    )
    stage_metrics: List[StageMetrics] = Field(
        default_factory=list, description="Per-stage metrics"
    )
    optimization_suggestions: List[OptimizationSuggestion] = Field(
        default_factory=list, description="Performance optimization suggestions"
    )
    analysis_timestamp: datetime = Field(
        default_factory=datetime.now, description="When the analysis was performed"
    )
    analysis_config: AnalysisConfig = Field(
        default_factory=lambda: AnalysisConfig(), description="Configuration used for analysis"
    )

# Report Models
class VisualizationData(BaseModel):
    """Data for creating visualizations"""
    chart_type: str = Field(..., description="Type of chart (bar, line, pie, etc.)")
    title: str = Field(..., description="Chart title")
    data: Dict[str, Any] = Field(..., description="Chart data in format expected by plotting library")
    config: Dict[str, Any] = Field(default_factory=dict, description="Chart configuration options")

class ReportSection(BaseModel):
    """A section in the generated report"""
    section_id: str = Field(..., description="Unique identifier for this section")
    title: str = Field(..., description="Section title")
    content: str = Field(..., description="Section content (HTML or markdown)")
    visualizations: List[VisualizationData] = Field(
        default_factory=list, description="Visualizations for this section"
    )
    metrics: Dict[str, Any] = Field(
        default_factory=dict, description="Key metrics for this section"
    )

class GeneratedReport(BaseModel):
    """Complete generated report"""
    report_id: str = Field(..., description="Unique report identifier")
    title: str = Field(..., description="Report title")
    generated_at: datetime = Field(default_factory=datetime.now, description="Report generation time")
    analysis_result: AnalysisResult = Field(..., description="Analysis results used for the report")
    sections: List[ReportSection] = Field(default_factory=list, description="Report sections")
    summary: Dict[str, Any] = Field(default_factory=dict, description="Executive summary")
    report_format: str = Field("html", description="Report format")

# Tool Input Models (for MCP tools)
class ParseEventLogInput(BaseModel):
    """Input for parse_eventlog tool"""
    data_source: DataSource = Field(..., description="Data source configuration")

class AnalyzePerformanceInput(BaseModel):
    """Input for analyze_performance tool"""
    analysis_config: AnalysisConfig = Field(
        default_factory=AnalysisConfig, description="Analysis configuration"
    )
    data_source: Optional[DataSource] = Field(
        None, description="Data source (if not already parsed)"
    )

class GenerateReportInput(BaseModel):
    """
    Input for end-to-end generate_report tool
    
    This input model supports complete end-to-end report generation from raw data sources
    to formatted reports with analysis and optimization suggestions. It can work with:
    1. Raw data sources (performs parsing, analysis, and report generation)
    2. Existing analysis results (skips parsing and analysis, generates report only)
    3. Current session data (uses cached data from previous operations)
    """
    report_config: ReportConfig = Field(
        default_factory=ReportConfig, 
        description="Report configuration including format, visualizations, and content options"
    )
    data_source: Optional[DataSource] = Field(
        None,
        description="Data source for end-to-end processing (S3, URL, or local file path). If not provided, uses current session data or analysis_result"
    )
    analysis_config: Optional[AnalysisConfig] = Field(
        default_factory=AnalysisConfig,
        description="Analysis configuration for performance analysis (depth, included metrics, etc.)"
    )
    analysis_result: Optional[AnalysisResult] = Field(
        None, 
        description="Existing analysis result (if provided, skips data parsing and analysis phases)"
    )

class GetOptimizationSuggestionsInput(BaseModel):
    """Input for get_optimization_suggestions tool"""
    focus_areas: List[Literal["shuffle", "resource", "performance", "configuration"]] = Field(
        default_factory=list, description="Areas to focus optimization suggestions on"
    )
    priority_filter: Optional[Literal["HIGH", "MEDIUM", "LOW"]] = Field(
        None, description="Filter suggestions by priority level"
    )