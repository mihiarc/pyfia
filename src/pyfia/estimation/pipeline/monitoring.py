"""
Pipeline Monitoring and Observability for pyFIA Phase 4.

This module provides comprehensive monitoring, performance tracking, and
observability capabilities for pipeline execution.
"""

import json
import os
import sqlite3
import threading
import time
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable, Deque

import polars as pl
from rich.console import Console
from rich.live import Live
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn,
    TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
)
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


# === Enums ===

class MetricType(str, Enum):
    """Types of metrics collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class MetricUnit(str, Enum):
    """Units for metrics."""
    COUNT = "count"
    BYTES = "bytes"
    SECONDS = "seconds"
    PERCENTAGE = "percentage"
    ROWS = "rows"


class AlertLevel(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# === Data Classes ===

@dataclass
class Metric:
    """Represents a single metric measurement."""
    
    name: str
    value: float
    type: MetricType
    unit: MetricUnit
    timestamp: float = field(default_factory=time.time)
    step_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "value": self.value,
            "type": self.type.value,
            "unit": self.unit.value,
            "timestamp": self.timestamp,
            "step_id": self.step_id,
            "tags": self.tags,
            "metadata": self.metadata
        }


@dataclass
class PerformanceSnapshot:
    """Snapshot of system performance."""
    
    timestamp: float = field(default_factory=time.time)
    cpu_percent: float = 0.0
    memory_used_bytes: int = 0
    memory_percent: float = 0.0
    disk_io_read_bytes: int = 0
    disk_io_write_bytes: int = 0
    network_sent_bytes: int = 0
    network_recv_bytes: int = 0
    process_threads: int = 0
    open_files: int = 0
    
    @classmethod
    def capture(cls) -> "PerformanceSnapshot":
        """Capture current performance snapshot."""
        snapshot = cls()
        
        if PSUTIL_AVAILABLE:
            try:
                # CPU and memory
                snapshot.cpu_percent = psutil.cpu_percent(interval=0.1)
                mem = psutil.virtual_memory()
                snapshot.memory_used_bytes = mem.used
                snapshot.memory_percent = mem.percent
                
                # Disk I/O
                disk_io = psutil.disk_io_counters()
                if disk_io:
                    snapshot.disk_io_read_bytes = disk_io.read_bytes
                    snapshot.disk_io_write_bytes = disk_io.write_bytes
                
                # Network
                net_io = psutil.net_io_counters()
                if net_io:
                    snapshot.network_sent_bytes = net_io.bytes_sent
                    snapshot.network_recv_bytes = net_io.bytes_recv
                
                # Process info
                process = psutil.Process()
                snapshot.process_threads = process.num_threads()
                snapshot.open_files = len(process.open_files())
                
            except Exception:
                pass  # Fail silently if metrics unavailable
        
        return snapshot


@dataclass
class StepMetrics:
    """Metrics for a single pipeline step."""
    
    step_id: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    # Performance metrics
    execution_time: float = 0.0
    memory_peak_bytes: int = 0
    memory_delta_bytes: int = 0
    cpu_time_seconds: float = 0.0
    
    # Data metrics
    input_rows: int = 0
    output_rows: int = 0
    rows_per_second: float = 0.0
    
    # Resource metrics
    cache_hits: int = 0
    cache_misses: int = 0
    retries: int = 0
    
    # Custom metrics
    custom_metrics: Dict[str, float] = field(default_factory=dict)
    
    @property
    def completed(self) -> bool:
        """Check if step is completed."""
        return self.end_time is not None
    
    def complete(self) -> None:
        """Mark step as completed."""
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time
        
        if self.output_rows > 0 and self.execution_time > 0:
            self.rows_per_second = self.output_rows / self.execution_time


@dataclass
class Alert:
    """Represents a monitoring alert."""
    
    level: AlertLevel
    message: str
    step_id: Optional[str] = None
    metric_name: Optional[str] = None
    threshold: Optional[float] = None
    actual_value: Optional[float] = None
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        """String representation."""
        parts = [f"[{self.level.value.upper()}] {self.message}"]
        if self.step_id:
            parts.append(f"Step: {self.step_id}")
        if self.metric_name and self.actual_value is not None:
            parts.append(f"{self.metric_name}={self.actual_value}")
        if self.threshold is not None:
            parts.append(f"Threshold: {self.threshold}")
        return " | ".join(parts)


# === Metrics Collector ===

class MetricsCollector:
    """Collects and aggregates pipeline metrics."""
    
    def __init__(self, max_history: int = 1000):
        """
        Initialize metrics collector.
        
        Parameters
        ----------
        max_history : int
            Maximum metrics to keep in history
        """
        self.max_history = max_history
        self.metrics: Deque[Metric] = deque(maxlen=max_history)
        self.aggregates: Dict[str, List[float]] = defaultdict(list)
        self.step_metrics: Dict[str, StepMetrics] = {}
        self._lock = threading.Lock()
    
    def record(
        self,
        name: str,
        value: float,
        type: MetricType = MetricType.GAUGE,
        unit: MetricUnit = MetricUnit.COUNT,
        step_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Record a metric.
        
        Parameters
        ----------
        name : str
            Metric name
        value : float
            Metric value
        type : MetricType
            Type of metric
        unit : MetricUnit
            Unit of measurement
        step_id : Optional[str]
            Associated step ID
        tags : Optional[Dict[str, str]]
            Additional tags
        """
        metric = Metric(
            name=name,
            value=value,
            type=type,
            unit=unit,
            step_id=step_id,
            tags=tags or {}
        )
        
        with self._lock:
            self.metrics.append(metric)
            self.aggregates[name].append(value)
    
    def start_step(self, step_id: str) -> None:
        """Start tracking metrics for a step."""
        with self._lock:
            self.step_metrics[step_id] = StepMetrics(step_id=step_id)
    
    def complete_step(
        self,
        step_id: str,
        input_rows: Optional[int] = None,
        output_rows: Optional[int] = None
    ) -> None:
        """Complete tracking for a step."""
        with self._lock:
            if step_id in self.step_metrics:
                metrics = self.step_metrics[step_id]
                metrics.complete()
                
                if input_rows is not None:
                    metrics.input_rows = input_rows
                if output_rows is not None:
                    metrics.output_rows = output_rows
                
                # Record step completion metrics
                self.record(
                    name="step_execution_time",
                    value=metrics.execution_time,
                    type=MetricType.TIMER,
                    unit=MetricUnit.SECONDS,
                    step_id=step_id
                )
    
    def get_step_metrics(self, step_id: str) -> Optional[StepMetrics]:
        """Get metrics for a specific step."""
        return self.step_metrics.get(step_id)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        with self._lock:
            summary = {
                "total_metrics": len(self.metrics),
                "unique_metrics": len(self.aggregates),
                "steps_tracked": len(self.step_metrics),
                "completed_steps": sum(1 for m in self.step_metrics.values() if m.completed)
            }
            
            # Calculate aggregates
            for name, values in self.aggregates.items():
                if values:
                    summary[f"{name}_avg"] = sum(values) / len(values)
                    summary[f"{name}_min"] = min(values)
                    summary[f"{name}_max"] = max(values)
                    summary[f"{name}_last"] = values[-1]
            
            return summary
    
    def get_metrics_by_step(self, step_id: str) -> List[Metric]:
        """Get all metrics for a specific step."""
        with self._lock:
            return [m for m in self.metrics if m.step_id == step_id]
    
    def clear(self) -> None:
        """Clear all metrics."""
        with self._lock:
            self.metrics.clear()
            self.aggregates.clear()
            self.step_metrics.clear()


# === Performance Monitor ===

class PerformanceMonitor:
    """Monitors system and pipeline performance."""
    
    def __init__(
        self,
        sampling_interval: float = 1.0,
        enable_profiling: bool = False
    ):
        """
        Initialize performance monitor.
        
        Parameters
        ----------
        sampling_interval : float
            Interval between performance samples (seconds)
        enable_profiling : bool
            Whether to enable detailed profiling
        """
        self.sampling_interval = sampling_interval
        self.enable_profiling = enable_profiling
        self.snapshots: Deque[PerformanceSnapshot] = deque(maxlen=1000)
        self.baseline_snapshot: Optional[PerformanceSnapshot] = None
        self._monitoring = False
        self._thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """Start performance monitoring."""
        if self._monitoring:
            return
        
        self._monitoring = True
        self.baseline_snapshot = PerformanceSnapshot.capture()
        
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop performance monitoring."""
        self._monitoring = False
        if self._thread:
            self._thread.join(timeout=2)
    
    def _monitor_loop(self) -> None:
        """Monitoring loop running in background thread."""
        while self._monitoring:
            snapshot = PerformanceSnapshot.capture()
            self.snapshots.append(snapshot)
            time.sleep(self.sampling_interval)
    
    def get_current_snapshot(self) -> Optional[PerformanceSnapshot]:
        """Get most recent performance snapshot."""
        return self.snapshots[-1] if self.snapshots else None
    
    def get_performance_delta(self) -> Dict[str, float]:
        """Get performance change since baseline."""
        if not self.baseline_snapshot or not self.snapshots:
            return {}
        
        current = self.snapshots[-1]
        baseline = self.baseline_snapshot
        
        return {
            "memory_delta_bytes": current.memory_used_bytes - baseline.memory_used_bytes,
            "cpu_avg_percent": sum(s.cpu_percent for s in self.snapshots) / len(self.snapshots),
            "disk_io_read_delta": current.disk_io_read_bytes - baseline.disk_io_read_bytes,
            "disk_io_write_delta": current.disk_io_write_bytes - baseline.disk_io_write_bytes,
            "network_sent_delta": current.network_sent_bytes - baseline.network_sent_bytes,
            "network_recv_delta": current.network_recv_bytes - baseline.network_recv_bytes
        }
    
    @contextmanager
    def profile_step(self, step_id: str):
        """Context manager for profiling a step."""
        if not self.enable_profiling:
            yield
            return
        
        start_snapshot = PerformanceSnapshot.capture()
        start_time = time.time()
        
        try:
            yield
        finally:
            end_snapshot = PerformanceSnapshot.capture()
            duration = time.time() - start_time
            
            # Calculate deltas
            memory_delta = end_snapshot.memory_used_bytes - start_snapshot.memory_used_bytes
            disk_read_delta = end_snapshot.disk_io_read_bytes - start_snapshot.disk_io_read_bytes
            disk_write_delta = end_snapshot.disk_io_write_bytes - start_snapshot.disk_io_write_bytes
            
            # Store profiling results
            if hasattr(self, "_profiling_results"):
                self._profiling_results[step_id] = {
                    "duration": duration,
                    "memory_delta": memory_delta,
                    "disk_read": disk_read_delta,
                    "disk_write": disk_write_delta
                }


# === Alert Manager ===

class AlertManager:
    """Manages monitoring alerts and thresholds."""
    
    def __init__(self):
        """Initialize alert manager."""
        self.alerts: List[Alert] = []
        self.thresholds: Dict[str, Tuple[float, AlertLevel]] = {}
        self.alert_handlers: List[Callable[[Alert], None]] = []
        self._lock = threading.Lock()
    
    def set_threshold(
        self,
        metric_name: str,
        threshold: float,
        level: AlertLevel = AlertLevel.WARNING
    ) -> None:
        """
        Set alert threshold for a metric.
        
        Parameters
        ----------
        metric_name : str
            Name of metric
        threshold : float
            Threshold value
        level : AlertLevel
            Alert level when threshold exceeded
        """
        self.thresholds[metric_name] = (threshold, level)
    
    def check_threshold(
        self,
        metric_name: str,
        value: float,
        step_id: Optional[str] = None
    ) -> Optional[Alert]:
        """
        Check if metric exceeds threshold.
        
        Parameters
        ----------
        metric_name : str
            Metric name
        value : float
            Metric value
        step_id : Optional[str]
            Associated step
            
        Returns
        -------
        Optional[Alert]
            Alert if threshold exceeded
        """
        if metric_name not in self.thresholds:
            return None
        
        threshold, level = self.thresholds[metric_name]
        
        if value > threshold:
            alert = Alert(
                level=level,
                message=f"{metric_name} exceeded threshold",
                step_id=step_id,
                metric_name=metric_name,
                threshold=threshold,
                actual_value=value
            )
            
            self.add_alert(alert)
            return alert
        
        return None
    
    def add_alert(self, alert: Alert) -> None:
        """Add an alert."""
        with self._lock:
            self.alerts.append(alert)
            
            # Trigger handlers
            for handler in self.alert_handlers:
                try:
                    handler(alert)
                except Exception:
                    pass  # Don't let handler errors break monitoring
    
    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        """Add alert handler."""
        self.alert_handlers.append(handler)
    
    def get_recent_alerts(
        self,
        limit: int = 10,
        level: Optional[AlertLevel] = None
    ) -> List[Alert]:
        """Get recent alerts."""
        with self._lock:
            alerts = self.alerts
            
            if level:
                alerts = [a for a in alerts if a.level == level]
            
            return sorted(alerts, key=lambda a: a.timestamp, reverse=True)[:limit]
    
    def clear_alerts(self) -> None:
        """Clear all alerts."""
        with self._lock:
            self.alerts.clear()


# === Execution History ===

class ExecutionHistory:
    """Tracks and persists pipeline execution history."""
    
    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize execution history.
        
        Parameters
        ----------
        db_path : Optional[Path]
            Path to SQLite database for persistence
        """
        self.db_path = db_path or Path.home() / ".pyfia" / "execution_history.db"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS executions (
                    execution_id TEXT PRIMARY KEY,
                    pipeline_id TEXT,
                    start_time REAL,
                    end_time REAL,
                    duration REAL,
                    status TEXT,
                    steps_completed INTEGER,
                    steps_total INTEGER,
                    error_message TEXT,
                    metadata TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS step_executions (
                    execution_id TEXT,
                    step_id TEXT,
                    start_time REAL,
                    end_time REAL,
                    duration REAL,
                    status TEXT,
                    input_rows INTEGER,
                    output_rows INTEGER,
                    memory_used INTEGER,
                    error_message TEXT,
                    PRIMARY KEY (execution_id, step_id),
                    FOREIGN KEY (execution_id) REFERENCES executions(execution_id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    execution_id TEXT,
                    step_id TEXT,
                    metric_name TEXT,
                    metric_value REAL,
                    metric_type TEXT,
                    timestamp REAL,
                    FOREIGN KEY (execution_id) REFERENCES executions(execution_id)
                )
            """)
    
    def record_execution(
        self,
        execution_id: str,
        pipeline_id: str,
        start_time: float,
        end_time: float,
        status: str,
        steps_completed: int,
        steps_total: int,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record pipeline execution."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO executions
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                execution_id,
                pipeline_id,
                start_time,
                end_time,
                end_time - start_time,
                status,
                steps_completed,
                steps_total,
                error_message,
                json.dumps(metadata or {})
            ))
    
    def record_step_execution(
        self,
        execution_id: str,
        step_metrics: StepMetrics,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """Record step execution."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO step_executions
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                execution_id,
                step_metrics.step_id,
                step_metrics.start_time,
                step_metrics.end_time,
                step_metrics.execution_time,
                status,
                step_metrics.input_rows,
                step_metrics.output_rows,
                step_metrics.memory_peak_bytes,
                error_message
            ))
    
    def record_metric(
        self,
        execution_id: str,
        metric: Metric
    ) -> None:
        """Record a metric."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO metrics
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                execution_id,
                metric.step_id,
                metric.name,
                metric.value,
                metric.type.value,
                metric.timestamp
            ))
    
    def get_execution_summary(
        self,
        pipeline_id: Optional[str] = None,
        limit: int = 10
    ) -> pl.DataFrame:
        """Get execution summary."""
        query = """
            SELECT 
                execution_id,
                pipeline_id,
                datetime(start_time, 'unixepoch') as start_time,
                duration,
                status,
                steps_completed,
                steps_total,
                ROUND(100.0 * steps_completed / steps_total, 1) as completion_rate
            FROM executions
        """
        
        if pipeline_id:
            query += f" WHERE pipeline_id = '{pipeline_id}'"
        
        query += f" ORDER BY start_time DESC LIMIT {limit}"
        
        with sqlite3.connect(self.db_path) as conn:
            return pl.read_database(query, conn)
    
    def get_performance_trends(
        self,
        pipeline_id: str,
        metric_name: str = "execution_time"
    ) -> pl.DataFrame:
        """Get performance trends over time."""
        query = f"""
            SELECT 
                e.execution_id,
                datetime(e.start_time, 'unixepoch') as execution_time,
                m.metric_value as {metric_name}
            FROM executions e
            JOIN metrics m ON e.execution_id = m.execution_id
            WHERE e.pipeline_id = '{pipeline_id}'
              AND m.metric_name = '{metric_name}'
            ORDER BY e.start_time
        """
        
        with sqlite3.connect(self.db_path) as conn:
            return pl.read_database(query, conn)


# === Rich Progress Display ===

class RichProgressDisplay:
    """Rich console display for pipeline progress."""
    
    def __init__(self):
        """Initialize progress display."""
        self.console = Console()
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console
        )
        self.layout = Layout()
        self.live: Optional[Live] = None
        
        # Task tracking
        self.main_task: Optional[int] = None
        self.step_tasks: Dict[str, int] = {}
    
    def start(self, total_steps: int) -> None:
        """Start progress display."""
        # Set up layout
        self.layout.split_column(
            Layout(name="header", size=3),
            Layout(name="progress", size=10),
            Layout(name="metrics", size=10),
            Layout(name="alerts", size=5)
        )
        
        # Create main progress task
        self.main_task = self.progress.add_task(
            "[cyan]Pipeline Execution",
            total=total_steps
        )
        
        # Start live display
        self.live = Live(self.layout, console=self.console, refresh_per_second=2)
        self.live.start()
        
        # Update header
        self._update_header("Pipeline Starting")
    
    def stop(self) -> None:
        """Stop progress display."""
        if self.live:
            self.live.stop()
    
    def start_step(self, step_id: str, description: str) -> None:
        """Start tracking a step."""
        task_id = self.progress.add_task(
            f"[yellow]{description}",
            total=100
        )
        self.step_tasks[step_id] = task_id
    
    def update_step(self, step_id: str, progress: float, message: Optional[str] = None) -> None:
        """Update step progress."""
        if step_id in self.step_tasks:
            task_id = self.step_tasks[step_id]
            self.progress.update(task_id, completed=progress * 100)
            
            if message:
                self.progress.update(task_id, description=f"[yellow]{message}")
    
    def complete_step(self, step_id: str) -> None:
        """Mark step as complete."""
        if step_id in self.step_tasks:
            task_id = self.step_tasks[step_id]
            self.progress.update(task_id, completed=100)
        
        if self.main_task is not None:
            self.progress.advance(self.main_task)
    
    def update_metrics(self, metrics: Dict[str, Any]) -> None:
        """Update metrics display."""
        table = Table(title="Performance Metrics", show_header=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        for key, value in metrics.items():
            if isinstance(value, float):
                table.add_row(key, f"{value:.2f}")
            else:
                table.add_row(key, str(value))
        
        self.layout["metrics"].update(Panel(table))
    
    def show_alert(self, alert: Alert) -> None:
        """Display an alert."""
        color = {
            AlertLevel.INFO: "blue",
            AlertLevel.WARNING: "yellow",
            AlertLevel.ERROR: "red",
            AlertLevel.CRITICAL: "bold red"
        }.get(alert.level, "white")
        
        alert_text = Text(str(alert), style=color)
        self.layout["alerts"].update(Panel(alert_text, title="Alerts"))
    
    def _update_header(self, message: str) -> None:
        """Update header message."""
        header_text = Text(message, style="bold cyan", justify="center")
        self.layout["header"].update(Panel(header_text))
    
    def update(self) -> None:
        """Update the display."""
        self.layout["progress"].update(Panel(self.progress))


# === Pipeline Monitor ===

class PipelineMonitor:
    """
    Comprehensive pipeline monitoring orchestrator.
    
    Coordinates metrics collection, performance monitoring, alerts,
    and progress display for pipeline execution.
    """
    
    def __init__(
        self,
        enable_metrics: bool = True,
        enable_performance: bool = True,
        enable_alerts: bool = True,
        enable_history: bool = True,
        enable_display: bool = True,
        db_path: Optional[Path] = None
    ):
        """
        Initialize pipeline monitor.
        
        Parameters
        ----------
        enable_metrics : bool
            Enable metrics collection
        enable_performance : bool
            Enable performance monitoring
        enable_alerts : bool
            Enable alert management
        enable_history : bool
            Enable execution history
        enable_display : bool
            Enable rich progress display
        db_path : Optional[Path]
            Path for history database
        """
        self.enable_metrics = enable_metrics
        self.enable_performance = enable_performance
        self.enable_alerts = enable_alerts
        self.enable_history = enable_history
        self.enable_display = enable_display
        
        # Initialize components
        self.metrics_collector = MetricsCollector() if enable_metrics else None
        self.performance_monitor = PerformanceMonitor() if enable_performance else None
        self.alert_manager = AlertManager() if enable_alerts else None
        self.execution_history = ExecutionHistory(db_path) if enable_history else None
        self.progress_display = RichProgressDisplay() if enable_display else None
        
        # Set up alert handlers
        if self.alert_manager and self.progress_display:
            self.alert_manager.add_handler(self.progress_display.show_alert)
        
        # Execution state
        self.current_execution_id: Optional[str] = None
        self.pipeline_id: Optional[str] = None
        self.start_time: Optional[float] = None
    
    def start_pipeline(
        self,
        pipeline_id: str,
        execution_id: str,
        total_steps: int
    ) -> None:
        """Start monitoring a pipeline execution."""
        self.pipeline_id = pipeline_id
        self.current_execution_id = execution_id
        self.start_time = time.time()
        
        # Start components
        if self.performance_monitor:
            self.performance_monitor.start()
        
        if self.progress_display:
            self.progress_display.start(total_steps)
        
        # Set default alert thresholds
        if self.alert_manager:
            self.alert_manager.set_threshold("memory_percent", 80, AlertLevel.WARNING)
            self.alert_manager.set_threshold("memory_percent", 95, AlertLevel.CRITICAL)
            self.alert_manager.set_threshold("step_execution_time", 300, AlertLevel.WARNING)
            self.alert_manager.set_threshold("cpu_percent", 90, AlertLevel.WARNING)
    
    def start_step(self, step_id: str, description: str) -> None:
        """Start monitoring a step."""
        if self.metrics_collector:
            self.metrics_collector.start_step(step_id)
        
        if self.progress_display:
            self.progress_display.start_step(step_id, description)
    
    def update_step_progress(
        self,
        step_id: str,
        progress: float,
        message: Optional[str] = None
    ) -> None:
        """Update step progress."""
        if self.progress_display:
            self.progress_display.update_step(step_id, progress, message)
    
    def complete_step(
        self,
        step_id: str,
        status: str = "completed",
        input_rows: Optional[int] = None,
        output_rows: Optional[int] = None,
        error: Optional[str] = None
    ) -> None:
        """Complete monitoring for a step."""
        # Complete metrics collection
        if self.metrics_collector:
            self.metrics_collector.complete_step(step_id, input_rows, output_rows)
            
            # Check execution time threshold
            step_metrics = self.metrics_collector.get_step_metrics(step_id)
            if step_metrics and self.alert_manager:
                self.alert_manager.check_threshold(
                    "step_execution_time",
                    step_metrics.execution_time,
                    step_id
                )
        
        # Update progress display
        if self.progress_display:
            self.progress_display.complete_step(step_id)
        
        # Record in history
        if self.execution_history and self.metrics_collector:
            step_metrics = self.metrics_collector.get_step_metrics(step_id)
            if step_metrics:
                self.execution_history.record_step_execution(
                    self.current_execution_id,
                    step_metrics,
                    status,
                    error
                )
    
    def record_metric(
        self,
        name: str,
        value: float,
        step_id: Optional[str] = None,
        check_threshold: bool = True
    ) -> None:
        """Record a metric."""
        if self.metrics_collector:
            self.metrics_collector.record(
                name=name,
                value=value,
                step_id=step_id
            )
        
        # Check thresholds
        if check_threshold and self.alert_manager:
            self.alert_manager.check_threshold(name, value, step_id)
        
        # Record in history
        if self.execution_history:
            metric = Metric(
                name=name,
                value=value,
                type=MetricType.GAUGE,
                unit=MetricUnit.COUNT,
                step_id=step_id
            )
            self.execution_history.record_metric(self.current_execution_id, metric)
    
    def complete_pipeline(
        self,
        status: str = "completed",
        error: Optional[str] = None
    ) -> None:
        """Complete pipeline monitoring."""
        end_time = time.time()
        
        # Stop performance monitoring
        if self.performance_monitor:
            self.performance_monitor.stop()
        
        # Record execution in history
        if self.execution_history and self.metrics_collector:
            steps_total = len(self.metrics_collector.step_metrics)
            steps_completed = sum(
                1 for m in self.metrics_collector.step_metrics.values()
                if m.completed
            )
            
            self.execution_history.record_execution(
                execution_id=self.current_execution_id,
                pipeline_id=self.pipeline_id,
                start_time=self.start_time,
                end_time=end_time,
                status=status,
                steps_completed=steps_completed,
                steps_total=steps_total,
                error_message=error
            )
        
        # Stop progress display
        if self.progress_display:
            self.progress_display.stop()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get monitoring summary."""
        summary = {
            "pipeline_id": self.pipeline_id,
            "execution_id": self.current_execution_id
        }
        
        if self.metrics_collector:
            summary.update(self.metrics_collector.get_summary())
        
        if self.performance_monitor:
            summary.update(self.performance_monitor.get_performance_delta())
        
        if self.alert_manager:
            summary["alerts"] = len(self.alert_manager.alerts)
            summary["critical_alerts"] = len(
                self.alert_manager.get_recent_alerts(level=AlertLevel.CRITICAL)
            )
        
        return summary
    
    @contextmanager
    def monitor_step(self, step_id: str, description: str):
        """Context manager for monitoring a step."""
        self.start_step(step_id, description)
        
        try:
            if self.performance_monitor:
                with self.performance_monitor.profile_step(step_id):
                    yield
            else:
                yield
        finally:
            self.complete_step(step_id)


# === Export public API ===

__all__ = [
    "PipelineMonitor",
    "MetricsCollector",
    "PerformanceMonitor",
    "AlertManager",
    "ExecutionHistory",
    "RichProgressDisplay",
    "Metric",
    "MetricType",
    "MetricUnit",
    "Alert",
    "AlertLevel",
    "StepMetrics",
    "PerformanceSnapshot"
]