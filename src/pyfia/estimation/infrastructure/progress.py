"""
Progress tracking for pyFIA lazy evaluation operations.

This module provides Rich-based progress tracking for lazy operations,
including collection progress, operation tracking, and performance monitoring.
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Callable
import threading
import time
from enum import Enum, auto

import polars as pl
from rich.console import Console
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn,
    ProgressColumn, Task
)
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text


class OperationType(Enum):
    """Types of operations for progress tracking."""
    LOAD_DATA = auto()
    FILTER = auto()
    JOIN = auto()
    AGGREGATE = auto()
    COLLECT = auto()
    WRITE_CACHE = auto()
    READ_CACHE = auto()
    OPTIMIZE = auto()
    COMPUTE = auto()


@dataclass
class OperationMetrics:
    """Metrics for a single operation."""
    
    operation_type: OperationType
    name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    rows_processed: Optional[int] = None
    rows_output: Optional[int] = None
    memory_used_mb: Optional[float] = None
    error: Optional[str] = None
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get operation duration."""
        if self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return datetime.now() - self.start_time
        return None
    
    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        dur = self.duration
        return dur.total_seconds() if dur else 0.0
    
    @property
    def rows_per_second(self) -> Optional[float]:
        """Calculate rows processed per second."""
        if self.rows_processed and self.duration_seconds > 0:
            return self.rows_processed / self.duration_seconds
        return None
    
    def complete(self, rows_output: Optional[int] = None,
                memory_used_mb: Optional[float] = None):
        """Mark operation as complete."""
        self.end_time = datetime.now()
        if rows_output is not None:
            self.rows_output = rows_output
        if memory_used_mb is not None:
            self.memory_used_mb = memory_used_mb
    
    def fail(self, error: str):
        """Mark operation as failed."""
        self.end_time = datetime.now()
        self.error = error


class LazyOperationProgress:
    """
    Progress tracker for lazy evaluation operations using Rich.
    
    This class provides visual progress tracking for long-running operations,
    including data loading, filtering, aggregation, and collection.
    """
    
    def __init__(self, console: Optional[Console] = None,
                 show_details: bool = True,
                 auto_refresh: bool = True):
        """
        Initialize progress tracker.
        
        Parameters
        ----------
        console : Optional[Console]
            Rich console instance
        show_details : bool
            Whether to show detailed progress information
        auto_refresh : bool
            Whether to auto-refresh progress display
        """
        self.console = console or Console()
        self.show_details = show_details
        self.auto_refresh = auto_refresh
        
        # Progress tracking
        self._operations: List[OperationMetrics] = []
        self._active_operations: Dict[str, OperationMetrics] = {}
        self._progress: Optional[Progress] = None
        self._tasks: Dict[str, int] = {}
        
        # Performance statistics
        self._total_rows_processed = 0
        self._total_time_seconds = 0.0
        self._total_memory_mb = 0.0
    
    def start_operation(self, operation_type: OperationType,
                       name: str,
                       total: Optional[int] = None) -> str:
        """
        Start tracking a new operation.
        
        Parameters
        ----------
        operation_type : OperationType
            Type of operation
        name : str
            Operation name/description
        total : Optional[int]
            Total items to process (for progress bar)
            
        Returns
        -------
        str
            Operation ID
        """
        # Create operation metrics
        operation = OperationMetrics(
            operation_type=operation_type,
            name=name,
            start_time=datetime.now()
        )
        
        # Generate operation ID
        op_id = f"{operation_type.name}_{len(self._operations)}"
        
        # Track operation
        self._operations.append(operation)
        self._active_operations[op_id] = operation
        
        # Add to progress display if active
        if self._progress:
            task_id = self._progress.add_task(
                f"[cyan]{name}",
                total=total or 100
            )
            self._tasks[op_id] = task_id
        
        return op_id
    
    def update_operation(self, op_id: str,
                        advance: Optional[int] = None,
                        completed: Optional[int] = None,
                        description: Optional[str] = None):
        """
        Update operation progress.
        
        Parameters
        ----------
        op_id : str
            Operation ID
        advance : Optional[int]
            Amount to advance progress
        completed : Optional[int]
            Absolute progress value
        description : Optional[str]
            Update operation description
        """
        if op_id not in self._active_operations:
            return
        
        # Update progress display
        if self._progress and op_id in self._tasks:
            task_id = self._tasks[op_id]
            
            if description:
                self._progress.update(task_id, description=f"[cyan]{description}")
            
            if advance is not None:
                self._progress.advance(task_id, advance)
            elif completed is not None:
                self._progress.update(task_id, completed=completed)
    
    def complete_operation(self, op_id: str,
                         rows_output: Optional[int] = None,
                         memory_used_mb: Optional[float] = None):
        """
        Mark operation as complete.
        
        Parameters
        ----------
        op_id : str
            Operation ID
        rows_output : Optional[int]
            Number of rows in output
        memory_used_mb : Optional[float]
            Memory used in MB
        """
        if op_id not in self._active_operations:
            return
        
        operation = self._active_operations.pop(op_id)
        operation.complete(rows_output, memory_used_mb)
        
        # Update statistics
        if operation.rows_processed:
            self._total_rows_processed += operation.rows_processed
        self._total_time_seconds += operation.duration_seconds
        if memory_used_mb:
            self._total_memory_mb = max(self._total_memory_mb, memory_used_mb)
        
        # Complete progress task
        if self._progress and op_id in self._tasks:
            task_id = self._tasks.pop(op_id)
            self._progress.update(task_id, completed=100)
            self._progress.remove_task(task_id)
    
    def fail_operation(self, op_id: str, error: str):
        """
        Mark operation as failed.
        
        Parameters
        ----------
        op_id : str
            Operation ID
        error : str
            Error message
        """
        if op_id not in self._active_operations:
            return
        
        operation = self._active_operations.pop(op_id)
        operation.fail(error)
        
        # Update progress display
        if self._progress and op_id in self._tasks:
            task_id = self._tasks.pop(op_id)
            self._progress.update(
                task_id,
                description=f"[red]Failed: {operation.name}"
            )
            self._progress.remove_task(task_id)
    
    @contextmanager
    def track_operation(self, operation_type: OperationType,
                       name: str,
                       total: Optional[int] = None):
        """
        Context manager for tracking an operation.
        
        Parameters
        ----------
        operation_type : OperationType
            Type of operation
        name : str
            Operation name
        total : Optional[int]
            Total items to process
        """
        op_id = self.start_operation(operation_type, name, total)
        
        try:
            yield op_id
            self.complete_operation(op_id)
        except Exception as e:
            self.fail_operation(op_id, str(e))
            raise
    
    @contextmanager
    def progress_display(self):
        """Context manager for progress display."""
        # Create progress instance
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self.console,
            refresh_per_second=10 if self.auto_refresh else 1
        )
        
        try:
            with self._progress:
                yield self
        finally:
            self._progress = None
            self._tasks.clear()
    
    def get_summary_table(self) -> Table:
        """
        Get summary table of all operations.
        
        Returns
        -------
        Table
            Rich table with operation summary
        """
        table = Table(title="Operation Summary", show_header=True)
        
        table.add_column("Operation", style="cyan")
        table.add_column("Duration", justify="right")
        table.add_column("Rows In", justify="right")
        table.add_column("Rows Out", justify="right")
        table.add_column("Rows/sec", justify="right")
        table.add_column("Memory MB", justify="right")
        table.add_column("Status", justify="center")
        
        for op in self._operations:
            status = "[red]Failed" if op.error else "[green]Complete"
            if op.end_time is None:
                status = "[yellow]Running"
            
            table.add_row(
                op.name,
                f"{op.duration_seconds:.1f}s" if op.duration else "-",
                f"{op.rows_processed:,}" if op.rows_processed else "-",
                f"{op.rows_output:,}" if op.rows_output else "-",
                f"{op.rows_per_second:,.0f}" if op.rows_per_second else "-",
                f"{op.memory_used_mb:.1f}" if op.memory_used_mb else "-",
                status
            )
        
        return table
    
    def print_summary(self):
        """Print operation summary to console."""
        self.console.print("\n")
        self.console.print(self.get_summary_table())
        
        # Print overall statistics
        if self._operations:
            stats_text = Text()
            stats_text.append("\nOverall Statistics:\n", style="bold")
            stats_text.append(f"Total operations: {len(self._operations)}\n")
            stats_text.append(f"Total time: {self._total_time_seconds:.1f}s\n")
            stats_text.append(f"Total rows: {self._total_rows_processed:,}\n")
            
            if self._total_time_seconds > 0:
                overall_rate = self._total_rows_processed / self._total_time_seconds
                stats_text.append(f"Average rate: {overall_rate:,.0f} rows/sec\n")
            
            stats_text.append(f"Peak memory: {self._total_memory_mb:.1f} MB\n")
            
            self.console.print(Panel(stats_text, title="Performance Summary"))


class EstimatorProgressMixin:
    """
    Mixin class that adds progress tracking to estimators.
    
    This mixin integrates with LazyOperationProgress to provide
    automatic progress tracking for estimator operations.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize progress tracking."""
        super().__init__(*args, **kwargs)
        
        # Progress tracking
        self._progress_tracker: Optional[LazyOperationProgress] = None
        self._enable_progress = getattr(kwargs.get('config', {}), 
                                      'show_progress', True)
        
        # Operation tracking
        self._current_operations: List[str] = []
    
    def enable_progress_tracking(self, console: Optional[Console] = None):
        """
        Enable progress tracking.
        
        Parameters
        ----------
        console : Optional[Console]
            Rich console instance
        """
        self._progress_tracker = LazyOperationProgress(console)
        self._enable_progress = True
    
    def disable_progress_tracking(self):
        """Disable progress tracking."""
        self._progress_tracker = None
        self._enable_progress = False
    
    @contextmanager
    def _track_operation(self, operation_type: OperationType,
                        name: str,
                        total: Optional[int] = None):
        """
        Track an operation with progress.
        
        Parameters
        ----------
        operation_type : OperationType
            Type of operation
        name : str
            Operation name
        total : Optional[int]
            Total items
        """
        if self._progress_tracker and self._enable_progress:
            with self._progress_tracker.track_operation(
                operation_type, name, total
            ) as op_id:
                self._current_operations.append(op_id)
                try:
                    yield op_id
                finally:
                    self._current_operations.remove(op_id)
        else:
            yield None
    
    def _update_progress(self, advance: Optional[int] = None,
                        completed: Optional[int] = None,
                        description: Optional[str] = None):
        """Update progress for current operation."""
        if (self._progress_tracker and 
            self._enable_progress and 
            self._current_operations):
            
            op_id = self._current_operations[-1]
            self._progress_tracker.update_operation(
                op_id, advance, completed, description
            )
    
    @contextmanager
    def progress_context(self):
        """Context manager for entire estimation with progress."""
        if self._enable_progress and self._progress_tracker:
            with self._progress_tracker.progress_display():
                try:
                    yield
                finally:
                    # Print summary
                    self._progress_tracker.print_summary()
        else:
            yield


class CollectionProgress:
    """
    Specialized progress tracker for lazy frame collection.
    
    This class monitors the collection of lazy frames, providing
    feedback on memory usage and collection time.
    """
    
    def __init__(self, console: Optional[Console] = None):
        """
        Initialize collection progress tracker.
        
        Parameters
        ----------
        console : Optional[Console]
            Rich console instance
        """
        self.console = console or Console()
        self._collections: List[Dict[str, Any]] = []
        self._active_collection: Optional[Dict[str, Any]] = None
    
    @contextmanager
    def track_collection(self, frame_name: str,
                        estimated_rows: Optional[int] = None):
        """
        Track a lazy frame collection.
        
        Parameters
        ----------
        frame_name : str
            Name of the frame being collected
        estimated_rows : Optional[int]
            Estimated number of rows
        """
        # Start collection tracking
        collection_info = {
            "name": frame_name,
            "start_time": datetime.now(),
            "estimated_rows": estimated_rows,
            "actual_rows": None,
            "duration_seconds": None,
            "memory_before_mb": self._get_memory_usage(),
            "memory_after_mb": None,
            "memory_delta_mb": None
        }
        
        self._active_collection = collection_info
        
        # Show collection message
        if estimated_rows:
            self.console.print(
                f"[cyan]Collecting {frame_name} "
                f"(~{estimated_rows:,} rows)...[/cyan]"
            )
        else:
            self.console.print(f"[cyan]Collecting {frame_name}...[/cyan]")
        
        start_time = time.time()
        
        try:
            yield
            
            # Complete collection
            end_time = time.time()
            collection_info["duration_seconds"] = end_time - start_time
            collection_info["memory_after_mb"] = self._get_memory_usage()
            collection_info["memory_delta_mb"] = (
                collection_info["memory_after_mb"] - 
                collection_info["memory_before_mb"]
            )
            
            self._collections.append(collection_info)
            
            # Show completion message
            self.console.print(
                f"[green]✓ Collected {frame_name} in "
                f"{collection_info['duration_seconds']:.1f}s "
                f"(+{collection_info['memory_delta_mb']:.1f} MB)[/green]"
            )
            
        except Exception as e:
            collection_info["error"] = str(e)
            self._collections.append(collection_info)
            
            self.console.print(
                f"[red]✗ Failed to collect {frame_name}: {e}[/red]"
            )
            raise
        
        finally:
            self._active_collection = None
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except ImportError:
            return 0.0
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics about collections."""
        if not self._collections:
            return {}
        
        total_time = sum(c["duration_seconds"] or 0 for c in self._collections)
        total_memory = sum(c["memory_delta_mb"] or 0 for c in self._collections)
        
        return {
            "total_collections": len(self._collections),
            "total_time_seconds": total_time,
            "total_memory_mb": total_memory,
            "average_time_seconds": total_time / len(self._collections),
            "average_memory_mb": total_memory / len(self._collections),
            "collections": self._collections
        }


class ProgressConfig:
    """Configuration for progress tracking behavior."""
    
    def __init__(self,
                 show_progress: bool = True,
                 show_details: bool = True,
                 show_memory: bool = True,
                 show_summary: bool = True,
                 update_frequency: float = 0.1):
        """
        Initialize progress configuration.
        
        Parameters
        ----------
        show_progress : bool
            Whether to show progress bars
        show_details : bool
            Whether to show detailed progress info
        show_memory : bool
            Whether to track memory usage
        show_summary : bool
            Whether to show summary at end
        update_frequency : float
            Update frequency in seconds
        """
        self.show_progress = show_progress
        self.show_details = show_details
        self.show_memory = show_memory
        self.show_summary = show_summary
        self.update_frequency = update_frequency


def create_progress_layout() -> Layout:
    """
    Create a Rich layout for progress display.
    
    Returns
    -------
    Layout
        Rich layout for progress tracking
    """
    layout = Layout()
    
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="progress", size=10),
        Layout(name="stats", size=5),
        Layout(name="footer", size=3)
    )
    
    return layout


class RealTimeProgressMonitor:
    """
    Real-time progress monitor with live updates.
    
    This class provides a live-updating display of estimation progress
    with multiple panels showing different aspects of the computation.
    """
    
    def __init__(self, console: Optional[Console] = None):
        """
        Initialize real-time monitor.
        
        Parameters
        ----------
        console : Optional[Console]
            Rich console instance
        """
        self.console = console or Console()
        self._layout = create_progress_layout()
        self._live: Optional[Live] = None
        self._update_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        
        # Data for display
        self._current_operation = "Initializing..."
        self._operations_completed = 0
        self._total_operations = 0
        self._start_time = datetime.now()
        self._memory_usage_mb = 0.0
        self._rows_processed = 0
    
    def start(self):
        """Start the real-time monitor."""
        self._live = Live(
            self._layout,
            console=self.console,
            refresh_per_second=4
        )
        
        self._live.start()
        
        # Start update thread
        self._update_thread = threading.Thread(
            target=self._update_loop,
            daemon=True
        )
        self._update_thread.start()
    
    def stop(self):
        """Stop the real-time monitor."""
        self._stop_event.set()
        
        if self._update_thread:
            self._update_thread.join(timeout=1)
        
        if self._live:
            self._live.stop()
    
    def update_operation(self, operation: str,
                        completed: int,
                        total: int):
        """Update current operation info."""
        self._current_operation = operation
        self._operations_completed = completed
        self._total_operations = total
    
    def update_stats(self, rows_processed: int,
                    memory_usage_mb: float):
        """Update statistics."""
        self._rows_processed = rows_processed
        self._memory_usage_mb = memory_usage_mb
    
    def _update_loop(self):
        """Update loop for live display."""
        while not self._stop_event.is_set():
            self._update_display()
            time.sleep(0.25)
    
    def _update_display(self):
        """Update the display layout."""
        # Header
        elapsed = (datetime.now() - self._start_time).total_seconds()
        header_text = Text()
        header_text.append("pyFIA Estimation Progress\n", style="bold cyan")
        header_text.append(f"Elapsed: {elapsed:.1f}s", style="dim")
        
        self._layout["header"].update(
            Panel(header_text, border_style="cyan")
        )
        
        # Progress
        progress_text = Text()
        progress_text.append(f"Current: {self._current_operation}\n")
        progress_text.append(
            f"Progress: {self._operations_completed}/{self._total_operations}"
        )
        
        self._layout["progress"].update(
            Panel(progress_text, title="Operations", border_style="green")
        )
        
        # Stats
        stats_text = Text()
        stats_text.append(f"Rows processed: {self._rows_processed:,}\n")
        stats_text.append(f"Memory usage: {self._memory_usage_mb:.1f} MB\n")
        
        if elapsed > 0:
            rate = self._rows_processed / elapsed
            stats_text.append(f"Processing rate: {rate:,.0f} rows/sec")
        
        self._layout["stats"].update(
            Panel(stats_text, title="Statistics", border_style="yellow")
        )
        
        # Footer
        footer_text = Text("Press Ctrl+C to stop", style="dim")
        self._layout["footer"].update(
            Panel(footer_text, border_style="dim")
        )