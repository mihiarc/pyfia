"""
Timing utilities for benchmarking.

Provides consistent timing infrastructure across all benchmark tests.
"""

import gc
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from rich.console import Console
from rich.table import Table

console = Console()


@dataclass
class TimingResult:
    """Result of a single benchmark run."""

    name: str
    tool: str  # "pyfia", "rfia", "evalidator"
    iterations: int
    times_ms: List[float]
    mean_ms: float = field(init=False)
    std_ms: float = field(init=False)
    min_ms: float = field(init=False)
    max_ms: float = field(init=False)
    median_ms: float = field(init=False)
    cold_start_ms: Optional[float] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.times_ms:
            self.mean_ms = statistics.mean(self.times_ms)
            self.std_ms = statistics.stdev(self.times_ms) if len(self.times_ms) > 1 else 0.0
            self.min_ms = min(self.times_ms)
            self.max_ms = max(self.times_ms)
            self.median_ms = statistics.median(self.times_ms)
        else:
            self.mean_ms = 0.0
            self.std_ms = 0.0
            self.min_ms = 0.0
            self.max_ms = 0.0
            self.median_ms = 0.0


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results for comparison."""

    name: str
    description: str
    results: List[TimingResult] = field(default_factory=list)

    def add_result(self, result: TimingResult):
        self.results.append(result)

    def get_comparison(self) -> Dict[str, Dict[str, float]]:
        """Get comparison ratios between tools."""
        by_tool = {}
        for r in self.results:
            if r.tool not in by_tool:
                by_tool[r.tool] = []
            by_tool[r.tool].append(r)

        # Calculate speedup ratios relative to slowest
        comparisons = {}
        tools = list(by_tool.keys())

        if "pyfia" in tools:
            pyfia_results = by_tool["pyfia"]
            for tool in tools:
                if tool != "pyfia":
                    tool_results = by_tool[tool]
                    # Match by benchmark name
                    for pr in pyfia_results:
                        for tr in tool_results:
                            if pr.name == tr.name:
                                key = f"{pr.name}_vs_{tool}"
                                if tr.mean_ms > 0:
                                    comparisons[key] = {
                                        "pyfia_ms": pr.mean_ms,
                                        f"{tool}_ms": tr.mean_ms,
                                        "speedup": tr.mean_ms / pr.mean_ms,
                                    }
        return comparisons


def benchmark_function(
    func: Callable,
    name: str,
    tool: str,
    iterations: int = 10,
    warmup: int = 2,
    gc_collect: bool = True,
) -> TimingResult:
    """
    Benchmark a function with proper methodology.

    Parameters
    ----------
    func : Callable
        Function to benchmark (should take no arguments)
    name : str
        Name of the benchmark
    tool : str
        Tool being benchmarked ("pyfia", "rfia", "evalidator")
    iterations : int
        Number of timed iterations
    warmup : int
        Number of warmup iterations (not timed)
    gc_collect : bool
        Whether to run garbage collection before each iteration

    Returns
    -------
    TimingResult
        Benchmark results including timing statistics
    """
    times = []
    cold_start = None
    error = None

    try:
        # Cold start measurement (first ever call)
        if gc_collect:
            gc.collect()
        start = time.perf_counter()
        _ = func()
        cold_start = (time.perf_counter() - start) * 1000

        # Warmup iterations (not timed, but executed)
        for _ in range(warmup - 1):  # -1 because cold start counts as 1
            _ = func()

        # Timed iterations
        for _ in range(iterations):
            if gc_collect:
                gc.collect()
            start = time.perf_counter()
            _ = func()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)

    except Exception as e:
        error = str(e)
        console.print(f"[red]Error in {name}: {e}[/red]")

    return TimingResult(
        name=name,
        tool=tool,
        iterations=iterations,
        times_ms=times,
        cold_start_ms=cold_start,
        error=error,
    )


def print_comparison_table(suite: BenchmarkSuite):
    """Print a rich table comparing benchmark results."""
    table = Table(title=f"Benchmark Results: {suite.name}")

    table.add_column("Benchmark", style="cyan")
    table.add_column("Tool", style="magenta")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Std Dev", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Cold Start", justify="right")

    # Group by benchmark name
    by_name = {}
    for r in suite.results:
        if r.name not in by_name:
            by_name[r.name] = []
        by_name[r.name].append(r)

    for name, results in by_name.items():
        # Sort by tool for consistent display
        results = sorted(results, key=lambda x: x.tool)
        for i, r in enumerate(results):
            if r.error:
                table.add_row(
                    name if i == 0 else "",
                    r.tool,
                    f"[red]ERROR[/red]",
                    "-",
                    "-",
                    "-",
                    "-",
                )
            else:
                table.add_row(
                    name if i == 0 else "",
                    r.tool,
                    f"{r.mean_ms:.2f}",
                    f"{r.std_ms:.2f}",
                    f"{r.min_ms:.2f}",
                    f"{r.max_ms:.2f}",
                    f"{r.cold_start_ms:.2f}" if r.cold_start_ms else "-",
                )

    console.print(table)

    # Print speedup summary
    comparisons = suite.get_comparison()
    if comparisons:
        console.print("\n[bold]Speedup Summary (pyFIA vs alternatives):[/bold]")
        for key, vals in comparisons.items():
            speedup = vals["speedup"]
            if speedup > 1:
                console.print(f"  {key}: [green]{speedup:.1f}x faster[/green]")
            else:
                console.print(f"  {key}: [yellow]{1/speedup:.1f}x slower[/yellow]")
