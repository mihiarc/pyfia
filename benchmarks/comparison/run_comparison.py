#!/usr/bin/env python
"""
Run complete benchmark comparison between pyFIA, rFIA, and EVALIDator.

This script produces publication-ready performance comparisons.

Usage:
    # Run with existing database
    python -m benchmarks.comparison.run_comparison --db data/ri/ri.duckdb --state RI

    # Download data and run
    python -m benchmarks.comparison.run_comparison --state RI --download

    # Run only specific tools
    python -m benchmarks.comparison.run_comparison --state RI --tools pyfia,evalidator

    # Export results to CSV
    python -m benchmarks.comparison.run_comparison --state RI --export results.csv
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from benchmarks.comparison.timing import BenchmarkSuite, TimingResult, print_comparison_table

console = Console()

# State FIPS codes
STATE_FIPS = {
    "RI": 44,  # Rhode Island
    "DE": 10,  # Delaware
    "CT": 9,   # Connecticut
    "NC": 37,  # North Carolina
    "GA": 13,  # Georgia
    "CA": 6,   # California
    "TX": 48,  # Texas
}


def download_data_if_needed(state: str, data_dir: Path) -> Path:
    """Download FIA data if not already present."""
    from pyfia import download

    db_path = data_dir / state.lower() / f"{state.lower()}.duckdb"

    if db_path.exists():
        console.print(f"[green]Using existing database: {db_path}[/green]")
        return db_path

    console.print(f"[cyan]Downloading FIA data for {state}...[/cyan]")
    return download(state, dir=data_dir)


def run_pyfia_benchmarks(db_path: Path, state_fips: int, iterations: int) -> List[TimingResult]:
    """Run pyFIA benchmarks."""
    from benchmarks.comparison.bench_pyfia import run_pyfia_benchmarks

    return run_pyfia_benchmarks(db_path, state_fips, iterations)


def run_rfia_benchmarks(state: str, data_dir: Path, iterations: int) -> List[TimingResult]:
    """Run rFIA benchmarks if R is available."""
    from benchmarks.comparison.bench_rfia import check_rfia_available, run_rfia_benchmarks

    if not check_rfia_available():
        console.print("[yellow]rFIA not available - skipping R benchmarks[/yellow]")
        return []

    # rFIA needs CSV files, not DuckDB
    csv_dir = data_dir / state.lower() / "csv"
    return run_rfia_benchmarks(state, csv_dir, iterations)


def run_evalidator_benchmarks(state: str, iterations: int) -> List[TimingResult]:
    """Run EVALIDator API benchmarks."""
    from benchmarks.comparison.bench_evalidator import (
        check_evalidator_available,
        run_evalidator_benchmarks,
    )

    if not check_evalidator_available():
        console.print("[yellow]EVALIDator API not reachable - skipping[/yellow]")
        return []

    return run_evalidator_benchmarks(state, iterations)


def compute_speedup_statistics(results: List[TimingResult]) -> Dict:
    """Compute speedup statistics for publication."""
    by_benchmark = {}

    for r in results:
        if r.name not in by_benchmark:
            by_benchmark[r.name] = {}
        by_benchmark[r.name][r.tool] = r

    speedups = []
    stats = {
        "benchmarks": {},
        "summary": {},
    }

    for name, tools in by_benchmark.items():
        if "pyfia" not in tools:
            continue

        pyfia_time = tools["pyfia"].mean_ms
        if pyfia_time == 0:
            continue

        benchmark_stats = {"pyfia_ms": pyfia_time}

        for tool, result in tools.items():
            if tool == "pyfia" or result.error:
                continue

            tool_time = result.mean_ms
            if tool_time > 0:
                speedup = tool_time / pyfia_time
                benchmark_stats[f"{tool}_ms"] = tool_time
                benchmark_stats[f"{tool}_speedup"] = speedup
                speedups.append((name, tool, speedup))

        stats["benchmarks"][name] = benchmark_stats

    # Compute summary statistics
    if speedups:
        # Group by tool
        by_tool = {}
        for name, tool, speedup in speedups:
            if tool not in by_tool:
                by_tool[tool] = []
            by_tool[tool].append(speedup)

        for tool, tool_speedups in by_tool.items():
            import statistics

            stats["summary"][tool] = {
                "mean_speedup": statistics.mean(tool_speedups),
                "median_speedup": statistics.median(tool_speedups),
                "min_speedup": min(tool_speedups),
                "max_speedup": max(tool_speedups),
                "n_benchmarks": len(tool_speedups),
            }

    return stats


def print_publication_summary(stats: Dict, state: str):
    """Print publication-ready summary."""
    console.print("\n")
    console.print(Panel.fit(
        "[bold]Publication Summary[/bold]\n\n"
        f"State: {state}\n"
        f"Date: {datetime.now().strftime('%Y-%m-%d')}",
        title="pyFIA Performance Benchmarks"
    ))

    if "summary" in stats and stats["summary"]:
        console.print("\n[bold]Speedup Summary (pyFIA vs alternatives):[/bold]\n")

        table = Table()
        table.add_column("Comparison", style="cyan")
        table.add_column("Mean Speedup", justify="right")
        table.add_column("Median Speedup", justify="right")
        table.add_column("Range", justify="right")
        table.add_column("N Benchmarks", justify="right")

        for tool, tool_stats in stats["summary"].items():
            mean_sp = tool_stats["mean_speedup"]
            median_sp = tool_stats["median_speedup"]
            min_sp = tool_stats["min_speedup"]
            max_sp = tool_stats["max_speedup"]

            table.add_row(
                f"pyFIA vs {tool}",
                f"[green]{mean_sp:.1f}x[/green]" if mean_sp > 1 else f"[red]{mean_sp:.2f}x[/red]",
                f"{median_sp:.1f}x",
                f"{min_sp:.1f}x - {max_sp:.1f}x",
                str(tool_stats["n_benchmarks"]),
            )

        console.print(table)

        # Publication-ready text
        console.print("\n[bold]Suggested text for publication:[/bold]\n")

        for tool, tool_stats in stats["summary"].items():
            mean_sp = tool_stats["mean_speedup"]
            median_sp = tool_stats["median_speedup"]
            min_sp = tool_stats["min_speedup"]
            max_sp = tool_stats["max_speedup"]

            if tool == "rfia":
                tool_name = "rFIA"
            elif tool == "evalidator":
                tool_name = "EVALIDator API"
            else:
                tool_name = tool

            console.print(f'  "pyFIA demonstrated a mean speedup of {mean_sp:.1f}x '
                         f'(median: {median_sp:.1f}x, range: {min_sp:.1f}x-{max_sp:.1f}x) '
                         f'compared to {tool_name} across {tool_stats["n_benchmarks"]} '
                         f'benchmark operations."')


def export_results(
    results: List[TimingResult],
    stats: Dict,
    output_path: Path,
    format: str = "csv",
):
    """Export results to file."""
    if format == "csv":
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "benchmark", "tool", "mean_ms", "std_ms", "min_ms", "max_ms",
                "median_ms", "cold_start_ms", "iterations", "error"
            ])

            for r in results:
                writer.writerow([
                    r.name, r.tool, r.mean_ms, r.std_ms, r.min_ms, r.max_ms,
                    r.median_ms, r.cold_start_ms or "", r.iterations, r.error or ""
                ])

        console.print(f"[green]Results exported to {output_path}[/green]")

    elif format == "json":
        output = {
            "results": [
                {
                    "benchmark": r.name,
                    "tool": r.tool,
                    "mean_ms": r.mean_ms,
                    "std_ms": r.std_ms,
                    "min_ms": r.min_ms,
                    "max_ms": r.max_ms,
                    "median_ms": r.median_ms,
                    "cold_start_ms": r.cold_start_ms,
                    "iterations": r.iterations,
                    "error": r.error,
                }
                for r in results
            ],
            "statistics": stats,
            "metadata": {
                "date": datetime.now().isoformat(),
                "pyfia_version": _get_pyfia_version(),
            }
        }

        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)

        console.print(f"[green]Results exported to {output_path}[/green]")


def _get_pyfia_version() -> str:
    """Get pyFIA version."""
    try:
        from pyfia import __version__

        return __version__
    except ImportError:
        return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Run pyFIA performance benchmark comparison"
    )
    parser.add_argument(
        "--state", "-s",
        required=True,
        help="State abbreviation (e.g., RI, DE, NC)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Path to existing DuckDB database",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download FIA data if not present",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory for FIA data (default: data/)",
    )
    parser.add_argument(
        "--tools",
        default="pyfia,rfia,evalidator",
        help="Comma-separated list of tools to benchmark",
    )
    parser.add_argument(
        "--iterations", "-n",
        type=int,
        default=10,
        help="Number of benchmark iterations (default: 10)",
    )
    parser.add_argument(
        "--export",
        type=Path,
        help="Export results to file (CSV or JSON based on extension)",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Reduce output verbosity",
    )

    args = parser.parse_args()

    state = args.state.upper()
    if state not in STATE_FIPS:
        console.print(f"[red]Unknown state: {state}[/red]")
        console.print(f"Supported states: {', '.join(STATE_FIPS.keys())}")
        sys.exit(1)

    state_fips = STATE_FIPS[state]
    tools = [t.strip().lower() for t in args.tools.split(",")]

    console.print(Panel.fit(
        f"[bold]pyFIA Performance Benchmark[/bold]\n\n"
        f"State: {state} (FIPS: {state_fips})\n"
        f"Tools: {', '.join(tools)}\n"
        f"Iterations: {args.iterations}",
        title="Configuration"
    ))

    # Get or download database
    if args.db:
        db_path = args.db
        if not db_path.exists():
            console.print(f"[red]Database not found: {db_path}[/red]")
            sys.exit(1)
    elif args.download or "pyfia" in tools:
        db_path = download_data_if_needed(state, args.data_dir)
    else:
        db_path = None

    all_results = []

    # Run pyFIA benchmarks
    if "pyfia" in tools:
        if db_path is None:
            console.print("[red]Need database for pyFIA benchmarks[/red]")
            console.print("Use --db or --download")
            sys.exit(1)

        console.print("\n" + "=" * 60)
        pyfia_results = run_pyfia_benchmarks(db_path, state_fips, args.iterations)
        all_results.extend(pyfia_results)

    # Run rFIA benchmarks
    if "rfia" in tools:
        console.print("\n" + "=" * 60)
        rfia_results = run_rfia_benchmarks(state, args.data_dir, args.iterations)
        all_results.extend(rfia_results)

    # Run EVALIDator benchmarks
    if "evalidator" in tools:
        console.print("\n" + "=" * 60)
        # Use fewer iterations for API calls
        evalidator_iterations = min(args.iterations, 5)
        evalidator_results = run_evalidator_benchmarks(state, evalidator_iterations)
        all_results.extend(evalidator_results)

    # Compute statistics
    stats = compute_speedup_statistics(all_results)

    # Print comparison table
    console.print("\n" + "=" * 60)
    suite = BenchmarkSuite(
        name=f"pyFIA vs Alternatives ({state})",
        description=f"Performance comparison for {state}",
    )
    for r in all_results:
        suite.add_result(r)

    print_comparison_table(suite)

    # Print publication summary
    print_publication_summary(stats, state)

    # Export if requested
    if args.export:
        export_format = "json" if args.export.suffix == ".json" else "csv"
        export_results(all_results, stats, args.export, export_format)

    console.print("\n[bold green]Benchmark complete![/bold green]")


if __name__ == "__main__":
    main()
