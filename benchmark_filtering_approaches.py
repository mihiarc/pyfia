#!/usr/bin/env python
"""
Benchmark the performance of different filtering approaches:
1. clip_by_state (with state filter + EVALID finding)
2. Direct clip_by_evalid (no state filter needed)
"""

import time
from pyfia import FIA, area
from rich.console import Console
from rich.table import Table
import statistics

console = Console()

def benchmark_clip_by_state(db_path: str, iterations: int = 5):
    """Benchmark the clip_by_state approach."""
    times = []
    
    for i in range(iterations):
        start_time = time.perf_counter()
        
        with FIA(db_path) as db:
            # This method has to:
            # 1. Set state filter
            # 2. Call find_evalid() which queries POP_EVAL and POP_EVAL_TYP
            # 3. Filter and find most recent
            # 4. Set EVALID filter
            db.clip_by_state(40, most_recent=True)
            results = area(db, totals=True)
        
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        times.append(elapsed)
        
        if i == 0:
            # Store first result for verification
            first_result = results['AREA'][0]
    
    return times, first_result

def benchmark_direct_evalid(db_path: str, iterations: int = 5):
    """Benchmark the direct EVALID approach."""
    times = []
    
    for i in range(iterations):
        start_time = time.perf_counter()
        
        with FIA(db_path) as db:
            # This method just:
            # 1. Set EVALID filter directly
            # No database queries needed to find EVALID
            db.clip_by_evalid(402300)
            results = area(db, totals=True)
        
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        times.append(elapsed)
        
        if i == 0:
            # Store first result for verification
            first_result = results['AREA'][0]
    
    return times, first_result

def format_stats(times):
    """Calculate and format statistics for timing results."""
    return {
        'mean': statistics.mean(times),
        'median': statistics.median(times),
        'stdev': statistics.stdev(times) if len(times) > 1 else 0,
        'min': min(times),
        'max': max(times)
    }

# Run benchmarks
console.print("\n[bold cyan]Performance Comparison: Filtering Approaches[/bold cyan]")
console.print("=" * 60)

console.print("\n[yellow]Running benchmarks (5 iterations each)...[/yellow]")

# Benchmark Method 1: clip_by_state
console.print("\nMethod 1: clip_by_state()")
times1, result1 = benchmark_clip_by_state("nfi_south.duckdb", iterations=5)
stats1 = format_stats(times1)

# Benchmark Method 2: direct EVALID
console.print("Method 2: clip_by_evalid()")
times2, result2 = benchmark_direct_evalid("nfi_south.duckdb", iterations=5)
stats2 = format_stats(times2)

# Create results table
table = Table(title="Performance Comparison (times in seconds)")
table.add_column("Method", style="cyan")
table.add_column("Mean", style="yellow", justify="right")
table.add_column("Median", style="green", justify="right")
table.add_column("Min", style="blue", justify="right")
table.add_column("Max", style="red", justify="right")
table.add_column("StdDev", style="magenta", justify="right")

table.add_row(
    "clip_by_state(40)",
    f"{stats1['mean']:.4f}",
    f"{stats1['median']:.4f}",
    f"{stats1['min']:.4f}",
    f"{stats1['max']:.4f}",
    f"{stats1['stdev']:.4f}"
)

table.add_row(
    "clip_by_evalid(402300)",
    f"{stats2['mean']:.4f}",
    f"{stats2['median']:.4f}",
    f"{stats2['min']:.4f}",
    f"{stats2['max']:.4f}",
    f"{stats2['stdev']:.4f}"
)

console.print("\n")
console.print(table)

# Calculate speedup
speedup = stats1['mean'] / stats2['mean']
time_saved = stats1['mean'] - stats2['mean']
percent_faster = ((stats1['mean'] - stats2['mean']) / stats1['mean']) * 100

console.print("\n[bold]Analysis:[/bold]")
console.print(f"• Direct EVALID filtering is [green]{speedup:.2f}x faster[/green]")
console.print(f"• Time saved per operation: [yellow]{time_saved*1000:.1f} ms[/yellow]")
console.print(f"• Performance improvement: [cyan]{percent_faster:.1f}%[/cyan]")

# Verify results are identical
console.print(f"\n[bold]Result Verification:[/bold]")
console.print(f"• clip_by_state result: {result1:,.0f} acres")
console.print(f"• clip_by_evalid result: {result2:,.0f} acres")
if abs(result1 - result2) < 1:
    console.print("[green]✓ Results are identical[/green]")
else:
    console.print("[red]✗ Results differ![/red]")

# Explain why direct EVALID is faster
console.print("\n[bold]Why is direct EVALID faster?[/bold]")
console.print("1. No need to query POP_EVAL and POP_EVAL_TYP tables")
console.print("2. No need to join tables to find matching EVALIDs")
console.print("3. No need to filter by state and year")
console.print("4. No need to group and find most recent")
console.print("5. Just sets one filter value directly")

# Show detailed timing breakdown
console.print("\n[bold]Detailed Timing Results:[/bold]")
console.print("\nclip_by_state times (seconds):")
for i, t in enumerate(times1, 1):
    console.print(f"  Run {i}: {t:.4f}")

console.print("\nclip_by_evalid times (seconds):")
for i, t in enumerate(times2, 1):
    console.print(f"  Run {i}: {t:.4f}")