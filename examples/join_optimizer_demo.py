#!/usr/bin/env python
"""
Demonstration of the JoinManager for pyFIA.

This example shows how the JoinManager improves performance for complex
FIA queries by optimizing join order, pushing filters, and selecting
appropriate join strategies.
"""

import time
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Import pyFIA components
from pyfia.estimation.join import (
    JoinManager,
    JoinOptimizer,
    JoinPlan,
    JoinType,
    JoinStrategy as JoinStrategyType,
    TableStatistics,
    FIATableInfo,
    get_join_manager
)
from pyfia.estimation.query_builders import (
    QueryPlan,
    QueryJoin,
    QueryFilter,
    QueryColumn,
    JoinStrategy
)
from pyfia.estimation.lazy_evaluation import LazyFrameWrapper
from pyfia.estimation.config import EstimatorConfig
from pyfia.estimation.caching import MemoryCache


console = Console()


def create_sample_data():
    """Create sample FIA-like data for demonstration."""
    
    # Create sample TREE data
    tree_data = pl.DataFrame({
        "CN": list(range(1, 10001)),
        "PLT_CN": [i % 1000 + 1 for i in range(10000)],
        "SPCD": [131 if i % 3 == 0 else 110 if i % 3 == 1 else 833 for i in range(10000)],
        "DIA": [10.0 + (i % 20) for i in range(10000)],
        "STATUSCD": [1 if i % 10 != 0 else 2 for i in range(10000)],
        "HT": [50.0 + (i % 30) for i in range(10000)]
    })
    
    # Create sample PLOT data
    plot_data = pl.DataFrame({
        "CN": list(range(1, 1001)),
        "STATECD": [37 if i % 3 == 0 else 45 if i % 3 == 1 else 51 for i in range(1000)],
        "COUNTYCD": [(i % 100) + 1 for i in range(1000)],
        "INVYR": [2020 + (i % 3) for i in range(1000)],
        "STRATUM_CN": [(i % 50) + 1 for i in range(1000)]
    })
    
    # Create sample COND data
    cond_data = pl.DataFrame({
        "CN": list(range(1, 1501)),
        "PLT_CN": [(i % 1000) + 1 for i in range(1500)],
        "CONDID": [(i % 3) + 1 for i in range(1500)],
        "COND_STATUS_CD": [1 if i % 5 != 0 else 2 for i in range(1500)],
        "FORTYPCD": [100 + (i % 50) for i in range(1500)],
        "CONDPROP_UNADJ": [1.0 if i % 3 == 0 else 0.5 for i in range(1500)]
    })
    
    # Create sample POP_STRATUM data (small reference table)
    strata_data = pl.DataFrame({
        "CN": list(range(1, 51)),
        "EVALID": [370001 + (i % 3) for i in range(50)],
        "ESTN_UNIT": [(i % 5) + 1 for i in range(50)],
        "STRATUMCD": [i + 1 for i in range(50)],
        "ACRES": [10000.0 + (i * 100) for i in range(50)],
        "P1POINTCNT": [20 + i for i in range(50)],
        "P2POINTCNT": [15 + i for i in range(50)]
    })
    
    # Create sample REF_SPECIES data (small reference table)
    species_data = pl.DataFrame({
        "SPCD": [131, 110, 833],
        "COMMON_NAME": ["Loblolly pine", "Virginia pine", "Chestnut oak"],
        "GENUS": ["Pinus", "Pinus", "Quercus"],
        "SPECIES": ["taeda", "virginiana", "prinus"]
    })
    
    return {
        "TREE": LazyFrameWrapper(tree_data.lazy()),
        "PLOT": LazyFrameWrapper(plot_data.lazy()),
        "COND": LazyFrameWrapper(cond_data.lazy()),
        "POP_STRATUM": LazyFrameWrapper(strata_data.lazy()),
        "REF_SPECIES": LazyFrameWrapper(species_data.lazy())
    }


def demonstrate_basic_optimization():
    """Demonstrate basic join optimization."""
    console.print("\n[bold cyan]Basic Join Optimization Demo[/bold cyan]\n")
    
    # Create sample data
    data_sources = create_sample_data()
    
    # Create a query plan with multiple joins
    plan = QueryPlan(
        tables=["TREE", "PLOT", "REF_SPECIES"],
        columns=[
            QueryColumn("CN", "TREE"),
            QueryColumn("DIA", "TREE"),
            QueryColumn("STATECD", "PLOT"),
            QueryColumn("COMMON_NAME", "REF_SPECIES")
        ],
        filters=[
            QueryFilter("STATUSCD", "==", 1, "TREE"),
            QueryFilter("DIA", ">", 15.0, "TREE"),
            QueryFilter("STATECD", "==", 37, "PLOT")
        ],
        joins=[
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN", strategy=JoinStrategy.AUTO),
            QueryJoin("TREE", "REF_SPECIES", "SPCD", "SPCD", strategy=JoinStrategy.AUTO)
        ]
    )
    
    # Create optimizer
    optimizer = JoinOptimizer()
    
    # Show original plan
    console.print("[yellow]Original Query Plan:[/yellow]")
    console.print(f"  Tables: {plan.tables}")
    console.print(f"  Joins: {len(plan.joins)} joins")
    console.print(f"  Filters: {len(plan.filters)} filters")
    console.print(f"  Default strategy: AUTO\n")
    
    # Optimize the plan
    console.print("[green]Optimizing...[/green]")
    optimized_plan = optimizer.optimize(plan)
    
    # Show optimized plan
    console.print("\n[green]Optimized Query Plan:[/green]")
    console.print(f"  Filters pushed: {len(plan.filters) - len(optimized_plan.filters)}")
    console.print(f"  Preferred strategy: {optimized_plan.preferred_strategy}")
    
    # Show optimization statistics
    stats = optimizer.get_optimization_stats()
    console.print(f"\n[cyan]Optimization Statistics:[/cyan]")
    for key, value in stats.items():
        console.print(f"  {key}: {value}")


def demonstrate_fia_patterns():
    """Demonstrate FIA-specific join patterns."""
    console.print("\n[bold cyan]FIA-Specific Join Patterns Demo[/bold cyan]\n")
    
    # Show tree-plot-condition pattern
    console.print("[yellow]1. Tree-Plot-Condition Pattern[/yellow]")
    pattern = FIAJoinPatterns.tree_plot_condition_pattern()
    console.print(f"   Optimized for: Large tree table with plot and condition filters")
    console.print(f"   Join order: Plot ⟶ Condition ⟶ Tree")
    console.print(f"   Strategy: {pattern.strategy.name}\n")
    
    # Show stratification pattern
    console.print("[yellow]2. Stratification Pattern[/yellow]")
    pattern = FIAJoinPatterns.stratification_pattern()
    console.print(f"   Optimized for: Plot stratification with small strata table")
    console.print(f"   Join order: Assignment ⟶ Plot ⟶ Strata")
    console.print(f"   Strategy: {pattern.strategy.name} (small table)\n")
    
    # Show species reference pattern
    console.print("[yellow]3. Species Reference Pattern[/yellow]")
    pattern = FIAJoinPatterns.species_reference_pattern()
    console.print(f"   Optimized for: Tree-species lookup")
    console.print(f"   Join type: {pattern.join_type.name}")
    console.print(f"   Strategy: {pattern.strategy.name} (reference table)")


def demonstrate_performance_comparison():
    """Demonstrate performance improvements from optimization."""
    console.print("\n[bold cyan]Performance Comparison Demo[/bold cyan]\n")
    
    # Create sample data
    data_sources = create_sample_data()
    
    # Create a complex query
    complex_plan = QueryPlan(
        tables=["TREE", "PLOT", "COND", "POP_STRATUM", "REF_SPECIES"],
        columns=[
            QueryColumn("CN", "TREE"),
            QueryColumn("DIA", "TREE"),
            QueryColumn("STATECD", "PLOT"),
            QueryColumn("FORTYPCD", "COND"),
            QueryColumn("ACRES", "POP_STRATUM"),
            QueryColumn("COMMON_NAME", "REF_SPECIES")
        ],
        filters=[
            QueryFilter("STATUSCD", "==", 1, "TREE"),
            QueryFilter("DIA", "BETWEEN", [10.0, 20.0], "TREE"),
            QueryFilter("STATECD", "IN", [37, 45], "PLOT"),
            QueryFilter("COND_STATUS_CD", "==", 1, "COND")
        ],
        joins=[
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN"),
            QueryJoin("PLOT", "COND", "CN", "PLT_CN"),
            QueryJoin("PLOT", "POP_STRATUM", "STRATUM_CN", "CN"),
            QueryJoin("TREE", "REF_SPECIES", "SPCD", "SPCD")
        ]
    )
    
    # Create optimizer and executor
    optimizer = JoinOptimizer()
    executor = OptimizedQueryExecutor(optimizer)
    
    # Execute without optimization (simulate)
    console.print("[yellow]Executing unoptimized query...[/yellow]")
    start_time = time.time()
    
    # Simulate unoptimized execution
    unopt_result = executor.execute_plan(complex_plan, data_sources)
    unopt_df = unopt_result.collect()
    
    unopt_time = time.time() - start_time
    console.print(f"  Execution time: {unopt_time:.3f}s")
    console.print(f"  Result rows: {len(unopt_df)}")
    
    # Execute with optimization
    console.print("\n[green]Executing optimized query...[/green]")
    start_time = time.time()
    
    optimized_plan = optimizer.optimize(complex_plan)
    opt_result = executor.execute_plan(optimized_plan, data_sources)
    opt_df = opt_result.collect()
    
    opt_time = time.time() - start_time
    console.print(f"  Execution time: {opt_time:.3f}s")
    console.print(f"  Result rows: {len(opt_df)}")
    
    # Show improvement
    if unopt_time > 0:
        improvement = ((unopt_time - opt_time) / unopt_time) * 100
        console.print(f"\n[bold green]Performance improvement: {improvement:.1f}%[/bold green]")
    
    # Show optimization details
    stats = optimizer.get_optimization_stats()
    
    # Create summary table
    table = Table(title="Optimization Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Joins Optimized", str(stats["joins_optimized"]))
    table.add_row("Filters Pushed", str(stats["filters_pushed"]))
    table.add_row("Broadcast Joins", str(stats["broadcast_joins"]))
    
    console.print("\n")
    console.print(table)


def main():
    """Run all demonstrations."""
    console.print(Panel.fit(
        "[bold magenta]pyFIA Phase 3: Join Optimizer Demonstration[/bold magenta]\n"
        "This demo showcases the join optimization capabilities for FIA data processing.",
        title="Welcome"
    ))
    
    # Run demonstrations
    demonstrate_basic_optimization()
    demonstrate_fia_patterns()
    demonstrate_performance_comparison()
    
    console.print("\n" + "="*60)
    console.print("[bold green]✓ Join Optimizer demonstration complete![/bold green]")
    console.print("\nThe JoinOptimizer provides:")
    console.print("  • Automatic join order optimization")
    console.print("  • Filter push-down to reduce data size")
    console.print("  • Smart join strategy selection")
    console.print("  • FIA-specific optimizations")
    console.print("  • Seamless integration with lazy evaluation")


if __name__ == "__main__":
    main()