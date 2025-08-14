"""
Example demonstrating lazy evaluation support in pyFIA.

This example shows how to use the new lazy evaluation features for
improved performance with large FIA datasets.
"""

import polars as pl
from rich.console import Console

from pyfia import FIA
from pyfia.estimation import (
    LazyBaseEstimator,
    EstimatorConfig,
    LazyConfigMixin,
    CacheConfig,
    volume
)
from pyfia.estimation.lazy_evaluation import CollectionStrategy


class LazyVolumeEstimator(LazyBaseEstimator):
    """Example volume estimator with lazy evaluation."""
    
    def get_required_tables(self):
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_response_columns(self):
        return {
            "VOLCFNET_ACRE": "VOLCFNET_ACRE",
            "VOLBFNET_ACRE": "VOLBFNET_ACRE"
        }
    
    def calculate_values(self, data):
        """Calculate volume values with lazy support."""
        # Calculate volume per acre
        return data.with_columns([
            (pl.col("VOLCFNET") * pl.col("TPA_UNADJ") * pl.col("CONDPROP_UNADJ"))
            .alias("VOLCFNET_ACRE"),
            (pl.col("VOLBFNET") * pl.col("TPA_UNADJ") * pl.col("CONDPROP_UNADJ"))
            .alias("VOLBFNET_ACRE")
        ])
    
    def get_output_columns(self):
        return ["VOLCFNET_ACRE", "VOLCFNET_ACRE_SE", 
                "VOLBFNET_ACRE", "VOLBFNET_ACRE_SE",
                "YEAR", "N"]


def demonstrate_lazy_evaluation():
    """Demonstrate lazy evaluation features."""
    
    console = Console()
    
    # Example configuration with lazy evaluation enabled
    config = EstimatorConfig(
        land_type="forest",
        tree_type="live",
        by_species=True,
        grp_by=["OWNGRPCD"],
        extra_params={
            "lazy_enabled": True,  # Enable lazy evaluation
            "lazy_threshold_rows": 5000,  # Auto-lazy for tables > 5000 rows
            "collection_strategy": CollectionStrategy.ADAPTIVE
        }
    )
    
    # Cache configuration
    cache_config = CacheConfig(
        memory_cache_size_mb=256,
        disk_cache_size_gb=5,
        cache_query_plans=True
    )
    
    console.print("[bold cyan]pyFIA Lazy Evaluation Example[/bold cyan]\n")
    
    # Example 1: Basic lazy evaluation
    console.print("[yellow]Example 1: Basic Lazy Evaluation[/yellow]")
    console.print("Creating estimator with lazy evaluation enabled...")
    
    # Note: This would use a real FIA database in practice
    # db = FIA("path/to/fia_database.db")
    # estimator = LazyVolumeEstimator(db, config)
    
    console.print("✓ Estimator configured for lazy evaluation")
    console.print(f"  - Auto-lazy threshold: {config.extra_params['lazy_threshold_rows']:,} rows")
    console.print(f"  - Collection strategy: {config.extra_params['collection_strategy'].name}")
    
    # Example 2: Progress tracking
    console.print("\n[yellow]Example 2: Progress Tracking[/yellow]")
    
    # Enable progress tracking
    # estimator.enable_progress_tracking(console)
    
    # with estimator.progress_context():
    #     results = estimator.estimate()
    
    console.print("✓ Progress tracking provides real-time feedback during operations")
    
    # Example 3: Caching
    console.print("\n[yellow]Example 3: Caching Support[/yellow]")
    
    # Create cache
    cache = cache_config.create_cache()
    
    console.print("✓ Two-tier cache created:")
    console.print(f"  - Memory cache: {cache_config.memory_cache_size_mb} MB")
    console.print(f"  - Disk cache: {cache_config.disk_cache_size_gb} GB")
    
    # Example 4: Query optimization
    console.print("\n[yellow]Example 4: Query Plan Optimization[/yellow]")
    
    # Create example lazy frame
    df = pl.DataFrame({
        "TREE_ID": range(10000),
        "VOLCFNET": [100.0] * 10000,
        "TPA_UNADJ": [1.0] * 10000
    })
    lazy_df = df.lazy()
    
    # Show query plan
    filtered = lazy_df.filter(pl.col("VOLCFNET") > 50)
    aggregated = filtered.group_by("TPA_UNADJ").agg(pl.sum("VOLCFNET"))
    
    console.print("Query plan (optimized):")
    console.print(aggregated.explain(optimized=True))
    
    # Example 5: Collection strategies
    console.print("\n[yellow]Example 5: Collection Strategies[/yellow]")
    
    strategies = [
        (CollectionStrategy.SEQUENTIAL, "Collect frames one by one"),
        (CollectionStrategy.PARALLEL, "Collect frames in parallel"),
        (CollectionStrategy.STREAMING, "Use streaming engine for large data"),
        (CollectionStrategy.ADAPTIVE, "Automatically choose best strategy")
    ]
    
    for strategy, description in strategies:
        console.print(f"  - {strategy.name}: {description}")
    
    # Summary
    console.print("\n[bold green]Summary[/bold green]")
    console.print("Lazy evaluation provides:")
    console.print("  ✓ Deferred computation for better optimization")
    console.print("  ✓ Automatic memory management")
    console.print("  ✓ Query plan caching")
    console.print("  ✓ Progress tracking")
    console.print("  ✓ Multi-tier caching")
    console.print("  ✓ Adaptive collection strategies")


if __name__ == "__main__":
    demonstrate_lazy_evaluation()