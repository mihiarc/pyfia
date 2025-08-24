"""
Example usage of query builders for optimized FIA data access.

This script demonstrates how to use the specialized query builders
to create optimized queries with filter push-down, column selection,
and intelligent join strategies.
"""

from pathlib import Path

from pyfia.core import FIA
from pyfia.estimation.config import EstimatorConfig, LazyEvaluationConfig
from pyfia.estimation.query_builders import (
    QueryBuilderFactory,
    CompositeQueryBuilder,
    TreeQueryBuilder,
    PlotQueryBuilder,
)
from pyfia.estimation.caching import MemoryCache


def example_tree_query(db: FIA, config: EstimatorConfig):
    """Example of optimized tree query with filter push-down."""
    print("\n=== Tree Query Example ===")
    
    # Create tree query builder with caching
    cache = MemoryCache(max_size_mb=256)
    builder = TreeQueryBuilder(db, config, cache)
    
    # Build optimized query plan
    # Filters will be pushed down to the database level
    plan = builder.build_query_plan(
        tree_domain="DIA > 10 AND STATUSCD == 1",  # Live trees > 10 inches
        species=[131, 110, 833],  # Loblolly pine, Virginia pine, Chestnut oak
        include_seedlings=False,
        columns=["VOLCFNET", "VOLCFGRS", "VOLBFNET"]  # Volume columns
    )
    
    print(f"Query plan cache key: {plan.cache_key}")
    print(f"Estimated filter selectivity: {plan.filter_selectivity:.3f}")
    print(f"Tables: {plan.tables}")
    print(f"Number of filters: {len(plan.filters)}")
    print(f"Pushdown filters: {len(plan.get_pushdown_filters('TREE'))}")
    
    # Execute query (returns LazyFrameWrapper)
    result = builder.execute(plan)
    
    # Collect results when needed
    df = result.collect()
    print(f"Result rows: {len(df)}")
    print(f"Result columns: {df.columns}")
    
    return df


def example_stratification_query(db: FIA, config: EstimatorConfig):
    """Example of stratification query with EVALID filtering."""
    print("\n=== Stratification Query Example ===")
    
    # Use factory to create builder
    builder = QueryBuilderFactory.create_builder(
        "stratification",
        db,
        config,
        cache=MemoryCache(max_size_mb=128)
    )
    
    # Build query for specific EVALID
    plan = builder.build_query_plan(
        evalid=[371801, 371802],  # North Carolina evaluations
        state_cd=[37],
        include_adjustment_factors=True
    )
    
    print(f"Required columns for POP_STRATUM: {plan.get_required_columns('POP_STRATUM')}")
    
    # Execute and get results
    result = builder.execute(plan)
    df = result.collect()
    
    print(f"Stratification records: {len(df)}")
    print(f"Unique ESTN_UNIT values: {df['ESTN_UNIT'].n_unique()}")
    
    return df


def example_plot_with_strata(db: FIA, config: EstimatorConfig):
    """Example of plot query with stratification join."""
    print("\n=== Plot Query with Stratification Join ===")
    
    builder = PlotQueryBuilder(db, config)
    
    # Build query that joins plots with stratification
    plan = builder.build_query_plan(
        evalid=[371801],
        state_cd=[37],
        county_cd=[1, 3, 5],  # Specific counties
        include_strata=True,  # This will trigger join with POP_PLOT_STRATUM_ASSGN
        columns=["DESIGNCD", "MEASMON", "MEASDAY", "MEASYEAR"]
    )
    
    print(f"Tables in query: {plan.tables}")
    print(f"Number of joins: {len(plan.joins)}")
    if plan.joins:
        join = plan.joins[0]
        print(f"Join: {join.left_table}.{join.left_on} -> {join.right_table}.{join.right_on}")
        print(f"Join strategy: {join.strategy}")
    
    # Execute
    result = builder.execute(plan)
    df = result.collect()
    
    print(f"Plot records after join: {len(df)}")
    
    return df


def example_composite_estimation(db: FIA, config: EstimatorConfig):
    """Example of composite query for complete estimation."""
    print("\n=== Composite Estimation Query ===")
    
    # Create composite builder with shared cache
    cache = MemoryCache(max_size_mb=512)
    builder = CompositeQueryBuilder(db, config, cache)
    
    # Build complete query set for volume estimation
    results = builder.build_estimation_query(
        estimation_type="volume",
        evalid=[371801],
        tree_domain="DIA > 5 AND STATUSCD == 1",
        area_domain="FORTYPCD IN (161, 171)",  # Loblolly/shortleaf, Longleaf/slash
        state_cd=[37]
    )
    
    print(f"Query components: {list(results.keys())}")
    
    # Collect all results
    collected = {}
    for name, wrapper in results.items():
        df = wrapper.collect()
        collected[name] = df
        print(f"  {name}: {len(df)} rows, {len(df.columns)} columns")
    
    # Example of join order optimization
    from pyfia.estimation.query_builders import QueryJoin
    
    joins = [
        QueryJoin("TREE", "PLOT", "PLT_CN", "CN"),
        QueryJoin("PLOT", "POP_PLOT_STRATUM_ASSGN", "CN", "PLT_CN"),
        QueryJoin("TREE", "COND", ["PLT_CN", "CONDID"], ["PLT_CN", "CONDID"]),
    ]
    
    optimized_joins = builder.optimize_join_order(
        ["TREE", "PLOT", "COND", "POP_PLOT_STRATUM_ASSGN"],
        joins
    )
    
    print("\nOptimized join order:")
    for i, join in enumerate(optimized_joins, 1):
        print(f"  {i}. {join.left_table} -> {join.right_table}")
    
    return collected


def main():
    """Run query builder examples."""
    
    # Setup database connection
    # Replace with actual FIA database path
    db_path = Path("data/fia_example.db")
    
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        print("Please update the path to point to your FIA database")
        return
    
    # Create configuration with lazy evaluation
    config = EstimatorConfig(
        lazy_config=LazyEvaluationConfig(
            mode="enabled",
            enable_predicate_pushdown=True,
            enable_projection_pushdown=True,
            collection_strategy="adaptive"
        )
    )
    
    # Initialize FIA database
    with FIA(db_path) as db:
        # Run examples
        try:
            # Tree query example
            tree_df = example_tree_query(db, config)
            
            # Stratification query example
            strat_df = example_stratification_query(db, config)
            
            # Plot with stratification join
            plot_df = example_plot_with_strata(db, config)
            
            # Composite estimation query
            estimation_data = example_composite_estimation(db, config)
            
            print("\n=== Query Builder Examples Complete ===")
            
        except Exception as e:
            print(f"Error running examples: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()