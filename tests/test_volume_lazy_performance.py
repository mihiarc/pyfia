"""
Test script to validate and benchmark LazyVolumeEstimator performance.

This script compares the performance and memory usage of the standard
VolumeEstimator against the new LazyVolumeEstimator.
"""

import time
import tracemalloc
from typing import Dict, Tuple
import polars as pl

from pyfia import FIA
from pyfia.estimation import volume, volume_lazy
from pyfia.estimation.base import EstimatorConfig
from pyfia.estimation.volume import VolumeEstimator
from pyfia.estimation.volume_lazy import LazyVolumeEstimator


def measure_memory_and_time(func, *args, **kwargs) -> Tuple[float, float, any]:
    """
    Measure memory usage and execution time of a function.
    
    Returns
    -------
    Tuple[float, float, any]
        (memory_mb, time_seconds, result)
    """
    # Start memory tracking
    tracemalloc.start()
    
    # Measure execution time
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    
    # Get peak memory usage
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    memory_mb = peak / (1024 * 1024)
    time_seconds = end_time - start_time
    
    return memory_mb, time_seconds, result


def compare_estimators(db_path: str) -> Dict[str, Dict[str, float]]:
    """
    Compare performance of standard vs lazy volume estimators.
    
    Parameters
    ----------
    db_path : str
        Path to FIA database
        
    Returns
    -------
    Dict[str, Dict[str, float]]
        Performance metrics for each estimator
    """
    results = {}
    
    # Test configurations
    test_configs = [
        {
            "name": "Basic volume",
            "kwargs": {
                "vol_type": "net",
                "land_type": "forest",
                "tree_type": "live"
            }
        },
        {
            "name": "Volume by species",
            "kwargs": {
                "vol_type": "net",
                "by_species": True,
                "totals": True
            }
        },
        {
            "name": "Volume with filters",
            "kwargs": {
                "vol_type": "gross",
                "tree_domain": "DIA >= 10.0",
                "land_type": "timber"
            }
        },
        {
            "name": "Volume by groups",
            "kwargs": {
                "vol_type": "net",
                "grp_by": "FORTYPCD",
                "by_size_class": True
            }
        }
    ]
    
    print("Comparing Volume Estimators Performance")
    print("=" * 60)
    
    for test in test_configs:
        print(f"\nTest: {test['name']}")
        print("-" * 40)
        
        # Test standard estimator
        print("Running standard volume estimator...")
        mem_std, time_std, result_std = measure_memory_and_time(
            volume, db_path, **test['kwargs']
        )
        
        # Test lazy estimator
        print("Running lazy volume estimator...")
        mem_lazy, time_lazy, result_lazy = measure_memory_and_time(
            volume_lazy, db_path, show_progress=False, **test['kwargs']
        )
        
        # Calculate improvements
        memory_reduction = (1 - mem_lazy / mem_std) * 100
        speed_improvement = time_std / time_lazy
        
        # Verify results match
        result_match = verify_results_match(result_std, result_lazy)
        
        # Store results
        results[test['name']] = {
            "memory_std_mb": mem_std,
            "memory_lazy_mb": mem_lazy,
            "memory_reduction_pct": memory_reduction,
            "time_std_s": time_std,
            "time_lazy_s": time_lazy,
            "speed_improvement_x": speed_improvement,
            "results_match": result_match
        }
        
        # Print summary
        print(f"Memory usage: {mem_std:.1f} MB → {mem_lazy:.1f} MB "
              f"({memory_reduction:.1f}% reduction)")
        print(f"Execution time: {time_std:.2f}s → {time_lazy:.2f}s "
              f"({speed_improvement:.1f}x faster)")
        print(f"Results match: {result_match}")
    
    return results


def verify_results_match(df1: pl.DataFrame, df2: pl.DataFrame, 
                        tolerance: float = 1e-6) -> bool:
    """
    Verify that two result dataframes match within tolerance.
    
    Parameters
    ----------
    df1, df2 : pl.DataFrame
        DataFrames to compare
    tolerance : float
        Numerical tolerance for comparison
        
    Returns
    -------
    bool
        True if results match
    """
    # Check same columns
    if set(df1.columns) != set(df2.columns):
        print(f"Column mismatch: {set(df1.columns) - set(df2.columns)}")
        return False
    
    # Check same shape
    if df1.shape != df2.shape:
        print(f"Shape mismatch: {df1.shape} vs {df2.shape}")
        return False
    
    # Sort both dataframes by all non-numeric columns for comparison
    str_cols = [col for col in df1.columns 
                if df1[col].dtype in [pl.Utf8, pl.Categorical]]
    
    if str_cols:
        df1 = df1.sort(str_cols)
        df2 = df2.sort(str_cols)
    
    # Compare numeric columns
    numeric_cols = [col for col in df1.columns 
                    if df1[col].dtype in [pl.Float32, pl.Float64]]
    
    for col in numeric_cols:
        if not pl.Series(df1[col]).is_nan().all():
            max_diff = (df1[col] - df2[col]).abs().max()
            if max_diff > tolerance:
                print(f"Column {col} differs by {max_diff}")
                return False
    
    return True


def test_lazy_features(db_path: str):
    """
    Test specific lazy evaluation features.
    
    Parameters
    ----------
    db_path : str
        Path to FIA database
    """
    print("\n\nTesting Lazy Evaluation Features")
    print("=" * 60)
    
    # Create config
    config = EstimatorConfig(
        vol_type="net",
        by_species=True,
        totals=True,
        extra_params={
            "vol_type": "net",
            "show_progress": True,
            "lazy_enabled": True
        }
    )
    
    # Test with progress tracking
    print("\n1. Testing with progress tracking:")
    with LazyVolumeEstimator(db_path, config) as estimator:
        result = estimator.estimate()
        
        # Get lazy statistics
        stats = estimator.get_lazy_statistics()
        print(f"\nLazy evaluation statistics:")
        print(f"  Operations deferred: {stats['operations_deferred']}")
        print(f"  Operations collected: {stats['operations_collected']}")
        print(f"  Cache hits: {stats['cache_hits']}")
        print(f"  Total execution time: {stats['total_execution_time']:.2f}s")
    
    # Test computation graph
    print("\n2. Testing computation graph optimization:")
    config.extra_params['show_progress'] = False
    
    with LazyVolumeEstimator(db_path, config) as estimator:
        # Build computation graph without executing
        estimator._load_required_tables()
        tree_wrapper, cond_wrapper = estimator._get_filtered_data()
        
        # Show execution plan
        print(estimator.get_execution_plan())
    
    print("\n3. Testing collection strategies:")
    strategies = ['SEQUENTIAL', 'ADAPTIVE', 'STREAMING']
    
    for strategy in strategies:
        print(f"\n  Strategy: {strategy}")
        config.extra_params['collection_strategy'] = strategy
        
        with LazyVolumeEstimator(db_path, config) as estimator:
            start = time.time()
            result = estimator.estimate()
            elapsed = time.time() - start
            print(f"    Time: {elapsed:.2f}s, Rows: {len(result)}")


def main():
    """Run performance comparison tests."""
    # Assume test database is available
    db_path = "tests/data/test_fia.db"
    
    try:
        # Run performance comparison
        results = compare_estimators(db_path)
        
        # Print summary
        print("\n\nPerformance Summary")
        print("=" * 60)
        
        avg_memory_reduction = sum(r['memory_reduction_pct'] 
                                 for r in results.values()) / len(results)
        avg_speed_improvement = sum(r['speed_improvement_x'] 
                                  for r in results.values()) / len(results)
        all_match = all(r['results_match'] for r in results.values())
        
        print(f"Average memory reduction: {avg_memory_reduction:.1f}%")
        print(f"Average speed improvement: {avg_speed_improvement:.1f}x")
        print(f"All results match: {all_match}")
        
        # Test lazy features
        test_lazy_features(db_path)
        
    except FileNotFoundError:
        print(f"Test database not found at {db_path}")
        print("Please ensure test data is available")
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()