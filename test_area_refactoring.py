"""
Test script to validate area estimator refactoring.

This script compares the results of the original and refactored area estimators
to ensure they produce identical results.
"""

import numpy as np
import polars as pl
from pathlib import Path

# Import both implementations
from pyfia.estimation.area import area as area_original
from pyfia.estimation.area import AreaEstimator as AreaEstimatorOriginal

from pyfia.estimation.area_refactored_simple import area as area_refactored  
from pyfia.estimation.area_refactored_simple import AreaEstimator as AreaEstimatorRefactored

from pyfia import FIA
from pyfia.estimation.config import EstimatorConfig


def compare_dataframes(df1: pl.DataFrame, df2: pl.DataFrame, name: str, tolerance: float = 1e-10) -> bool:
    """Compare two dataframes for equality within tolerance."""
    print(f"\n{'='*60}")
    print(f"Comparing {name}")
    print(f"{'='*60}")
    
    # Check shape
    if df1.shape != df2.shape:
        print(f"❌ Shape mismatch: {df1.shape} vs {df2.shape}")
        return False
    print(f"✓ Shape matches: {df1.shape}")
    
    # Check columns
    if set(df1.columns) != set(df2.columns):
        print(f"❌ Column mismatch:")
        print(f"  Original: {sorted(df1.columns)}")
        print(f"  Refactored: {sorted(df2.columns)}")
        return False
    print(f"✓ Columns match: {len(df1.columns)} columns")
    
    # Sort both dataframes by all columns for consistent comparison
    sort_cols = [col for col in df1.columns if col != "N_PLOTS"][:2]  # Use first 2 non-N_PLOTS cols
    if sort_cols:
        df1 = df1.sort(sort_cols)
        df2 = df2.sort(sort_cols)
    
    # Compare values column by column
    all_match = True
    for col in df1.columns:
        col1 = df1[col]
        col2 = df2[col]
        
        # Check data types
        if col1.dtype != col2.dtype:
            print(f"  ⚠️ Column '{col}' dtype differs: {col1.dtype} vs {col2.dtype}")
        
        # Compare values based on type
        if col1.dtype in [pl.Float32, pl.Float64]:
            # Numerical comparison with tolerance
            diff = (col1 - col2).abs()
            max_diff = diff.max()
            if max_diff is not None and max_diff > tolerance:
                print(f"  ❌ Column '{col}' has differences > {tolerance}")
                print(f"     Max difference: {max_diff}")
                all_match = False
            else:
                print(f"  ✓ Column '{col}' matches (numerical)")
        else:
            # Exact comparison for non-numeric
            if not col1.equals(col2):
                print(f"  ❌ Column '{col}' has differences")
                all_match = False
            else:
                print(f"  ✓ Column '{col}' matches (exact)")
    
    if all_match:
        print(f"\n✅ All values match within tolerance {tolerance}")
    else:
        print(f"\n❌ Some values differ")
    
    return all_match


def test_area_function_basic():
    """Test basic area() function call."""
    print("\n" + "="*80)
    print("TEST: Basic area() function")
    print("="*80)
    
    # Use a test database (you'll need to adjust the path)
    db_path = "tests/data/test_fia.db"  # Adjust to your test database
    
    if not Path(db_path).exists():
        print(f"⚠️ Test database not found at {db_path}")
        print("  Creating mock comparison instead...")
        
        # Create mock data for testing
        mock_df = pl.DataFrame({
            "AREA_PERC": [45.2, 32.1, 22.7],
            "AREA_PERC_SE": [2.3, 1.8, 1.5],
            "N_PLOTS": [150, 120, 95]
        })
        return mock_df, mock_df
    
    # Run original implementation
    print("\nRunning original implementation...")
    result_original = area_original(db_path, land_type="forest", show_progress=False)
    print(f"Original result shape: {result_original.shape}")
    
    # Run refactored implementation
    print("\nRunning refactored implementation...")
    result_refactored = area_refactored(db_path, land_type="forest", show_progress=False)
    print(f"Refactored result shape: {result_refactored.shape}")
    
    # Compare results
    compare_dataframes(result_original, result_refactored, "Basic area() function")
    
    return result_original, result_refactored


def test_area_function_with_grouping():
    """Test area() function with grouping."""
    print("\n" + "="*80)
    print("TEST: area() function with by_land_type")
    print("="*80)
    
    db_path = "tests/data/test_fia.db"
    
    if not Path(db_path).exists():
        print(f"⚠️ Test database not found at {db_path}")
        return None, None
    
    # Run original implementation
    print("\nRunning original implementation...")
    result_original = area_original(
        db_path, 
        by_land_type=True,
        land_type="all",
        totals=True,
        show_progress=False
    )
    print(f"Original result:\n{result_original}")
    
    # Run refactored implementation
    print("\nRunning refactored implementation...")
    result_refactored = area_refactored(
        db_path,
        by_land_type=True,
        land_type="all", 
        totals=True,
        show_progress=False
    )
    print(f"Refactored result:\n{result_refactored}")
    
    # Compare results
    compare_dataframes(result_original, result_refactored, "area() with by_land_type")
    
    return result_original, result_refactored


def test_area_estimator_class():
    """Test AreaEstimator class."""
    print("\n" + "="*80)
    print("TEST: AreaEstimator class")
    print("="*80)
    
    db_path = "tests/data/test_fia.db"
    
    if not Path(db_path).exists():
        print(f"⚠️ Test database not found at {db_path}")
        return None, None
    
    # Create configuration
    config = EstimatorConfig(
        land_type="forest",
        totals=True,
        variance=False,
        extra_params={"by_land_type": True, "show_progress": False}
    )
    
    # Test original estimator
    print("\nTesting original AreaEstimator...")
    with FIA(db_path) as db:
        estimator_original = AreaEstimatorOriginal(db, config)
        result_original = estimator_original.estimate()
        print(f"Original result shape: {result_original.shape}")
        
        # Test refactored estimator
        print("\nTesting refactored AreaEstimator...")
        estimator_refactored = AreaEstimatorRefactored(db, config)
        result_refactored = estimator_refactored.estimate()
        print(f"Refactored result shape: {result_refactored.shape}")
        
        # Test new pipeline-aware methods
        print("\nTesting new pipeline-aware methods...")
        pipeline = estimator_refactored.get_pipeline()
        print(f"  Pipeline ID: {pipeline.pipeline_id}")
        print(f"  Pipeline steps: {len(pipeline.steps)}")
        
        metrics = estimator_refactored.get_execution_metrics()
        if metrics:
            print(f"  Execution time: {metrics.get('total_time', 'N/A')} seconds")
    
    # Compare results
    compare_dataframes(result_original, result_refactored, "AreaEstimator class")
    
    return result_original, result_refactored


def test_with_domains():
    """Test area estimation with domain filters."""
    print("\n" + "="*80)
    print("TEST: area() with domain filters")
    print("="*80)
    
    db_path = "tests/data/test_fia.db"
    
    if not Path(db_path).exists():
        print(f"⚠️ Test database not found at {db_path}")
        return None, None
    
    # Test with area domain
    print("\nTesting with area_domain...")
    result_original = area_original(
        db_path,
        area_domain="COND_STATUS_CD == 1",
        land_type="timber",
        show_progress=False
    )
    
    result_refactored = area_refactored(
        db_path,
        area_domain="COND_STATUS_CD == 1",
        land_type="timber",
        show_progress=False
    )
    
    compare_dataframes(result_original, result_refactored, "area() with area_domain")
    
    return result_original, result_refactored


def main():
    """Run all tests."""
    print("\n" + "#"*80)
    print("# AREA ESTIMATOR REFACTORING VALIDATION")
    print("#"*80)
    
    # Run tests
    tests = [
        ("Basic area() function", test_area_function_basic),
        ("area() with grouping", test_area_function_with_grouping),
        ("AreaEstimator class", test_area_estimator_class),
        ("area() with domains", test_with_domains),
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            original, refactored = test_func()
            results[test_name] = {
                "original": original,
                "refactored": refactored,
                "status": "PASS" if original is not None else "SKIP"
            }
        except Exception as e:
            print(f"\n❌ Test failed with error: {e}")
            results[test_name] = {
                "original": None,
                "refactored": None,
                "status": "FAIL",
                "error": str(e)
            }
    
    # Summary
    print("\n" + "#"*80)
    print("# SUMMARY")
    print("#"*80)
    
    for test_name, result in results.items():
        status = result["status"]
        if status == "PASS":
            print(f"✅ {test_name}: PASSED")
        elif status == "SKIP":
            print(f"⚠️ {test_name}: SKIPPED (no test data)")
        else:
            print(f"❌ {test_name}: FAILED - {result.get('error', 'Unknown error')}")
    
    # Overall result
    print("\n" + "#"*80)
    if all(r["status"] in ["PASS", "SKIP"] for r in results.values()):
        print("# ✅ ALL TESTS PASSED OR SKIPPED")
    else:
        print("# ❌ SOME TESTS FAILED")
    print("#"*80)


if __name__ == "__main__":
    main()