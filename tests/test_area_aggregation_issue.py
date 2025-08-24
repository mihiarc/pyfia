"""
Test to reproduce and diagnose the area aggregation issue mentioned in Phase 2.5.

The issue: by_land_type=True returns 102,481 rows instead of 4
Root cause: Group-by aggregation not properly collapsing in lazy evaluation
"""

import pytest
import polars as pl
from pyfia import FIA
from pyfia.estimation import area


def test_area_by_land_type_aggregation(sample_fia_instance):
    """Test that by_land_type aggregation produces correct number of rows."""
    if not sample_fia_instance:
        pytest.skip("No test database available")
    
    with sample_fia_instance as db:
        # Test with by_land_type=True
        result = area(db, by_land_type=True, show_progress=False)
        
        print(f"\narea(by_land_type=True) results:")
        print(f"  Shape: {result.shape}")
        print(f"  Columns: {result.columns}")
        
        # Check if LAND_TYPE column exists
        if "LAND_TYPE" in result.columns:
            unique_land_types = result["LAND_TYPE"].unique()
            print(f"  Unique LAND_TYPE values: {len(unique_land_types)}")
            print(f"  Land types: {unique_land_types.to_list()}")
        
        # Expected: Should have ~4 rows (one per land type)
        # Actual: Getting 102,481 rows (issue to fix)
        
        # This should fail until the aggregation issue is fixed
        assert result.shape[0] < 100, f"Expected ~4 rows for land types, got {result.shape[0]}"


def test_area_grouping_consistency(sample_fia_instance):
    """Test that grouping produces consistent results."""
    if not sample_fia_instance:
        pytest.skip("No test database available")
    
    with sample_fia_instance as db:
        # Test basic area
        basic = area(db, show_progress=False)
        print(f"\nBasic area shape: {basic.shape}")
        
        # Test with FORTYPCD grouping
        grouped = area(db, grp_by=["FORTYPCD"], show_progress=False)
        print(f"Grouped by FORTYPCD shape: {grouped.shape}")
        
        # Test totals
        with_totals = area(db, grp_by=["FORTYPCD"], totals=True, show_progress=False)
        print(f"With totals shape: {with_totals.shape}")
        
        # The grouped result should have one row per unique FORTYPCD value
        if "FORTYPCD" in grouped.columns:
            unique_types = grouped["FORTYPCD"].unique()
            print(f"Unique FORTYPCD values: {len(unique_types)}")
            
            # Check if aggregation is working
            assert grouped.shape[0] == len(unique_types), \
                f"Expected {len(unique_types)} rows, got {grouped.shape[0]}"


def test_area_lazy_collection_points(sample_fia_instance):
    """Debug test to examine lazy evaluation collection points."""
    if not sample_fia_instance:
        pytest.skip("No test database available") 
    
    with sample_fia_instance as db:
        from pyfia.estimation import AreaEstimator
        from pyfia.estimation.base import EstimatorConfig
        
        # Create estimator with by_land_type
        config = EstimatorConfig(by_land_type=True)
        estimator = AreaEstimator(db, config)
        
        # Get lazy result before collection
        lazy_result = estimator._prepare_lazy_result()
        
        print(f"\nLazy computation graph:")
        print(f"  Type: {type(lazy_result)}")
        
        # Collect and examine
        result = lazy_result.collect()
        print(f"\nCollected result:")
        print(f"  Shape: {result.shape}")
        print(f"  Columns: {result.columns}")
        
        # Show first few rows to understand structure
        print(f"\nFirst 5 rows:")
        print(result.head())
        
        # Check for duplicate aggregations
        if "LAND_TYPE" in result.columns:
            land_type_counts = result.group_by("LAND_TYPE").count()
            print(f"\nRows per LAND_TYPE:")
            print(land_type_counts)


if __name__ == "__main__":
    # For manual testing
    pytest.main([__file__, "-xvs"])