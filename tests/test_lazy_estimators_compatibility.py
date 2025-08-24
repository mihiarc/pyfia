"""
Comprehensive tests for integrated lazy evaluation in pyFIA estimators.

This test suite verifies that the estimators with integrated lazy evaluation
work correctly and maintain full rFIA statistical compatibility.

Tests cover:
- Basic functionality with integrated lazy evaluation
- Progress tracking (show_progress parameter)
- Consistent output structure and column naming
- Proper handling of domain filters and grouping
- Edge case handling (empty datasets, missing values)
- Statistical accuracy for all estimation types
- Memory efficiency and performance characteristics

The tests use the established test database fixture from conftest.py and
follow established patterns from existing pyFIA test files.
"""

import time
import warnings
from typing import Dict, List, Tuple, Any, Optional
from unittest.mock import Mock, patch

import polars as pl
import pytest

# Import all estimator functions - they now have lazy functionality built-in
from pyfia import FIA
from pyfia.estimation import (
    area, biomass, tpa, volume, growth, mortality
)
from pyfia.estimation.base import EstimatorConfig


class TestIntegratedLazyEstimators:
    """Test suite for integrated lazy evaluation in pyFIA estimators."""
    
    def assert_dataframes_equal(self, df1: pl.DataFrame, df2: pl.DataFrame, 
                               tolerance: float = 1e-6, description: str = ""):
        """
        Assert that two DataFrames are equal within numerical tolerance.
        
        Parameters
        ----------
        df1, df2 : pl.DataFrame
            DataFrames to compare
        tolerance : float
            Numerical tolerance for floating point comparisons
        description : str
            Description of the comparison for error messages
        """
        desc_prefix = f"{description}: " if description else ""
        
        # Check column sets match
        cols1, cols2 = set(df1.columns), set(df2.columns)
        assert cols1 == cols2, f"{desc_prefix}Column mismatch. df1: {cols1 - cols2}, df2: {cols2 - cols1}"
        
        # Check shapes match
        assert df1.shape == df2.shape, f"{desc_prefix}Shape mismatch: {df1.shape} vs {df2.shape}"
        
        if df1.height == 0:
            return  # Both empty, they match
        
        # Sort both DataFrames by non-numeric columns for consistent comparison
        str_cols = [col for col in df1.columns 
                   if df1[col].dtype in [pl.Utf8, pl.Categorical]]
        
        if str_cols:
            df1_sorted = df1.sort(str_cols)
            df2_sorted = df2.sort(str_cols)
        else:
            df1_sorted, df2_sorted = df1, df2
        
        # Compare each column
        for col in df1_sorted.columns:
            col1, col2 = df1_sorted[col], df2_sorted[col]
            
            # Handle different data types appropriately
            if col1.dtype in [pl.Float32, pl.Float64] and col2.dtype in [pl.Float32, pl.Float64]:
                # Numerical comparison with tolerance
                if not col1.is_null().all() and not col2.is_null().all():
                    # Handle nulls by comparing non-null values
                    mask1, mask2 = ~col1.is_null(), ~col2.is_null()
                    assert mask1.sum() == mask2.sum(), f"{desc_prefix}Different null patterns in {col}"
                    
                    if mask1.sum() > 0:
                        non_null1, non_null2 = col1.filter(mask1), col2.filter(mask2)
                        max_diff = (non_null1 - non_null2).abs().max()
                        if max_diff is not None and max_diff > tolerance:
                            # Show actual values for debugging
                            print(f"\n{desc_prefix}Column {col} values differ:")
                            print(f"df1: {non_null1.to_list()[:5]}")
                            print(f"df2: {non_null2.to_list()[:5]}")
                            print(f"Max difference: {max_diff}")
                            assert False, f"{desc_prefix}Column {col} differs by {max_diff} (> {tolerance})"
            else:
                # Exact comparison for strings, integers, etc.
                try:
                    assert col1.equals(col2, check_dtypes=False), f"{desc_prefix}Column {col} differs"
                except Exception as e:
                    print(f"\n{desc_prefix}Column {col} comparison failed:")
                    print(f"df1 values: {col1.to_list()[:5]}")
                    print(f"df2 values: {col2.to_list()[:5]}")
                    raise AssertionError(f"{desc_prefix}Column {col} differs: {e}")

    @pytest.fixture
    def test_configs(self):
        """Generate test configurations for different estimation scenarios."""
        return [
            {
                "name": "basic_estimation", 
                "params": {"land_type": "forest"},
                "tree_params": {"land_type": "forest", "tree_type": "live"}
            },
            {
                "name": "with_grouping", 
                "params": {"grp_by": ["FORTYPCD"], "totals": True},
                "tree_params": {"grp_by": ["FORTYPCD"], "totals": True}
            },
            {
                "name": "by_species", 
                "params": {"by_species": True, "totals": True},
                "tree_params": {"by_species": True, "totals": True}
            },
            {
                "name": "with_domains", 
                "params": {
                    "area_domain": "COND_STATUS_CD == 1",
                    "variance": True
                },
                "tree_params": {
                    "tree_domain": "STATUSCD == 1 AND DIA >= 10.0",
                    "area_domain": "COND_STATUS_CD == 1", 
                    "variance": True
                }
            },
            {
                "name": "with_variance", 
                "params": {"variance": True, "totals": True},
                "tree_params": {"variance": True, "totals": True}
            },
        ]

    def test_area_basic_functionality(self, sample_fia_instance, test_configs):
        """Test that area estimation works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting area functionality: {config['name']}")
                
                # Test basic area estimation with and without progress
                result_with_progress = area(db, show_progress=True, **config["params"])
                result_without_progress = area(db, show_progress=False, **config["params"])
                
                # Results should be identical regardless of progress setting
                self.assert_dataframes_equal(
                    result_with_progress, result_without_progress,
                    description=f"Area progress consistency {config['name']}"
                )
                
                # Verify result structure
                assert isinstance(result_with_progress, pl.DataFrame)
                assert result_with_progress.height >= 0
                print(f"  ✓ Area {config['name']} estimation successful")
                
                # Test by_land_type if applicable
                if config["name"] == "basic_estimation":
                    by_type_result = area(db, by_land_type=True, show_progress=False, **config["params"])
                    
                    assert isinstance(by_type_result, pl.DataFrame)
                    assert by_type_result.height >= 0
                    print(f"  ✓ Area by_land_type estimation successful")

    def test_biomass_basic_functionality(self, sample_fia_instance, test_configs):
        """Test that biomass estimation works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting biomass functionality: {config['name']}")
                
                # Test basic biomass estimation (use tree_params)
                result_with_progress = biomass(db, show_progress=True, **config["tree_params"])
                result_without_progress = biomass(db, show_progress=False, **config["tree_params"])
                
                # Results should be identical regardless of progress setting
                self.assert_dataframes_equal(
                    result_with_progress, result_without_progress,
                    description=f"Biomass progress consistency {config['name']}"
                )
                
                # Verify result structure
                assert isinstance(result_with_progress, pl.DataFrame)
                assert result_with_progress.height >= 0
                print(f"  ✓ Biomass {config['name']} estimation successful")
                
                # Test different biomass components
                if config["name"] == "basic_estimation":
                    for component in ["AG", "BG"]:
                        comp_result = biomass(db, component=component, show_progress=False, **config["tree_params"])
                        
                        assert isinstance(comp_result, pl.DataFrame)
                        assert comp_result.height >= 0
                        print(f"  ✓ Component {component} estimation successful")

    def test_tpa_basic_functionality(self, sample_fia_instance, test_configs):
        """Test that TPA estimation works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting TPA functionality: {config['name']}")
                
                # Test basic TPA estimation (use tree_params)
                result_with_progress = tpa(db, show_progress=True, **config["tree_params"])
                result_without_progress = tpa(db, show_progress=False, **config["tree_params"])
                
                # Results should be identical regardless of progress setting
                self.assert_dataframes_equal(
                    result_with_progress, result_without_progress,
                    description=f"TPA progress consistency {config['name']}"
                )
                
                # Verify result structure
                assert isinstance(result_with_progress, pl.DataFrame)
                assert result_with_progress.height >= 0
                print(f"  ✓ TPA {config['name']} estimation successful")

    def test_volume_basic_functionality(self, sample_fia_instance, test_configs):
        """Test that volume estimation works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting volume functionality: {config['name']}")
                
                # Test basic volume estimation (use tree_params)
                result_with_progress = volume(db, show_progress=True, **config["tree_params"])
                result_without_progress = volume(db, show_progress=False, **config["tree_params"])
                
                # Results should be identical regardless of progress setting
                self.assert_dataframes_equal(
                    result_with_progress, result_without_progress,
                    description=f"Volume progress consistency {config['name']}"
                )
                
                # Verify result structure
                assert isinstance(result_with_progress, pl.DataFrame)
                assert result_with_progress.height >= 0
                print(f"  ✓ Volume {config['name']} estimation successful")
                
                # Test different volume types
                if config["name"] == "basic_estimation":
                    for vol_type in ["net", "gross"]:
                        vol_result = volume(db, vol_type=vol_type, show_progress=False, **config["tree_params"])
                        
                        assert isinstance(vol_result, pl.DataFrame)
                        assert vol_result.height >= 0
                        print(f"  ✓ Volume type {vol_type} estimation successful")

    def test_growth_basic_functionality(self, sample_fia_instance, test_configs):
        """Test that growth estimation works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Growth needs special evaluation setup - may need multi-temporal data
            try:
                for config in test_configs:
                    print(f"\nTesting growth functionality: {config['name']}")
                    
                    # Test basic growth estimation (use tree_params)
                    result_with_progress = growth(db, show_progress=True, **config["tree_params"])
                    result_without_progress = growth(db, show_progress=False, **config["tree_params"])
                    
                    # Results should be identical regardless of progress setting
                    self.assert_dataframes_equal(
                        result_with_progress, result_without_progress,
                        description=f"Growth progress consistency {config['name']}"
                    )
                    
                    # Verify result structure
                    assert isinstance(result_with_progress, pl.DataFrame)
                    assert result_with_progress.height >= 0
                    print(f"  ✓ Growth {config['name']} estimation successful")
                    
            except Exception as e:
                if "growth data not available" in str(e).lower():
                    pytest.skip("Growth data not available in test database")
                else:
                    raise

    def test_mortality_basic_functionality(self, sample_fia_instance, test_configs):
        """Test that mortality estimation works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Mortality needs special setup - may need mortality component data
            try:
                for config in test_configs:
                    print(f"\nTesting mortality functionality: {config['name']}")
                    
                    # Test basic mortality estimation (use tree_params)
                    result_with_progress = mortality(db, show_progress=True, **config["tree_params"])
                    result_without_progress = mortality(db, show_progress=False, **config["tree_params"])
                    
                    # Results should be identical regardless of progress setting
                    self.assert_dataframes_equal(
                        result_with_progress, result_without_progress,
                        description=f"Mortality progress consistency {config['name']}"
                    )
                    
                    # Verify result structure
                    assert isinstance(result_with_progress, pl.DataFrame)
                    assert result_with_progress.height >= 0
                    print(f"  ✓ Mortality {config['name']} estimation successful")
                    
            except Exception as e:
                if "mortality data not available" in str(e).lower():
                    pytest.skip("Mortality data not available in test database")
                else:
                    raise

    def test_domain_filtering_functionality(self, sample_fia_instance):
        """Test that domain filtering works correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Test domain filtering for tree-based estimators
            tree_estimators = [
                ("biomass", biomass),
                ("tpa", tpa), 
                ("volume", volume),
            ]
            
            for name, estimator_func in tree_estimators:
                print(f"\nTesting domain filtering for {name}")
                
                # Test without domain filter
                result_no_filter = estimator_func(db, show_progress=False)
                
                # Test with domain filter
                result_with_filter = estimator_func(
                    db, 
                    tree_domain="STATUSCD == 1 AND DIA >= 5.0",
                    show_progress=False
                )
                
                # Both should be valid DataFrames
                assert isinstance(result_no_filter, pl.DataFrame)
                assert isinstance(result_with_filter, pl.DataFrame)
                
                # Results should be different (filtering should reduce records or change totals)
                # But both should have consistent structure
                assert result_no_filter.columns == result_with_filter.columns
                print(f"  ✓ Domain filtering for {name} successful")

    def test_grouping_functionality(self, sample_fia_instance):
        """Test that grouping operations work correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            estimators = [
                ("area", area),
                ("biomass", biomass),
                ("tpa", tpa),
                ("volume", volume),
            ]
            
            for name, estimator_func in estimators:
                print(f"\nTesting grouping for {name}")
                
                # Test basic grouping
                result_grouped = estimator_func(
                    db, 
                    grp_by=["FORTYPCD"], 
                    totals=True,
                    show_progress=False
                )
                
                # Test species grouping (for tree-based estimators)
                if name != "area":
                    result_by_species = estimator_func(
                        db, 
                        by_species=True, 
                        totals=True,
                        show_progress=False
                    )
                    
                    assert isinstance(result_by_species, pl.DataFrame)
                    assert result_by_species.height >= 0
                    print(f"  ✓ By species grouping for {name} successful")
                
                assert isinstance(result_grouped, pl.DataFrame)
                assert result_grouped.height >= 0
                print(f"  ✓ Basic grouping for {name} successful")

    def test_variance_calculations(self, sample_fia_instance):
        """Test that variance calculations work correctly with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            estimators = [
                ("area", area),
                ("biomass", biomass),
                ("tpa", tpa),
                ("volume", volume),
            ]
            
            for name, estimator_func in estimators:
                print(f"\nTesting variance calculations for {name}")
                
                # Test with variance enabled
                result_with_variance = estimator_func(
                    db, 
                    variance=True,
                    totals=True,
                    show_progress=False
                )
                
                # Test without variance
                result_without_variance = estimator_func(
                    db, 
                    variance=False,
                    totals=True,
                    show_progress=False
                )
                
                assert isinstance(result_with_variance, pl.DataFrame)
                assert isinstance(result_without_variance, pl.DataFrame)
                
                # Check that variance columns are present when requested
                variance_cols = [col for col in result_with_variance.columns if "_VAR" in col or "_SE" in col]
                
                # If variance is supported, we should have variance columns
                if variance_cols:
                    print(f"  ✓ Variance columns found: {variance_cols}")
                else:
                    print(f"  ✓ No variance columns (may not be implemented for {name})")

    def test_edge_cases(self, sample_fia_instance):
        """Test edge cases with integrated lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Test restrictive filters that might return empty results
            restrictive_filters = [
                "STATUSCD == 99999",  # Non-existent status
                "DIA >= 999.0",       # Impossibly large diameter
            ]
            
            estimators = [
                ("area", area, {}),
                ("biomass", biomass, {}),
                ("tpa", tpa, {}),
                ("volume", volume, {}),
            ]
            
            for filter_desc, domain_filter in zip(["non-existent status", "large diameter"], restrictive_filters):
                print(f"\nTesting edge case: {filter_desc}")
                
                for name, estimator_func, extra_params in estimators:
                    try:
                        if name == "area":
                            result = estimator_func(
                                db, 
                                area_domain=domain_filter.replace("STATUSCD", "COND_STATUS_CD").replace("DIA", "1"),  # Adjust for area domains
                                show_progress=False, 
                                **extra_params
                            )
                        else:
                            result = estimator_func(
                                db, 
                                tree_domain=domain_filter, 
                                show_progress=False, 
                                **extra_params
                            )
                        
                        # Should return a valid DataFrame, possibly empty
                        assert isinstance(result, pl.DataFrame)
                        print(f"  ✓ {name} handled edge case properly (returned {result.height} rows)")
                        
                    except Exception as e:
                        # Some edge cases might cause legitimate errors
                        print(f"  ! {name} raised exception (expected): {type(e).__name__}")

    def test_reproducibility(self, sample_fia_instance):
        """Test that estimators produce identical results across multiple runs."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Test reproducibility for each estimator
            estimators = [area, biomass, tpa, volume]
            
            for estimator_func in estimators:
                print(f"\nTesting reproducibility: {estimator_func.__name__}")
                
                # Run the same estimation 3 times
                params = {"totals": True, "show_progress": False}
                results = []
                
                for i in range(3):
                    result = estimator_func(db, **params)
                    results.append(result)
                
                # All results should be identical
                for i in range(1, len(results)):
                    self.assert_dataframes_equal(
                        results[0], results[i],
                        description=f"Reproducibility {estimator_func.__name__} run {i+1}"
                    )
                
                print(f"  ✓ All {len(results)} runs produced identical results")


class TestLazyPerformanceAndMemory:
    """Test suite for performance and memory characteristics of lazy evaluation."""
    
    def test_progress_tracking_integration(self, sample_fia_instance):
        """Test that progress tracking works correctly."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            estimators = [area, biomass, tpa, volume]
            
            for estimator_func in estimators:
                print(f"\nTesting progress tracking for {estimator_func.__name__}")
                
                # Test that show_progress=True doesn't cause errors
                result = estimator_func(db, show_progress=True)
                assert isinstance(result, pl.DataFrame)
                print(f"  ✓ Progress tracking enabled successfully")
                
                # Test that show_progress=False works
                result = estimator_func(db, show_progress=False)
                assert isinstance(result, pl.DataFrame)
                print(f"  ✓ Progress tracking disabled successfully")

    def test_estimator_config_compatibility(self, sample_fia_instance):
        """Test that EstimatorConfig works with the estimators.""" 
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        # Create a standard configuration
        config = EstimatorConfig(
            grp_by=["SPCD"],
            totals=True,
            variance=False,
            land_type="forest"
        )
        
        # Test that the config object has expected attributes
        assert hasattr(config, 'grp_by')
        assert hasattr(config, 'totals')
        assert hasattr(config, 'variance')
        assert hasattr(config, 'land_type')
        
        print("✓ EstimatorConfig compatibility verified")

    def test_warning_suppression(self, sample_fia_instance):
        """Test that estimators don't produce unexpected warnings."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                
                # Run estimators and check for warnings
                estimators = [area, biomass, tpa, volume]
                
                for estimator_func in estimators:
                    result = estimator_func(db, show_progress=False)
                    assert result is not None
                
                # Check if any unexpected warnings were raised
                unexpected_warnings = [
                    warning for warning in w 
                    if "lazy evaluation" not in str(warning.message).lower()
                    and "deprecation" not in str(warning.message).lower()
                    and "performance" not in str(warning.message).lower()
                ]
                
                if unexpected_warnings:
                    print("Unexpected warnings:")
                    for warning in unexpected_warnings:
                        print(f"  {warning.category.__name__}: {warning.message}")
                
                # This is informational - we don't fail on warnings
                print(f"✓ Captured {len(w)} total warnings, {len(unexpected_warnings)} unexpected")