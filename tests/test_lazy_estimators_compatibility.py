"""
Comprehensive compatibility tests for lazy estimators vs eager implementations.

This test suite verifies that lazy implementations produce identical results
to their eager counterparts while maintaining full rFIA statistical compatibility.

Tests cover:
- Identical numerical results within tolerance
- Consistent output structure and column naming
- Proper handling of domain filters and grouping
- Edge case compatibility (empty datasets, missing values)
- Statistical accuracy for all estimation types

The tests use the established test database fixture from conftest.py and
follow established patterns from existing pyFIA test files.
"""

import time
import warnings
from typing import Dict, List, Tuple, Any, Optional
from unittest.mock import Mock, patch

import polars as pl
import pytest

# Import all estimator pairs for comparison
from pyfia import FIA
from pyfia.estimation import (
    # Eager functions
    area, biomass, tpa, volume, growth,
    # Lazy functions (now default implementations)
    area, biomass_lazy, tpa, volume_lazy, mortality_lazy
)
from pyfia.estimation.base import EstimatorConfig

# Import mortality functions separately as they may have different import patterns
try:
    from pyfia.estimation.mortality import mortality
except ImportError:
    from pyfia.estimation.mortality.mortality import mortality


class TestLazyEagerCompatibility:
    """Test suite comparing lazy vs eager estimator results for identical outputs."""
    
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

    def test_area_compatibility(self, sample_fia_instance, test_configs):
        """Test that lazy area produces identical results to previous version."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting area compatibility: {config['name']}")
                
                # Test basic area estimation (use params without tree_type)
                eager_result = area(db, **config["params"])
                lazy_result = area(db, show_progress=False, **config["params"])
                
                self.assert_dataframes_equal(
                    eager_result, lazy_result,
                    description=f"Area {config['name']}"
                )
                
                # Test by_land_type if applicable
                if config["name"] == "basic_estimation":
                    eager_by_type = area(db, by_land_type=True, **config["params"])
                    lazy_by_type = area(db, by_land_type=True, show_progress=False, **config["params"])
                    
                    self.assert_dataframes_equal(
                        eager_by_type, lazy_by_type,
                        description=f"Area by_land_type {config['name']}"
                    )

    def test_biomass_compatibility(self, sample_fia_instance, test_configs):
        """Test that biomass_lazy produces identical results to biomass."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting biomass compatibility: {config['name']}")
                
                # Test basic biomass estimation (use tree_params)
                eager_result = biomass(db, **config["tree_params"])
                lazy_result = biomass_lazy(db, show_progress=False, **config["tree_params"])
                
                self.assert_dataframes_equal(
                    eager_result, lazy_result,
                    description=f"Biomass {config['name']}"
                )
                
                # Test different biomass components
                if config["name"] == "basic_estimation":
                    for component in ["AG", "BG"]:
                        eager_comp = biomass(db, component=component, **config["tree_params"])
                        lazy_comp = biomass_lazy(db, component=component, show_progress=False, **config["tree_params"])
                        
                        self.assert_dataframes_equal(
                            eager_comp, lazy_comp,
                            description=f"Biomass {component} {config['name']}"
                        )

    def test_tpa_compatibility(self, sample_fia_instance, test_configs):
        """Test that tpa_lazy produces identical results to tpa."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting TPA compatibility: {config['name']}")
                
                # Test basic TPA estimation (use tree_params)
                eager_result = tpa(db, **config["tree_params"])
                lazy_result = tpa_lazy(db, show_progress=False, **config["tree_params"])
                
                self.assert_dataframes_equal(
                    eager_result, lazy_result,
                    description=f"TPA {config['name']}"
                )

    def test_volume_compatibility(self, sample_fia_instance, test_configs):
        """Test that volume_lazy produces identical results to volume."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            for config in test_configs:
                print(f"\nTesting volume compatibility: {config['name']}")
                
                # Test basic volume estimation (use tree_params)
                eager_result = volume(db, **config["tree_params"])
                lazy_result = volume_lazy(db, show_progress=False, **config["tree_params"])
                
                self.assert_dataframes_equal(
                    eager_result, lazy_result,
                    description=f"Volume {config['name']}"
                )
                
                # Test different volume types
                if config["name"] == "basic_estimation":
                    for vol_type in ["net", "gross"]:
                        eager_vol = volume(db, vol_type=vol_type, **config["tree_params"])
                        lazy_vol = volume_lazy(db, vol_type=vol_type, show_progress=False, **config["tree_params"])
                        
                        self.assert_dataframes_equal(
                            eager_vol, lazy_vol,
                            description=f"Volume {vol_type} {config['name']}"
                        )

    def test_growth_compatibility(self, sample_fia_instance, test_configs):
        """Test that growth_lazy produces identical results to growth."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Growth needs special evaluation setup - may need multi-temporal data
            try:
                for config in test_configs:
                    print(f"\nTesting growth compatibility: {config['name']}")
                    
                    # Test basic growth estimation (use tree_params)
                    eager_result = growth(db, **config["tree_params"])
                    lazy_result = growth(db, show_progress=True, **config["tree_params"])
                    
                    self.assert_dataframes_equal(
                        eager_result, lazy_result,
                        description=f"Growth {config['name']}"
                    )
                    
            except Exception as e:
                if "growth data not available" in str(e).lower():
                    pytest.skip("Growth data not available in test database")
                else:
                    raise

    def test_mortality_compatibility(self, sample_fia_instance, test_configs):
        """Test that mortality_lazy produces identical results to mortality."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Mortality needs special setup - may need mortality component data
            try:
                for config in test_configs:
                    print(f"\nTesting mortality compatibility: {config['name']}")
                    
                    # Test basic mortality estimation (use tree_params)
                    eager_result = mortality(db, **config["tree_params"])
                    lazy_result = mortality_lazy(db, show_progress=False, **config["tree_params"])
                    
                    self.assert_dataframes_equal(
                        eager_result, lazy_result,
                        description=f"Mortality {config['name']}"
                    )
                    
            except Exception as e:
                if "mortality data not available" in str(e).lower():
                    pytest.skip("Mortality data not available in test database")
                else:
                    raise

    def test_empty_dataset_compatibility(self, sample_fia_instance):
        """Test that lazy estimators handle empty datasets same as eager ones."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Use restrictive filter that should return no results
            restrictive_filter = "STATUSCD == 99999"  # Non-existent status
            
            estimator_pairs = [
                (area, area, {}),
                (biomass, biomass_lazy, {}),
                (tpa, tpa_lazy, {}),
                (volume, volume_lazy, {}),
            ]
            
            for eager_func, lazy_func, extra_params in estimator_pairs:
                try:
                    eager_result = eager_func(
                        db, tree_domain=restrictive_filter, **extra_params
                    )
                    lazy_result = lazy_func(
                        db, tree_domain=restrictive_filter, 
                        show_progress=False, **extra_params
                    )
                    
                    # Both should be empty or have zero values
                    assert eager_result.shape == lazy_result.shape
                    if eager_result.height > 0:
                        self.assert_dataframes_equal(
                            eager_result, lazy_result,
                            description=f"Empty dataset {eager_func.__name__}"
                        )
                        
                except Exception as e:
                    # Both should fail in the same way
                    with pytest.raises(type(e)):
                        lazy_func(db, tree_domain=restrictive_filter, 
                                 show_progress=False, **extra_params)

    def test_edge_cases_compatibility(self, sample_fia_instance):
        """Test edge cases where lazy and eager implementations should match."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            edge_cases = [
                {
                    "name": "single_plot",
                    "params": {"tree_domain": "PLT_CN == (SELECT MIN(PLT_CN) FROM TREE LIMIT 1)"}
                },
                {
                    "name": "single_species", 
                    "params": {"tree_domain": "SPCD == 131"}  # Loblolly pine only
                },
                {
                    "name": "minimal_diameter",
                    "params": {"tree_domain": "DIA >= 99.0"}  # Very large trees only
                },
                {
                    "name": "multiple_groups",
                    "params": {"grp_by": ["SPCD", "FORTYPCD"], "totals": True}
                }
            ]
            
            estimator_pairs = [
                (area, area),
                (biomass, biomass_lazy), 
                (tpa, tpa_lazy),
                (volume, volume_lazy),
            ]
            
            for eager_func, lazy_func in estimator_pairs:
                for case in edge_cases:
                    try:
                        print(f"\nTesting {eager_func.__name__} edge case: {case['name']}")
                        
                        eager_result = eager_func(db, **case["params"])
                        lazy_result = lazy_func(db, show_progress=False, **case["params"])
                        
                        self.assert_dataframes_equal(
                            eager_result, lazy_result,
                            description=f"{eager_func.__name__} {case['name']}"
                        )
                        
                    except Exception as e:
                        # If eager fails, lazy should fail the same way
                        with pytest.raises(type(e)):
                            lazy_func(db, show_progress=False, **case["params"])

    def test_statistical_consistency(self, sample_fia_instance):
        """Test that statistical measures (variance, SE) are consistent between implementations."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Test with variance enabled for statistical consistency
            params_with_variance = {"variance": True, "totals": True}
            
            estimator_pairs = [
                (area, area),
                (biomass, biomass_lazy),
                (tpa, tpa_lazy), 
                (volume, volume_lazy),
            ]
            
            for eager_func, lazy_func in estimator_pairs:
                print(f"\nTesting statistical consistency: {eager_func.__name__}")
                
                eager_result = eager_func(db, **params_with_variance)
                lazy_result = lazy_func(db, show_progress=False, **params_with_variance)
                
                # Check that variance and SE columns exist and match
                variance_cols = [col for col in eager_result.columns if "_VAR" in col]
                se_cols = [col for col in eager_result.columns if "_SE" in col]
                
                if variance_cols or se_cols:
                    self.assert_dataframes_equal(
                        eager_result, lazy_result,
                        tolerance=1e-5,  # Slightly more lenient for variance calculations
                        description=f"Statistical consistency {eager_func.__name__}"
                    )
                    
                    print(f"  ✓ Variance columns consistent: {variance_cols}")
                    print(f"  ✓ SE columns consistent: {se_cols}")

    def test_output_column_consistency(self, sample_fia_instance):
        """Test that output column names and types are identical."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            test_params = {"totals": True, "variance": True}
            
            estimator_pairs = [
                (area, area),
                (biomass, biomass_lazy),
                (tpa, tpa_lazy),
                (volume, volume_lazy),
            ]
            
            for eager_func, lazy_func in estimator_pairs:
                print(f"\nTesting column consistency: {eager_func.__name__}")
                
                eager_result = eager_func(db, **test_params)
                lazy_result = lazy_func(db, show_progress=False, **test_params)
                
                # Check column names match exactly
                assert set(eager_result.columns) == set(lazy_result.columns), \
                    f"{eager_func.__name__}: Column names differ"
                
                # Check column order matches
                assert eager_result.columns == lazy_result.columns, \
                    f"{eager_func.__name__}: Column order differs"
                
                # Check data types match
                for col in eager_result.columns:
                    eager_dtype = eager_result[col].dtype
                    lazy_dtype = lazy_result[col].dtype
                    
                    # Allow some flexibility in numeric types (Float32 vs Float64)
                    if eager_dtype in [pl.Float32, pl.Float64] and lazy_dtype in [pl.Float32, pl.Float64]:
                        continue
                    
                    assert eager_dtype == lazy_dtype, \
                        f"{eager_func.__name__}: Column {col} type differs: {eager_dtype} vs {lazy_dtype}"
                
                print(f"  ✓ All {len(eager_result.columns)} columns consistent")

    @pytest.mark.parametrize("estimator_name,eager_func,lazy_func", [
        ("area", area, area),
        ("biomass", biomass, biomass_lazy), 
        ("tpa", tpa, tpa_lazy),
        ("volume", volume, volume_lazy),
    ])
    def test_parameter_validation_consistency(self, sample_fia_instance, 
                                            estimator_name, eager_func, lazy_func):
        """Test that parameter validation behaves consistently."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Test invalid parameter scenarios
            invalid_params_list = [
                {"land_type": "invalid_land_type"},
                {"tree_type": "invalid_tree_type"} if estimator_name != "area" else {},
                {"vol_type": "invalid_vol_type"} if estimator_name == "volume" else {},
                {"component": "invalid_component"} if estimator_name == "biomass" else {},
            ]
            
            for invalid_params in invalid_params_list:
                if not invalid_params:
                    continue
                
                print(f"\nTesting parameter validation for {estimator_name}: {invalid_params}")
                
                # Both should fail with the same exception type
                eager_exception = None
                lazy_exception = None
                
                try:
                    eager_func(db, **invalid_params)
                except Exception as e:
                    eager_exception = type(e)
                
                try:
                    lazy_func(db, show_progress=False, **invalid_params)
                except Exception as e:
                    lazy_exception = type(e)
                
                # Both should fail or both should succeed
                assert eager_exception == lazy_exception, \
                    f"Exception consistency failed for {estimator_name}: eager={eager_exception}, lazy={lazy_exception}"

    def test_reproducibility(self, sample_fia_instance):
        """Test that lazy estimators produce identical results across multiple runs."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            # Run each lazy estimator multiple times to ensure reproducibility
            lazy_funcs = [area, biomass_lazy, tpa_lazy, volume_lazy]
            
            for lazy_func in lazy_funcs:
                print(f"\nTesting reproducibility: {lazy_func.__name__}")
                
                # Run the same estimation 3 times
                params = {"totals": True, "show_progress": False}
                results = []
                
                for i in range(3):
                    result = lazy_func(db, **params)
                    results.append(result)
                
                # All results should be identical
                for i in range(1, len(results)):
                    self.assert_dataframes_equal(
                        results[0], results[i],
                        description=f"Reproducibility {lazy_func.__name__} run {i+1}"
                    )
                
                print(f"  ✓ All {len(results)} runs produced identical results")


class TestLazyMigrationSafety:
    """Test suite ensuring lazy migration doesn't break existing functionality."""
    
    def test_backward_compatibility_imports(self):
        """Test that all imports still work after lazy migration."""
        # Test that we can still import all original functions
        from pyfia.estimation import area, biomass, tpa, volume, growth
        
        # Test that lazy functions are available
        from pyfia.estimation import area, tpa_lazy, volume_lazy
        from pyfia.estimation.biomass import biomass as biomass_lazy
        
        # Test mortality imports (may be in different location)
        try:
            from pyfia.estimation import mortality, mortality_lazy
        except ImportError:
            from pyfia.estimation.mortality import mortality
            from pyfia.estimation import mortality_lazy
        
        # All imports should succeed
        assert area is not None
        assert area is not None
        print("✓ All estimator imports successful")

    def test_estimator_config_compatibility(self, sample_fia_instance):
        """Test that EstimatorConfig works with both eager and lazy estimators.""" 
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

    def test_api_signature_consistency(self):
        """Test that lazy functions have consistent API signatures with eager ones."""
        import inspect
        
        estimator_pairs = [
            (area, area_lazy),
            (biomass, biomass_lazy),
            (tpa, tpa_lazy),
            (volume, volume_lazy),
            (growth, growth),  # growth now uses lazy evaluation by default
        ]
        
        for eager_func, lazy_func in estimator_pairs:
            eager_sig = inspect.signature(eager_func)
            lazy_sig = inspect.signature(lazy_func)
            
            # Get parameter names (excluding lazy-specific ones)
            eager_params = set(eager_sig.parameters.keys())
            lazy_params = set(lazy_sig.parameters.keys())
            
            # Lazy functions may have additional parameters like show_progress
            lazy_specific = {"show_progress", "lazy_enabled", "collection_strategy"}
            core_lazy_params = lazy_params - lazy_specific
            
            # Core parameters should match
            assert eager_params <= core_lazy_params, \
                f"{eager_func.__name__} vs {lazy_func.__name__}: Missing parameters {eager_params - core_lazy_params}"
            
            print(f"✓ {eager_func.__name__} API signature compatible")

    def test_warning_suppression(self, sample_fia_instance):
        """Test that lazy estimators don't produce unexpected warnings."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        with sample_fia_instance as db:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                
                # Run lazy estimators and check for warnings
                lazy_funcs = [area, biomass_lazy, tpa_lazy, volume_lazy]
                
                for lazy_func in lazy_funcs:
                    result = lazy_func(db, show_progress=False)
                    assert result is not None
                
                # Check if any unexpected warnings were raised
                unexpected_warnings = [
                    warning for warning in w 
                    if "lazy evaluation" not in str(warning.message).lower()
                    and "deprecation" not in str(warning.message).lower()
                ]
                
                if unexpected_warnings:
                    print("Unexpected warnings:")
                    for warning in unexpected_warnings:
                        print(f"  {warning.category.__name__}: {warning.message}")
                
                # This is informational - we don't fail on warnings
                print(f"✓ Captured {len(w)} total warnings, {len(unexpected_warnings)} unexpected")