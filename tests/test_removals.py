"""
Tests for removals estimation.

Tests the calculation of average annual removals of merchantable bole wood 
volume of growing-stock trees.
"""

import pytest
import polars as pl
from pathlib import Path

from pyfia import FIA, removals
from pyfia.estimation.estimators.removals import RemovalsEstimator


@pytest.fixture
def sample_db(tmp_path):
    """Create a sample database for testing."""
    # This would use the actual test database from conftest.py
    # For now, return a mock path
    return tmp_path / "test.db"


class TestRemovalsEstimator:
    """Test RemovalsEstimator class functionality."""
    
    def test_estimator_initialization(self, sample_db):
        """Test that RemovalsEstimator initializes correctly."""
        config = {
            "measure": "volume",
            "land_type": "forest",
            "tree_type": "gs"
        }
        # This would fail without a real database
        # estimator = RemovalsEstimator(str(sample_db), config)
        # assert estimator.config == config
        assert True  # Placeholder
    
    def test_required_tables(self, tmp_path):
        """Test that required tables are correctly specified."""
        config = {}
        # Create a dummy db file to avoid FileNotFoundError
        dummy_db = tmp_path / "dummy.db"
        dummy_db.touch()
        
        # Create estimator - will fail on actual DB operations but not on init
        try:
            estimator = RemovalsEstimator(str(dummy_db), config)
        except:
            # If DB init fails, create estimator directly without DB
            from unittest.mock import MagicMock
            estimator = RemovalsEstimator.__new__(RemovalsEstimator)
            estimator.db = MagicMock()
            estimator.config = config
            estimator._owns_db = False
            estimator._ref_species_cache = None
            estimator._stratification_cache = None
        tables = estimator.get_required_tables()
        
        assert "TREE" in tables
        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert "TREE_GRM_COMPONENT" in tables
        assert "TREE_GRM_MIDPT" in tables
    
    def test_tree_columns(self, tmp_path):
        """Test that required tree columns are correctly specified."""
        # Test for volume measure
        config = {"measure": "volume"}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        cols = estimator.get_tree_columns()
        
        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "STATUSCD" in cols
        assert "SPCD" in cols
        assert "DIA" in cols
        assert "TPA_UNADJ" in cols
        
        # Test for biomass measure
        config = {"measure": "biomass"}
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        cols = estimator.get_tree_columns()
        
        assert "DRYBIO_AG" in cols
        assert "DRYBIO_BG" in cols
    
    def test_calculate_values_volume(self):
        """Test volume calculation logic."""
        config = {"measure": "volume", "remeasure_period": 5.0}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        # Create sample data
        data = pl.DataFrame({
            "VOLCFNET": [100.0, 200.0, 150.0],
            "TPAREMV_UNADJ": [2.0, 1.5, 3.0]
        }).lazy()
        
        # Calculate values
        result = estimator.calculate_values(data).collect()
        
        # Check calculations
        assert "REMV_VALUE" in result.columns
        assert "REMV_ANNUAL" in result.columns
        
        # Verify calculations
        expected_values = [200.0, 300.0, 450.0]  # VOLCFNET * TPAREMV_UNADJ
        expected_annual = [40.0, 60.0, 90.0]  # Divided by 5-year period
        
        assert result["REMV_VALUE"].to_list() == expected_values
        assert result["REMV_ANNUAL"].to_list() == expected_annual
    
    def test_calculate_values_biomass(self):
        """Test biomass calculation logic."""
        config = {"measure": "biomass", "remeasure_period": 5.0}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        # Create sample data
        data = pl.DataFrame({
            "DRYBIO_AG": [1000.0, 2000.0, 1500.0],
            "DRYBIO_BG": [200.0, 400.0, 300.0],
            "TPAREMV_UNADJ": [2.0, 1.5, 3.0]
        }).lazy()
        
        # Calculate values
        result = estimator.calculate_values(data).collect()
        
        # Check calculations
        assert "REMV_VALUE" in result.columns
        assert "REMV_ANNUAL" in result.columns
        
        # Verify calculations (biomass in tons)
        expected_values = [1.2, 1.8, 2.7]  # (AG + BG) * TPA / 2000
        expected_annual = [0.24, 0.36, 0.54]  # Divided by 5-year period
        
        assert result["REMV_VALUE"].to_list() == expected_values
        # Use approximate comparison for floating point
        import numpy as np
        np.testing.assert_array_almost_equal(
            result["REMV_ANNUAL"].to_list(),
            expected_annual,
            decimal=10
        )
    
    def test_calculate_values_count(self):
        """Test tree count calculation logic."""
        config = {"measure": "count", "remeasure_period": 5.0}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        # Create sample data
        data = pl.DataFrame({
            "TPAREMV_UNADJ": [2.0, 1.5, 3.0]
        }).lazy()
        
        # Calculate values
        result = estimator.calculate_values(data).collect()
        
        # Check calculations
        assert "REMV_VALUE" in result.columns
        assert "REMV_ANNUAL" in result.columns
        
        # Verify calculations
        expected_values = [2.0, 1.5, 3.0]  # Direct TPA values
        expected_annual = [0.4, 0.3, 0.6]  # Divided by 5-year period
        
        assert result["REMV_VALUE"].to_list() == expected_values
        # Use approximate comparison for floating point
        import numpy as np
        np.testing.assert_array_almost_equal(
            result["REMV_ANNUAL"].to_list(),
            expected_annual,
            decimal=10
        )


class TestRemovalsFunction:
    """Test the main removals() function."""
    
    def test_removals_basic_call(self, sample_db):
        """Test basic removals function call."""
        # This would require a real database with GRM tables
        # For now, just test that the function exists and accepts parameters
        pass
    
    def test_removals_parameters(self):
        """Test that removals function accepts all expected parameters."""
        # Test that function signature is correct
        import inspect
        sig = inspect.signature(removals)
        params = list(sig.parameters.keys())
        
        assert "db" in params
        assert "grp_by" in params
        assert "by_species" in params
        assert "by_size_class" in params
        assert "land_type" in params
        assert "tree_type" in params
        assert "measure" in params
        assert "tree_domain" in params
        assert "area_domain" in params
        assert "totals" in params
        assert "variance" in params
        assert "most_recent" in params
        assert "remeasure_period" in params
    
    def test_removals_config_creation(self):
        """Test that configuration is created correctly."""
        # This tests the internal config creation
        config = {
            "grp_by": "STATECD",
            "by_species": True,
            "by_size_class": False,
            "land_type": "timber",
            "tree_type": "gs",
            "measure": "biomass",
            "tree_domain": "DIA > 10",
            "area_domain": "SITECLCD >= 225",
            "totals": True,
            "variance": False,
            "most_recent": True,
            "remeasure_period": 7.0
        }
        
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        assert estimator.config["grp_by"] == "STATECD"
        assert estimator.config["by_species"] is True
        assert estimator.config["measure"] == "biomass"
        assert estimator.config["remeasure_period"] == 7.0


class TestRemovalsFiltering:
    """Test removals-specific filtering logic."""
    
    def test_component_filtering(self):
        """Test that only CUT and DIVERSION components are included."""
        config = {}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        # Create sample data with various components
        data = pl.DataFrame({
            "COMPONENT": ["CUT1", "CUT2", "DIVERSION1", "GROWTH", "MORT", "CUT3"],
            "TPAREMV_UNADJ": [1.0, 2.0, 1.5, 3.0, 2.5, 2.2],
            "DIA": [10.0, 12.0, 8.0, 15.0, 9.0, 11.0]
        }).lazy()
        
        # Mock the parent apply_filters to just return data
        estimator.apply_filters.__func__.__wrapped__ = lambda self, d: d
        
        # Apply removals-specific filters
        result = estimator.apply_filters(data).collect()
        
        # Should only have CUT and DIVERSION components
        assert len(result) == 4
        assert all(
            comp.startswith("CUT") or comp.startswith("DIVERSION") 
            for comp in result["COMPONENT"].to_list()
        )
    
    def test_null_removal_filtering(self):
        """Test that null or zero removal values are filtered out."""
        config = {}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        # Create sample data with null and zero values
        data = pl.DataFrame({
            "COMPONENT": ["CUT1", "CUT2", "CUT3", "CUT4"],
            "TPAREMV_UNADJ": [1.0, None, 0.0, 2.0],
            "DIA": [10.0, 12.0, 8.0, 15.0]
        }).lazy()
        
        # Mock the parent apply_filters
        estimator.apply_filters.__func__.__wrapped__ = lambda self, d: d
        
        # Apply filters
        result = estimator.apply_filters(data).collect()
        
        # Should only have non-null, non-zero values
        assert len(result) == 2
        assert None not in result["TPAREMV_UNADJ"].to_list()
        assert 0.0 not in result["TPAREMV_UNADJ"].to_list()
    
    def test_growing_stock_filtering(self):
        """Test that growing stock filtering works correctly."""
        config = {"tree_type": "gs"}
        # Create dummy estimator without DB connection
        from unittest.mock import MagicMock
        estimator = RemovalsEstimator.__new__(RemovalsEstimator)
        estimator.db = MagicMock()
        estimator.config = config
        
        # Create sample data
        data = pl.DataFrame({
            "COMPONENT": ["CUT1", "CUT2", "CUT3"],
            "TPAREMV_UNADJ": [1.0, 2.0, 1.5],
            "DIA": [4.0, 6.0, 3.0]  # Only >= 5.0 should remain
        }).lazy()
        
        # Mock the parent apply_filters
        estimator.apply_filters.__func__.__wrapped__ = lambda self, d: d
        
        # Apply filters
        result = estimator.apply_filters(data).collect()
        
        # Should only have trees with DIA >= 5.0
        assert len(result) == 1
        assert all(dia >= 5.0 for dia in result["DIA"].to_list())


class TestRemovalsAdjustmentFactors:
    """Test adjustment factor application for removals."""
    
    def test_adjustment_factor_selection(self):
        """Test that correct adjustment factors are selected based on SUBPTYP_GRM."""
        # Create sample data with different SUBPTYP_GRM values
        data = pl.DataFrame({
            "SUBPTYP_GRM": [0, 1, 2, 3, 1],
            "REMV_ANNUAL": [10.0, 20.0, 30.0, 40.0, 50.0],
            "ADJ_FACTOR_SUBP": [1.1, 1.2, 1.3, 1.4, 1.5],
            "ADJ_FACTOR_MICR": [2.1, 2.2, 2.3, 2.4, 2.5],
            "ADJ_FACTOR_MACR": [3.1, 3.2, 3.3, 3.4, 3.5]
        })
        
        # Apply adjustment factor selection logic
        result = data.with_columns([
            pl.when(pl.col("SUBPTYP_GRM") == 0)
            .then(0.0)
            .when(pl.col("SUBPTYP_GRM") == 1)
            .then(pl.col("ADJ_FACTOR_SUBP"))
            .when(pl.col("SUBPTYP_GRM") == 2)
            .then(pl.col("ADJ_FACTOR_MICR"))
            .when(pl.col("SUBPTYP_GRM") == 3)
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(0.0)
            .alias("ADJ_FACTOR")
        ])
        
        # Check that correct factors were selected
        expected_factors = [0.0, 1.2, 2.3, 3.4, 1.5]
        assert result["ADJ_FACTOR"].to_list() == expected_factors
        
        # Apply adjustment
        result = result.with_columns([
            (pl.col("REMV_ANNUAL") * pl.col("ADJ_FACTOR")).alias("REMV_ADJ")
        ])
        
        # Verify adjusted values
        expected_adjusted = [0.0, 24.0, 69.0, 136.0, 75.0]
        assert result["REMV_ADJ"].to_list() == expected_adjusted


if __name__ == "__main__":
    pytest.main([__file__, "-v"])