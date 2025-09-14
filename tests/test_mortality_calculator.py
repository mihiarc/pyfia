"""
Tests for the enhanced mortality calculator.

This module tests the MortalityCalculator class and related components
for proper functionality and statistical accuracy.
"""

import pytest
import polars as pl

from pyfia import FIA
from pyfia.estimation import mortality, MortalityEstimator


class TestMortalityEstimatorConfig:
    """Test the mortality configuration class."""
    
    def test_basic_config(self):
        """Test basic configuration creation."""
        config = MortalityEstimatorConfig(
            by_species=True,
            land_type="forest",
            variance=True
        )
        
        assert config.by_species is True
        assert config.land_type == "forest"
        assert config.variance is True
        
    def test_grouping_variables(self):
        """Test grouping variable extraction."""
        config = MortalityEstimatorConfig(
            by_species=True,
            group_by_ownership=True,
            group_by_agent=True,
            grp_by=["UNITCD", "COUNTYCD"]
        )
        
        groups = config.get_grouping_variables()
        assert "SPCD" in groups
        assert "OWNGRPCD" in groups
        assert "AGENTCD" in groups
        assert "UNITCD" in groups
        assert "COUNTYCD" in groups
        
    def test_disturbance_grouping(self):
        """Test disturbance code grouping."""
        config = MortalityEstimatorConfig(
            group_by_disturbance=True
        )
        
        groups = config.get_grouping_variables()
        assert "DSTRBCD1" in groups
        assert "DSTRBCD2" in groups
        assert "DSTRBCD3" in groups


class TestMortalityCalculator:
    """Test the main mortality calculator."""
    
    @pytest.fixture
    def mock_db(self, mocker):
        """Create a mock FIA database."""
        mock = mocker.Mock(spec=FIA)
        mock.evalid = [123456]
        mock.tables = {}
        return mock
    
    def test_mortality_column_selection(self, mock_db):
        """Test correct mortality column selection based on config."""
        # Test growing stock forest
        config = MortalityEstimatorConfig(
            land_type="forest",
            extra_params={"tree_class": "growing_stock"}
        )
        calc = MortalityCalculator(mock_db, config)
        assert calc._mortality_col == "SUBP_TPAMORT_UNADJ_GS_FOREST"
        
        # Test all trees timber
        config = MortalityEstimatorConfig(
            land_type="timber",
            extra_params={"tree_class": "all"}
        )
        calc = MortalityCalculator(mock_db, config)
        assert calc._mortality_col == "SUBP_TPAMORT_UNADJ_AL_TIMBER"
        
    def test_required_tables(self, mock_db):
        """Test that all required tables are specified."""
        config = MortalityEstimatorConfig()
        calc = MortalityCalculator(mock_db, config)
        
        required = calc.get_required_tables()
        assert "PLOT" in required
        assert "COND" in required
        assert "TREE" in required
        assert "TREE_GRM_COMPONENT" in required
        assert "POP_STRATUM" in required
        assert "POP_PLOT_STRATUM_ASSGN" in required
        
    def test_response_columns(self, mock_db):
        """Test response column definitions."""
        config = MortalityEstimatorConfig()
        calc = MortalityCalculator(mock_db, config)
        
        response_cols = calc.get_response_columns()
        assert "mortality_tpa" in response_cols
        assert response_cols["mortality_tpa"] == "MORTALITY_TPA"
        assert "mortality_ba" in response_cols
        assert "mortality_vol" in response_cols
        
    def test_calculate_values(self, mock_db):
        """Test mortality value calculation."""
        config = MortalityEstimatorConfig()
        calc = MortalityCalculator(mock_db, config)
        
        # Create test data
        test_data = pl.DataFrame({
            "SUBP_TPAMORT_UNADJ_AL_FOREST": [10.0, 20.0, 30.0],
            "CONDPROP_UNADJ": [1.0, 0.5, 0.25]
        })
        
        result = calc.calculate_values(test_data)
        
        # Check that mortality values are calculated
        assert "mortality_tpa" in result.columns
        assert result["mortality_tpa"].to_list() == [10.0, 10.0, 7.5]


class TestMortalityVarianceCalculator:
    """Test the variance calculator component."""
    
    def test_stratum_variance(self):
        """Test stratum-level variance calculation."""
        calc = MortalityVarianceCalculator()
        
        # Create test data
        test_data = pl.DataFrame({
            "PLT_CN": [1, 2, 3, 4, 5, 6],
            "STRATUM_CN": [1, 1, 1, 2, 2, 2],
            "PLOT_MORTALITY_TPA": [10.0, 12.0, 14.0, 20.0, 22.0, 24.0],
            "EXPNS": [1000.0, 1000.0, 1000.0, 2000.0, 2000.0, 2000.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        })
        
        result = calc.calculate_stratum_variance(test_data, "PLOT_MORTALITY_TPA")
        
        # Check results
        assert len(result) == 2  # Two strata
        assert "n_h" in result.columns
        assert "PLOT_MORTALITY_TPA_bar_h" in result.columns
        assert "s_PLOT_MORTALITY_TPA_h" in result.columns
        assert "var_PLOT_MORTALITY_TPA_h" in result.columns
        
    def test_population_variance(self):
        """Test population-level variance calculation."""
        calc = MortalityVarianceCalculator()
        
        # Create stratum data
        stratum_data = pl.DataFrame({
            "STRATUM_CN": [1, 2],
            "n_h": [3, 3],
            "PLOT_MORTALITY_TPA_bar_h": [12.0, 22.0],
            "var_PLOT_MORTALITY_TPA_h": [1.33, 1.33],
            "A_h": [1000.0, 2000.0],
            "adj_h": [1.0, 1.0],
            "A_h_sq": [1000000.0, 4000000.0]
        })
        
        result = calc.calculate_population_variance(
            stratum_data, 
            "PLOT_MORTALITY_TPA"
        )
        
        # Check results
        assert "MORTALITY_TPA_TOTAL" in result.columns
        assert "MORTALITY_TPA_VAR" in result.columns
        assert "MORTALITY_TPA_SE" in result.columns
        assert "MORTALITY_TPA" in result.columns


class TestMortalityGroupHandler:
    """Test the group handler component."""
    
    def test_validate_groups(self):
        """Test group validation."""
        handler = MortalityGroupHandler()
        
        # Valid groups should pass
        handler.validate_groups(["SPCD", "OWNGRPCD", "AGENTCD"])
        
        # Invalid groups should raise error
        with pytest.raises(ValueError):
            handler.validate_groups(["SPCD", "INVALID_GROUP"])
            
    def test_get_group_summary(self):
        """Test group summary statistics."""
        handler = MortalityGroupHandler()
        
        # Create test data
        test_data = pl.DataFrame({
            "SPCD": [131, 131, 131, 110, 110],
            "OWNGRPCD": [10, 10, 20, 20, 20],
            "MORTALITY_TPA": [10.0, 20.0, 30.0, 40.0, 50.0]
        })
        
        summary = handler.get_group_summary(
            test_data, 
            ["SPCD", "OWNGRPCD"], 
            "MORTALITY_TPA"
        )
        
        # Check results
        assert len(summary) == 3  # Three unique combinations
        assert "N_RECORDS" in summary.columns
        assert "MORTALITY_TPA_TOTAL" in summary.columns
        assert "MORTALITY_TPA_MEAN" in summary.columns
        
    def test_filter_significant_groups(self):
        """Test filtering to significant groups."""
        handler = MortalityGroupHandler()
        
        # Create test data with varying plot counts
        test_data = pl.DataFrame({
            "PLT_CN": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "SPCD": [131, 131, 131, 131, 131, 131, 110, 110, 110, 833, 833],
            "MORTALITY_TPA": [10.0] * 11
        })
        
        # Filter to groups with at least 5 plots
        filtered = handler.filter_significant_groups(
            test_data,
            ["SPCD"],
            "MORTALITY_TPA",
            min_plots=5
        )
        
        # Should only have SPCD 131 (6 plots)
        assert filtered["SPCD"].unique().to_list() == [131]