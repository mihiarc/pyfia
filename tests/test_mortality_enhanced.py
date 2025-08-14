"""
Comprehensive tests for enhanced mortality estimation functionality.

Tests cover:
- Basic mortality calculations
- All grouping variable combinations
- Variance calculations
- Domain filtering
- Integration with database interfaces
"""

import pytest
import polars as pl
import numpy as np
from pathlib import Path

from pyfia import FIA
from pyfia.estimation import mortality
from pyfia.estimation.config import MortalityConfig
from pyfia.estimation.mortality import MortalityEstimator
from pyfia.estimation.mortality.query_builder import MortalityQueryBuilder


class TestMortalityEnhanced:
    """Test enhanced mortality estimation functionality."""
    
    def test_basic_mortality_tpa(self, sample_fia_db):
        """Test basic mortality calculation for trees per acre."""
        db = FIA(sample_fia_db)
        
        # Basic mortality estimation
        result = mortality(db, mortality_type="tpa")
        
        assert isinstance(result, pl.DataFrame)
        assert "MORTALITY_TPA" in result.columns
        assert "MORTALITY_TPA_SE" in result.columns
        assert "N_PLOTS" in result.columns
        assert len(result) > 0
        assert result["MORTALITY_TPA"].null_count() == 0
        
    def test_mortality_by_species(self, sample_fia_db):
        """Test mortality grouped by species."""
        db = FIA(sample_fia_db)
        
        result = mortality(
            db, 
            by_species=True,
            mortality_type="tpa",
            variance=False
        )
        
        assert "SPCD" in result.columns
        assert "MORTALITY_TPA" in result.columns
        assert "MORTALITY_TPA_SE" in result.columns
        assert len(result.filter(pl.col("SPCD").is_not_null())) > 0
        
    def test_mortality_by_ownership(self, sample_fia_db):
        """Test mortality grouped by ownership."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            group_by_ownership=True,
            mortality_type="tpa",
            variance=True
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        assert "OWNGRPCD" in result.columns
        assert "MORTALITY_TPA" in result.columns
        assert "MORTALITY_TPA_VAR" in result.columns
        assert len(result.filter(pl.col("OWNGRPCD").is_not_null())) > 0
        
    def test_mortality_by_agent(self, sample_fia_db):
        """Test mortality grouped by mortality agent."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            group_by_agent=True,
            mortality_type="tpa"
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        assert "AGENTCD" in result.columns
        assert "MORTALITY_TPA" in result.columns
        assert len(result) > 0
        
    def test_mortality_multiple_groups(self, sample_fia_db):
        """Test mortality with multiple grouping variables."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            by_species=True,
            group_by_ownership=True,
            group_by_agent=True,
            mortality_type="tpa",
            totals=True
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        # Check all grouping columns present
        for col in ["SPCD", "OWNGRPCD", "AGENTCD"]:
            assert col in result.columns
            
        # Check output columns
        assert "MORTALITY_TPA" in result.columns
        assert "MORTALITY_TPA_TOTAL" in result.columns
        
        # Verify grouping worked
        group_counts = result.group_by(["SPCD", "OWNGRPCD", "AGENTCD"]).count()
        assert len(group_counts) > 1
        
    def test_mortality_volume(self, sample_fia_db):
        """Test mortality volume calculation."""
        db = FIA(sample_fia_db)
        
        result = mortality(
            db,
            mortality_type="volume",
            tree_type="dead"  # Must use dead trees for mortality
        )
        
        assert "MORTALITY_VOL" in result.columns
        assert "MORTALITY_VOL_SE" in result.columns
        assert result["MORTALITY_VOL"].null_count() == 0
        
    def test_mortality_with_domain_filters(self, sample_fia_db):
        """Test mortality with domain filtering."""
        db = FIA(sample_fia_db)
        
        # Filter to specific diameter range
        result = mortality(
            db,
            tree_domain="DIA >= 10.0 AND DIA < 20.0",
            mortality_type="tpa"
        )
        
        assert len(result) > 0
        assert "MORTALITY_TPA" in result.columns
        
    def test_mortality_by_size_class(self, sample_fia_db):
        """Test mortality grouped by size class."""
        db = FIA(sample_fia_db)
        
        result = mortality(
            db,
            by_size_class=True,
            mortality_type="tpa"
        )
        
        assert "SIZE_CLASS" in result.columns
        assert len(result.filter(pl.col("SIZE_CLASS").is_not_null())) > 0
        
    def test_mortality_totals(self, sample_fia_db):
        """Test mortality with totals calculation."""
        db = FIA(sample_fia_db)
        
        result = mortality(
            db,
            mortality_type="tpa",
            totals=True
        )
        
        assert "MORTALITY_TPA" in result.columns
        assert "MORTALITY_TPA_TOTAL" in result.columns
        
        # Verify total calculation
        # Total = per_acre * total_area
        if "TOTAL_AREA" in result.columns:
            expected_total = result["MORTALITY_TPA"] * result["TOTAL_AREA"]
            actual_total = result["MORTALITY_TPA_TOTAL"]
            # Allow for small numerical differences
            assert np.allclose(
                expected_total.to_numpy(), 
                actual_total.to_numpy(),
                rtol=1e-5
            )
            
    def test_mortality_variance_calculations(self, sample_fia_db):
        """Test variance calculation methods."""
        db = FIA(sample_fia_db)
        
        # Test with variance output
        config = MortalityConfig(
            mortality_type="tpa",
            variance=True,
            variance_method="ratio"
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        assert "MORTALITY_TPA_VAR" in result.columns
        assert result["MORTALITY_TPA_VAR"].null_count() == 0
        assert (result["MORTALITY_TPA_VAR"] >= 0).all()
        
    def test_mortality_by_disturbance(self, sample_fia_db):
        """Test mortality grouped by disturbance codes."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            group_by_disturbance=True,
            mortality_type="tpa"
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        # Check disturbance columns
        for col in ["DSTRBCD1", "DSTRBCD2", "DSTRBCD3"]:
            assert col in result.columns
            
    def test_mortality_config_validation(self):
        """Test MortalityConfig validation rules."""
        # Valid config
        config = MortalityConfig(
            mortality_type="tpa",
            tree_type="dead"
        )
        assert config.mortality_type == "tpa"
        
        # Invalid: volume mortality with live trees
        with pytest.raises(ValueError, match="Cannot calculate volume mortality"):
            MortalityConfig(
                mortality_type="volume",
                tree_type="live"
            )
            
        # Invalid: timber tree class with forest land type
        with pytest.raises(ValueError, match="tree_class='timber' requires"):
            MortalityConfig(
                tree_class="timber",
                land_type="forest"
            )
            
    def test_mortality_query_builder(self, sample_fia_db):
        """Test the mortality query builder."""
        builder = MortalityQueryBuilder(db_type="duckdb")
        
        # Test plot query
        plot_query = builder.build_plot_query(
            evalid_list=[1, 2, 3],
            groups=["SPCD", "OWNGRPCD"],
            tree_domain="DIA > 5.0"
        )
        
        assert "SPCD" in plot_query
        assert "OWNGRPCD" in plot_query
        assert "DIA > 5.0" in plot_query
        assert "EVALID IN (1, 2, 3)" in plot_query
        
        # Test invalid groups
        with pytest.raises(ValueError, match="Invalid grouping variables"):
            builder.build_plot_query(
                evalid_list=[1],
                groups=["INVALID_COL"]
            )
            
    def test_mortality_components(self, sample_fia_db):
        """Test mortality with component breakdowns."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            mortality_type="both",
            include_components=True,
            tree_type="dead"
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        # Check for all component columns
        expected_cols = [
            "MORTALITY_TPA", "MORTALITY_VOL", "MORTALITY_BA"
        ]
        for col in expected_cols:
            assert col in result.columns
            
    def test_mortality_temporal_methods(self, sample_fia_db):
        """Test mortality with different temporal methods."""
        db = FIA(sample_fia_db)
        
        # Test SMA method
        result_sma = mortality(
            db,
            mortality_type="tpa",
            method="SMA"
        )
        
        # Test EMA method
        result_ema = mortality(
            db,
            mortality_type="tpa",
            method="EMA",
            lambda_=0.7
        )
        
        assert len(result_sma) > 0
        assert len(result_ema) > 0
        
    def test_mortality_plot_level(self, sample_fia_db):
        """Test plot-level mortality estimates."""
        db = FIA(sample_fia_db)
        
        result = mortality(
            db,
            mortality_type="tpa",
            by_plot=True
        )
        
        assert "PLT_CN" in result.columns
        assert len(result) > 0
        
    def test_mortality_land_type_filters(self, sample_fia_db):
        """Test mortality with different land type filters."""
        db = FIA(sample_fia_db)
        
        # Forest land only
        forest_result = mortality(
            db,
            land_type="forest",
            mortality_type="tpa"
        )
        
        # Timber land only
        timber_result = mortality(
            db,
            land_type="timber",
            mortality_type="tpa"
        )
        
        # All land
        all_result = mortality(
            db,
            land_type="all",
            mortality_type="tpa"
        )
        
        # Forest should be >= timber
        assert forest_result["MORTALITY_TPA"].sum() >= timber_result["MORTALITY_TPA"].sum()
        # All should be >= forest
        assert all_result["MORTALITY_TPA"].sum() >= forest_result["MORTALITY_TPA"].sum()
        
    def test_mortality_empty_results(self, sample_fia_db):
        """Test mortality with filters that return no data."""
        db = FIA(sample_fia_db)
        
        # Very restrictive filter
        result = mortality(
            db,
            tree_domain="DIA > 1000.0",  # No trees this large
            mortality_type="tpa"
        )
        
        # Should return empty dataframe with correct structure
        assert isinstance(result, pl.DataFrame)
        assert "MORTALITY_TPA" in result.columns
        assert len(result) == 0 or result["MORTALITY_TPA"].sum() == 0
        

class TestMortalityIntegration:
    """Integration tests with database interfaces."""
    
    def test_mortality_with_backend(self, sample_fia_db):
        """Test mortality using backend support."""
        # FIA class now automatically detects and uses appropriate backend
        db = FIA(sample_fia_db)
        
        result = mortality(
            db,
            by_species=True,
            mortality_type="tpa"
        )
        
        assert len(result) > 0
        assert "SPCD" in result.columns
            
    @pytest.mark.parametrize("grouping_vars", [
        ["SPCD"],
        ["OWNGRPCD"],
        ["SPCD", "OWNGRPCD"],
        ["SPCD", "AGENTCD"],
        ["UNITCD", "SPGRPCD"],
    ])
    def test_mortality_grouping_combinations(self, sample_fia_db, grouping_vars):
        """Test various grouping variable combinations."""
        db = FIA(sample_fia_db)
        
        result = mortality(
            db,
            grp_by=grouping_vars,
            mortality_type="tpa"
        )
        
        # All grouping vars should be in output
        for var in grouping_vars:
            assert var in result.columns
            
        # Should have mortality estimates
        assert "MORTALITY_TPA" in result.columns
        assert len(result) > 0