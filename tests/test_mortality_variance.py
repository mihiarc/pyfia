"""Tests for mortality variance calculations."""

import pytest
import polars as pl
import numpy as np

from pyfia import FIA
from pyfia.estimation.config import MortalityConfig
from pyfia.estimation.mortality import MortalityEstimator
from pyfia.estimation.mortality.variance import MortalityVarianceCalculator


class TestMortalityVariance:
    """Test mortality variance calculation functionality."""
    
    def test_variance_calculator_init(self):
        """Test variance calculator initialization."""
        calculator = MortalityVarianceCalculator()
        assert calculator is not None
        
    def test_stratum_variance_basic(self, sample_fia_db):
        """Test basic stratum-level variance calculation."""
        # Create sample data
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1, 2, 2, 2],
            "PLT_CN": [101, 102, 103, 201, 202, 203],
            "MORTALITY_EXPANDED": [10.0, 12.0, 8.0, 15.0, 18.0, 12.0],
            "STRATUM_WT": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
            "N_PLOTS": [3, 3, 3, 3, 3, 3]
        })
        
        calculator = MortalityVarianceCalculator()
        result = calculator.calculate_stratum_variance(
            data,
            mortality_col="MORTALITY_EXPANDED",
            groups=[]
        )
        
        assert "STRATUM_VAR" in result.columns
        assert "STRATUM_SE" in result.columns
        assert len(result) == 2  # Two strata
        
        # Variance should be positive
        assert (result["STRATUM_VAR"] >= 0).all()
        
    def test_stratum_variance_with_groups(self):
        """Test stratum variance with grouping variables."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1, 1, 2, 2, 2, 2],
            "SPCD": [131, 131, 110, 110, 131, 131, 110, 110],
            "PLT_CN": [101, 102, 103, 104, 201, 202, 203, 204],
            "MORTALITY_EXPANDED": [10.0, 12.0, 8.0, 9.0, 15.0, 18.0, 12.0, 14.0],
            "STRATUM_WT": [0.5] * 8,
            "N_PLOTS": [4] * 8
        })
        
        calculator = MortalityVarianceCalculator()
        result = calculator.calculate_stratum_variance(
            data,
            mortality_col="MORTALITY_EXPANDED",
            groups=["SPCD"]
        )
        
        # Should have variance for each stratum-species combination
        assert len(result) == 4  # 2 strata × 2 species
        assert "SPCD" in result.columns
        assert "STRATUM_VAR" in result.columns
        
    def test_population_variance_basic(self):
        """Test population-level variance calculation."""
        # Stratum-level data
        stratum_data = pl.DataFrame({
            "STRATUM_CN": [1, 2, 3],
            "ESTN_UNIT_CN": [10, 10, 10],
            "STRATUM_MORTALITY": [100.0, 150.0, 120.0],
            "STRATUM_VAR": [25.0, 30.0, 28.0],
            "STRATUM_WT": [0.3, 0.4, 0.3],
            "N_PLOTS": [20, 25, 18],
            "TOTAL_PLOTS": [63, 63, 63]
        })
        
        calculator = MortalityVarianceCalculator()
        result = calculator.calculate_population_variance(
            stratum_data,
            groups=[]
        )
        
        assert "POPULATION_VAR" in result.columns
        assert "POPULATION_SE" in result.columns
        assert "SE_PERCENT" in result.columns
        assert len(result) == 1
        
        # Variance should be positive
        assert result["POPULATION_VAR"][0] >= 0
        
        # SE percent should be reasonable
        se_pct = result["SE_PERCENT"][0]
        assert 0 <= se_pct <= 100
        
    def test_ratio_variance(self):
        """Test ratio variance calculation for per-acre estimates."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 2],
            "MORTALITY_TOTAL": [1000.0, 1500.0],
            "AREA_TOTAL": [100.0, 120.0],
            "MORTALITY_VAR": [100.0, 150.0],
            "AREA_VAR": [10.0, 12.0],
            "MORT_AREA_COV": [50.0, 60.0],
            "STRATUM_WT": [0.4, 0.6]
        })
        
        calculator = MortalityVarianceCalculator()
        result = calculator.calculate_ratio_variance(
            data,
            numerator_col="MORTALITY_TOTAL",
            denominator_col="AREA_TOTAL",
            numerator_var_col="MORTALITY_VAR",
            denominator_var_col="AREA_VAR",
            covariance_col="MORT_AREA_COV"
        )
        
        assert "RATIO_VAR" in result.columns
        assert "RATIO_SE" in result.columns
        assert result["RATIO_VAR"][0] >= 0
        
    def test_variance_edge_cases(self):
        """Test variance calculation edge cases."""
        calculator = MortalityVarianceCalculator()
        
        # Single plot in stratum
        single_plot = pl.DataFrame({
            "STRATUM_CN": [1],
            "PLT_CN": [101],
            "MORTALITY_EXPANDED": [10.0],
            "STRATUM_WT": [1.0],
            "N_PLOTS": [1]
        })
        
        result = calculator.calculate_stratum_variance(
            single_plot,
            mortality_col="MORTALITY_EXPANDED",
            groups=[]
        )
        
        # Variance undefined for n=1, should handle gracefully
        assert len(result) == 1
        assert result["STRATUM_VAR"][0] == 0 or pl.DataFrame.is_empty(result)
        
    def test_variance_with_zero_mortality(self):
        """Test variance when mortality is zero."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1],
            "PLT_CN": [101, 102, 103],
            "MORTALITY_EXPANDED": [0.0, 0.0, 0.0],
            "STRATUM_WT": [0.5, 0.5, 0.5],
            "N_PLOTS": [3, 3, 3]
        })
        
        calculator = MortalityVarianceCalculator()
        result = calculator.calculate_stratum_variance(
            data,
            mortality_col="MORTALITY_EXPANDED",
            groups=[]
        )
        
        # Variance should be zero when all values are zero
        assert result["STRATUM_VAR"][0] == 0
        
    def test_variance_integration(self, sample_fia_db):
        """Test variance calculation in full mortality estimation."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            mortality_type="tpa",
            variance=True,
            variance_method="ratio",
            by_species=True
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        # Check variance columns exist
        assert "MORTALITY_TPA_VAR" in result.columns
        
        # Variance should be non-negative
        assert (result["MORTALITY_TPA_VAR"] >= 0).all()
        
        # Should have reasonable SE percentages
        if "SE_PERCENT" in result.columns:
            se_values = result.filter(pl.col("MORTALITY_TPA") > 0)["SE_PERCENT"]
            assert (se_values >= 0).all()
            assert (se_values <= 200).all()  # Very high but possible
            
    @pytest.mark.parametrize("variance_method", ["standard", "ratio", "hybrid"])
    def test_variance_methods(self, sample_fia_db, variance_method):
        """Test different variance calculation methods."""
        db = FIA(sample_fia_db)
        
        config = MortalityConfig(
            mortality_type="tpa",
            variance=True,
            variance_method=variance_method
        )
        
        estimator = MortalityEstimator(db, config)
        result = estimator.estimate()
        
        assert "MORTALITY_TPA_VAR" in result.columns
        assert len(result) > 0
        
    def test_variance_formula_accuracy(self):
        """Test variance formula implementation accuracy."""
        # Known example with hand-calculated variance
        data = pl.DataFrame({
            "STRATUM_CN": [1] * 5,
            "PLT_CN": list(range(101, 106)),
            "MORTALITY_EXPANDED": [10.0, 12.0, 8.0, 11.0, 9.0],
            "STRATUM_WT": [1.0] * 5,
            "N_PLOTS": [5] * 5
        })
        
        calculator = MortalityVarianceCalculator()
        result = calculator.calculate_stratum_variance(
            data,
            mortality_col="MORTALITY_EXPANDED",
            groups=[]
        )
        
        # Manual calculation
        values = [10.0, 12.0, 8.0, 11.0, 9.0]
        mean = np.mean(values)
        variance = np.var(values, ddof=1)  # Sample variance
        
        # Should match within floating point precision
        assert abs(result["STRATUM_VAR"][0] - variance) < 0.01
        
    def test_grouped_variance_consistency(self, sample_fia_db):
        """Test that grouped variances are consistent."""
        db = FIA(sample_fia_db)
        
        # Get total variance
        total_config = MortalityConfig(
            mortality_type="tpa",
            variance=True
        )
        total_result = MortalityEstimator(db, total_config).estimate()
        
        # Get grouped variance
        grouped_config = MortalityConfig(
            mortality_type="tpa",
            variance=True,
            by_species=True
        )
        grouped_result = MortalityEstimator(db, grouped_config).estimate()
        
        # Total estimate should exist in both
        assert len(total_result) > 0
        assert len(grouped_result) > 0
        
        # Variance should be calculated for all groups
        assert (grouped_result["MORTALITY_TPA_VAR"] >= 0).all()