"""
Comprehensive unit tests for mortality variance calculation.

This module tests the MortalityVarianceCalculator class with synthetic data
and known variance values to verify the implementation of the stratified
variance formula.

Reference: Bechtold & Patterson (2005) stratified variance formula:
Var(Ŷ) = Σ_h [N_h²/n * (1-f_h) * s²_h / n_h]

Where s²_h = [Σy_i² - n_h * ȳ_h²] / (n_h - 1)
"""

import pytest
import polars as pl
import numpy as np
from typing import List, Dict, Any
from unittest.mock import Mock, patch

from pyfia.estimation.mortality.variance import MortalityVarianceCalculator


class TestMortalityVarianceCalculator:
    """Test MortalityVarianceCalculator with synthetic data and known values."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = MortalityVarianceCalculator()
    
    def test_init_without_db(self):
        """Test calculator initialization without database."""
        calc = MortalityVarianceCalculator()
        assert calc.db is None
    
    def test_init_with_string_db_path(self):
        """Test calculator initialization with string database path."""
        # Mock FIA class to avoid actual database connection
        with patch('pyfia.estimation.mortality.variance.FIA') as mock_fia:
            calc = MortalityVarianceCalculator("/path/to/db.duckdb")
            mock_fia.assert_called_once_with("/path/to/db.duckdb")
    
    def test_init_with_fia_instance(self):
        """Test calculator initialization with FIA instance."""
        mock_db = Mock()
        calc = MortalityVarianceCalculator(mock_db)
        assert calc.db is mock_db
    
    def test_stratum_variance_known_values(self):
        """Test stratum variance calculation with manually verified values."""
        # Create test data with known variance
        # Values: [10.0, 12.0, 8.0] in stratum 1
        # Mean = 10.0, variance = [(10-10)² + (12-10)² + (8-10)²] / (3-1) = [0 + 4 + 4] / 2 = 4.0
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1, 2, 2],
            "ESTN_UNIT_CN": [10, 10, 10, 10, 10],
            "mortality_values": [10.0, 12.0, 8.0, 20.0, 24.0],
            "EXPNS": [100.0, 100.0, 100.0, 150.0, 150.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0, 1.0, 1.0]
        })
        
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values"
        )
        
        # Check basic structure
        assert len(result) == 2  # Two strata
        assert "stratum_var" in result.columns
        assert "y_mean" in result.columns
        assert "n_h" in result.columns
        
        # Verify stratum 1 calculations
        stratum_1 = result.filter(pl.col("STRATUM_CN") == 1)
        assert len(stratum_1) == 1
        assert abs(stratum_1["y_mean"][0] - 10.0) < 1e-10
        assert abs(stratum_1["stratum_var"][0] - 4.0) < 1e-10
        assert stratum_1["n_h"][0] == 3
        
        # Verify stratum 2 calculations  
        # Values: [20.0, 24.0], Mean = 22.0, variance = [(20-22)² + (24-22)²] / (2-1) = [4 + 4] / 1 = 8.0
        stratum_2 = result.filter(pl.col("STRATUM_CN") == 2)
        assert len(stratum_2) == 1
        assert abs(stratum_2["y_mean"][0] - 22.0) < 1e-10
        assert abs(stratum_2["stratum_var"][0] - 8.0) < 1e-10
        assert stratum_2["n_h"][0] == 2
    
    def test_stratum_variance_with_grouping(self):
        """Test stratum variance calculation with grouping variables."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1, 1, 2, 2, 2, 2],
            "ESTN_UNIT_CN": [10, 10, 10, 10, 10, 10, 10, 10],
            "SPCD": [131, 131, 110, 110, 131, 131, 110, 110],
            "mortality_values": [10.0, 12.0, 15.0, 17.0, 20.0, 22.0, 25.0, 27.0],
            "EXPNS": [100.0] * 8,
            "ADJ_FACTOR_SUBP": [1.0] * 8
        })
        
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values",
            group_cols=["SPCD"]
        )
        
        # Should have 4 groups: 2 strata × 2 species
        assert len(result) == 4
        assert "SPCD" in result.columns
        
        # Check that each stratum-species combination has correct sample size
        for row in result.iter_rows(named=True):
            assert row["n_h"] == 2  # 2 observations per stratum-species combination
    
    def test_stratum_variance_edge_case_single_observation(self):
        """Test variance calculation with single observation (n_h = 1)."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 2],
            "ESTN_UNIT_CN": [10, 10],
            "mortality_values": [15.0, 20.0],
            "EXPNS": [100.0, 100.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0]
        })
        
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values"
        )
        
        # With n_h = 1, variance should be 0 (degrees of freedom = 0)
        assert len(result) == 2
        assert all(result["stratum_var"] == 0.0)
        assert all(result["n_h"] == 1)
    
    def test_stratum_variance_edge_case_zero_values(self):
        """Test variance calculation when all values are zero."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1],
            "ESTN_UNIT_CN": [10, 10, 10],
            "mortality_values": [0.0, 0.0, 0.0],
            "EXPNS": [100.0, 100.0, 100.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0]
        })
        
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values"
        )
        
        assert len(result) == 1
        assert result["y_mean"][0] == 0.0
        assert result["stratum_var"][0] == 0.0
        assert result["n_h"][0] == 3
    
    def test_stratum_variance_missing_group_columns(self):
        """Test that missing grouping columns are handled gracefully."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 2, 2],
            "ESTN_UNIT_CN": [10, 10, 10, 10],
            "mortality_values": [10.0, 12.0, 20.0, 22.0],
            "EXPNS": [100.0, 100.0, 150.0, 150.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0, 1.0]
        })
        
        # Try to group by a column that doesn't exist
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values",
            group_cols=["NONEXISTENT_COL"]
        )
        
        # Should work without the missing column
        assert len(result) == 2
        assert "NONEXISTENT_COL" not in result.columns
    
    def test_population_variance_known_calculation(self):
        """Test population variance with manually calculated example."""
        # Create mock population data
        mock_pop_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10, 10, 10],
            "STRATUM_CN": [1, 2, 3],
            "AREA_USED": [1000.0, 1000.0, 1000.0],
            "P1PNTCNT_EU": [100, 100, 100],
            "w_h": [0.3, 0.4, 0.3],  # stratum weights
            "n": [60, 60, 60],  # total plots in estimation unit
            "EXPNS": [300.0, 400.0, 300.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0]
        })
        
        # Create stratum data
        stratum_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10, 10, 10],
            "STRATUM_CN": [1, 2, 3],
            "y_sum": [150.0, 200.0, 180.0],  # sum of mortality values
            "y_sum_sq": [1200.0, 2100.0, 1800.0],  # sum of squared values
            "n_h": [20, 25, 15],  # plots per stratum
            "y_mean": [7.5, 8.0, 12.0],  # mean mortality per stratum
            "df": [19, 24, 14],  # degrees of freedom
            "stratum_var": [2.5, 3.0, 4.0],  # stratum variances
            "EXPNS": [300.0, 400.0, 300.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0]
        })
        
        result = self.calculator.calculate_population_variance(
            stratum_data=stratum_data,
            pop_data=mock_pop_data
        )
        
        # Verify structure
        assert len(result) == 1
        assert "var_of_estimate" in result.columns
        assert "se_of_estimate" in result.columns
        assert "se_of_estimate_pct" in result.columns
        
        # Variance should be positive
        assert result["var_of_estimate"][0] >= 0
        assert result["se_of_estimate"][0] >= 0
        
        # SE should equal sqrt(variance)
        variance = result["var_of_estimate"][0]
        se = result["se_of_estimate"][0]
        assert abs(se - np.sqrt(variance)) < 1e-10
    
    def test_population_variance_no_pop_data_no_db(self):
        """Test that population variance raises error without pop_data or db."""
        stratum_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10],
            "STRATUM_CN": [1],
            "y_sum": [100.0],
            "n_h": [10],
            "stratum_var": [2.5]
        })
        
        calc = MortalityVarianceCalculator()  # No db
        
        with pytest.raises(ValueError, match="Either pop_data or db must be provided"):
            calc.calculate_population_variance(stratum_data=stratum_data)
    
    def test_load_population_factors_no_db(self):
        """Test that _load_population_factors raises error without db."""
        calc = MortalityVarianceCalculator()
        
        with pytest.raises(ValueError, match="Database connection required"):
            calc._load_population_factors()
    
    def test_sql_formula_reference_calculation(self):
        """
        Test variance calculation against the SQL formula reference.
        
        SQL formula from the code comments:
        Var = (AREA²/n) * [Σ(w_h * n_h * s²_h) + (1/n) * Σ((1-w_h) * n_h * s²_h)]
        
        Manual calculation with known values:
        - AREA = 1000
        - n = 60 (total plots)
        - Stratum 1: w_h=0.3, n_h=20, s²_h=2.5
        - Stratum 2: w_h=0.4, n_h=25, s²_h=3.0  
        - Stratum 3: w_h=0.3, n_h=15, s²_h=4.0
        
        Expected calculation:
        var_component_1 = 0.3*20*2.5 + 0.4*25*3.0 + 0.3*15*4.0 = 15 + 30 + 18 = 63
        var_component_2 = (1-0.3)*20*2.5 + (1-0.4)*25*3.0 + (1-0.3)*15*4.0 
                        = 0.7*20*2.5 + 0.6*25*3.0 + 0.7*15*4.0 
                        = 35 + 45 + 42 = 122
        
        Var = (1000²/60) * [63 + (1/60) * 122] = 16666.67 * [63 + 2.033] = 16666.67 * 65.033 = 1,083,883.33
        """
        # Set up test data with known values
        mock_pop_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10, 10, 10],
            "STRATUM_CN": [1, 2, 3],
            "AREA_USED": [1000.0, 1000.0, 1000.0],
            "w_h": [0.3, 0.4, 0.3],
            "n": [60, 60, 60],
            "EXPNS": [300.0, 400.0, 300.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0]
        })
        
        stratum_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10, 10, 10],
            "STRATUM_CN": [1, 2, 3],
            "y_sum": [150.0, 200.0, 180.0],
            "n_h": [20, 25, 15],
            "stratum_var": [2.5, 3.0, 4.0],
            "EXPNS": [300.0, 400.0, 300.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0]
        })
        
        result = self.calculator.calculate_population_variance(
            stratum_data=stratum_data,
            pop_data=mock_pop_data
        )
        
        # Manual calculation
        area = 1000.0
        n_total = 60
        
        # var_component_1 = Σ(w_h * n_h * s²_h)
        var_comp_1 = (0.3 * 20 * 2.5) + (0.4 * 25 * 3.0) + (0.3 * 15 * 4.0)
        expected_var_comp_1 = 15 + 30 + 18  # = 63
        
        # var_component_2 = Σ((1-w_h) * n_h * s²_h)  
        var_comp_2 = (0.7 * 20 * 2.5) + (0.6 * 25 * 3.0) + (0.7 * 15 * 4.0)
        expected_var_comp_2 = 35 + 45 + 42  # = 122
        
        # Final variance
        expected_variance = (area**2 / n_total) * (var_comp_1 + (1/n_total) * var_comp_2)
        expected_variance = (1000000 / 60) * (63 + (1/60) * 122)
        expected_variance = 16666.666667 * (63 + 2.033333)
        expected_variance = 16666.666667 * 65.033333  # ≈ 1,083,888.89
        
        calculated_variance = result["var_of_estimate"][0]
        
        # Allow for floating point precision differences
        assert abs(calculated_variance - expected_variance) < 1.0
        
        # Verify intermediate calculations match expected
        assert abs(var_comp_1 - expected_var_comp_1) < 1e-10
        assert abs(var_comp_2 - expected_var_comp_2) < 1e-10
    
    def test_variance_formula_implementation_accuracy(self):
        """Test the stratum variance formula s²_h = [Σy_i² - n_h * ȳ_h²] / (n_h - 1)."""
        # Test data with known variance calculation
        values = [5.0, 7.0, 9.0, 11.0, 13.0]  # n=5
        n = len(values)
        mean = sum(values) / n  # = 9.0
        sum_squares = sum(v**2 for v in values)  # = 25 + 49 + 81 + 121 + 169 = 445
        
        # Expected variance using formula: [Σy_i² - n_h * ȳ_h²] / (n_h - 1)
        expected_variance = (sum_squares - n * mean**2) / (n - 1)
        expected_variance = (445 - 5 * 81) / 4  # = (445 - 405) / 4 = 40/4 = 10.0
        
        data = pl.DataFrame({
            "STRATUM_CN": [1] * 5,
            "ESTN_UNIT_CN": [10] * 5,
            "mortality_values": values,
            "EXPNS": [100.0] * 5,
            "ADJ_FACTOR_SUBP": [1.0] * 5
        })
        
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values"
        )
        
        assert len(result) == 1
        calculated_variance = result["stratum_var"][0]
        calculated_mean = result["y_mean"][0]
        
        # Verify calculations match expected values
        assert abs(calculated_mean - 9.0) < 1e-10
        assert abs(calculated_variance - 10.0) < 1e-10
        
        # Cross-check with numpy calculation
        numpy_variance = np.var(values, ddof=1)
        assert abs(calculated_variance - numpy_variance) < 1e-10
    
    def test_standard_error_calculation(self):
        """Test that standard error is correctly calculated as sqrt(variance)."""
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 1, 1],
            "ESTN_UNIT_CN": [10, 10, 10, 10],
            "mortality_values": [8.0, 10.0, 12.0, 14.0],
            "EXPNS": [100.0, 100.0, 100.0, 100.0],
            "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0, 1.0]
        })
        
        # Calculate stratum variance
        stratum_result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values"
        )
        
        variance = stratum_result["stratum_var"][0]
        expected_se = np.sqrt(variance)
        
        # For population variance, create mock data
        mock_pop_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10],
            "STRATUM_CN": [1],
            "AREA_USED": [1000.0],
            "w_h": [1.0],
            "n": [4],
            "EXPNS": [100.0],
            "ADJ_FACTOR_SUBP": [1.0]
        })
        
        pop_result = self.calculator.calculate_population_variance(
            stratum_data=stratum_result,
            pop_data=mock_pop_data
        )
        
        pop_variance = pop_result["var_of_estimate"][0]
        pop_se = pop_result["se_of_estimate"][0]
        
        # Standard error should equal sqrt(variance)
        assert abs(pop_se - np.sqrt(pop_variance)) < 1e-10
    
    def test_coefficient_of_variation_calculation(self):
        """Test coefficient of variation calculation (CV% = SE/estimate * 100)."""
        # Create data that will result in known estimate and variance
        stratum_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10],
            "STRATUM_CN": [1],
            "y_sum": [100.0],  # This will be multiplied by EXPNS for estimate
            "n_h": [10],
            "stratum_var": [4.0],
            "EXPNS": [10.0],  # estimate = 100 * 10 = 1000
            "ADJ_FACTOR_SUBP": [1.0]
        })
        
        mock_pop_data = pl.DataFrame({
            "ESTN_UNIT_CN": [10],
            "STRATUM_CN": [1],
            "AREA_USED": [1000.0],
            "w_h": [1.0],
            "n": [10],
            "EXPNS": [10.0],
            "ADJ_FACTOR_SUBP": [1.0]
        })
        
        result = self.calculator.calculate_population_variance(
            stratum_data=stratum_data,
            pop_data=mock_pop_data
        )
        
        estimate = result["estimate"][0]
        se = result["se_of_estimate"][0]
        cv_percent = result["se_of_estimate_pct"][0]
        
        # CV% should equal (SE / |estimate|) * 100
        expected_cv = (se / abs(estimate)) * 100 if estimate != 0 else 0.0
        
        assert abs(cv_percent - expected_cv) < 1e-10
    
    def test_multiple_strata_different_sizes(self):
        """Test variance calculation with strata of varying sizes."""
        # Create strata with different sample sizes: 2, 5, 10, 1
        data = pl.DataFrame({
            "STRATUM_CN": [1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4],
            "ESTN_UNIT_CN": [10] * 18,
            "mortality_values": [
                # Stratum 1: n=2, values=[10, 20]
                10.0, 20.0,
                # Stratum 2: n=5, values=[5, 7, 9, 11, 13]  
                5.0, 7.0, 9.0, 11.0, 13.0,
                # Stratum 3: n=10, values=[1, 2, ..., 10]
                1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
                # Stratum 4: n=1, value=[100]
                100.0
            ],
            "EXPNS": [100.0] * 18,
            "ADJ_FACTOR_SUBP": [1.0] * 18
        })
        
        result = self.calculator.calculate_stratum_variance(
            data=data,
            response_col="mortality_values"
        )
        
        # Should have 4 strata
        assert len(result) == 4
        
        # Check sample sizes
        sample_sizes = dict(zip(result["STRATUM_CN"], result["n_h"]))
        assert sample_sizes[1] == 2
        assert sample_sizes[2] == 5
        assert sample_sizes[3] == 10
        assert sample_sizes[4] == 1
        
        # Stratum 4 should have variance = 0 (single observation)
        stratum_4_var = result.filter(pl.col("STRATUM_CN") == 4)["stratum_var"][0]
        assert stratum_4_var == 0.0
        
        # Other strata should have positive variance
        for stratum in [1, 2, 3]:
            stratum_var = result.filter(pl.col("STRATUM_CN") == stratum)["stratum_var"][0]
            assert stratum_var > 0.0


if __name__ == "__main__":
    pytest.main([__file__])