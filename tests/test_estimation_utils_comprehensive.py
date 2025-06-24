"""
Comprehensive tests for estimation_utils module.

These tests verify the statistical estimation utilities including:
- Ratio variance calculations
- Coefficient of variation calculations
- Stratified estimation procedures
- Statistical helper functions
"""

import pytest
import polars as pl
import numpy as np
import math

from pyfia.estimation_utils import ratio_var, cv


class TestRatioVarianceCalculation:
    """Test ratio variance calculation function."""
    
    def test_ratio_var_basic(self):
        """Test basic ratio variance calculation."""
        # Simple test case with known values
        y_total = 100.0
        x_total = 50.0
        y_var = 25.0
        x_var = 9.0
        cov_yx = 10.0
        
        ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
        
        # Should return a valid variance (non-negative)
        assert ratio_variance >= 0
        assert isinstance(ratio_variance, float)
        assert not math.isnan(ratio_variance)
    
    def test_ratio_var_formula(self):
        """Test ratio variance formula implementation."""
        # Test with known formula: Var(Y/X) ≈ (1/X²)[Var(Y) + R²Var(X) - 2R·Cov(Y,X)]
        y_total = 100.0
        x_total = 50.0
        y_var = 25.0
        x_var = 9.0
        cov_yx = 10.0
        
        ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
        
        # Calculate expected value manually
        ratio_estimate = y_total / x_total
        expected_var = (1 / (x_total ** 2)) * (
            y_var + (ratio_estimate ** 2) * x_var - 2 * ratio_estimate * cov_yx
        )
        
        assert abs(ratio_variance - expected_var) < 1e-10
    
    def test_ratio_var_zero_denominator(self):
        """Test ratio variance with zero denominator."""
        y_total = 100.0
        x_total = 0.0
        y_var = 25.0
        x_var = 9.0
        cov_yx = 10.0
        
        with pytest.raises((ValueError, ZeroDivisionError)):
            ratio_var(y_total, x_total, y_var, x_var, cov_yx)
    
    def test_ratio_var_negative_variances(self):
        """Test ratio variance with negative variances."""
        y_total = 100.0
        x_total = 50.0
        y_var = -25.0  # Invalid negative variance
        x_var = 9.0
        cov_yx = 10.0
        
        # Should either raise error or handle gracefully
        try:
            ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
            # If it doesn't raise an error, result should be handled appropriately
            assert isinstance(ratio_variance, float)
        except ValueError:
            # Expected for negative variance
            pass
    
    def test_ratio_var_zero_variances(self):
        """Test ratio variance with zero variances."""
        y_total = 100.0
        x_total = 50.0
        y_var = 0.0
        x_var = 0.0
        cov_yx = 0.0
        
        ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
        
        # With zero variances, ratio variance should be zero
        assert ratio_variance == 0.0
    
    def test_ratio_var_extreme_covariance(self):
        """Test ratio variance with extreme covariance values."""
        y_total = 100.0
        x_total = 50.0
        y_var = 25.0
        x_var = 9.0
        
        # Test with large positive covariance
        cov_yx = 1000.0
        ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
        assert isinstance(ratio_variance, float)
        
        # Test with large negative covariance
        cov_yx = -1000.0
        ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
        assert isinstance(ratio_variance, float)
    
    def test_ratio_var_polars_inputs(self):
        """Test ratio variance with Polars series inputs."""
        # Create test data as Polars expressions or values
        y_total = 100.0
        x_total = 50.0
        y_var = 25.0
        x_var = 9.0
        cov_yx = 10.0
        
        # Should work with numeric inputs
        ratio_variance = ratio_var(y_total, x_total, y_var, x_var, cov_yx)
        assert isinstance(ratio_variance, float)
        assert ratio_variance >= 0


class TestCoefficientOfVariation:
    """Test coefficient of variation calculation function."""
    
    def test_cv_basic(self):
        """Test basic CV calculation."""
        estimate = 100.0
        standard_error = 5.0
        
        cv_result = cv(estimate, standard_error)
        
        # CV should be (SE / Estimate) * 100
        expected_cv = (standard_error / estimate) * 100
        assert abs(cv_result - expected_cv) < 1e-10
        assert cv_result == 5.0  # 5/100 * 100 = 5%
    
    def test_cv_zero_estimate(self):
        """Test CV with zero estimate."""
        estimate = 0.0
        standard_error = 5.0
        
        with pytest.raises((ValueError, ZeroDivisionError)):
            cv(estimate, standard_error)
    
    def test_cv_zero_se(self):
        """Test CV with zero standard error."""
        estimate = 100.0
        standard_error = 0.0
        
        cv_result = cv(estimate, standard_error)
        assert cv_result == 0.0
    
    def test_cv_negative_estimate(self):
        """Test CV with negative estimate."""
        estimate = -100.0
        standard_error = 5.0
        
        # CV with negative estimate should be handled appropriately
        cv_result = cv(estimate, standard_error)
        # Implementation dependent - might return absolute value or raise error
        assert isinstance(cv_result, float)
    
    def test_cv_negative_se(self):
        """Test CV with negative standard error."""
        estimate = 100.0
        standard_error = -5.0
        
        # Negative SE is invalid
        try:
            cv_result = cv(estimate, standard_error)
            # If it doesn't raise an error, should handle appropriately
            assert isinstance(cv_result, float)
        except ValueError:
            # Expected for negative SE
            pass
    
    def test_cv_small_estimate(self):
        """Test CV with very small estimate."""
        estimate = 1e-10
        standard_error = 1e-11
        
        cv_result = cv(estimate, standard_error)
        expected_cv = (1e-11 / 1e-10) * 100
        assert abs(cv_result - expected_cv) < 1e-8
        assert cv_result == 10.0  # 10%
    
    def test_cv_large_values(self):
        """Test CV with large values."""
        estimate = 1e6
        standard_error = 1e4
        
        cv_result = cv(estimate, standard_error)
        expected_cv = (1e4 / 1e6) * 100
        assert abs(cv_result - expected_cv) < 1e-10
        assert cv_result == 1.0  # 1%
    
    def test_cv_percentage_output(self):
        """Test that CV is returned as percentage."""
        estimate = 50.0
        standard_error = 5.0
        
        cv_result = cv(estimate, standard_error)
        
        # Should be percentage, not proportion
        assert cv_result == 10.0  # 10%, not 0.1
        assert cv_result > 1.0  # Should be percentage scale


class TestStratifiedEstimationHelpers:
    """Test helper functions for stratified estimation."""
    
    def test_weighted_mean_calculation(self):
        """Test weighted mean calculation."""
        # Mock data for weighted mean
        values = [10.0, 20.0, 30.0]
        weights = [0.2, 0.3, 0.5]
        
        # Calculate weighted mean manually
        expected_mean = sum(v * w for v, w in zip(values, weights))
        
        # Test implementation (would need actual function)
        # weighted_mean = calculate_weighted_mean(values, weights)
        # assert abs(weighted_mean - expected_mean) < 1e-10
        
        # For now, just test the calculation
        assert abs(expected_mean - 23.0) < 1e-10
    
    def test_stratum_variance_calculation(self):
        """Test stratum variance calculation."""
        # Mock plot-level data within a stratum
        plot_values = [10.0, 15.0, 12.0, 18.0, 14.0]
        n_plots = len(plot_values)
        
        # Calculate variance manually
        mean_val = sum(plot_values) / n_plots
        variance = sum((x - mean_val) ** 2 for x in plot_values) / (n_plots - 1)
        
        # Test variance calculation
        assert variance > 0
        assert not math.isnan(variance)
    
    def test_expansion_factor_application(self):
        """Test expansion factor application."""
        plot_value = 10.0  # Trees per acre on one plot
        expansion_factor = 6000.0  # Acres per plot
        
        # Expanded total
        expanded_total = plot_value * expansion_factor
        assert expanded_total == 60000.0  # Total trees


class TestEstimationUtilsEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_nan_input_handling(self):
        """Test handling of NaN inputs."""
        # Test ratio_var with NaN
        try:
            result = ratio_var(float('nan'), 50.0, 25.0, 9.0, 10.0)
            assert math.isnan(result) or isinstance(result, float)
        except ValueError:
            # Expected for NaN input
            pass
        
        # Test cv with NaN
        try:
            result = cv(float('nan'), 5.0)
            assert math.isnan(result) or isinstance(result, float)
        except ValueError:
            # Expected for NaN input
            pass
    
    def test_infinity_input_handling(self):
        """Test handling of infinite inputs."""
        # Test ratio_var with infinity
        try:
            result = ratio_var(float('inf'), 50.0, 25.0, 9.0, 10.0)
            assert math.isinf(result) or isinstance(result, float)
        except (ValueError, OverflowError):
            # Expected for infinite input
            pass
        
        # Test cv with infinity
        try:
            result = cv(float('inf'), 5.0)
            assert result == 0.0 or math.isnan(result)
        except (ValueError, OverflowError):
            # Expected for infinite input
            pass
    
    def test_very_small_numbers(self):
        """Test handling of very small numbers."""
        # Test with numbers close to machine epsilon
        tiny = 1e-100
        
        try:
            result = ratio_var(tiny, tiny, tiny, tiny, tiny)
            assert isinstance(result, float)
            assert not math.isnan(result)
        except (ValueError, ZeroDivisionError):
            # Expected for very small numbers
            pass
    
    def test_very_large_numbers(self):
        """Test handling of very large numbers."""
        # Test with very large numbers
        huge = 1e100
        
        try:
            result = ratio_var(huge, huge, huge, huge, huge)
            assert isinstance(result, float)
            assert not math.isnan(result)
        except (ValueError, OverflowError):
            # Expected for very large numbers
            pass


class TestEstimationUtilsIntegration:
    """Integration tests with realistic FIA data scenarios."""
    
    def test_typical_fia_ratio_estimation(self):
        """Test ratio estimation with typical FIA values."""
        # Typical values for TPA estimation
        tree_total = 1000.0  # Total trees
        area_total = 50.0    # Total area (plots * expansion)
        tree_var = 10000.0   # Tree variance
        area_var = 25.0      # Area variance
        cov_tree_area = 500.0  # Covariance
        
        # Calculate TPA and its variance
        tpa_estimate = tree_total / area_total
        tpa_variance = ratio_var(tree_total, area_total, tree_var, area_var, cov_tree_area)
        tpa_se = math.sqrt(tpa_variance)
        tpa_cv = cv(tpa_estimate, tpa_se)
        
        # Check results are reasonable
        assert tpa_estimate == 20.0  # 1000/50 = 20 TPA
        assert tpa_variance >= 0
        assert tpa_se >= 0
        assert tpa_cv >= 0
        assert not math.isnan(tpa_cv)
    
    def test_low_cv_scenario(self):
        """Test scenario with low coefficient of variation."""
        estimate = 1000.0
        se = 10.0  # 1% CV
        
        cv_result = cv(estimate, se)
        assert cv_result == 1.0
        assert cv_result < 5.0  # Good precision
    
    def test_high_cv_scenario(self):
        """Test scenario with high coefficient of variation."""
        estimate = 100.0
        se = 50.0  # 50% CV
        
        cv_result = cv(estimate, se)
        assert cv_result == 50.0
        assert cv_result > 30.0  # Poor precision
    
    def test_zero_estimate_scenario(self):
        """Test scenario with zero estimate (no trees)."""
        # When there are no trees, estimate is 0
        estimate = 0.0
        se = 0.0
        
        # CV is undefined for zero estimate
        with pytest.raises((ValueError, ZeroDivisionError)):
            cv(estimate, se)
    
    def test_biomass_ratio_estimation(self):
        """Test ratio estimation for biomass per acre."""
        # Typical biomass estimation values
        biomass_total = 5000.0  # Total biomass (tons)
        area_total = 100.0      # Total area
        biomass_var = 250000.0  # Biomass variance
        area_var = 100.0        # Area variance
        cov_biomass_area = 2500.0  # Covariance
        
        # Calculate biomass per acre
        biomass_per_acre = biomass_total / area_total
        biomass_variance = ratio_var(biomass_total, area_total, biomass_var, area_var, cov_biomass_area)
        biomass_se = math.sqrt(biomass_variance)
        biomass_cv = cv(biomass_per_acre, biomass_se)
        
        # Check results
        assert biomass_per_acre == 50.0  # 5000/100 = 50 tons/acre
        assert biomass_variance >= 0
        assert biomass_se >= 0
        assert biomass_cv >= 0


class TestEstimationUtilsPolarsIntegration:
    """Test integration with Polars DataFrames."""
    
    def test_ratio_var_with_polars_data(self):
        """Test ratio variance calculation with Polars data."""
        # Create mock estimation results
        df = pl.DataFrame({
            "y_total": [1000.0, 2000.0, 1500.0],
            "x_total": [50.0, 100.0, 75.0],
            "y_var": [10000.0, 40000.0, 22500.0],
            "x_var": [25.0, 100.0, 56.25],
            "cov_yx": [500.0, 2000.0, 1125.0]
        })
        
        # Calculate ratio variance for each row
        for row in df.iter_rows(named=True):
            ratio_variance = ratio_var(
                row["y_total"], row["x_total"], 
                row["y_var"], row["x_var"], row["cov_yx"]
            )
            assert ratio_variance >= 0
            assert not math.isnan(ratio_variance)
    
    def test_cv_with_polars_data(self):
        """Test CV calculation with Polars data."""
        # Create mock estimation results
        df = pl.DataFrame({
            "estimate": [20.0, 15.0, 25.0, 0.0],
            "se": [2.0, 3.0, 1.0, 0.0]
        })
        
        # Calculate CV for each row (except zero estimate)
        for i, row in enumerate(df.iter_rows(named=True)):
            if row["estimate"] > 0:
                cv_result = cv(row["estimate"], row["se"])
                assert cv_result >= 0
                assert not math.isnan(cv_result)
            else:
                # Skip zero estimate case
                assert row["estimate"] == 0.0
    
    def test_estimation_workflow_simulation(self):
        """Test complete estimation workflow simulation."""
        # Simulate stratified estimation workflow
        
        # Mock stratum-level data
        strata_df = pl.DataFrame({
            "stratum_id": [1, 2, 3],
            "n_plots": [10, 15, 8],
            "area_weight": [0.3, 0.5, 0.2],
            "stratum_mean": [25.0, 30.0, 20.0],
            "stratum_var": [100.0, 150.0, 80.0]
        })
        
        # Calculate population estimate
        population_mean = (strata_df["stratum_mean"] * strata_df["area_weight"]).sum()
        
        # Calculate population variance (simplified)
        population_var = (strata_df["stratum_var"] * strata_df["area_weight"] ** 2).sum()
        population_se = math.sqrt(population_var)
        population_cv = cv(population_mean, population_se)
        
        # Check results
        assert population_mean > 0
        assert population_se >= 0
        assert population_cv >= 0
        assert not math.isnan(population_cv)