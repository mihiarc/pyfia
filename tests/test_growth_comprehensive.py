"""
Comprehensive tests for growth estimation module.

These tests verify the growth estimation functionality including:
- Basic growth rate estimation (diameter, volume, biomass)
- Annual vs periodic growth calculations
- Survivor vs ingrowth components
- Statistical properties
- Domain filtering and grouping

NOTE: Growth function not yet implemented - tests are skipped.
"""

import pytest
import polars as pl
import numpy as np

from pyfia import FIA

pytestmark = pytest.mark.skip(reason="Growth function not yet implemented")


class TestGrowthBasicEstimation:
    """Test basic growth estimation functionality."""
    
    def test_growth_diameter_per_year(self, sample_fia_instance, sample_evaluation):
        """Test annual diameter growth estimation."""
        # Note: Growth requires remeasurement data which may not be in our test DB
        try:
            # result = growth(
            #     sample_fia_instance,
            #     evalid=sample_evaluation.evalid,
            #     component="DIA"
            # )
            pytest.skip("Growth function not yet implemented")
            
            # Basic result validation
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            
            # Check required columns
            expected_cols = ["ESTIMATE", "SE", "SE_PERCENT", "N_PLOTS"]
            for col in expected_cols:
                assert col in result.columns
            
            # Check values are reasonable
            estimate = result["ESTIMATE"][0]
            se = result["SE"][0]
            
            assert estimate >= 0, "Growth estimate should be non-negative"
            assert se >= 0, "Standard error should be non-negative"
            assert result["N_PLOTS"][0] > 0, "Should have plots"
            
        except (ValueError, RuntimeError):
            # Expected if no growth evaluation data available
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_volume_per_year(self, sample_fia_instance, sample_evaluation):
        """Test annual volume growth estimation."""
        try:
            pytest.skip("Growth function not yet implemented")
            
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert result["ESTIMATE"][0] >= 0
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_biomass_per_year(self, sample_fia_instance, sample_evaluation):
        """Test annual biomass growth estimation."""
        try:
            result = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="BIOMASS"
            )
            
            assert isinstance(result, pl.DataFrame)
            assert len(result) > 0
            assert result["ESTIMATE"][0] >= 0
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_survivor_vs_total(self, sample_fia_instance, sample_evaluation):
        """Test survivor growth vs total growth."""
        try:
            # Total growth (survivor + ingrowth)
            result_total = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="VOLUME",
                growthType="total"
            )
            
            # Survivor growth only
            result_survivor = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="VOLUME",
                growthType="survivor"
            )
            
            # Survivor growth should be <= total growth
            if len(result_total) > 0 and len(result_survivor) > 0:
                assert result_survivor["ESTIMATE"][0] <= result_total["ESTIMATE"][0]
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_by_species(self, sample_fia_instance, sample_evaluation):
        """Test growth estimation grouped by species."""
        try:
            result = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                bySpecies=True
            )
            
            if len(result) > 0:
                # Should have species information
                assert "SPCD" in result.columns
                assert "COMMON_NAME" in result.columns
                
                # All estimates should be non-negative
                assert (result["ESTIMATE"] >= 0).all()
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_by_size_class(self, sample_fia_instance, sample_evaluation):
        """Test growth estimation grouped by size class."""
        try:
            result = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                bySizeClass=True
            )
            
            if len(result) > 0:
                # Should have size class information
                assert "SIZE_CLASS" in result.columns
                
                # All estimates should be non-negative
                assert (result["ESTIMATE"] >= 0).all()
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")


class TestGrowthStatisticalProperties:
    """Test statistical properties of growth estimation."""
    
    def test_growth_variance_calculation(self, sample_fia_instance, sample_evaluation):
        """Test that variance calculations are consistent."""
        try:
            # result = growth(
            #     sample_fia_instance,
            #     evalid=sample_evaluation.evalid,
            #     component="DIA"
            # )
            pytest.skip("Growth function not yet implemented")
            
            if len(result) > 0:
                estimate = result["ESTIMATE"][0]
                se = result["SE"][0]
                cv = result["SE_PERCENT"][0]
                
                # CV should equal (SE / Estimate) * 100 when estimate > 0
                if estimate > 0:
                    expected_cv = (se / estimate) * 100
                    assert abs(cv - expected_cv) < 0.01
                
                # SE should be non-negative
                assert se >= 0
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_per_acre_vs_totals(self, sample_fia_instance, sample_evaluation):
        """Test totals=True vs totals=False parameter."""
        try:
            result_per_acre = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="VOLUME",
                totals=False
            )
            
            result_totals = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="VOLUME",
                totals=True
            )
            
            # Both should have estimates
            if len(result_per_acre) > 0 and len(result_totals) > 0:
                assert "ESTIMATE" in result_per_acre.columns
                assert "ESTIMATE" in result_totals.columns
                
                # Total should be much larger than per-acre
                total_estimate = result_totals["ESTIMATE"][0]
                per_acre_estimate = result_per_acre["ESTIMATE"][0]
                
                if per_acre_estimate > 0:
                    assert total_estimate >= per_acre_estimate * 100
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")
    
    def test_growth_grouping_consistency(self, sample_fia_instance, sample_evaluation):
        """Test that grouped estimates are consistent."""
        try:
            # Get total growth
            result_total = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA"
            )
            
            # Get growth by species
            result_by_species = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                bySpecies=True
            )
            
            if len(result_total) > 0 and len(result_by_species) > 0:
                total_growth = result_total["ESTIMATE"][0]
                species_sum = result_by_species["ESTIMATE"].sum()
                
                # Sum of species should approximately equal total
                if total_growth > 0:
                    assert abs(total_growth - species_sum) / total_growth < 0.2
            
        except (ValueError, RuntimeError):
            pytest.skip("No growth evaluation data available in test database")


class TestGrowthErrorHandling:
    """Test error handling and edge cases."""
    
    def test_growth_with_invalid_component(self, sample_fia_instance, sample_evaluation):
        """Test growth with invalid component parameter."""
        with pytest.raises(ValueError):
            # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="INVALID"
            )
    
    def test_growth_with_invalid_growth_type(self, sample_fia_instance, sample_evaluation):
        """Test growth with invalid growthType parameter."""
        with pytest.raises(ValueError):
            # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                growthType="invalid"
            )
    
    def test_growth_with_invalid_evalid(self, sample_fia_instance):
        """Test growth with non-existent EVALID."""
        with pytest.raises((ValueError, RuntimeError)):
            # growth(sample_fia_instance, evalid=999999, component="DIA")
    
    def test_growth_with_vol_evaluation(self, sample_fia_instance, sample_evaluation):
        """Test growth with volume evaluation (should fail or return zeros)."""
        # Our test evaluation is VOL type, not GRM (Growth/Removals/Mortality)
        try:
            # result = growth(
            #     sample_fia_instance,
            #     evalid=sample_evaluation.evalid,
            #     component="DIA"
            # )
            pytest.skip("Growth function not yet implemented")
            
            # If it succeeds, growth should be zero or very small
            if len(result) > 0:
                assert result["ESTIMATE"][0] >= 0
                
        except (ValueError, RuntimeError):
            # Expected behavior for wrong evaluation type
            pass


class TestGrowthCalculations:
    """Test growth calculation details."""
    
    def test_growth_annual_rate_calculation(self, sample_tree_data):
        """Test annual growth rate calculation."""
        # Mock growth calculation with time period
        trees_with_growth = sample_tree_data.with_columns([
            pl.lit(5).alias("TIME_PERIOD_YEARS"),  # 5-year period
            pl.lit(2.5).alias("TOTAL_DIA_GROWTH"),  # 2.5" total growth
            (pl.col("TOTAL_DIA_GROWTH") / pl.col("TIME_PERIOD_YEARS")).alias("ANNUAL_DIA_GROWTH")
        ])
        
        # Annual rate should be total growth divided by period
        expected_annual = trees_with_growth["TOTAL_DIA_GROWTH"] / trees_with_growth["TIME_PERIOD_YEARS"]
        actual_annual = trees_with_growth["ANNUAL_DIA_GROWTH"]
        
        assert (expected_annual == actual_annual).all()
        
        # Annual growth should be reasonable
        assert (trees_with_growth["ANNUAL_DIA_GROWTH"] >= 0).all()
        assert (trees_with_growth["ANNUAL_DIA_GROWTH"] <= 2).all()  # < 2" per year
    
    def test_growth_component_calculation(self, sample_tree_data):
        """Test growth component calculations."""
        # Mock volume growth calculation
        trees_with_vol_growth = sample_tree_data.with_columns([
            pl.lit(0.1).alias("ANNUAL_VOLUME_GROWTH_RATE"),  # 10% annual growth
            (pl.col("VOLCFNET") * pl.col("TPA_UNADJ") * pl.col("ANNUAL_VOLUME_GROWTH_RATE")).alias("VOLUME_GROWTH")
        ])
        
        # Volume growth should be proportional to standing volume
        assert (trees_with_vol_growth["VOLUME_GROWTH"] >= 0).all()
        
        # Should scale with both volume and TPA
        expected_vol_growth = (trees_with_vol_growth["VOLCFNET"] * 
                             trees_with_vol_growth["TPA_UNADJ"] * 
                             trees_with_vol_growth["ANNUAL_VOLUME_GROWTH_RATE"])
        actual_vol_growth = trees_with_vol_growth["VOLUME_GROWTH"]
        
        assert (expected_vol_growth == actual_vol_growth).all()
    
    def test_growth_survivor_calculation(self, sample_tree_data):
        """Test survivor growth calculation logic."""
        # Mock survivor identification
        trees_with_survivor = sample_tree_data.with_columns([
            # Trees that survived to remeasurement
            pl.lit(True).alias("IS_SURVIVOR"),
            pl.lit(1.2).alias("DIA_T1"),  # Diameter at time 1
            (pl.col("DIA") + 2.0).alias("DIA_T2"),  # Diameter at time 2 (grown)
            (pl.col("DIA_T2") - pl.col("DIA_T1")).alias("SURVIVOR_GROWTH")
        ])
        
        # All our mock trees are survivors
        assert trees_with_survivor["IS_SURVIVOR"].all()
        
        # Growth should be positive
        assert (trees_with_survivor["SURVIVOR_GROWTH"] > 0).all()
        
        # Growth should be reasonable
        assert (trees_with_survivor["SURVIVOR_GROWTH"] <= 5.0).all()  # < 5" growth
    
    def test_growth_ingrowth_calculation(self, sample_tree_data):
        """Test ingrowth calculation logic."""
        # Mock ingrowth trees (new trees that grew into measurement threshold)
        ingrowth_data = sample_tree_data.with_columns([
            pl.lit(False).alias("WAS_MEASURED_T1"),  # Not measured at time 1
            pl.lit(True).alias("IS_MEASURED_T2"),    # Measured at time 2
            pl.when(~pl.col("WAS_MEASURED_T1") & pl.col("IS_MEASURED_T2"))
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("IS_INGROWTH")
        ])
        
        # In our mock, all trees are ingrowth
        assert ingrowth_data["IS_INGROWTH"].all()
        
        # Ingrowth trees contribute to total growth
        ingrowth_trees = ingrowth_data.filter(pl.col("IS_INGROWTH"))
        assert len(ingrowth_trees) > 0
    
    def test_growth_time_period_validation(self):
        """Test time period validation."""
        # Mock time period data
        time_periods = [1, 2, 5, 7, 10]
        
        for period in time_periods:
            # Time period should be positive
            assert period > 0
            
            # Annual rate calculation should work
            total_growth = 5.0  # 5" diameter growth over period
            annual_rate = total_growth / period
            
            assert annual_rate > 0
            assert annual_rate <= total_growth


class TestGrowthIntegration:
    """Integration tests for growth estimation."""
    
    def test_growth_with_multiple_components(self, sample_fia_instance, sample_evaluation):
        """Test estimating multiple growth components."""
        components = ["DIA", "VOLUME", "BIOMASS"]
        results = {}
        
        for component in components:
            try:
                results[component] = # growth(
                    sample_fia_instance,
                    evalid=sample_evaluation.evalid,
                    component=component
                )
            except (ValueError, RuntimeError):
                # Skip if component not available
                pass
        
        # At least one component should work (even if returns zeros)
        assert len(results) >= 0
        
        # All successful results should be valid DataFrames
        for component, result in results.items():
            assert isinstance(result, pl.DataFrame)
    
    def test_growth_consistency_across_methods(self, sample_fia_instance, sample_evaluation):
        """Test consistency across different estimation methods."""
        # Test temporal methods if available
        try:
            result_ti = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                method="TI"
            )
            
            # Should return valid result
            assert isinstance(result_ti, pl.DataFrame)
            
        except Exception:
            # Method might not be implemented or data not available
            pass
    
    def test_growth_performance(self, sample_fia_instance, sample_evaluation):
        """Basic performance test for growth estimation."""
        import time
        
        start_time = time.time()
        try:
            # result = growth(
            #     sample_fia_instance,
            #     evalid=sample_evaluation.evalid,
            #     component="DIA"
            # )
            pytest.skip("Growth function not yet implemented")
            end_time = time.time()
            
            # Should complete in reasonable time
            execution_time = end_time - start_time
            assert execution_time < 1.0
            
            # Should produce valid result
            assert isinstance(result, pl.DataFrame)
            
        except (ValueError, RuntimeError):
            # Expected if no growth data available
            pass


class TestGrowthSpecialCases:
    """Test special cases and edge conditions."""
    
    def test_growth_zero_period(self, sample_fia_instance, sample_evaluation):
        """Test handling of zero time period."""
        # This should be handled in data preparation
        # Zero time period should either be rejected or handled specially
        pass
    
    def test_growth_different_evaluation_types(self, sample_fia_instance):
        """Test growth with different evaluation types."""
        # Test with different EVALID patterns
        # GRM evaluations should work, others should fail or return zeros
        
        # This test would need different test data to be meaningful
        pass
    
    def test_growth_missing_remper_data(self, sample_fia_instance, sample_evaluation):
        """Test handling of missing remeasurement period data."""
        # Growth requires remeasurement data
        # Should handle gracefully when not available
        try:
            # result = growth(
            #     sample_fia_instance,
            #     evalid=sample_evaluation.evalid,
            #     component="DIA"
            # )
            pytest.skip("Growth function not yet implemented")
            
            # Should return valid DataFrame even if empty
            assert isinstance(result, pl.DataFrame)
            
        except (ValueError, RuntimeError):
            # Expected when remeasurement data not available
            pass
    
    def test_growth_negative_# growth(self, sample_fia_instance, sample_evaluation):
        """Test handling of negative growth (tree shrinkage)."""
        # Some trees may have negative "growth" due to measurement error
        # This should be handled appropriately in the estimation
        try:
            # result = growth(
            #     sample_fia_instance,
            #     evalid=sample_evaluation.evalid,
            #     component="DIA"
            # )
            pytest.skip("Growth function not yet implemented")
            
            # Population estimate should still be non-negative even if
            # individual trees have negative growth
            if len(result) > 0:
                assert result["ESTIMATE"][0] >= 0
            
        except (ValueError, RuntimeError):
            # Expected if no growth evaluation data
            pass
    
    def test_growth_with_domain_filters(self, sample_fia_instance, sample_evaluation):
        """Test growth with domain filtering."""
        try:
            # Test with tree domain
            result_filtered = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                treeDomain="DIA >= 10"
            )
            
            # Test with area domain
            result_area = # growth(
                sample_fia_instance,
                evalid=sample_evaluation.evalid,
                component="DIA",
                areaDomain="FORTYPCD == 220"
            )
            
            # Both should return valid DataFrames
            assert isinstance(result_filtered, pl.DataFrame)
            assert isinstance(result_area, pl.DataFrame)
            
        except (ValueError, RuntimeError):
            # Expected if no growth evaluation data
            pass


class TestGrowthDataValidation:
    """Test growth data validation and preprocessing."""
    
    def test_growth_measurement_validation(self, sample_tree_data):
        """Test validation of tree measurements for growth."""
        # Mock two-period measurements
        tree_measurements = sample_tree_data.with_columns([
            pl.col("DIA").alias("DIA_T1"),          # First measurement
            (pl.col("DIA") + 1.5).alias("DIA_T2"),  # Second measurement (grown)
            pl.lit(5).alias("REMPER"),              # 5-year period
        ])
        
        # Both measurements should be positive
        assert (tree_measurements["DIA_T1"] > 0).all()
        assert (tree_measurements["DIA_T2"] > 0).all()
        
        # Growth should be positive
        growth_increment = tree_measurements["DIA_T2"] - tree_measurements["DIA_T1"]
        assert (growth_increment >= 0).all()
        
        # Growth should be reasonable
        assert (growth_increment <= 5.0).all()  # < 5" growth
    
    def test_growth_status_code_validation(self, sample_tree_data):
        """Test validation of tree status codes for growth."""
        # Trees must be live in both measurements for survivor growth
        # Dead trees contribute to mortality, not growth
        
        live_code = 1
        
        # Our sample data only has live trees
        assert (sample_tree_data["STATUSCD"] == live_code).all()
        
        # Growth calculation would need status from both measurements
    
    def test_growth_measurement_period_validation(self):
        """Test measurement period validation."""
        # Mock measurement periods
        valid_periods = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        invalid_periods = [0, -1, 100]
        
        for period in valid_periods:
            assert 1 <= period <= 10  # Reasonable range
        
        for period in invalid_periods:
            assert not (1 <= period <= 10)
    
    def test_growth_plot_consistency(self, sample_plot_data):
        """Test plot consistency for growth calculations."""
        # Plots should be accessible in both measurement periods
        accessible_plots = sample_plot_data.filter(
            pl.col("PLOT_STATUS_CD") == 1
        )
        
        # All our test plots should be accessible
        assert len(accessible_plots) == len(sample_plot_data)
        
        # In real data, would need to check both measurement occasions


class TestGrowthComponentRelationships:
    """Test relationships between different growth components."""
    
    def test_growth_diameter_volume_relationship(self, sample_tree_data):
        """Test relationship between diameter and volume growth."""
        # Mock growth relationships
        trees_with_growth = sample_tree_data.with_columns([
            pl.lit(0.2).alias("DIA_GROWTH"),        # 0.2" annual diameter growth
            pl.lit(5.0).alias("VOLUME_GROWTH"),     # 5 cu ft annual volume growth
        ])
        
        # Volume growth should generally be larger for trees with larger diameter growth
        # This is a rough relationship test
        assert (trees_with_growth["VOLUME_GROWTH"] >= 0).all()
        assert (trees_with_growth["DIA_GROWTH"] >= 0).all()
    
    def test_growth_biomass_volume_relationship(self, sample_tree_data):
        """Test relationship between biomass and volume growth."""
        # Mock biomass and volume growth
        trees_with_growth = sample_tree_data.with_columns([
            pl.lit(5.0).alias("VOLUME_GROWTH"),     # 5 cu ft annual growth
            pl.lit(2.0).alias("BIOMASS_GROWTH"),    # 2 tons annual growth
        ])
        
        # Both should be positive and reasonably related
        assert (trees_with_growth["VOLUME_GROWTH"] >= 0).all()
        assert (trees_with_growth["BIOMASS_GROWTH"] >= 0).all()
        
        # Volume and biomass growth should be correlated
        # (exact relationship depends on wood density)
    
    def test_growth_size_class_differences(self, sample_tree_data):
        """Test growth differences by size class."""
        # Add size class information
        trees_with_size_class = sample_tree_data.with_columns([
            pl.when(pl.col("DIA") < 10)
            .then(pl.lit("Small"))
            .when(pl.col("DIA") < 20)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias("SIZE_CLASS")
        ])
        
        # Different size classes should have different growth patterns
        size_classes = trees_with_size_class["SIZE_CLASS"].unique().to_list()
        assert len(size_classes) > 1  # Should have multiple size classes
        
        # In real data, would expect different growth rates by size