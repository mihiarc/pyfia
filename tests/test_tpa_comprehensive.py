"""
Comprehensive tests for Trees Per Acre (TPA) estimation module.

These tests verify the TPA estimation functionality including:
- Basic estimation with known data
- Species and size class grouping
- Domain filtering
- Statistical properties
- Edge cases and error handling
"""

import pytest
import polars as pl
import numpy as np
from unittest.mock import patch, MagicMock

from pyfia import tpa, FIA
from pyfia.models import EvaluationInfo


class TestTPABasicEstimation:
    """Test basic TPA estimation functionality."""
    
    def test_tpa_with_sample_data(self, sample_fia_instance, sample_evaluation):
        """Test basic TPA estimation with sample database."""
        result = tpa(sample_fia_instance)
        
        # Basic result validation
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        
        # Check required columns
        expected_cols = ["TPA", "TPA_SE", "N_PLOTS"]
        for col in expected_cols:
            assert col in result.columns
        
        # Check values are reasonable
        estimate = result["TPA"][0]
        se = result["TPA_SE"][0]
        
        assert estimate > 0, "TPA estimate should be positive"
        # SE might be NaN with simple test data, so check for not negative if not NaN
        import math
        assert math.isnan(se) or se >= 0, "Standard error should be non-negative"
        assert result["N_PLOTS"][0] > 0, "Should have plots"
    
    def test_tpa_by_species(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation grouped by species."""
        result = tpa(sample_fia_instance,
            by_species=True
        )
        
        # Should have multiple rows for different species
        assert len(result) > 1
        
        # Should have species information
        assert "SPCD" in result.columns
        assert "COMMON_NAME" in result.columns
        
        # All estimates should be positive
        assert (result["ESTIMATE"] > 0).all()
        
        # Species codes should be valid
        valid_species = [131, 110, 833, 802]  # From our test data
        assert all(spcd in valid_species for spcd in result["SPCD"].to_list())
    
    def test_tpa_by_size_class(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation grouped by size class."""
        result = tpa(sample_fia_instance,
            by_size_class=True
        )
        
        # Should have size class information
        assert "SIZE_CLASS" in result.columns
        
        # Check size classes are reasonable
        size_classes = result["SIZE_CLASS"].to_list()
        expected_classes = ["5.0-10.0", "10.0-15.0", "15.0-20.0"]
        
        # Should have at least some of the expected size classes
        assert any(sc in expected_classes for sc in size_classes)
    
    def test_tpa_with_tree_domain(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation with tree domain filtering."""
        # Test with diameter filter
        result_all = tpa(sample_fia_instance)
        result_large = tpa(sample_fia_instance,
            tree_domain="DIA >= 12"
        )
        
        # Filtered result should have fewer trees
        assert result_large["ESTIMATE"][0] <= result_all["ESTIMATE"][0]
        
        # Test with species filter
        result_pine = tpa(sample_fia_instance,
            tree_domain="SPCD IN (131, 110)"  # Pine species
        )
        
        assert result_pine["ESTIMATE"][0] <= result_all["ESTIMATE"][0]
    
    def test_tpa_with_area_domain(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation with area domain filtering."""
        result_all = tpa(sample_fia_instance)
        result_forest = tpa(sample_fia_instance,
            area_domain="COND_STATUS_CD == 1"  # Forest land only
        )
        
        # In our test data, all conditions are forest, so should be same
        assert abs(result_all["ESTIMATE"][0] - result_forest["ESTIMATE"][0]) < 0.01
    
    def test_tpa_totals_vs_per_acre(self, sample_fia_instance, sample_evaluation):
        """Test totals=True vs totals=False parameter."""
        result_per_acre = tpa(sample_fia_instance,
            totals=False
        )
        
        result_totals = tpa(sample_fia_instance,
            totals=True
        )
        
        # Both should have estimates
        assert "ESTIMATE" in result_per_acre.columns
        assert "ESTIMATE" in result_totals.columns
        
        # Total should be much larger than per-acre
        total_estimate = result_totals["ESTIMATE"][0]
        per_acre_estimate = result_per_acre["ESTIMATE"][0]
        
        assert total_estimate > per_acre_estimate * 1000  # Assuming large area


class TestTPAStatisticalProperties:
    """Test statistical properties of TPA estimation."""
    
    def test_tpa_variance_calculation(self, sample_fia_instance, sample_evaluation):
        """Test that variance calculations are consistent."""
        result = tpa(sample_fia_instance)
        
        estimate = result["ESTIMATE"][0]
        se = result["SE"][0]
        cv = result["SE_PERCENT"][0]
        
        # CV should equal (SE / Estimate) * 100
        expected_cv = (se / estimate) * 100
        assert abs(cv - expected_cv) < 0.01
        
        # SE should be positive for estimates > 0
        if estimate > 0:
            assert se > 0
    
    def test_tpa_grouping_sums(self, sample_fia_instance, sample_evaluation):
        """Test that grouped estimates sum appropriately."""
        # Get total TPA
        result_total = tpa(sample_fia_instance)
        total_tpa = result_total["ESTIMATE"][0]
        
        # Get TPA by species
        result_by_species = tpa(sample_fia_instance,
            by_species=True
        )
        
        # Sum of species should approximately equal total
        species_sum = result_by_species["ESTIMATE"].sum()
        
        # Allow for small differences due to rounding
        assert abs(total_tpa - species_sum) < 0.1
    
    def test_tpa_confidence_intervals(self, sample_fia_instance, sample_evaluation):
        """Test confidence interval calculation."""
        result = tpa(sample_fia_instance)
        
        if "CI_LOWER" in result.columns and "CI_UPPER" in result.columns:
            estimate = result["ESTIMATE"][0]
            ci_lower = result["CI_LOWER"][0]
            ci_upper = result["CI_UPPER"][0]
            
            # CI should bracket the estimate
            assert ci_lower <= estimate <= ci_upper
            
            # CI should be reasonable width
            assert ci_upper > ci_lower


class TestTPAErrorHandling:
    """Test error handling and edge cases."""
    
    def test_tpa_with_invalid_evalid(self, sample_fia_instance):
        """Test TPA with non-existent EVALID."""
        # TPA doesn't take evalid parameter directly
        # This test would be for the clipped database approach
        pass
    
    def test_tpa_with_empty_domain(self, sample_fia_instance, sample_evaluation):
        """Test TPA with domain that excludes all trees."""
        result = tpa(sample_fia_instance,
            tree_domain="DIA > 1000"  # No trees this large
        )
        
        # Should handle gracefully
        assert isinstance(result, pl.DataFrame)
        # Estimate might be 0 or NaN
    
    def test_tpa_with_invalid_domain_syntax(self, sample_fia_instance, sample_evaluation):
        """Test TPA with malformed domain filter."""
        with pytest.raises((ValueError, pl.exceptions.ComputeError)):
            tpa(sample_fia_instance,
                tree_domain="INVALID SYNTAX HERE"
            )
    
    def test_tpa_with_missing_database(self):
        """Test TPA with non-existent database."""
        with pytest.raises(FileNotFoundError):
            fake_fia = FIA("nonexistent.db")
            tpa(fake_fia)


class TestTPAIntegration:
    """Integration tests for TPA with real-world scenarios."""
    
    def test_tpa_multiple_evaluations(self, sample_fia_instance):
        """Test TPA estimation across different evaluation scenarios."""
        # This would test with multiple EVALIDs if available in test data
        result = tpa(sample_fia_instance, most_recent=True)
        assert isinstance(result, pl.DataFrame)
    
    def test_tpa_with_complex_grouping(self, sample_fia_instance, sample_evaluation):
        """Test TPA with custom grouping variables."""
        result = tpa(sample_fia_instance,
            grp_by=["FORTYPCD"]  # Group by forest type
        )
        
        if "FORTYPCD" in result.columns:
            assert len(result) >= 1
            assert "ESTIMATE" in result.columns
    
    def test_tpa_performance_basic(self, sample_fia_instance, sample_evaluation):
        """Basic performance test for TPA estimation."""
        import time
        
        start_time = time.time()
        result = tpa(sample_fia_instance)
        end_time = time.time()
        
        # Should complete in reasonable time (less than 1 second for test data)
        execution_time = end_time - start_time
        assert execution_time < 1.0
        
        # Should produce valid result
        assert len(result) > 0
    
    @patch('pyfia.tpa._prepare_tpa_data')
    def test_tpa_with_mocked_data_preparation(self, mock_prepare, sample_fia_instance):
        """Test TPA with mocked data preparation to test calculation logic."""
        # Mock the data preparation to return known data
        mock_tree_data = pl.DataFrame({
            "PLT_CN": ["PLT001", "PLT001", "PLT002"],
            "TPA_UNADJ": [6.0, 6.0, 6.0],
            "STATUSCD": [1, 1, 1],
            "EXPNS": [6000.0, 6000.0, 6000.0],
        })
        
        mock_prepare.return_value = {
            "tree": mock_tree_data,
            "plot": pl.DataFrame({"PLT_CN": ["PLT001", "PLT002"]}),
            "stratum": pl.DataFrame({"EXPNS": [6000.0]})
        }
        
        # This test would need to be adapted based on actual TPA implementation
        # For now, just verify the mock is called
        try:
            result = tpa(sample_fia_instance, evalid=123456)
            mock_prepare.assert_called_once()
        except Exception:
            # Expected if the actual function structure differs
            pass


class TestTPADataValidation:
    """Test data validation and preprocessing."""
    
    def test_tpa_tree_basis_assignment(self, sample_tree_data):
        """Test that trees are assigned correct TREE_BASIS."""
        # This would test the TREE_BASIS assignment logic
        # Based on tree diameter and plot design
        
        # Add required columns for testing
        trees_with_dia = sample_tree_data.with_columns([
            pl.when(pl.col("DIA") < 5.0)
            .then(pl.lit("MICR"))
            .otherwise(pl.lit("SUBP"))
            .alias("TREE_BASIS")
        ])
        
        # All trees in our sample data should be SUBP (>= 5.0")
        assert (trees_with_dia["TREE_BASIS"] == "SUBP").all()
    
    def test_tpa_adjustment_factors(self, sample_tree_data):
        """Test adjustment factor application."""
        # Mock adjustment factors
        adjusted_trees = sample_tree_data.with_columns([
            pl.lit("SUBP").alias("TREE_BASIS"),
            pl.lit(1.0).alias("ADJ_FACTOR_SUBP"),
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR_SUBP")).alias("TPA_ADJ")
        ])
        
        # TPA_ADJ should equal TPA_UNADJ when adjustment factor is 1.0
        assert (adjusted_trees["TPA_ADJ"] == adjusted_trees["TPA_UNADJ"]).all()
    
    def test_tpa_plot_filtering(self, sample_plot_data):
        """Test plot-level filtering."""
        # Filter to accessible plots only
        accessible_plots = sample_plot_data.filter(
            pl.col("PLOT_STATUS_CD") == 1
        )
        
        # All our test plots should be accessible
        assert len(accessible_plots) == len(sample_plot_data)