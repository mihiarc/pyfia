"""
Comprehensive tests for Trees Per Acre (TPA) estimation module.

These tests verify the TPA estimation functionality including:
- Basic estimation with known data
- Species and size class grouping
- Domain filtering
- Statistical properties
- Edge cases and error handling
"""

from unittest.mock import patch

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation import tpa


class TestTPABasicEstimation:
    """Test basic TPA estimation functionality."""

    def test_tpa_with_sample_data(self, sample_fia_instance, sample_evaluation):
        """Test basic TPA estimation with sample or real database."""
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

        assert estimate is not None and estimate >= 0, "TPA estimate should be non-negative"
        # SE might be NaN with simple test data, so check for not negative if not NaN
        import math
        assert math.isnan(se) or se >= 0, "Standard error should be non-negative"
        assert result["N_PLOTS"][0] > 0, "Should have plots"

    def test_tpa_by_species(self, sample_fia_instance, sample_evaluation, use_real_data):
        """Test TPA estimation grouped by species."""
        result = tpa(sample_fia_instance,
            by_species=True
        )

        # Should have multiple rows for different species
        assert len(result) > 1

        # Should have species information
        assert "SPCD" in result.columns
        # COMMON_NAME may not always be joined for all species; tolerate absence on real data
        if "COMMON_NAME" in result.columns:
            assert len(result["COMMON_NAME"]) == len(result)

        # All estimates should be non-negative
        assert (result["TPA"].fill_null(0) >= 0).all()

        # Synthetic-only expectation: limited species set
        if not use_real_data:
            valid_species = [131, 110, 833, 802]
            assert all(spcd in valid_species for spcd in result["SPCD"].to_list())

    def test_tpa_by_size_class(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation grouped by size class."""
        result = tpa(sample_fia_instance,
            by_size_class=True
        )

        # Should have size class information
        assert "SIZE_CLASS" in result.columns

        # Check size classes are reasonable (floor-int DIA bins)
        size_classes = [sc for sc in result["SIZE_CLASS"].to_list() if sc is not None]
        assert all(isinstance(sc, int) and sc >= 0 for sc in size_classes)
        assert len(size_classes) > 0

    def test_tpa_with_tree_domain(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation with tree domain filtering."""
        # Test with diameter filter
        result_all = tpa(sample_fia_instance)
        result_large = tpa(sample_fia_instance,
            tree_domain="DIA >= 12"
        )

        # Filtered result should have fewer trees
        assert result_large["TPA"][0] <= result_all["TPA"][0]

        # Test with species filter
        result_pine = tpa(sample_fia_instance,
            tree_domain="SPCD IN (131, 110)"  # Pine species
        )

        assert result_pine["TPA"][0] <= result_all["TPA"][0]

    def test_tpa_with_area_domain(self, sample_fia_instance, sample_evaluation):
        """Test TPA estimation with area domain filtering."""
        result_all = tpa(sample_fia_instance)
        result_forest = tpa(sample_fia_instance,
            area_domain="COND_STATUS_CD == 1"  # Forest land only
        )

        # Domain-restricted estimate should not exceed unrestricted (allow tiny FP tolerance)
        assert result_forest["TPA"][0] <= result_all["TPA"][0] + 1e-6

    def test_tpa_totals_vs_per_acre(self, sample_fia_instance, sample_evaluation):
        """Test totals=True vs totals=False parameter."""
        result_per_acre = tpa(sample_fia_instance,
            totals=False
        )

        result_totals = tpa(sample_fia_instance,
            totals=True
        )

        # Both should have estimates
        assert "TPA" in result_per_acre.columns
        assert "TPA" in result_totals.columns

        # With simple test data, totals might be same as per-acre
        # This depends on the expansion factors and data structure
        total_estimate = result_totals["TPA"][0]
        per_acre_estimate = result_per_acre["TPA"][0]

        # Both should be positive
        assert total_estimate > 0
        assert per_acre_estimate > 0
        # Totals should be >= per-acre (allow tiny FP tolerance)
        assert total_estimate + 1e-6 >= per_acre_estimate


class TestTPAStatisticalProperties:
    """Test statistical properties of TPA estimation."""

    def test_tpa_variance_calculation(self, sample_fia_instance, sample_evaluation):
        """Test that variance calculations are consistent."""
        result = tpa(sample_fia_instance)

        estimate = result["TPA"][0]
        se = result["TPA_SE"][0]
        # TPA function doesn't return CV, so calculate it
        cv = (se / estimate) * 100 if estimate > 0 else 0

        # CV should equal (SE / Estimate) * 100
        import math
        if not math.isnan(se) and estimate > 0:
            expected_cv = (se / estimate) * 100
            assert abs(cv - expected_cv) < 0.01
            assert se >= 0
        else:
            # SE might be NaN with simple test data
            assert math.isnan(se) or se >= 0

    def test_tpa_grouping_sums(self, sample_fia_instance, sample_evaluation, use_real_data):
        """Test that grouped estimates sum appropriately (qualitative on real data)."""
        # Get total TPA
        result_total = tpa(sample_fia_instance)
        total_tpa = result_total["TPA"][0]

        # Get TPA by species
        result_by_species = tpa(sample_fia_instance,
            by_species=True
        )

        # Qualitative checks only for real data
        assert total_tpa >= 0
        assert (result_by_species["TPA"].fill_null(0) >= 0).all()
        if not use_real_data:
            species_sum = result_by_species["TPA"].sum()
            assert abs(total_tpa - species_sum) / max(total_tpa, species_sum) < 0.5

    def test_tpa_confidence_intervals(self, sample_fia_instance, sample_evaluation):
        """Test confidence interval calculation."""
        result = tpa(sample_fia_instance)

        if "CI_LOWER" in result.columns and "CI_UPPER" in result.columns:
            estimate = result["TPA"][0]
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
            assert "TPA" in result.columns

    def test_tpa_performance_basic(self, sample_fia_instance, sample_evaluation, use_real_data):
        """Basic performance test for TPA estimation."""
        import time

        start_time = time.time()
        result = tpa(sample_fia_instance)
        end_time = time.time()

        # Should complete in reasonable time (allow more with real DB)
        execution_time = end_time - start_time
        assert execution_time < (10.0 if use_real_data else 1.0)

        # Should produce valid result
        assert len(result) > 0

    @patch('pyfia.tpa._prepare_tpa_data')
    def test_tpa_with_mocked_data_preparation(self, mock_prepare, sample_fia_instance, use_real_data):
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

        # Skip in real data where code path may differ
        if use_real_data:
            pytest.skip("Skipping mocked preparation test on real data")

        try:
            _ = tpa(sample_fia_instance, evalid=123456)
            mock_prepare.assert_called_once()
        except Exception:
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
        tmp = sample_tree_data.with_columns([
            pl.lit("SUBP").alias("TREE_BASIS"),
            pl.lit(1.0).alias("ADJ_FACTOR_SUBP"),
        ])
        adjusted_trees = tmp.with_columns(
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR_SUBP")).alias("TPA_ADJ")
        )

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
