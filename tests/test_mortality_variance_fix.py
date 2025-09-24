"""
Test mortality variance calculation against EVALIDator values.

This test verifies that issue #49 is fixed - the variance calculation
should produce sampling errors close to EVALIDator's published values.
"""

import pytest
import polars as pl
from pyfia import FIA, mortality


class TestMortalityVariance:
    """Test mortality variance calculation against known EVALIDator values."""

    @pytest.fixture(scope="class")
    def georgia_db(self):
        """Set up Georgia database for testing."""
        db = FIA("data/test_southern.duckdb")
        db.clip_by_evalid(132303)  # Georgia 2023 mortality evaluation
        return db

    def test_georgia_sawlog_mortality_variance(self, georgia_db):
        """Test Georgia sawlog mortality variance matches EVALIDator.

        EVALIDator values for Georgia sawlog mortality of sawtimber:
        - Total: 307,168,403 cu ft
        - Sampling Error: 5.527%
        - Non-zero plots: 924
        """
        # Run mortality estimation with variance
        result = mortality(
            georgia_db,
            measure="sawlog",
            tree_type="sawtimber",
            land_type="timber",
            totals=True,
            variance=True
        )

        # Check total value (should match exactly after our fixes)
        assert abs(result["MORT_TOTAL"][0] - 307_168_403) < 1000, \
            f"Total mismatch: {result['MORT_TOTAL'][0]:,.0f} vs 307,168,403"

        # Check sampling error percentage
        se_pct = result["MORT_TOTAL_SE"][0] / result["MORT_TOTAL"][0] * 100

        # Allow for small differences due to rounding and methodology
        # EVALIDator: 5.527%, we expect within 0.5%
        assert abs(se_pct - 5.527) < 0.5, \
            f"SE% mismatch: {se_pct:.3f}% vs 5.527%"

        # Check plot count
        assert result["N_PLOTS"][0] == 924, \
            f"Plot count mismatch: {result['N_PLOTS'][0]} vs 924"

    def test_mortality_variance_with_grouping(self, georgia_db):
        """Test that variance calculation works with grouped estimates."""
        # Run mortality by ownership group
        result = mortality(
            georgia_db,
            grp_by="OWNGRPCD",
            measure="volume",
            tree_type="gs",
            land_type="forest",
            totals=True,
            variance=True
        )

        # Should have multiple groups
        assert len(result) > 1, "Expected multiple ownership groups"

        # Each group should have variance calculated
        for i in range(len(result)):
            assert "MORT_TOTAL_SE" in result.columns
            assert result["MORT_TOTAL_SE"][i] >= 0, \
                f"SE should be non-negative for group {i}"

            # SE% should be reasonable (typically 5-50% for mortality, but can be higher for small groups)
            if result["MORT_TOTAL"][i] > 0:
                se_pct = result["MORT_TOTAL_SE"][i] / result["MORT_TOTAL"][i] * 100
                assert 0 < se_pct < 200, \
                    f"SE% should be reasonable for group {i}: {se_pct:.1f}%"

    def test_mortality_variance_different_measures(self, georgia_db):
        """Test variance calculation for different mortality measures."""
        measures = ["volume", "biomass", "tpa"]

        for measure in measures:
            result = mortality(
                georgia_db,
                measure=measure,
                tree_type="gs",
                land_type="forest",
                totals=True,
                variance=True
            )

            # Should have SE columns
            assert "MORT_ACRE_SE" in result.columns, \
                f"Missing MORT_ACRE_SE for measure={measure}"
            assert "MORT_TOTAL_SE" in result.columns, \
                f"Missing MORT_TOTAL_SE for measure={measure}"

            # SE should be positive
            assert result["MORT_ACRE_SE"][0] >= 0, \
                f"MORT_ACRE_SE should be non-negative for {measure}"
            assert result["MORT_TOTAL_SE"][0] >= 0, \
                f"MORT_TOTAL_SE should be non-negative for {measure}"

            # SE% should be in reasonable range
            if result["MORT_TOTAL"][0] > 0:
                se_pct = result["MORT_TOTAL_SE"][0] / result["MORT_TOTAL"][0] * 100
                assert 0 < se_pct < 50, \
                    f"SE% should be reasonable for {measure}: {se_pct:.1f}%"

    @pytest.mark.skip(reason="include_cv parameter not yet implemented in mortality()")
    def test_mortality_variance_with_cv(self, georgia_db):
        """Test that CV (coefficient of variation) is calculated correctly."""
        # This test is skipped as include_cv is not yet a parameter in mortality()
        # When implemented, it should calculate CV = SE/estimate * 100
        pass

    def test_variance_for_zero_mortality_strata(self, georgia_db):
        """Test that variance handles strata with no mortality correctly."""
        # Use a very restrictive filter that might result in zero mortality in some strata
        result = mortality(
            georgia_db,
            tree_domain="DIA_MIDPT > 30.0",  # Very large trees only
            measure="volume",
            tree_type="gs",
            land_type="forest",
            totals=True,
            variance=True
        )

        # Should still calculate variance even with limited data
        assert "MORT_TOTAL_SE" in result.columns
        assert result["MORT_TOTAL_SE"][0] >= 0, "SE should be non-negative"

        # If there's any mortality, SE should be positive
        if result["MORT_TOTAL"][0] > 0:
            assert result["MORT_TOTAL_SE"][0] > 0, \
                "SE should be positive when there's mortality"