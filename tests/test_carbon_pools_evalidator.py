"""
EVALIDator validation tests for CarbonPoolEstimator.

Tests that pyFIA's carbon estimation using FIA pre-calculated carbon columns
matches EVALIDator snum=55000 exactly.
"""

import pytest
import polars as pl

from pyfia import FIA
from pyfia.estimation import carbon, carbon_pool


# Georgia test configuration
GEORGIA_DB_PATH = "data/georgia.duckdb"
GEORGIA_EVALID = 132301  # Current inventory (EXPCURR)

# EVALIDator reference values (snum=55000 - total live tree carbon)
# Validated December 2025
EVALIDATOR_CARBON_TOTAL = 767_736_994  # short tons

# Tolerance for floating point comparison
FLOAT_TOLERANCE = 1.0  # 1 short ton


@pytest.fixture
def georgia_db():
    """Georgia FIA database clipped to EVALID 132301."""
    with FIA(GEORGIA_DB_PATH) as db:
        db.clip_by_evalid(GEORGIA_EVALID)
        yield db


class TestCarbonPoolEstimatorEVALIDator:
    """EVALIDator validation tests for carbon estimation."""

    def test_total_carbon_matches_evalidator_exactly(self, georgia_db):
        """
        Test that total live tree carbon matches EVALIDator snum=55000 exactly.

        EVALIDator snum=55000 returns total aboveground + belowground carbon
        for all live trees on forest land.

        This test validates the core fix for Issue 1 in the Carbon Methodology
        Investigation spec - the 1.62% discrepancy was due to using biomass-derived
        carbon (47% of DRYBIO) instead of FIA's pre-calculated CARBON columns.
        """
        result = carbon(georgia_db, pool="live")

        pyfia_total = result["CARBON_TOTAL"][0]

        # Check exact match within floating point tolerance
        diff = abs(pyfia_total - EVALIDATOR_CARBON_TOTAL)
        diff_pct = diff / EVALIDATOR_CARBON_TOTAL * 100

        print(f"\npyFIA CARBON_TOTAL: {pyfia_total:,.0f} short tons")
        print(f"EVALIDator snum=55000: {EVALIDATOR_CARBON_TOTAL:,.0f} short tons")
        print(f"Difference: {diff:.2f} short tons ({diff_pct:.6f}%)")

        assert diff <= FLOAT_TOLERANCE, (
            f"Carbon estimate differs from EVALIDator by {diff:.2f} tons "
            f"({diff_pct:.6f}%). Expected exact match within {FLOAT_TOLERANCE} tons."
        )

    def test_carbon_pool_total_equals_carbon_live(self, georgia_db):
        """Test that carbon(pool='total') equals carbon(pool='live')."""
        result_live = carbon(georgia_db, pool="live")
        result_total = carbon(georgia_db, pool="total")

        # Use approximate equality due to floating point precision
        diff = abs(result_live["CARBON_TOTAL"][0] - result_total["CARBON_TOTAL"][0])
        assert diff < FLOAT_TOLERANCE, (
            f"pool='live' and pool='total' differ by {diff:.6f} tons"
        )

    def test_ag_plus_bg_equals_total(self, georgia_db):
        """Test that AG + BG carbon equals total carbon."""
        result_ag = carbon(georgia_db, pool="ag")
        result_bg = carbon(georgia_db, pool="bg")
        result_total = carbon(georgia_db, pool="total")

        ag_carbon = result_ag["CARBON_TOTAL"][0]
        bg_carbon = result_bg["CARBON_TOTAL"][0]
        total_carbon = result_total["CARBON_TOTAL"][0]

        # Sum should equal total within floating point tolerance
        assert abs(ag_carbon + bg_carbon - total_carbon) < FLOAT_TOLERANCE, (
            f"AG ({ag_carbon:,.0f}) + BG ({bg_carbon:,.0f}) = {ag_carbon + bg_carbon:,.0f} "
            f"does not equal TOTAL ({total_carbon:,.0f})"
        )

    def test_carbon_matches_evalidator_with_high_precision(self, georgia_db):
        """
        Test that carbon estimation matches EVALIDator with high precision.

        The FIA pre-calculated CARBON_AG and CARBON_BG columns use species-specific
        conversion factors, which matches EVALIDator snum=55000 exactly.
        """
        result = carbon(georgia_db, pool="live")
        fia_carbon = result["CARBON_TOTAL"][0]

        diff = abs(fia_carbon - EVALIDATOR_CARBON_TOTAL)
        diff_pct = diff / EVALIDATOR_CARBON_TOTAL * 100

        print(f"\npyFIA carbon: {fia_carbon:,.0f}")
        print(f"EVALIDator: {EVALIDATOR_CARBON_TOTAL:,.0f}")
        print(f"Difference: {diff:.2f} tons ({diff_pct:.6f}%)")

        # Should match EVALIDator exactly
        assert diff <= FLOAT_TOLERANCE, (
            f"Carbon should match EVALIDator exactly (diff: {diff:.2f})"
        )

    def test_carbon_pool_function_matches_estimator(self, georgia_db):
        """Test that carbon_pool() function produces same results as carbon()."""
        result_carbon = carbon(georgia_db, pool="total")
        result_carbon_pool = carbon_pool(georgia_db, pool="total")

        # Use approximate equality due to floating point precision
        diff_total = abs(result_carbon["CARBON_TOTAL"][0] - result_carbon_pool["CARBON_TOTAL"][0])
        diff_acre = abs(result_carbon["CARBON_ACRE"][0] - result_carbon_pool["CARBON_ACRE"][0])

        assert diff_total < FLOAT_TOLERANCE, f"CARBON_TOTAL differs by {diff_total:.6f}"
        assert diff_acre < 0.0001, f"CARBON_ACRE differs by {diff_acre:.6f}"

    def test_result_structure(self, georgia_db):
        """Test that result DataFrame has expected columns."""
        result = carbon(georgia_db, pool="total")

        expected_cols = ["YEAR", "POOL", "CARBON_ACRE", "CARBON_TOTAL", "N_PLOTS", "N_TREES"]
        for col in expected_cols:
            assert col in result.columns, f"Missing expected column: {col}"

        # POOL should be 'TOTAL'
        assert result["POOL"][0] == "TOTAL"

        # N_PLOTS should be reasonable
        assert result["N_PLOTS"][0] > 1000, "Expected > 1000 plots for Georgia"

    def test_variance_calculation(self, georgia_db):
        """Test that variance calculation produces reasonable results."""
        result = carbon(georgia_db, pool="total", variance=True)

        # Should have SE columns
        assert "CARBON_ACRE_SE" in result.columns
        assert "CARBON_TOTAL_SE" in result.columns

        # SE should be positive and reasonable (< 50% of estimate)
        se_pct = result["CARBON_ACRE_SE"][0] / result["CARBON_ACRE"][0] * 100
        assert se_pct > 0, "SE should be positive"
        assert se_pct < 50, f"SE seems too high: {se_pct:.1f}%"

        print(f"\nCarbon estimate: {result['CARBON_ACRE'][0]:.4f} tons/acre")
        print(f"Standard error: {result['CARBON_ACRE_SE'][0]:.4f} ({se_pct:.2f}%)")


class TestCarbonPoolBySpecies:
    """Tests for carbon estimation grouped by species."""

    def test_by_species_produces_multiple_rows(self, georgia_db):
        """Test that by_species=True produces multiple result rows."""
        result = carbon(georgia_db, pool="total", by_species=True)

        # Georgia should have many species
        assert len(result) > 50, "Expected > 50 species in Georgia"
        assert "SPCD" in result.columns

    def test_species_totals_sum_to_overall(self, georgia_db):
        """Test that sum of species carbon equals overall total."""
        result_overall = carbon(georgia_db, pool="total")
        result_by_species = carbon(georgia_db, pool="total", by_species=True)

        overall_total = result_overall["CARBON_TOTAL"][0]
        species_sum = result_by_species["CARBON_TOTAL"].sum()

        # Should match within tolerance
        diff = abs(species_sum - overall_total)
        diff_pct = diff / overall_total * 100

        print(f"\nOverall total: {overall_total:,.0f}")
        print(f"Species sum: {species_sum:,.0f}")
        print(f"Difference: {diff_pct:.4f}%")

        assert diff_pct < 0.1, f"Species sum differs from overall by {diff_pct:.4f}%"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
