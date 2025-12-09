"""
Tests for validating GRM (Growth, Removals, Mortality) estimates against EVALIDator.

These tests compare pyFIA GRM estimates with official USFS EVALIDator values
to verify the accuracy of growth, mortality, and removals calculations.
"""

from pathlib import Path

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation import growth, mortality, removals
from pyfia.evalidator import EVALIDatorClient, compare_estimates


@pytest.fixture
def fia_database_path():
    """Path to the real FIA DuckDB database."""
    db_path = Path("fia.duckdb")
    if not db_path.exists():
        pytest.skip("fia.duckdb not found - GRM validation requires this database")
    return str(db_path)


@pytest.fixture
def evalidator_client():
    """EVALIDator API client."""
    return EVALIDatorClient(timeout=60)


class TestGrowthValidation:
    """Validate growth estimates against EVALIDator."""

    def test_growth_volume_georgia(self, fia_database_path, evalidator_client):
        """
        Validate Georgia net volume growth against EVALIDator snum=202.

        EVALIDator snum 202: Average annual net growth of merchantable bole
        wood volume of growing-stock trees (at least 5 inches d.b.h.) on
        forest land, in cubic feet.
        """
        state_code = 13  # Georgia
        year = 2023

        # Get EVALIDator estimate
        evalidator_result = evalidator_client.get_growth(
            state_code=state_code,
            year=year,
            measure="volume"
        )

        # Get pyFIA estimate
        # Note: Use eval_type="GROW" for growth (maps to EXPGROW in POP_EVAL_TYP)
        # "EXPGRM" doesn't exist - FIA splits GRM into EXPGROW, EXPMORT, EXPREMV
        with FIA(fia_database_path) as db:
            db.clip_by_state(state_code, most_recent=True, eval_type="GROW")
            result = growth(
                db,
                measure="volume",
                land_type="forest",
                tree_type="gs",  # Growing stock
                totals=True,
                variance=True,
            )

        pyfia_value = result["GROWTH_TOTAL"][0]
        pyfia_se = result["GROWTH_TOTAL_SE"][0] if "GROWTH_TOTAL_SE" in result.columns else 0.0

        # Compare with 20% tolerance (GRM estimates have higher variance than inventory)
        validation = compare_estimates(
            pyfia_value=pyfia_value,
            pyfia_se=pyfia_se,
            evalidator_result=evalidator_result,
            tolerance_pct=20.0
        )

        print(f"\n=== GEORGIA NET VOLUME GROWTH ===")
        print(f"pyFIA:      {pyfia_value:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"EVALIDator: {evalidator_result.estimate:,.0f} cu ft/year (SE: {evalidator_result.sampling_error:,.0f})")
        print(f"Difference: {validation.pct_diff:.2f}%")
        print(f"Status: {'PASS' if validation.passed else 'FAIL'}")

        assert validation.passed, f"Growth volume validation failed: {validation.message}"

    def test_growth_biomass_georgia(self, fia_database_path, evalidator_client):
        """
        Validate Georgia net biomass growth against EVALIDator snum=311.

        EVALIDator snum 311: Average annual net growth of aboveground biomass
        of trees (at least 1 inch d.b.h.) on forest land, in dry short tons.
        """
        state_code = 13  # Georgia
        year = 2023

        # Get EVALIDator estimate
        evalidator_result = evalidator_client.get_growth(
            state_code=state_code,
            year=year,
            measure="biomass"
        )

        # Get pyFIA estimate
        # Use eval_type="GROW" for growth estimation (maps to EXPGROW)
        with FIA(fia_database_path) as db:
            db.clip_by_state(state_code, most_recent=True, eval_type="GROW")
            result = growth(
                db,
                measure="biomass",
                land_type="forest",
                tree_type="live",  # All live trees (snum 311 includes all trees >=1" DBH)
                totals=True,
                variance=True,
            )

        pyfia_value = result["GROWTH_TOTAL"][0]
        pyfia_se = result["GROWTH_TOTAL_SE"][0] if "GROWTH_TOTAL_SE" in result.columns else 0.0

        # Compare with 20% tolerance (GRM estimates have higher variance)
        validation = compare_estimates(
            pyfia_value=pyfia_value,
            pyfia_se=pyfia_se,
            evalidator_result=evalidator_result,
            tolerance_pct=20.0
        )

        print(f"\n=== GEORGIA NET BIOMASS GROWTH ===")
        print(f"pyFIA:      {pyfia_value:,.0f} dry tons/year (SE: {pyfia_se:,.0f})")
        print(f"EVALIDator: {evalidator_result.estimate:,.0f} dry tons/year (SE: {evalidator_result.sampling_error:,.0f})")
        print(f"Difference: {validation.pct_diff:.2f}%")
        print(f"Status: {'PASS' if validation.passed else 'FAIL'}")

        assert validation.passed, f"Growth biomass validation failed: {validation.message}"


class TestMortalityValidation:
    """Validate mortality estimates against EVALIDator."""

    def test_mortality_volume_georgia(self, fia_database_path, evalidator_client):
        """
        Validate Georgia mortality volume against EVALIDator snum=214.

        EVALIDator snum 214: Average annual mortality of merchantable bole
        wood volume of growing-stock trees (at least 5 inches d.b.h.) on
        forest land, in cubic feet.
        """
        state_code = 13  # Georgia
        year = 2023

        # Get EVALIDator estimate
        evalidator_result = evalidator_client.get_mortality(
            state_code=state_code,
            year=year,
            measure="volume"
        )

        # Get pyFIA estimate
        # Use eval_type="MORT" for mortality estimation (maps to EXPMORT)
        with FIA(fia_database_path) as db:
            db.clip_by_state(state_code, most_recent=True, eval_type="MORT")
            result = mortality(
                db,
                measure="volume",
                land_type="forest",
                tree_type="gs",  # Growing stock
                totals=True,
                variance=True,
            )

        pyfia_value = result["MORT_TOTAL"][0]
        pyfia_se = result["MORT_TOTAL_SE"][0] if "MORT_TOTAL_SE" in result.columns else 0.0

        # Compare with 20% tolerance (GRM estimates have higher variance)
        validation = compare_estimates(
            pyfia_value=pyfia_value,
            pyfia_se=pyfia_se,
            evalidator_result=evalidator_result,
            tolerance_pct=20.0
        )

        print(f"\n=== GEORGIA MORTALITY VOLUME ===")
        print(f"pyFIA:      {pyfia_value:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"EVALIDator: {evalidator_result.estimate:,.0f} cu ft/year (SE: {evalidator_result.sampling_error:,.0f})")
        print(f"Difference: {validation.pct_diff:.2f}%")
        print(f"Status: {'PASS' if validation.passed else 'FAIL'}")

        assert validation.passed, f"Mortality volume validation failed: {validation.message}"


class TestRemovalsValidation:
    """Validate removals estimates against EVALIDator."""

    def test_removals_volume_georgia(self, fia_database_path, evalidator_client):
        """
        Validate Georgia removals volume against EVALIDator snum=226.

        EVALIDator snum 226: Average annual removals of merchantable bole
        wood volume of growing-stock trees (at least 5 inches d.b.h.) on
        forest land, in cubic feet.
        """
        state_code = 13  # Georgia
        year = 2023

        # Get EVALIDator estimate
        evalidator_result = evalidator_client.get_removals(
            state_code=state_code,
            year=year,
            measure="volume"
        )

        # Get pyFIA estimate
        # Use eval_type="REMV" for removals estimation (maps to EXPREMV)
        with FIA(fia_database_path) as db:
            db.clip_by_state(state_code, most_recent=True, eval_type="REMV")
            result = removals(
                db,
                measure="volume",
                land_type="forest",
                tree_type="gs",  # Growing stock
                totals=True,
                variance=True,
            )

        pyfia_value = result["REMOVALS_TOTAL"][0]
        pyfia_se = result["REMOVALS_TOTAL_SE"][0] if "REMOVALS_TOTAL_SE" in result.columns else 0.0

        # Compare with 20% tolerance (GRM estimates have higher variance)
        validation = compare_estimates(
            pyfia_value=pyfia_value,
            pyfia_se=pyfia_se,
            evalidator_result=evalidator_result,
            tolerance_pct=20.0
        )

        print(f"\n=== GEORGIA REMOVALS VOLUME ===")
        print(f"pyFIA:      {pyfia_value:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"EVALIDator: {evalidator_result.estimate:,.0f} cu ft/year (SE: {evalidator_result.sampling_error:,.0f})")
        print(f"Difference: {validation.pct_diff:.2f}%")
        print(f"Status: {'PASS' if validation.passed else 'FAIL'}")

        assert validation.passed, f"Removals volume validation failed: {validation.message}"


class TestCarbonFluxComponents:
    """Test that carbon flux component estimates are reasonable relative to EVALIDator."""

    def test_carbon_flux_direction(self, fia_database_path, evalidator_client):
        """
        Verify carbon flux sign matches EVALIDator component relationships.

        If EVALIDator growth > mortality + removals, flux should be positive (sink).
        If EVALIDator growth < mortality + removals, flux should be negative (source).
        """
        from pyfia.estimation import carbon_flux

        state_code = 13  # Georgia
        year = 2023

        # Get EVALIDator component estimates (biomass)
        ev_growth = evalidator_client.get_growth(state_code, year, measure="biomass")
        ev_mortality = evalidator_client.get_mortality(state_code, year, measure="biomass")
        ev_removals = evalidator_client.get_removals(state_code, year, measure="biomass")

        # Expected flux direction from EVALIDator
        ev_net = ev_growth.estimate - ev_mortality.estimate - ev_removals.estimate
        expected_sink = ev_net > 0

        print(f"\n=== EVALIDATOR COMPONENTS (Biomass) ===")
        print(f"Growth:    {ev_growth.estimate:,.0f} tons/year")
        print(f"Mortality: {ev_mortality.estimate:,.0f} tons/year")
        print(f"Removals:  {ev_removals.estimate:,.0f} tons/year")
        print(f"Net:       {ev_net:,.0f} tons/year ({'SINK' if expected_sink else 'SOURCE'})")

        # Get pyFIA carbon flux
        # Use eval_type="GROW" for GRM data (EXPGRM doesn't exist, use EXPGROW)
        # Carbon flux uses growth/mortality/removals from same remeasured plots
        with FIA(fia_database_path) as db:
            db.clip_by_state(state_code, most_recent=True, eval_type="GROW")
            result = carbon_flux(
                db,
                land_type="forest",
                tree_type="gs",
                include_components=True,
            )

        pyfia_net = result["NET_CARBON_FLUX_TOTAL"][0]
        pyfia_is_sink = pyfia_net > 0

        print(f"\n=== PYFIA CARBON FLUX ===")
        print(f"Net flux:  {pyfia_net:,.0f} tons C/year ({'SINK' if pyfia_is_sink else 'SOURCE'})")

        # Verify flux direction matches
        assert pyfia_is_sink == expected_sink, (
            f"Carbon flux direction mismatch: pyFIA says "
            f"{'SINK' if pyfia_is_sink else 'SOURCE'} but EVALIDator suggests "
            f"{'SINK' if expected_sink else 'SOURCE'}"
        )
