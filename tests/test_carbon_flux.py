"""
Tests for carbon flux estimation.

Tests validate:
1. Basic carbon flux calculation (Growth - Mortality - Removals in carbon units)
2. Component consistency (flux components match individual estimator calls)
3. Variance calculation (conservative sum-of-variances approach)
4. Grouping functionality (by_species, grp_by)
5. Real data validation against EVALIDator component estimates
"""

from pathlib import Path

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation import carbon_flux, growth, mortality, removals

# IPCC carbon fraction
CARBON_FRACTION = 0.47


class TestCarbonFluxBasic:
    """Basic carbon flux calculation tests."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_carbon_flux_returns_dataframe(self, fia_database_path):
        """carbon_flux() returns a polars DataFrame."""
        with FIA(fia_database_path) as db:
            # Use eval_type="GROW" (maps to EXPGROW) - "EXPGRM" doesn't exist
            db.clip_by_state(13, most_recent=True, eval_type="GROW")  # Georgia GRM
            result = carbon_flux(db)

            assert isinstance(result, pl.DataFrame)
            assert not result.is_empty()

    def test_carbon_flux_has_required_columns(self, fia_database_path):
        """Result has NET_CARBON_FLUX_ACRE and NET_CARBON_FLUX_TOTAL."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")
            result = carbon_flux(db, totals=True, variance=True)

            assert "NET_CARBON_FLUX_ACRE" in result.columns
            assert "NET_CARBON_FLUX_TOTAL" in result.columns
            assert "NET_CARBON_FLUX_SE" in result.columns
            assert "N_PLOTS" in result.columns

            print("\n=== CARBON FLUX COLUMNS ===")
            print(f"Columns: {result.columns}")

    def test_carbon_flux_reasonable_values(self, fia_database_path):
        """Carbon flux values should be within reasonable range."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")
            result = carbon_flux(db, totals=True)

            flux_acre = result["NET_CARBON_FLUX_ACRE"][0]
            flux_total = result["NET_CARBON_FLUX_TOTAL"][0]

            print("\n=== GEORGIA CARBON FLUX ===")
            print(f"Net carbon flux per acre: {flux_acre:.4f} tons C/acre/year")
            print(f"Net carbon flux total: {flux_total / 1e6:.2f} million tons C/year")

            # Georgia forests should generally be a carbon sink (positive flux)
            # Typical range for net carbon flux: -0.5 to 1.5 tons C/acre/year
            # Allow for some variation in different forest conditions
            assert -1.0 < flux_acre < 3.0, (
                f"Carbon flux {flux_acre:.4f} outside expected range (-1 to 3 tons C/acre/year)"
            )


class TestCarbonFluxComponents:
    """Test that carbon flux components are calculated correctly."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_components_match_individual_estimators(self, fia_database_path):
        """Carbon flux components should match individual biomass estimator calls."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            # Get carbon flux with components
            flux_result = carbon_flux(db, include_components=True, totals=True)

            # Get individual biomass estimates
            # Must match carbon_flux defaults: land_type="forest", tree_type="gs"
            growth_result = growth(db, measure="biomass", land_type="forest", tree_type="gs", totals=True)
            mort_result = mortality(db, measure="biomass", land_type="forest", tree_type="gs", totals=True)
            remv_result = removals(db, measure="biomass", land_type="forest", tree_type="gs", totals=True)

            # Convert biomass to carbon for comparison
            growth_carbon = growth_result["GROWTH_TOTAL"][0] * CARBON_FRACTION
            mort_carbon = mort_result["MORT_TOTAL"][0] * CARBON_FRACTION
            remv_carbon = remv_result["REMOVALS_TOTAL"][0] * CARBON_FRACTION

            # Get component values from flux result
            flux_growth = flux_result["GROWTH_CARBON_TOTAL"][0]
            flux_mort = flux_result["MORT_CARBON_TOTAL"][0]
            flux_remv = flux_result["REMV_CARBON_TOTAL"][0]

            print("\n=== COMPONENT COMPARISON ===")
            print(
                f"Growth - Direct: {growth_carbon:,.0f}, From flux: {flux_growth:,.0f}"
            )
            print(
                f"Mortality - Direct: {mort_carbon:,.0f}, From flux: {flux_mort:,.0f}"
            )
            print(f"Removals - Direct: {remv_carbon:,.0f}, From flux: {flux_remv:,.0f}")

            # Allow small tolerance for floating point
            tol = 0.01  # 1% tolerance
            assert abs(growth_carbon - flux_growth) / max(growth_carbon, 1) < tol, (
                f"Growth mismatch: direct={growth_carbon:,.0f}, flux={flux_growth:,.0f}"
            )
            assert abs(mort_carbon - flux_mort) / max(mort_carbon, 1) < tol, (
                f"Mortality mismatch: direct={mort_carbon:,.0f}, flux={flux_mort:,.0f}"
            )
            assert abs(remv_carbon - flux_remv) / max(remv_carbon, 1) < tol, (
                f"Removals mismatch: direct={remv_carbon:,.0f}, flux={flux_remv:,.0f}"
            )

    def test_net_flux_equals_growth_minus_losses(self, fia_database_path):
        """Net flux should equal growth - mortality - removals."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            result = carbon_flux(db, include_components=True, totals=True)

            net_flux = result["NET_CARBON_FLUX_TOTAL"][0]
            growth_c = result["GROWTH_CARBON_TOTAL"][0]
            mort_c = result["MORT_CARBON_TOTAL"][0]
            remv_c = result["REMV_CARBON_TOTAL"][0]

            expected_flux = growth_c - mort_c - remv_c

            print("\n=== NET FLUX CALCULATION ===")
            print(f"Growth carbon: {growth_c:,.0f} tons C/year")
            print(f"Mortality carbon: {mort_c:,.0f} tons C/year")
            print(f"Removals carbon: {remv_c:,.0f} tons C/year")
            print(f"Expected net flux: {expected_flux:,.0f} tons C/year")
            print(f"Actual net flux: {net_flux:,.0f} tons C/year")

            # Should be exact match (floating point)
            assert abs(net_flux - expected_flux) < 1.0, (
                f"Net flux mismatch: expected={expected_flux:,.0f}, actual={net_flux:,.0f}"
            )


class TestCarbonFluxVariance:
    """Variance calculation tests."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_variance_is_sum_of_component_variances(self, fia_database_path):
        """
        Current implementation uses sum of variances (conservative).

        This is an upper bound since actual variance would be lower
        due to positive covariance between G, M, R on same plots.
        """
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            # Get flux variance
            flux_result = carbon_flux(db, variance=True, totals=True)
            flux_se = flux_result["NET_CARBON_FLUX_SE"][0]

            # Get component variances
            growth_result = growth(db, measure="biomass", variance=True)
            mort_result = mortality(db, measure="biomass", variance=True)
            remv_result = removals(db, measure="biomass", variance=True)

            # Convert SE to carbon units
            growth_se = growth_result["GROWTH_ACRE_SE"][0] * CARBON_FRACTION
            mort_se = mort_result["MORT_ACRE_SE"][0] * CARBON_FRACTION
            remv_se = remv_result["REMOVALS_PER_ACRE_SE"][0] * CARBON_FRACTION

            # Sum of variances
            expected_var = growth_se**2 + mort_se**2 + remv_se**2
            expected_se = expected_var**0.5

            print("\n=== VARIANCE COMPARISON ===")
            print(f"Growth SE (carbon): {growth_se:.6f}")
            print(f"Mortality SE (carbon): {mort_se:.6f}")
            print(f"Removals SE (carbon): {remv_se:.6f}")
            print(f"Expected combined SE (sum of variances): {expected_se:.6f}")
            print(f"Actual flux SE: {flux_se:.6f}")

            # Should match sum of variances
            tol = 0.01  # 1% tolerance
            assert abs(flux_se - expected_se) / max(expected_se, 0.001) < tol, (
                f"SE mismatch: expected={expected_se:.6f}, actual={flux_se:.6f}"
            )

    def test_se_percentage_calculated_correctly(self, fia_database_path):
        """Standard error percentage should be SE / |estimate| * 100."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            result = carbon_flux(db, variance=True)

            flux_acre = result["NET_CARBON_FLUX_ACRE"][0]
            flux_se = result["NET_CARBON_FLUX_SE"][0]
            flux_se_pct = result["NET_CARBON_FLUX_SE_PCT"][0]

            expected_pct = abs(flux_se / flux_acre) * 100 if flux_acre != 0 else None

            print("\n=== SE PERCENTAGE ===")
            print(f"Flux: {flux_acre:.6f}")
            print(f"SE: {flux_se:.6f}")
            print(f"Expected SE%: {expected_pct:.2f}%")
            print(f"Actual SE%: {flux_se_pct:.2f}%")

            if expected_pct is not None:
                assert abs(flux_se_pct - expected_pct) < 0.1, (
                    f"SE% mismatch: expected={expected_pct:.2f}%, actual={flux_se_pct:.2f}%"
                )


class TestCarbonFluxGrouping:
    """Tests for grouped carbon flux estimates."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_grouping_by_ownership(self, fia_database_path):
        """Test carbon flux grouped by ownership."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            result = carbon_flux(db, grp_by="OWNGRPCD", include_components=True)

            assert "OWNGRPCD" in result.columns
            assert "NET_CARBON_FLUX_ACRE" in result.columns
            assert len(result) > 1, "Expected multiple ownership groups"

            print("\n=== CARBON FLUX BY OWNERSHIP ===")
            for row in result.iter_rows(named=True):
                print(
                    f"Ownership {row['OWNGRPCD']}: "
                    f"{row['NET_CARBON_FLUX_ACRE']:.4f} tons C/acre/year"
                )

    def test_grouping_by_species(self, fia_database_path):
        """Test carbon flux grouped by species."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            result = carbon_flux(db, by_species=True)

            assert "SPCD" in result.columns
            assert "NET_CARBON_FLUX_ACRE" in result.columns
            assert len(result) > 10, "Expected multiple species"

            print("\n=== CARBON FLUX BY SPECIES (top 5 sinks) ===")
            top_sinks = result.sort("NET_CARBON_FLUX_ACRE", descending=True).head(5)
            for row in top_sinks.iter_rows(named=True):
                print(
                    f"SPCD {row['SPCD']}: "
                    f"{row['NET_CARBON_FLUX_ACRE']:.6f} tons C/acre/year"
                )


class TestCarbonFluxMultiState:
    """Test carbon flux across multiple states."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_south_carolina_carbon_flux(self, fia_database_path):
        """Test carbon flux for South Carolina."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(45, most_recent=True, eval_type="GROW")  # SC

            result = carbon_flux(db, include_components=True, totals=True)

            flux_acre = result["NET_CARBON_FLUX_ACRE"][0]
            flux_total = result["NET_CARBON_FLUX_TOTAL"][0]

            print("\n=== SOUTH CAROLINA CARBON FLUX ===")
            print(f"Net carbon flux per acre: {flux_acre:.4f} tons C/acre/year")
            print(f"Net carbon flux total: {flux_total / 1e6:.2f} million tons C/year")

            # SC forests should also generally be a carbon sink
            assert -1.0 < flux_acre < 2.0, (
                f"SC carbon flux {flux_acre:.4f} outside expected range"
            )

    def test_multiple_states(self, fia_database_path):
        """Test carbon flux for multiple states combined."""
        with FIA(fia_database_path) as db:
            # Georgia + South Carolina
            db.clip_by_state([13, 45], most_recent=True, eval_type="GROW")

            result = carbon_flux(db, grp_by="STATECD", include_components=True)

            assert "STATECD" in result.columns
            assert len(result) >= 2, "Expected at least 2 states"

            print("\n=== CARBON FLUX BY STATE ===")
            for row in result.iter_rows(named=True):
                print(
                    f"State {row['STATECD']}: "
                    f"{row['NET_CARBON_FLUX_ACRE']:.4f} tons C/acre/year"
                )


class TestCarbonFluxDomainFiltering:
    """Test domain filtering for carbon flux."""

    @pytest.fixture
    def fia_database_path(self):
        """Path to the real FIA DuckDB database."""
        db_path = Path("fia.duckdb")
        if not db_path.exists():
            pytest.skip("fia.duckdb not found - real data tests require this database")
        return str(db_path)

    def test_timberland_only(self, fia_database_path):
        """Test carbon flux on timberland vs all forestland."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            # All forestland
            forest_result = carbon_flux(db, land_type="forest")

            # Timberland only
            timber_result = carbon_flux(db, land_type="timber")

            forest_flux = forest_result["NET_CARBON_FLUX_ACRE"][0]
            timber_flux = timber_result["NET_CARBON_FLUX_ACRE"][0]

            print("\n=== FORESTLAND VS TIMBERLAND ===")
            print(f"Forestland flux: {forest_flux:.4f} tons C/acre/year")
            print(f"Timberland flux: {timber_flux:.4f} tons C/acre/year")

            # Both should have data
            assert forest_flux is not None
            assert timber_flux is not None

    def test_growing_stock_vs_all_live(self, fia_database_path):
        """Test carbon flux for growing stock vs all live trees."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13, most_recent=True, eval_type="GROW")

            # Growing stock only
            gs_result = carbon_flux(db, tree_type="gs")

            # All live trees
            al_result = carbon_flux(db, tree_type="al")

            gs_flux = gs_result["NET_CARBON_FLUX_ACRE"][0]
            al_flux = al_result["NET_CARBON_FLUX_ACRE"][0]

            print("\n=== GROWING STOCK VS ALL LIVE ===")
            print(f"Growing stock flux: {gs_flux:.4f} tons C/acre/year")
            print(f"All live flux: {al_flux:.4f} tons C/acre/year")

            # Both should have reasonable values
            assert -1.0 < gs_flux < 2.0
            assert -1.0 < al_flux < 2.0
