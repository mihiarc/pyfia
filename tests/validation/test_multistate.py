"""Multi-state validation tests.

These tests run against all configured states (Georgia, Oregon, etc.)
to ensure pyFIA produces correct estimates across different regions.

All estimates (area, volume, biomass, TPA) MUST match EVALIDator exactly.
"""

from pathlib import Path

from pyfia import FIA, area, volume, biomass, tpa
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    StateConfig,
    EXACT_MATCH_TOLERANCE_PCT,
    values_match,
)


class TestMultiStateArea:
    """Validate area estimates across multiple states."""

    def test_forest_area(self, state_config: StateConfig, evalidator_client):
        """Validate total forest area matches EVALIDator for each state."""
        db_path = Path.cwd() / state_config.db_path

        with FIA(str(db_path)) as db:
            db.clip_by_evalid(state_config.evalid)
            result = area(db, land_type="forest", totals=True)
            pyfia_area = result["AREA"][0]
            pyfia_se = result["AREA_SE"][0]

        ev_result = evalidator_client.get_forest_area(
            state_code=state_config.state_code,
            year=state_config.year
        )

        validation = compare_estimates(
            pyfia_value=pyfia_area,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\n{state_config.name} Forest Area Validation ({state_config.year}):")
        print(f"  pyFIA:      {pyfia_area:,.0f} acres (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} acres (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_area, ev_result.estimate), (
            f"{state_config.name} forest area MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_area} vs EVALIDator: {ev_result.estimate}"
        )

    def test_timberland_area(self, state_config: StateConfig, evalidator_client):
        """Validate timberland area matches EVALIDator for each state."""
        db_path = Path.cwd() / state_config.db_path

        with FIA(str(db_path)) as db:
            db.clip_by_evalid(state_config.evalid)
            result = area(db, land_type="timber", totals=True)
            pyfia_area = result["AREA"][0]

        ev_result = evalidator_client.get_forest_area(
            state_code=state_config.state_code,
            year=state_config.year,
            land_type="timber"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_area,
            pyfia_se=0,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\n{state_config.name} Timberland Area Validation ({state_config.year}):")
        print(f"  pyFIA:      {pyfia_area:,.0f} acres")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} acres")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_area, ev_result.estimate), (
            f"{state_config.name} timberland area MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_area} vs EVALIDator: {ev_result.estimate}"
        )


class TestMultiStateVolume:
    """Validate volume estimates across multiple states."""

    def test_growing_stock_volume(self, state_config: StateConfig, evalidator_client):
        """Validate growing stock volume matches EVALIDator for each state."""
        db_path = Path.cwd() / state_config.db_path

        with FIA(str(db_path)) as db:
            db.clip_by_evalid(state_config.evalid)
            result = volume(db, land_type="forest", vol_type="net", tree_type="gs", totals=True)
            pyfia_vol = result["VOLCFNET_TOTAL"][0]

        ev_result = evalidator_client.get_volume(
            state_code=state_config.state_code,
            year=state_config.year,
            vol_type="net"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_vol,
            pyfia_se=0,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\n{state_config.name} Growing Stock Volume Validation ({state_config.year}):")
        print(f"  pyFIA:      {pyfia_vol:,.0f} cu ft")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_vol, ev_result.estimate), (
            f"{state_config.name} volume MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_vol} vs EVALIDator: {ev_result.estimate}"
        )


class TestMultiStateBiomass:
    """Validate biomass estimates across multiple states."""

    def test_aboveground_biomass(self, state_config: StateConfig, evalidator_client):
        """Validate aboveground biomass matches EVALIDator for each state."""
        db_path = Path.cwd() / state_config.db_path

        with FIA(str(db_path)) as db:
            db.clip_by_evalid(state_config.evalid)
            result = biomass(db, land_type="forest", tree_type="live", totals=True)
            pyfia_bio = result["BIO_TOTAL"][0]

        ev_result = evalidator_client.get_biomass(
            state_code=state_config.state_code,
            year=state_config.year
        )

        validation = compare_estimates(
            pyfia_value=pyfia_bio,
            pyfia_se=0,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\n{state_config.name} Aboveground Biomass Validation ({state_config.year}):")
        print(f"  pyFIA:      {pyfia_bio:,.0f} dry tons")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} dry tons")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_bio, ev_result.estimate), (
            f"{state_config.name} biomass MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_bio} vs EVALIDator: {ev_result.estimate}"
        )


class TestMultiStateTPA:
    """Validate tree count estimates across multiple states."""

    def test_total_tree_count(self, state_config: StateConfig, evalidator_client):
        """Validate total tree count matches EVALIDator for each state."""
        db_path = Path.cwd() / state_config.db_path

        with FIA(str(db_path)) as db:
            db.clip_by_evalid(state_config.evalid)
            result = tpa(db, land_type="forest", tree_type="live", totals=True)
            pyfia_count = result["TPA_TOTAL"][0]

        ev_result = evalidator_client.get_tree_count(
            state_code=state_config.state_code,
            year=state_config.year
        )

        validation = compare_estimates(
            pyfia_value=pyfia_count,
            pyfia_se=0,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\n{state_config.name} Tree Count Validation ({state_config.year}):")
        print(f"  pyFIA:      {pyfia_count:,.0f} trees")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} trees")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_count, ev_result.estimate), (
            f"{state_config.name} tree count MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_count} vs EVALIDator: {ev_result.estimate}"
        )
