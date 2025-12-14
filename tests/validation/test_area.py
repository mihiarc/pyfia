"""Area estimation validation against EVALIDator."""

import pytest

from pyfia import FIA, area
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    values_match,
    se_values_match,
)


class TestAreaValidation:
    """Validate area estimates against EVALIDator."""

    def test_forest_area(self, fia_db, evalidator_client):
        """Validate total forest area matches EVALIDator (snum=2)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = area(db, land_type="forest", totals=True)
            pyfia_area = result["AREA"][0]
            pyfia_se = result["AREA_SE"][0]

        ev_result = evalidator_client.get_forest_area(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_area,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nForest Area Validation:")
        print(f"  pyFIA:      {pyfia_area:,.0f} acres (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} acres (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_area, ev_result.estimate), (
            f"Forest area MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_area} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error), (
            f"Forest area SE should match EVALIDator.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

    def test_timberland_area(self, fia_db, evalidator_client):
        """Validate timberland area matches EVALIDator (snum=3)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = area(db, land_type="timber", totals=True)
            pyfia_area = result["AREA"][0]
            pyfia_se = result["AREA_SE"][0]

        ev_result = evalidator_client.get_forest_area(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            land_type="timber"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_area,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nTimberland Area Validation:")
        print(f"  pyFIA:      {pyfia_area:,.0f} acres (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} acres (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_area, ev_result.estimate), (
            f"Timberland area MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_area} vs EVALIDator: {ev_result.estimate}"
        )
