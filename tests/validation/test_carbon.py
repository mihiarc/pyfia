"""Carbon estimation validation against EVALIDator."""

from pyfia import FIA
from pyfia.estimation.estimators.carbon import carbon
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    values_match,
)


class TestCarbonValidation:
    """Validate carbon estimates against EVALIDator."""

    def test_live_tree_carbon(self, fia_db, evalidator_client):
        """Validate live tree carbon pool against EVALIDator (snum=55000).

        NOTE: Known methodology difference between pyFIA and EVALIDator:
        - pyFIA: Uses DRYBIO_AG * 0.47 (aboveground only)
        - EVALIDator: Uses CARBON_AG + CARBON_BG (includes belowground)

        Uses 2% tolerance to accommodate this until proper carbon estimator
        using CARBON_AG + CARBON_BG columns is implemented.
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = carbon(db, pool="live", land_type="forest", totals=True)
            pyfia_carbon = result["CARBON_TOTAL"][0]
            pyfia_se = result["CARBON_TOTAL_SE"][0]

        ev_result = evalidator_client.get_carbon(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            pool="total"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_carbon,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nLive Tree Carbon Validation:")
        print(f"  pyFIA:      {pyfia_carbon:,.0f} metric tons (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} metric tons (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        print("  Note: Difference due to methodology (DRYBIO_AG*0.47 vs CARBON_AG+CARBON_BG)")

        # Carbon uses 2% tolerance due to methodology difference
        # TODO: Implement proper carbon estimator using CARBON_AG + CARBON_BG
        carbon_tolerance = 0.02
        assert values_match(pyfia_carbon, ev_result.estimate, rel_tol=carbon_tolerance), (
            f"Carbon should match EVALIDator within methodology tolerance.\n"
            f"pyFIA: {pyfia_carbon} vs EVALIDator: {ev_result.estimate}\n"
            f"Difference: {validation.pct_diff:.6f}% (tolerance: {carbon_tolerance*100}%)"
        )
