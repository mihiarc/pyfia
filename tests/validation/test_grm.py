"""Growth, Removals, Mortality (GRM) validation against EVALIDator."""

from pyfia import FIA, growth, mortality, removals
from pyfia.evalidator import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID_GRM,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    DATA_SYNC_TOLERANCE,
    values_match,
    extract_grm_estimate,
)


class TestGRMValidation:
    """Validate Growth, Removals, Mortality estimates against EVALIDator."""

    def test_growth_volume(self, fia_db, evalidator_client):
        """Validate annual net growth volume matches EVALIDator (snum=202).

        Uses EVALID 132303 (GRM evaluation with EXPGROW type).

        Note: Growth may have small data sync difference (~0.5-1%) due to
        differences between our DuckDB export and EVALIDator's Oracle database.
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = growth(db, land_type="forest", tree_type="gs", totals=True)
            pyfia_growth, pyfia_se = extract_grm_estimate(result, "growth")

        ev_result = evalidator_client.get_growth(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_growth,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nAnnual Growth Volume Validation:")
        print(f"  pyFIA:      {pyfia_growth:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft/year (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_growth, ev_result.estimate, rel_tol=DATA_SYNC_TOLERANCE), (
            f"Growth should match EVALIDator within data sync tolerance.\n"
            f"pyFIA: {pyfia_growth} vs EVALIDator: {ev_result.estimate}"
        )

    def test_mortality_volume(self, fia_db, evalidator_client):
        """Validate annual mortality volume matches EVALIDator (snum=214).

        Uses EVALID 132303 (GRM evaluation with EXPMORT type).
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = mortality(db, land_type="forest", tree_type="gs", totals=True)
            pyfia_mort, pyfia_se = extract_grm_estimate(result, "mortality")

        ev_result = evalidator_client.get_mortality(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_mort,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nAnnual Mortality Volume Validation:")
        print(f"  pyFIA:      {pyfia_mort:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft/year (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_mort, ev_result.estimate), (
            f"Mortality MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_mort} vs EVALIDator: {ev_result.estimate}"
        )

    def test_removals_volume(self, fia_db, evalidator_client):
        """Validate annual removals volume matches EVALIDator (snum=226).

        Uses EVALID 132303 (GRM evaluation with EXPREMV type).
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = removals(db, land_type="forest", tree_type="gs", totals=True)
            pyfia_rem, pyfia_se = extract_grm_estimate(result, "removals")

        ev_result = evalidator_client.get_removals(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_rem,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nAnnual Removals Volume Validation:")
        print(f"  pyFIA:      {pyfia_rem:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft/year (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_rem, ev_result.estimate), (
            f"Removals MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_rem} vs EVALIDator: {ev_result.estimate}"
        )
