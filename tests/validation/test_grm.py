"""Growth, Removals, Mortality (GRM) validation against EVALIDator."""

from pyfia import FIA, growth, mortality, removals
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID_GRM,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    DATA_SYNC_TOLERANCE,
    SE_TOLERANCE_GRM,
    values_match,
    se_values_match,
    plot_counts_match,
    extract_grm_estimate,
)


class TestGRMValidation:
    """Validate Growth, Removals, Mortality estimates against EVALIDator."""

    def test_growth_volume(self, fia_db, evalidator_client):
        """Validate annual net growth volume matches EVALIDator (snum=202).

        Uses EVALID 132303 (GRM evaluation with EXPGROW type).

        Note: Growth may have small data sync difference (~0.5-1%) due to
        differences between our DuckDB export and EVALIDator's Oracle database.

        Note: Plot count may differ slightly because pyFIA includes all plots
        with trees that go through the estimation process, even if they
        contribute zero net growth (e.g., DIVERSION1 trees where beginning
        volume = ending volume). EVALIDator excludes these zero-contribution
        plots from its count. The estimates match exactly regardless.
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = growth(db, land_type="forest", tree_type="gs", totals=True)
            pyfia_growth, pyfia_se, pyfia_plot_count = extract_grm_estimate(result, "growth")

        ev_result = evalidator_client.get_growth(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_growth,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nAnnual Growth Volume Validation:")
        print(f"  pyFIA:      {pyfia_growth:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft/year (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        if pyfia_plot_count is not None:
            print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_growth, ev_result.estimate, rel_tol=DATA_SYNC_TOLERANCE), (
            f"Growth should match EVALIDator within data sync tolerance.\n"
            f"pyFIA: {pyfia_growth} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_GRM), (
            f"Growth SE should match EVALIDator within {SE_TOLERANCE_GRM*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        # Note: Growth plot counts may differ by ~10 plots due to zero-contribution
        # plots (DIVERSION1 trees with equal beginning/ending volumes). These plots
        # don't affect the estimate but are counted differently by pyFIA vs EVALIDator.
        if pyfia_plot_count is not None:
            plot_diff = abs(pyfia_plot_count - ev_result.plot_count)
            if plot_diff > 0:
                print(f"  Note: {plot_diff} zero-contribution plots counted differently")
            # Allow up to 10 plots difference for growth (known zero-contribution plots)
            assert plot_diff <= 10, (
                f"Plot count difference exceeds expected threshold.\n"
                f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}\n"
                f"Difference: {plot_diff} (max allowed: 10 for zero-contribution plots)"
            )

    def test_mortality_volume(self, fia_db, evalidator_client):
        """Validate annual mortality volume matches EVALIDator (snum=214).

        Uses EVALID 132303 (GRM evaluation with EXPMORT type).
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = mortality(db, land_type="forest", tree_type="gs", totals=True)
            pyfia_mort, pyfia_se, pyfia_plot_count = extract_grm_estimate(result, "mortality")

        ev_result = evalidator_client.get_mortality(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_mort,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nAnnual Mortality Volume Validation:")
        print(f"  pyFIA:      {pyfia_mort:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft/year (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        if pyfia_plot_count is not None:
            print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_mort, ev_result.estimate), (
            f"Mortality MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_mort} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_GRM), (
            f"Mortality SE should match EVALIDator within {SE_TOLERANCE_GRM*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        if pyfia_plot_count is not None:
            assert plot_counts_match(pyfia_plot_count, ev_result.plot_count), (
                f"Plot counts should match exactly.\n"
                f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}"
            )

    def test_removals_volume(self, fia_db, evalidator_client):
        """Validate annual removals volume matches EVALIDator (snum=226).

        Uses EVALID 132303 (GRM evaluation with EXPREMV type).
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = removals(db, land_type="forest", tree_type="gs", totals=True)
            pyfia_rem, pyfia_se, pyfia_plot_count = extract_grm_estimate(result, "removals")

        ev_result = evalidator_client.get_removals(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_rem,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nAnnual Removals Volume Validation:")
        print(f"  pyFIA:      {pyfia_rem:,.0f} cu ft/year (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft/year (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        if pyfia_plot_count is not None:
            print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_rem, ev_result.estimate), (
            f"Removals MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_rem} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_GRM), (
            f"Removals SE should match EVALIDator within {SE_TOLERANCE_GRM*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        if pyfia_plot_count is not None:
            assert plot_counts_match(pyfia_plot_count, ev_result.plot_count), (
                f"Plot counts should match exactly.\n"
                f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}"
            )
