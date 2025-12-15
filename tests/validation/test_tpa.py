"""Tree count (TPA) estimation validation against EVALIDator."""

from pyfia import FIA, tpa
from pyfia.evalidator.estimate_types import EstimateType
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    SE_TOLERANCE_TREE,
    values_match,
    se_values_match,
    plot_counts_match,
)


class TestTPAValidation:
    """Validate tree count estimates against EVALIDator."""

    def test_total_tree_count(self, fia_db, evalidator_client):
        """Validate total live tree count matches EVALIDator (snum=4)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = tpa(db, land_type="forest", tree_type="live", totals=True)
            pyfia_count = result["TPA_TOTAL"][0]
            pyfia_se = result["TPA_TOTAL_SE"][0]
            pyfia_plot_count = int(result["N_PLOTS"][0])

        ev_result = evalidator_client.get_tree_count(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_count,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nTotal Tree Count Validation:")
        print(f"  pyFIA:      {pyfia_count:,.0f} trees (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} trees (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_count, ev_result.estimate), (
            f"Tree count MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_count} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_TREE), (
            f"Total tree count SE should match EVALIDator within {SE_TOLERANCE_TREE*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        assert plot_counts_match(pyfia_plot_count, ev_result.plot_count), (
            f"Plot counts should match exactly.\n"
            f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}"
        )

    def test_growing_stock_tree_count(self, fia_db, evalidator_client):
        """Validate growing stock tree count >=5" DBH matches EVALIDator (snum=5)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = tpa(
                db,
                land_type="forest",
                tree_type="gs",
                tree_domain="DIA >= 5.0",
                totals=True
            )
            pyfia_count = result["TPA_TOTAL"][0]
            pyfia_se = result["TPA_TOTAL_SE"][0]
            pyfia_plot_count = int(result["N_PLOTS"][0])

        ev_result = evalidator_client.get_custom_estimate(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            snum=EstimateType.TREE_COUNT_5INCH_FOREST,
            row_var="NONE",
            col_var="NONE",
            units="trees",
            estimate_type="tree_count_gs"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_count,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nGrowing Stock Tree Count Validation:")
        print(f"  pyFIA:      {pyfia_count:,.0f} trees (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} trees (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_count, ev_result.estimate), (
            f"Growing stock tree count MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_count} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_TREE), (
            f"Growing stock tree count SE should match EVALIDator within {SE_TOLERANCE_TREE*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        assert plot_counts_match(pyfia_plot_count, ev_result.plot_count), (
            f"Plot counts should match exactly.\n"
            f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}"
        )
