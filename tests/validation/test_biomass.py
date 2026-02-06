"""Biomass estimation validation against EVALIDator."""

import pytest

from pyfia import FIA, biomass
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


class TestBiomassValidation:
    """Validate biomass estimates against EVALIDator."""

    def test_aboveground_biomass(self, fia_db, evalidator_client):
        """Validate aboveground biomass for all live trees (snum=10)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = biomass(db, land_type="forest", tree_type="live", totals=True)
            pyfia_bio = result["BIO_TOTAL"][0]
            pyfia_se = result["BIO_TOTAL_SE"][0]
            pyfia_plot_count = int(result["N_PLOTS"][0])

        ev_result = evalidator_client.get_biomass(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_bio,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nAboveground Biomass Validation:")
        print(f"  pyFIA:      {pyfia_bio:,.0f} dry tons (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} dry tons (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_bio, ev_result.estimate), (
            f"Biomass MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_bio} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_TREE), (
            f"Aboveground biomass SE should match EVALIDator within {SE_TOLERANCE_TREE*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        assert plot_counts_match(pyfia_plot_count, ev_result.plot_count), (
            f"Plot counts should match exactly.\n"
            f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}"
        )

    def test_belowground_biomass(self, fia_db, evalidator_client):
        """Validate belowground biomass for all live trees (snum=59)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = biomass(db, land_type="forest", tree_type="live", component="bg", totals=True)
            pyfia_bio = result["BIO_TOTAL"][0]
            pyfia_se = result["BIO_TOTAL_SE"][0]
            pyfia_plot_count = int(result["N_PLOTS"][0])

        ev_result = evalidator_client.get_biomass(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            component="bg"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_bio,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT,
            pyfia_plot_count=pyfia_plot_count
        )

        print(f"\nBelowground Biomass Validation:")
        print(f"  pyFIA:      {pyfia_bio:,.0f} dry tons (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} dry tons (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")
        print(f"  Plot count: pyFIA={pyfia_plot_count}, EVALIDator={ev_result.plot_count}")

        assert values_match(pyfia_bio, ev_result.estimate), (
            f"Belowground biomass MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_bio} vs EVALIDator: {ev_result.estimate}"
        )

        assert se_values_match(pyfia_se, ev_result.sampling_error, rel_tol=SE_TOLERANCE_TREE), (
            f"Belowground biomass SE should match EVALIDator within {SE_TOLERANCE_TREE*100:.0f}% tolerance.\n"
            f"pyFIA SE: {pyfia_se:,.0f} vs EVALIDator SE: {ev_result.sampling_error:,.0f}"
        )

        assert plot_counts_match(pyfia_plot_count, ev_result.plot_count), (
            f"Plot counts should match exactly.\n"
            f"pyFIA: {pyfia_plot_count} vs EVALIDator: {ev_result.plot_count}"
        )

    @pytest.mark.skip(
        reason="No direct EVALIDator snum for 'live tree biomass >=5\" DBH'. "
               "EVALIDator only offers growing-stock (snum=96 is dead trees, "
               "snum=312 is growth). Would need strFilter parameter to validate."
    )
    def test_biomass_5inch_trees(self, fia_db, evalidator_client):
        """Validate aboveground biomass for trees >=5" DBH.

        NOTE: Skipped - EVALIDator doesn't have a direct estimate for
        "aboveground biomass of live trees >= 5 inches DBH". Options:
        - snum=10: Live trees >= 1" DBH (too inclusive)
        - snum=96: Dead trees >= 5" DBH (wrong tree type)
        - snum=312: Growing-stock growth >= 5" DBH (wrong metric)

        To validate this, would need to use EVALIDator's strFilter parameter
        to filter snum=10 to DIA >= 5.
        """
        pass
