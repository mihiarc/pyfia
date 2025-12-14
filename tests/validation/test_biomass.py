"""Biomass estimation validation against EVALIDator."""

import pytest

from pyfia import FIA, biomass
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    values_match,
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

        ev_result = evalidator_client.get_biomass(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR
        )

        validation = compare_estimates(
            pyfia_value=pyfia_bio,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nAboveground Biomass Validation:")
        print(f"  pyFIA:      {pyfia_bio:,.0f} dry tons (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} dry tons (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_bio, ev_result.estimate), (
            f"Biomass MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_bio} vs EVALIDator: {ev_result.estimate}"
        )

    def test_belowground_biomass(self, fia_db, evalidator_client):
        """Validate belowground biomass for all live trees (snum=59)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = biomass(db, land_type="forest", tree_type="live", component="bg", totals=True)
            pyfia_bio = result["BIO_TOTAL"][0]
            pyfia_se = result["BIO_TOTAL_SE"][0]

        ev_result = evalidator_client.get_biomass(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            component="bg"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_bio,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nBelowground Biomass Validation:")
        print(f"  pyFIA:      {pyfia_bio:,.0f} dry tons (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} dry tons (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_bio, ev_result.estimate), (
            f"Belowground biomass MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_bio} vs EVALIDator: {ev_result.estimate}"
        )

    @pytest.mark.skip(reason="snum=13 returns trees >=1\" not >=5\" - need correct snum")
    def test_biomass_5inch_trees(self, fia_db, evalidator_client):
        """Validate aboveground biomass for trees >=5" DBH.

        NOTE: Skipped - EVALIDator snum=13 is for trees >=1" DBH, not >=5".
        Need to verify correct snum or use strFilter parameter.
        """
        pass
