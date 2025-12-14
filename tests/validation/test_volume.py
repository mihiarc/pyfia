"""Volume estimation validation against EVALIDator."""

from pyfia import FIA, volume
from pyfia.evalidator.validation import compare_estimates

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_YEAR,
    EXACT_MATCH_TOLERANCE_PCT,
    values_match,
)


class TestVolumeValidation:
    """Validate volume estimates against EVALIDator."""

    def test_growing_stock_volume(self, fia_db, evalidator_client):
        """Validate growing stock net volume matches EVALIDator (snum=15)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = volume(db, land_type="forest", vol_type="net", tree_type="gs", totals=True)
            pyfia_vol = result["VOLCFNET_TOTAL"][0]
            pyfia_se = result["VOLCFNET_TOTAL_SE"][0]

        ev_result = evalidator_client.get_volume(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            vol_type="net"
        )

        validation = compare_estimates(
            pyfia_value=pyfia_vol,
            pyfia_se=pyfia_se,
            evalidator_result=ev_result,
            tolerance_pct=EXACT_MATCH_TOLERANCE_PCT
        )

        print(f"\nGrowing Stock Volume Validation:")
        print(f"  pyFIA:      {pyfia_vol:,.0f} cu ft (SE: {pyfia_se:,.0f})")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft (SE: {ev_result.sampling_error:,.0f})")
        print(f"  Difference: {validation.pct_diff:.6f}%")

        assert values_match(pyfia_vol, ev_result.estimate), (
            f"Volume MUST match EVALIDator exactly.\n"
            f"pyFIA: {pyfia_vol} vs EVALIDator: {ev_result.estimate}"
        )

    def test_volume_with_explicit_treeclcd_filter(self, fia_db, evalidator_client):
        """Validate volume using explicit TREECLCD=2 filter matches EVALIDator."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = volume(
                db,
                land_type="forest",
                vol_type="net",
                tree_type="live",
                tree_domain="TREECLCD == 2",
                totals=True
            )
            pyfia_vol = result["VOLCFNET_TOTAL"][0]

        ev_result = evalidator_client.get_volume(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            vol_type="net"
        )

        print(f"\nVolume (explicit TREECLCD=2) Validation:")
        print(f"  pyFIA:      {pyfia_vol:,.0f} cu ft")
        print(f"  EVALIDator: {ev_result.estimate:,.0f} cu ft")

        assert values_match(pyfia_vol, ev_result.estimate), (
            f"Volume with explicit filter MUST match EVALIDator.\n"
            f"pyFIA: {pyfia_vol} vs EVALIDator: {ev_result.estimate}"
        )
