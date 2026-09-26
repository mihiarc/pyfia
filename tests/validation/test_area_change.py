"""Area change estimation validation against EVALIDator.

EVALIDator reports forest area on remeasured conditions where both
measurements are forest land (snum 127, or 136 per year) and where either
measurement is forest land (snum 128, or 137 per year). Their difference is the
area that was forest at exactly one measurement: pyFIA's gross_gain plus
gross_loss. pyFIA's net change (gain minus loss) has no EVALIDator counterpart.

The per-period comparison is exact. The annual one divides by PLOT.REMPER,
which can differ between the FIADB release in a local database and the one
EVALIDator serves, so it is checked to 2%.

References:
- Bechtold & Patterson (2005), Chapter 4: Area Change Estimation
- EVALIDator snum table: https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/snum
"""

import pytest

from pyfia import FIA, area_change

from .conftest import (
    FLOAT_TOLERANCE,
    GEORGIA_EVALID_GRM,
    GEORGIA_STATE_CODE,
    GEORGIA_YEAR,
)


class TestAreaChangeValidation:
    """Validate area_change estimates against EVALIDator (see module docstring)."""

    def test_internal_consistency(self, fia_db):
        """Verify net = gross_gain - gross_loss (internal pyFIA check)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            net_result = area_change(db, change_type="net")
            gain_result = area_change(db, change_type="gross_gain")
            loss_result = area_change(db, change_type="gross_loss")

            net = net_result["AREA_CHANGE_TOTAL"][0]
            gain = gain_result["AREA_CHANGE_TOTAL"][0]
            loss = loss_result["AREA_CHANGE_TOTAL"][0]

            calculated_net = gain - loss

            print("\nInternal Consistency Check:")
            print(f"  Gross Gain:     {gain:+,.0f} acres/year")
            print(f"  Gross Loss:     {loss:+,.0f} acres/year")
            print(f"  Net (computed): {calculated_net:+,.0f} acres/year")
            print(f"  Net (direct):   {net:+,.0f} acres/year")
            print(f"  Difference:     {abs(net - calculated_net):.1f} acres/year")

            assert abs(net - calculated_net) < 1, (
                f"Net change must equal gross_gain - gross_loss.\n"
                f"Net: {net:,.0f}, Gain: {gain:,.0f}, Loss: {loss:,.0f}\n"
                f"Calculated: {calculated_net:,.0f}"
            )

    def test_gross_gain_non_negative(self, fia_db):
        """Verify gross gain is non-negative."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = area_change(db, change_type="gross_gain")
            gain = result["AREA_CHANGE_TOTAL"][0]

            print(f"\nGross Gain: {gain:,.0f} acres/year")

            assert gain >= 0, f"Gross gain must be non-negative, got {gain}"

    def test_gross_loss_non_negative(self, fia_db):
        """Verify gross loss is non-negative."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = area_change(db, change_type="gross_loss")
            loss = result["AREA_CHANGE_TOTAL"][0]

            print(f"\nGross Loss: {loss:,.0f} acres/year")

            assert loss >= 0, f"Gross loss must be non-negative, got {loss}"

    def test_annual_vs_total_relationship(self, fia_db):
        """Verify annual rate relates to total by REMPER."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            annual_result = area_change(db, annual=True)
            total_result = area_change(db, annual=False)

            annual = abs(annual_result["AREA_CHANGE_TOTAL"][0])
            total = abs(total_result["AREA_CHANGE_TOTAL"][0])

            # Ratio should be approximately REMPER (typically 5-7 years)
            if annual > 0:
                ratio = total / annual

                print("\nAnnual vs Total Relationship:")
                print(f"  Annual: {annual:,.0f} acres/year")
                print(f"  Total:  {total:,.0f} acres")
                print(f"  Ratio (implied REMPER): {ratio:.1f} years")

                assert 2 < ratio < 10, (
                    f"Ratio of total/annual should be approximately REMPER (5-7 years).\n"
                    f"Got ratio: {ratio:.1f}"
                )

    def test_transition_area_matches_evalidator(self, fia_db, evalidator_client):
        """gross_gain + gross_loss over the period equals snum 128 - snum 127 (#151)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            gain = area_change(db, change_type="gross_gain", annual=False)
            loss = area_change(db, change_type="gross_loss", annual=False)
        pyfia_transition = gain["AREA_CHANGE_TOTAL"][0] + loss["AREA_CHANGE_TOTAL"][0]

        ev_both = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            annual=False,
            measurement="remeasured",
        )
        ev_either = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            annual=False,
            measurement="either",
        )
        ev_transition = ev_either.estimate - ev_both.estimate

        print(f"\n  EVALIDator snum 128 - 127: {ev_transition:,.1f} acres")
        print(f"  pyFIA gain + loss:         {pyfia_transition:,.1f} acres")

        assert pyfia_transition == pytest.approx(ev_transition, rel=FLOAT_TOLERANCE)

    def test_annual_transition_area_matches_evalidator(self, fia_db, evalidator_client):
        """Annual gross_gain + gross_loss is close to snum 137 - snum 136."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            gain = area_change(db, change_type="gross_gain")
            loss = area_change(db, change_type="gross_loss")
        pyfia_transition = gain["AREA_CHANGE_TOTAL"][0] + loss["AREA_CHANGE_TOTAL"][0]

        ev_both = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE, year=GEORGIA_YEAR, measurement="both"
        )
        ev_either = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE, year=GEORGIA_YEAR, measurement="either"
        )
        ev_transition = ev_either.estimate - ev_both.estimate

        print(f"\n  EVALIDator snum 137 - 136: {ev_transition:,.1f} acres/year")
        print(f"  pyFIA gain + loss:         {pyfia_transition:,.1f} acres/year")

        # REMPER can differ between FIADB releases; see the module docstring.
        assert pyfia_transition == pytest.approx(ev_transition, rel=0.02)

    def test_area_change_has_plots(self, fia_db):
        """Verify area change estimate includes remeasured plots."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = area_change(db)

            n_plots = result["N_PLOTS"][0]

            print(f"\nRemeasured plots used: {n_plots:,}")

            assert n_plots > 0, "Area change estimate should include remeasured plots"

    def test_area_change_by_ownership(self, fia_db):
        """Verify area change can be grouped by ownership."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            result = area_change(db, grp_by="OWNGRPCD")

            print("\nArea change by ownership:")
            print(result)

            assert len(result) > 1, "Should have multiple ownership groups"
            assert "OWNGRPCD" in result.columns
            assert "AREA_CHANGE_TOTAL" in result.columns

    def test_area_change_summary(self, fia_db, evalidator_client):
        """Print comprehensive summary of area change estimates."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            net = area_change(db, change_type="net")
            gain = area_change(db, change_type="gross_gain")
            loss = area_change(db, change_type="gross_loss")

        ev_both = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            measurement="both",
        )

        ev_either = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            measurement="either",
        )

        print(f"\n{'=' * 70}")
        print("GEORGIA FOREST AREA CHANGE SUMMARY")
        print(f"{'=' * 70}")
        print(f"EVALID: {GEORGIA_EVALID_GRM} | Year: {GEORGIA_YEAR}")
        print(f"{'=' * 70}")

        print("\npyFIA Estimates (using SUBP_COND_CHNG_MTRX table):")
        print(
            f"  Net annual change:    {net['AREA_CHANGE_TOTAL'][0]:+12,.0f} acres/year"
        )
        print(
            f"  Gross annual gain:    {gain['AREA_CHANGE_TOTAL'][0]:+12,.0f} acres/year"
        )
        print(
            f"  Gross annual loss:    {loss['AREA_CHANGE_TOTAL'][0]:+12,.0f} acres/year"
        )
        print(f"  Remeasured plots:     {net['N_PLOTS'][0]:12,}")

        print("\nEVALIDator Estimates:")
        print(f"  Forest at BOTH (snum 136):    {ev_both.estimate:12,.0f} acres/year")
        print(f"  Forest at EITHER (snum 137):  {ev_either.estimate:12,.0f} acres/year")
        print(
            f"  Difference:                   {ev_either.estimate - ev_both.estimate:12,.0f} acres/year"
        )

        print("\nInterpretation:")
        net_val = net["AREA_CHANGE_TOTAL"][0]
        if net_val < 0:
            print(
                f"  Georgia is experiencing NET FOREST LOSS of ~{abs(net_val):,.0f} acres/year"
            )
        else:
            print(
                f"  Georgia is experiencing NET FOREST GAIN of ~{net_val:,.0f} acres/year"
            )

        print(f"\n{'=' * 70}")
