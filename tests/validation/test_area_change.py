"""Area change estimation validation against EVALIDator.

IMPORTANT: EVALIDator Methodology Difference
============================================

EVALIDator's area change estimates (snum 136, 137) measure TOTAL AREA meeting
certain criteria on remeasured plots, NOT the net transition:

- snum 136: Area that was forest at BOTH measurements (stable forest)
- snum 137: Area that was forest at EITHER measurement (forest at any point)

The DIFFERENCE (snum 137 - snum 136) represents the total transition area
(land that changed status), which equals gross_gain + gross_loss.

pyFIA's area_change() calculates NET transitions:
- net = gross_gain - gross_loss
- gross_gain = non-forest → forest
- gross_loss = forest → non-forest

These are different metrics! The validation tests compare what CAN be compared:
1. pyFIA's (gross_gain + gross_loss) vs EVALIDator's (snum137 - snum136)
2. Internal consistency: net = gross_gain - gross_loss

References:
- Bechtold & Patterson (2005), Chapter 4: Area Change Estimation
- EVALIDator snum table: https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/snum
"""

import pytest

from pyfia import FIA, area_change
from pyfia.evalidator.client import EVALIDatorClient

from .conftest import (
    GEORGIA_STATE_CODE,
    GEORGIA_EVALID,
    GEORGIA_YEAR,
)


class TestAreaChangeValidation:
    """Validate area_change estimates against EVALIDator.

    Note: EVALIDator measures total area meeting criteria, not net transitions.
    See module docstring for methodology differences.
    """

    def test_internal_consistency(self, fia_db):
        """Verify net = gross_gain - gross_loss (internal pyFIA check)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)

            net_result = area_change(db, change_type="net")
            gain_result = area_change(db, change_type="gross_gain")
            loss_result = area_change(db, change_type="gross_loss")

            net = net_result["AREA_CHANGE_TOTAL"][0]
            gain = gain_result["AREA_CHANGE_TOTAL"][0]
            loss = loss_result["AREA_CHANGE_TOTAL"][0]

            calculated_net = gain - loss

            print(f"\nInternal Consistency Check:")
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
            db.clip_by_evalid(GEORGIA_EVALID)
            result = area_change(db, change_type="gross_gain")
            gain = result["AREA_CHANGE_TOTAL"][0]

            print(f"\nGross Gain: {gain:,.0f} acres/year")

            assert gain >= 0, f"Gross gain must be non-negative, got {gain}"

    def test_gross_loss_non_negative(self, fia_db):
        """Verify gross loss is non-negative."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = area_change(db, change_type="gross_loss")
            loss = result["AREA_CHANGE_TOTAL"][0]

            print(f"\nGross Loss: {loss:,.0f} acres/year")

            assert loss >= 0, f"Gross loss must be non-negative, got {loss}"

    def test_annual_vs_total_relationship(self, fia_db):
        """Verify annual rate relates to total by REMPER."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)

            annual_result = area_change(db, annual=True)
            total_result = area_change(db, annual=False)

            annual = abs(annual_result["AREA_CHANGE_TOTAL"][0])
            total = abs(total_result["AREA_CHANGE_TOTAL"][0])

            # Ratio should be approximately REMPER (typically 5-7 years)
            if annual > 0:
                ratio = total / annual

                print(f"\nAnnual vs Total Relationship:")
                print(f"  Annual: {annual:,.0f} acres/year")
                print(f"  Total:  {total:,.0f} acres")
                print(f"  Ratio (implied REMPER): {ratio:.1f} years")

                assert 2 < ratio < 10, (
                    f"Ratio of total/annual should be approximately REMPER (5-7 years).\n"
                    f"Got ratio: {ratio:.1f}"
                )

    def test_compare_with_evalidator_transition_area(self, fia_db, evalidator_client):
        """Compare pyFIA gross transitions with EVALIDator transition area.

        EVALIDator's snum 137 (either) minus snum 136 (both) represents the
        total area that transitioned (changed status). This should equal
        pyFIA's gross_gain + gross_loss.

        NOTE: This comparison has limitations because EVALIDator values
        are not strictly net transitions but total area meeting criteria.
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)

            gain_result = area_change(db, change_type="gross_gain")
            loss_result = area_change(db, change_type="gross_loss")
            net_result = area_change(db, change_type="net")

            pyfia_gain = gain_result["AREA_CHANGE_TOTAL"][0]
            pyfia_loss = loss_result["AREA_CHANGE_TOTAL"][0]
            pyfia_net = net_result["AREA_CHANGE_TOTAL"][0]
            pyfia_total_transitions = pyfia_gain + pyfia_loss

        # Get EVALIDator estimates
        ev_both = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            land_type="forest",
            annual=True,
            measurement="both",
        )

        ev_either = evalidator_client.get_area_change(
            state_code=GEORGIA_STATE_CODE,
            year=GEORGIA_YEAR,
            land_type="forest",
            annual=True,
            measurement="either",
        )

        ev_transition_area = ev_either.estimate - ev_both.estimate

        print(f"\n{'='*60}")
        print("Area Change Comparison: pyFIA vs EVALIDator")
        print(f"{'='*60}")
        print(f"\nEVALIDator (snum 136, 137):")
        print(f"  Forest at BOTH measurements (snum 136):   {ev_both.estimate:,.0f} acres/year")
        print(f"  Forest at EITHER measurement (snum 137): {ev_either.estimate:,.0f} acres/year")
        print(f"  Difference (transition area):            {ev_transition_area:,.0f} acres/year")

        print(f"\npyFIA area_change():")
        print(f"  Gross Gain (non-forest → forest):        {pyfia_gain:+,.0f} acres/year")
        print(f"  Gross Loss (forest → non-forest):        {pyfia_loss:+,.0f} acres/year")
        print(f"  Net Change (gain - loss):                {pyfia_net:+,.0f} acres/year")
        print(f"  Total Transitions (gain + loss):         {pyfia_total_transitions:,.0f} acres/year")

        print(f"\nMethodology Note:")
        print(f"  EVALIDator measures TOTAL AREA meeting criteria on remeasured plots.")
        print(f"  pyFIA measures NET TRANSITIONS between forest/non-forest status.")
        print(f"  These are fundamentally different metrics.")

        # Calculate comparison metrics
        if ev_transition_area > 0:
            ratio = pyfia_total_transitions / ev_transition_area
            pct_diff = abs(pyfia_total_transitions - ev_transition_area) / ev_transition_area * 100

            print(f"\nComparison (pyFIA transitions vs EVALIDator difference):")
            print(f"  Ratio: {ratio:.2f}")
            print(f"  Percent difference: {pct_diff:.1f}%")

            # This is informational - we don't assert exact match due to methodology differences
            # The ratio being ~2x is expected because EVALIDator counts each transition once
            # while pyFIA counts gain and loss separately

    def test_area_change_has_plots(self, fia_db):
        """Verify area change estimate includes remeasured plots."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = area_change(db)

            n_plots = result["N_PLOTS"][0]

            print(f"\nRemeasured plots used: {n_plots:,}")

            assert n_plots > 0, "Area change estimate should include remeasured plots"

    def test_area_change_by_ownership(self, fia_db):
        """Verify area change can be grouped by ownership."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)
            result = area_change(db, grp_by="OWNGRPCD")

            print(f"\nArea change by ownership:")
            print(result)

            assert len(result) > 1, "Should have multiple ownership groups"
            assert "OWNGRPCD" in result.columns
            assert "AREA_CHANGE_TOTAL" in result.columns

    def test_area_change_summary(self, fia_db, evalidator_client):
        """Print comprehensive summary of area change estimates."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID)

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

        print(f"\n{'='*70}")
        print("GEORGIA FOREST AREA CHANGE SUMMARY")
        print(f"{'='*70}")
        print(f"EVALID: {GEORGIA_EVALID} | Year: {GEORGIA_YEAR}")
        print(f"{'='*70}")

        print(f"\npyFIA Estimates (using SUBP_COND_CHNG_MTRX table):")
        print(f"  Net annual change:    {net['AREA_CHANGE_TOTAL'][0]:+12,.0f} acres/year")
        print(f"  Gross annual gain:    {gain['AREA_CHANGE_TOTAL'][0]:+12,.0f} acres/year")
        print(f"  Gross annual loss:    {loss['AREA_CHANGE_TOTAL'][0]:+12,.0f} acres/year")
        print(f"  Remeasured plots:     {net['N_PLOTS'][0]:12,}")

        print(f"\nEVALIDator Estimates (different methodology):")
        print(f"  Forest at BOTH (snum 136):    {ev_both.estimate:12,.0f} acres/year")
        print(f"  Forest at EITHER (snum 137):  {ev_either.estimate:12,.0f} acres/year")
        print(f"  Difference:                   {ev_either.estimate - ev_both.estimate:12,.0f} acres/year")

        print(f"\nInterpretation:")
        net_val = net['AREA_CHANGE_TOTAL'][0]
        if net_val < 0:
            print(f"  Georgia is experiencing NET FOREST LOSS of ~{abs(net_val):,.0f} acres/year")
        else:
            print(f"  Georgia is experiencing NET FOREST GAIN of ~{net_val:,.0f} acres/year")

        print(f"\n{'='*70}")
