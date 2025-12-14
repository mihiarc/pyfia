"""Carbon flux validation tests.

These tests validate that carbon_flux estimates are internally consistent
with the underlying growth, mortality, and removals estimates. Since
EVALIDator doesn't provide a direct "net carbon flux" metric, we validate
by verifying the calculation is consistent with validated GRM components.
"""

from pyfia import FIA, growth, mortality, removals
from pyfia.estimation.estimators.carbon_flux import CARBON_FRACTION, carbon_flux

from .conftest import (
    GEORGIA_EVALID_GRM,
    values_match,
)


class TestCarbonFluxValidation:
    """Validate carbon_flux internal consistency."""

    def test_carbon_flux_equals_component_sum(self, fia_db):
        """Validate that NET_CARBON = GROWTH_C - MORT_C - REMV_C.

        This test verifies the core calculation is correct by comparing
        the carbon_flux result with manually calculated values from
        the individual GRM estimators.
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            # Get carbon flux with components
            flux_result = carbon_flux(db, include_components=True)

            # Get individual GRM estimates as biomass
            growth_result = growth(db, measure="biomass", totals=True)
            mort_result = mortality(db, measure="biomass", totals=True)
            remv_result = removals(db, measure="biomass", totals=True)

        # Extract values
        net_total = flux_result["NET_CARBON_FLUX_TOTAL"][0]
        growth_c = flux_result["GROWTH_CARBON_TOTAL"][0]
        mort_c = flux_result["MORT_CARBON_TOTAL"][0]
        remv_c = flux_result["REMV_CARBON_TOTAL"][0]

        # Manual calculation from components
        growth_biomass = growth_result["GROWTH_TOTAL"][0]
        mort_biomass = mort_result["MORT_TOTAL"][0]
        remv_biomass = remv_result["REMOVALS_TOTAL"][0]

        manual_growth_c = growth_biomass * CARBON_FRACTION
        manual_mort_c = mort_biomass * CARBON_FRACTION
        manual_remv_c = remv_biomass * CARBON_FRACTION
        manual_net = manual_growth_c - manual_mort_c - manual_remv_c

        print(f"\nCarbon Flux Component Validation:")
        print(f"  Growth carbon:    {growth_c:,.0f} (manual: {manual_growth_c:,.0f})")
        print(f"  Mortality carbon: {mort_c:,.0f} (manual: {manual_mort_c:,.0f})")
        print(f"  Removals carbon:  {remv_c:,.0f} (manual: {manual_remv_c:,.0f})")
        print(f"  Net carbon flux:  {net_total:,.0f} (manual: {manual_net:,.0f})")

        # Verify component carbon values match
        # Allow 3% tolerance due to differences in how estimators handle land_type
        # when called from carbon_flux vs called directly
        assert values_match(growth_c, manual_growth_c, rel_tol=0.03), (
            f"Growth carbon mismatch: {growth_c} vs {manual_growth_c}"
        )
        assert values_match(mort_c, manual_mort_c, rel_tol=0.03), (
            f"Mortality carbon mismatch: {mort_c} vs {manual_mort_c}"
        )
        assert values_match(remv_c, manual_remv_c, rel_tol=0.03), (
            f"Removals carbon mismatch: {remv_c} vs {manual_remv_c}"
        )

        # Verify net equals sum of components
        component_sum = growth_c - mort_c - remv_c
        assert values_match(net_total, component_sum, rel_tol=0.001), (
            f"Net flux should equal component sum: {net_total} vs {component_sum}"
        )

    def test_per_acre_consistent_with_total(self, fia_db):
        """Validate that NET_ACRE = NET_TOTAL / AREA."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            flux_result = carbon_flux(db)

        net_acre = flux_result["NET_CARBON_FLUX_ACRE"][0]
        net_total = flux_result["NET_CARBON_FLUX_TOTAL"][0]
        area = flux_result["AREA_TOTAL"][0]

        calculated_acre = net_total / area if area > 0 else 0

        print(f"\nPer-Acre Consistency Validation:")
        print(f"  Net total:  {net_total:,.0f} tons C/year")
        print(f"  Area:       {area:,.0f} acres")
        print(f"  Net/acre:   {net_acre:.6f} (calculated: {calculated_acre:.6f})")

        assert values_match(net_acre, calculated_acre, rel_tol=0.0001), (
            f"Per-acre should equal total/area: {net_acre} vs {calculated_acre}"
        )

    def test_carbon_fraction_applied_correctly(self, fia_db):
        """Validate that IPCC carbon fraction (0.47) is applied."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            flux_result = carbon_flux(db, include_components=True)
            growth_result = growth(db, measure="biomass", totals=True)

        growth_biomass = growth_result["GROWTH_TOTAL"][0]
        growth_carbon = flux_result["GROWTH_CARBON_TOTAL"][0]

        expected_carbon = growth_biomass * CARBON_FRACTION
        actual_fraction = growth_carbon / growth_biomass if growth_biomass > 0 else 0

        print(f"\nCarbon Fraction Validation:")
        print(f"  Growth biomass: {growth_biomass:,.0f} tons/year")
        print(f"  Growth carbon:  {growth_carbon:,.0f} tons C/year")
        print(f"  Applied fraction: {actual_fraction:.4f} (expected: {CARBON_FRACTION})")

        assert values_match(actual_fraction, CARBON_FRACTION, rel_tol=0.001), (
            f"Carbon fraction should be {CARBON_FRACTION}: got {actual_fraction}"
        )

    def test_grouped_results_structure(self, fia_db):
        """Validate that grouped results have correct structure.

        Note: Grouped sums may not exactly equal ungrouped totals due to:
        - Different plot selection when grouping (null values)
        - Different stratification when computing by group
        - Estimation variance in small groups

        This test focuses on structural correctness rather than exact equality.
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            # Get ungrouped total
            total_result = carbon_flux(db)
            total_net = total_result["NET_CARBON_FLUX_TOTAL"][0]

            # Get grouped by ownership
            grouped_result = carbon_flux(db, grp_by="OWNGRPCD")

        print(f"\nGrouped Results Structure Validation:")
        print(f"  Total (ungrouped): {total_net:,.0f} tons C/year")
        print(f"  Number of groups:  {len(grouped_result)}")
        print(f"  Groups: {grouped_result['OWNGRPCD'].to_list()}")

        # Structural checks
        assert "OWNGRPCD" in grouped_result.columns, "Group column should be present"
        assert len(grouped_result) >= 2, "Should have multiple ownership groups"
        assert "NET_CARBON_FLUX_TOTAL" in grouped_result.columns
        assert "NET_CARBON_FLUX_ACRE" in grouped_result.columns
        assert "AREA_TOTAL" in grouped_result.columns

        # Each group should have reasonable values (not all zeros or nulls)
        non_null_count = grouped_result["NET_CARBON_FLUX_TOTAL"].drop_nulls().len()
        assert non_null_count > 0, "Should have non-null flux values"

    def test_variance_propagation(self, fia_db):
        """Validate that variance is propagated (sum of component variances)."""
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)

            flux_result = carbon_flux(db, variance=True)
            growth_result = growth(db, measure="biomass", variance=True, totals=True)
            mort_result = mortality(db, measure="biomass", variance=True, totals=True)
            remv_result = removals(db, measure="biomass", variance=True, totals=True)

        # Get SEs
        flux_se = flux_result["NET_CARBON_FLUX_TOTAL_SE"][0]

        g_se = growth_result["GROWTH_TOTAL_SE"][0] * CARBON_FRACTION
        m_se = mort_result["MORT_TOTAL_SE"][0] * CARBON_FRACTION
        r_se = remv_result["REMOVALS_TOTAL_SE"][0] * CARBON_FRACTION

        # Sum of variances (conservative estimate)
        expected_se = (g_se**2 + m_se**2 + r_se**2) ** 0.5

        print(f"\nVariance Propagation Validation:")
        print(f"  Growth SE (C):    {g_se:,.0f}")
        print(f"  Mortality SE (C): {m_se:,.0f}")
        print(f"  Removals SE (C):  {r_se:,.0f}")
        print(f"  Combined SE:      {flux_se:,.0f} (expected: {expected_se:,.0f})")

        assert values_match(flux_se, expected_se, rel_tol=0.01), (
            f"Combined SE should follow variance propagation: {flux_se} vs {expected_se}"
        )

    def test_georgia_is_carbon_sink(self, fia_db):
        """Validate that Georgia forests are a net carbon sink.

        Based on FIA data, Georgia's forests should show net carbon
        sequestration (positive net flux = sink).
        """
        with FIA(fia_db) as db:
            db.clip_by_evalid(GEORGIA_EVALID_GRM)
            flux_result = carbon_flux(db, include_components=True)

        net_total = flux_result["NET_CARBON_FLUX_TOTAL"][0]
        growth_c = flux_result["GROWTH_CARBON_TOTAL"][0]
        mort_c = flux_result["MORT_CARBON_TOTAL"][0]
        remv_c = flux_result["REMV_CARBON_TOTAL"][0]

        print(f"\nGeorgia Carbon Balance:")
        print(f"  Growth:    +{growth_c/1e6:,.2f} million tons C/year")
        print(f"  Mortality: -{mort_c/1e6:,.2f} million tons C/year")
        print(f"  Removals:  -{remv_c/1e6:,.2f} million tons C/year")
        print(f"  Net flux:  {'+' if net_total > 0 else ''}{net_total/1e6:,.2f} million tons C/year")

        if net_total > 0:
            print(f"  Result: CARBON SINK (sequestering carbon)")
        else:
            print(f"  Result: CARBON SOURCE (emitting carbon)")

        # Georgia should be a carbon sink
        assert net_total > 0, (
            f"Georgia forests should be a carbon sink (positive net flux), "
            f"but got {net_total:,.0f} tons C/year"
        )

        # Growth should exceed losses
        assert growth_c > mort_c + remv_c, (
            f"Growth ({growth_c:,.0f}) should exceed mortality + removals "
            f"({mort_c + remv_c:,.0f})"
        )
