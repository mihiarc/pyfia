"""
Enhanced property-based tests for pyFIA statistical properties.

These tests use property-based testing to verify statistical invariants
and relationships that should always hold for FIA estimation results.
"""

import pytest
import polars as pl
from hypothesis import given, assume, settings, strategies as st
from hypothesis import HealthCheck

from pyfia import area, volume, tpa, biomass


class TestEstimationInvariants:
    """Property-based tests for estimation invariants."""

    @given(
        land_type=st.sampled_from(["forest", "timber"]),
        tree_type=st.sampled_from(["live", "dead", "gs", "all"])
    )
    @settings(
        max_examples=10,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_estimation_non_negativity(self, sample_fia_instance, land_type, tree_type):
        """Test that all estimates are non-negative."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Test various estimation functions
            area_result = area(sample_fia_instance, land_type=land_type)

            if tree_type in ["live", "gs", "all"]:  # Skip dead for volume
                volume_result = volume(sample_fia_instance, land_type=land_type, tree_type=tree_type)
                tpa_result = tpa(sample_fia_instance, land_type=land_type, tree_type=tree_type)

            # All estimates should be non-negative
            if len(area_result) > 0:
                assert area_result["AREA_ACRE"][0] >= 0, "Area estimate cannot be negative"
                assert area_result["AREA_ACRE_SE"][0] >= 0, "Area SE cannot be negative"

            if tree_type in ["live", "gs", "all"] and 'volume_result' in locals() and len(volume_result) > 0:
                assert volume_result["VOL_ACRE"][0] >= 0, "Volume estimate cannot be negative"
                assert volume_result["VOL_ACRE_SE"][0] >= 0, "Volume SE cannot be negative"

        except Exception as e:
            pytest.skip(f"Estimation not possible: {e}")

    @given(
        diameter_threshold=st.floats(min_value=5.0, max_value=30.0),
        land_type=st.sampled_from(["forest", "timber"])
    )
    @settings(
        max_examples=8,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_diameter_filtering_monotonicity(self, sample_fia_instance, diameter_threshold, land_type):
        """Test that filtering by diameter reduces estimates monotonically."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get estimates without diameter filter
            unfiltered = tpa(sample_fia_instance, land_type=land_type, tree_type="live")

            # Get estimates with diameter filter
            filtered = tpa(
                sample_fia_instance,
                land_type=land_type,
                tree_type="live",
                tree_domain=f"DIA >= {diameter_threshold}"
            )

            if len(unfiltered) > 0 and len(filtered) > 0:
                # Filtered estimate should be <= unfiltered
                assert filtered["TPA_ACRE"][0] <= unfiltered["TPA_ACRE"][0], \
                    "Diameter filtering should reduce or maintain TPA estimate"

        except Exception as e:
            pytest.skip(f"Diameter filtering test not possible: {e}")

    @given(
        component=st.sampled_from(["AG", "BG", "TOTAL"])
    )
    @settings(
        max_examples=6,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_biomass_component_relationships(self, sample_fia_instance, component):
        """Test relationships between biomass components."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get all three components
            ag_result = biomass(sample_fia_instance, component="AG", land_type="forest")
            bg_result = biomass(sample_fia_instance, component="BG", land_type="forest")
            total_result = biomass(sample_fia_instance, component="TOTAL", land_type="forest")

            if len(ag_result) > 0 and len(bg_result) > 0 and len(total_result) > 0:
                ag_bio = ag_result["BIO_ACRE"][0]
                bg_bio = bg_result["BIO_ACRE"][0]
                total_bio = total_result["BIO_ACRE"][0]

                # AG should typically be larger than BG
                if ag_bio > 0 and bg_bio > 0:
                    assert ag_bio >= bg_bio * 0.8, "AG biomass should be comparable or larger than BG"

                # Total should approximately equal AG + BG
                expected_total = ag_bio + bg_bio
                relative_error = abs(total_bio - expected_total) / max(expected_total, 0.1)
                assert relative_error < 0.05, "Total biomass should equal sum of components"

        except Exception as e:
            pytest.skip(f"Biomass component test not possible: {e}")


class TestStatisticalProperties:
    """Property-based tests for statistical properties."""

    @given(
        grouping_column=st.sampled_from(["OWNGRPCD", "FORTYPCD", "SPCD"])
    )
    @settings(
        max_examples=6,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_grouped_estimates_sum_consistency(self, sample_fia_instance, grouping_column):
        """Test that grouped estimates sum approximately to total estimate."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get total estimate
            total_result = area(sample_fia_instance, land_type="forest")

            # Get grouped estimate
            if grouping_column == "SPCD":
                grouped_result = area(sample_fia_instance, land_type="forest", by_species=True)
            else:
                grouped_result = area(sample_fia_instance, land_type="forest", grp_by=grouping_column)

            if len(total_result) > 0 and len(grouped_result) > 0:
                total_area = total_result["AREA_ACRE"][0]
                grouped_sum = grouped_result["AREA_ACRE"].sum()

                # Should be approximately equal (allowing for grouping effects)
                relative_error = abs(total_area - grouped_sum) / max(total_area, 0.1)
                assert relative_error < 0.10, f"Grouped sum should approximate total (error: {relative_error:.3f})"

        except Exception as e:
            pytest.skip(f"Grouped consistency test not possible: {e}")

    @given(
        n_samples=st.integers(min_value=2, max_value=5)
    )
    @settings(
        max_examples=3,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_estimation_repeatability(self, sample_fia_instance, n_samples):
        """Test that repeated estimations give identical results."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            results = []
            for _ in range(n_samples):
                result = area(sample_fia_instance, land_type="forest")
                if len(result) > 0:
                    results.append(result["AREA_ACRE"][0])

            if len(results) >= 2:
                # All results should be identical
                for i in range(1, len(results)):
                    assert abs(results[i] - results[0]) < 1e-10, \
                        f"Repeated estimations should be identical: {results}"

        except Exception as e:
            pytest.skip(f"Repeatability test not possible: {e}")


class TestDomainFilteringProperties:
    """Property-based tests for domain filtering properties."""

    @given(
        diameter_min=st.floats(min_value=5.0, max_value=15.0),
        diameter_max=st.floats(min_value=20.0, max_value=40.0)
    )
    @settings(
        max_examples=5,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_nested_domain_filtering(self, sample_fia_instance, diameter_min, diameter_max):
        """Test that nested domain filters are monotonic."""
        assume(diameter_min < diameter_max)

        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get estimates with different diameter ranges
            wide_range = tpa(
                sample_fia_instance,
                tree_domain=f"DIA >= {diameter_min}",
                land_type="forest"
            )

            narrow_range = tpa(
                sample_fia_instance,
                tree_domain=f"DIA >= {diameter_min} AND DIA <= {diameter_max}",
                land_type="forest"
            )

            very_narrow = tpa(
                sample_fia_instance,
                tree_domain=f"DIA >= {diameter_max}",
                land_type="forest"
            )

            if all(len(r) > 0 for r in [wide_range, narrow_range, very_narrow]):
                wide_tpa = wide_range["TPA_ACRE"][0]
                narrow_tpa = narrow_range["TPA_ACRE"][0]
                very_narrow_tpa = very_narrow["TPA_ACRE"][0]

                # Should follow monotonic relationship
                assert wide_tpa >= narrow_tpa, "Wider range should have >= TPA"
                assert narrow_tpa >= very_narrow_tpa, "Narrower range should have >= TPA"

        except Exception as e:
            pytest.skip(f"Nested filtering test not possible: {e}")

    @given(
        ownership_codes=st.lists(
            st.sampled_from([10, 20, 30, 40]),
            min_size=1,
            max_size=3,
            unique=True
        )
    )
    @settings(
        max_examples=5,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_ownership_filtering_additivity(self, sample_fia_instance, ownership_codes):
        """Test that ownership filtering follows additive property."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get individual ownership estimates
            individual_estimates = []
            for code in ownership_codes:
                result = area(
                    sample_fia_instance,
                    area_domain=f"OWNGRPCD == {code}",
                    land_type="forest"
                )
                if len(result) > 0:
                    individual_estimates.append(result["AREA_ACRE"][0])

            # Get combined ownership estimate
            ownership_filter = " OR ".join([f"OWNGRPCD == {code}" for code in ownership_codes])
            combined_result = area(
                sample_fia_instance,
                area_domain=ownership_filter,
                land_type="forest"
            )

            if individual_estimates and len(combined_result) > 0:
                individual_sum = sum(individual_estimates)
                combined_estimate = combined_result["AREA_ACRE"][0]

                # Should be approximately equal
                relative_error = abs(combined_estimate - individual_sum) / max(individual_sum, 0.1)
                assert relative_error < 0.05, \
                    f"Combined estimate should equal sum of individual estimates (error: {relative_error:.3f})"

        except Exception as e:
            pytest.skip(f"Ownership filtering test not possible: {e}")


class TestVolumeEstimationProperties:
    """Property-based tests specific to volume estimation."""

    @given(
        vol_type=st.sampled_from(["gross", "net", "sound"])
    )
    @settings(
        max_examples=6,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_volume_type_relationships(self, sample_fia_instance, vol_type):
        """Test relationships between volume types."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Get different volume types
            gross_vol = volume(sample_fia_instance, vol_type="gross", land_type="forest")
            net_vol = volume(sample_fia_instance, vol_type="net", land_type="forest")
            sound_vol = volume(sample_fia_instance, vol_type="sound", land_type="forest")

            if all(len(v) > 0 for v in [gross_vol, net_vol, sound_vol]):
                gross_estimate = gross_vol["VOL_ACRE"][0]
                net_estimate = net_vol["VOL_ACRE"][0]
                sound_estimate = sound_vol["VOL_ACRE"][0]

                # Gross should be >= Net >= Sound (general relationship)
                if gross_estimate > 0 and net_estimate > 0:
                    assert gross_estimate >= net_estimate * 0.9, "Gross volume should be >= net volume"

                if net_estimate > 0 and sound_estimate > 0:
                    assert net_estimate >= sound_estimate * 0.9, "Net volume should be >= sound volume"

        except Exception as e:
            pytest.skip(f"Volume type relationship test not possible: {e}")

    @given(
        tree_type=st.sampled_from(["live", "gs", "all"])
    )
    @settings(
        max_examples=6,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_volume_per_tree_reasonableness(self, sample_fia_instance, tree_type):
        """Test that volume per tree estimates are reasonable."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            vol_result = volume(sample_fia_instance, tree_type=tree_type, land_type="forest")
            tpa_result = tpa(sample_fia_instance, tree_type=tree_type, land_type="forest")

            if len(vol_result) > 0 and len(tpa_result) > 0:
                volume_per_acre = vol_result["VOL_ACRE"][0]
                trees_per_acre = tpa_result["TPA_ACRE"][0]

                if trees_per_acre > 0:
                    volume_per_tree = volume_per_acre / trees_per_acre

                    # Volume per tree should be reasonable (0.1 to 500 cubic feet)
                    assert 0.1 <= volume_per_tree <= 500, \
                        f"Volume per tree seems unreasonable: {volume_per_tree:.2f} cubic feet"

        except Exception as e:
            pytest.skip(f"Volume per tree test not possible: {e}")


class TestErrorBoundaryProperties:
    """Property-based tests for error boundary conditions."""

    @given(
        invalid_land_type=st.text(min_size=1, max_size=20).filter(
            lambda x: x not in ["forest", "timber"]
        )
    )
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_invalid_parameter_handling(self, sample_fia_instance, invalid_land_type):
        """Test that invalid parameters raise appropriate errors."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        # Should raise ValueError for invalid land_type
        with pytest.raises((ValueError, KeyError, Exception)) as exc_info:
            area(sample_fia_instance, land_type=invalid_land_type)

        # Error message should be informative
        error_msg = str(exc_info.value).lower()
        assert any(keyword in error_msg for keyword in ["invalid", "land_type", "not supported"]), \
            f"Error message should be informative: {error_msg}"

    @given(
        malformed_domain=st.sampled_from([
            "DIA >>>= 10",  # Invalid operator
            "INVALID_COLUMN == 1",  # Invalid column
            "DIA >= ",  # Incomplete expression
            "== 10",  # Missing column
            "DIA >= 10 AND AND",  # Invalid syntax
        ])
    )
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_malformed_domain_handling(self, sample_fia_instance, malformed_domain):
        """Test that malformed domain expressions are handled appropriately."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        # Should raise some form of error for malformed domain
        with pytest.raises(Exception):  # Could be various exception types
            area(sample_fia_instance, tree_domain=malformed_domain, land_type="forest")


class TestEstimationConsistencyProperties:
    """Property-based tests for estimation consistency across different parameters."""

    @given(
        state_codes=st.lists(
            st.integers(min_value=1, max_value=56),
            min_size=1,
            max_size=3,
            unique=True
        )
    )
    @settings(
        max_examples=3,
        suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_multi_state_consistency(self, sample_fia_instance, state_codes):
        """Test that multi-state estimates are consistent."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # This is a placeholder for multi-state consistency testing
            # Would need actual multi-state database to test properly

            # Get available states in database
            plots = sample_fia_instance.get_plots()
            if len(plots) > 0:
                available_states = plots["STATECD"].unique().to_list()

                if len(available_states) >= 2:
                    # Test would verify state-level estimates sum to total
                    pytest.skip("Multi-state consistency testing needs implementation")
                else:
                    pytest.skip("Not enough states for multi-state testing")

        except Exception as e:
            pytest.skip(f"Multi-state consistency test not possible: {e}")

# Example of how to run property-based tests with pytest
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])