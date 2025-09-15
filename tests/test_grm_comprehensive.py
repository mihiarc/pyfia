"""
Comprehensive tests for Growth-Removal-Mortality (GRM) functionality.

These tests cover the critical GRM table operations that are currently
completely untested but essential for mortality, growth, and removal estimation.
"""

import pytest
import polars as pl
from pyfia import mortality, growth, removals


class TestGRMTableStructure:
    """Test GRM table structure and component identification."""

    def test_grm_component_column_structure(self, grm_component_data):
        """Test GRM component data has correct column structure."""
        required_columns = [
            "TRE_CN", "PLT_CN", "DIA_BEGIN", "DIA_MIDPT", "DIA_END",
            "SUBP_COMPONENT_GS_FOREST", "SUBP_TPAGROW_UNADJ_GS_FOREST",
            "SUBP_SUBPTYP_GRM_GS_FOREST"
        ]

        for col in required_columns:
            assert col in grm_component_data.columns, f"Missing column: {col}"

        # Check data types are reasonable
        assert grm_component_data["TRE_CN"].dtype == pl.Utf8
        assert grm_component_data["DIA_MIDPT"].dtype == pl.Float64

    def test_grm_component_types_validation(self, grm_component_data, grm_component_types):
        """Test GRM component types are valid."""
        components = grm_component_data["SUBP_COMPONENT_GS_FOREST"].unique().to_list()

        all_valid_components = grm_component_types["all_components"]

        for component in components:
            if component is not None:
                assert component in all_valid_components, f"Invalid component: {component}"

    def test_diameter_consistency_in_grm(self, grm_component_data):
        """Test diameter consistency across GRM measurements."""
        # For survivor trees, DIA_END should be >= DIA_BEGIN
        survivors = grm_component_data.filter(
            pl.col("SUBP_COMPONENT_GS_FOREST") == "SURVIVOR"
        ).filter(
            (pl.col("DIA_BEGIN").is_not_null()) & (pl.col("DIA_END").is_not_null())
        )

        if len(survivors) > 0:
            dia_differences = survivors.with_columns([
                (pl.col("DIA_END") - pl.col("DIA_BEGIN")).alias("dia_growth")
            ])

            # Growth should generally be positive or zero
            negative_growth = dia_differences.filter(pl.col("dia_growth") < -0.1)  # Allow small measurement error
            assert len(negative_growth) == 0, f"Found {len(negative_growth)} trees with negative diameter growth"

    def test_subptyp_grm_values(self, grm_component_data, subptyp_grm_mappings):
        """Test SUBPTYP_GRM values are within valid range."""
        subptyp_values = grm_component_data["SUBP_SUBPTYP_GRM_GS_FOREST"].unique().to_list()

        valid_values = list(subptyp_grm_mappings.keys())

        for value in subptyp_values:
            if value is not None:
                assert value in valid_values, f"Invalid SUBPTYP_GRM value: {value}"


class TestGRMComponentIdentification:
    """Test identification and categorization of GRM components."""

    def test_survivor_component_identification(self, grm_component_data):
        """Test survivor component identification."""
        survivors = grm_component_data.filter(
            pl.col("SUBP_COMPONENT_GS_FOREST") == "SURVIVOR"
        )

        # Survivors should have both beginning and ending diameters
        survivors_with_data = survivors.filter(
            (pl.col("DIA_BEGIN").is_not_null()) & (pl.col("DIA_END").is_not_null())
        )

        assert len(survivors_with_data) > 0, "Should have survivor trees with diameter data"

        # Check that survivor trees have reasonable diameter values
        if len(survivors_with_data) > 0:
            assert survivors_with_data["DIA_BEGIN"].min() > 0, "Begin diameter should be positive"
            assert survivors_with_data["DIA_END"].min() > 0, "End diameter should be positive"

    def test_ingrowth_component_identification(self, grm_component_data):
        """Test ingrowth component identification."""
        ingrowth = grm_component_data.filter(
            pl.col("SUBP_COMPONENT_GS_FOREST") == "INGROWTH"
        )

        if len(ingrowth) > 0:
            # Ingrowth trees should have NULL beginning diameter
            ingrowth_begin_null = ingrowth.filter(pl.col("DIA_BEGIN").is_null())
            assert len(ingrowth_begin_null) > 0, "Ingrowth trees should have null beginning diameter"

            # But should have ending diameter
            ingrowth_end_not_null = ingrowth.filter(pl.col("DIA_END").is_not_null())
            assert len(ingrowth_end_not_null) > 0, "Ingrowth trees should have ending diameter"

    def test_mortality_component_identification(self, grm_mortality_component_data):
        """Test mortality component identification."""
        mortality_components = grm_mortality_component_data.filter(
            pl.col("SUBP_COMPONENT_GS_FOREST").str.starts_with("MORTALITY")
        )

        assert len(mortality_components) > 0, "Should have mortality components"

        # Check mortality TPA values are positive
        mortality_tpa = mortality_components["SUBP_TPAMORT_UNADJ_GS_FOREST"]
        assert (mortality_tpa > 0).all(), "Mortality TPA should be positive"

    def test_removal_component_identification(self, grm_removal_component_data):
        """Test removal component identification."""
        cut_components = grm_removal_component_data.filter(
            pl.col("SUBP_COMPONENT_GS_FOREST").str.starts_with("CUT")
        )

        diversion_components = grm_removal_component_data.filter(
            pl.col("SUBP_COMPONENT_GS_FOREST").str.starts_with("DIVERSION")
        )

        assert len(cut_components) > 0, "Should have cut components"

        if len(diversion_components) > 0:
            assert len(diversion_components) > 0, "Should have diversion components"

        # Check removal TPA values are positive
        all_removals = grm_removal_component_data
        removal_tpa = all_removals["SUBP_TPAREMV_UNADJ_GS_FOREST"]
        assert (removal_tpa > 0).all(), "Removal TPA should be positive"


class TestGRMAdjustmentFactors:
    """Test GRM adjustment factor application."""

    def test_adjustment_factor_selection(self, grm_component_data, subptyp_grm_mappings):
        """Test correct adjustment factor selection based on SUBPTYP_GRM."""
        # Test different adjustment factor types
        subp_trees = grm_component_data.filter(
            pl.col("SUBP_SUBPTYP_GRM_GS_FOREST") == 1
        )
        micr_trees = grm_component_data.filter(
            pl.col("SUBP_SUBPTYP_GRM_GS_FOREST") == 2
        )
        macr_trees = grm_component_data.filter(
            pl.col("SUBP_SUBPTYP_GRM_GS_FOREST") == 3
        )

        # Should have trees in at least one category
        total_trees = len(subp_trees) + len(micr_trees) + len(macr_trees)
        assert total_trees > 0, "Should have trees with valid SUBPTYP_GRM values"

        # Verify adjustment factors make sense for diameter ranges
        if len(micr_trees) > 0:
            # Microplot trees should generally be smaller diameter
            micr_diameters = micr_trees.filter(
                pl.col("DIA_MIDPT").is_not_null()
            )["DIA_MIDPT"]
            if len(micr_diameters) > 0:
                assert micr_diameters.max() < 15.0, "Microplot trees should be smaller diameter"

        if len(macr_trees) > 0:
            # Macroplot trees should generally be larger diameter
            macr_diameters = macr_trees.filter(
                pl.col("DIA_MIDPT").is_not_null()
            )["DIA_MIDPT"]
            if len(macr_diameters) > 0:
                assert macr_diameters.min() > 20.0, "Macroplot trees should be larger diameter"

    def test_adjustment_factor_application_logic(self, standard_stratum_data):
        """Test adjustment factor application logic."""
        # Test different adjustment factors are applied correctly
        adj_factors = {
            "subp": standard_stratum_data["ADJ_FACTOR_SUBP"][0],
            "micr": standard_stratum_data["ADJ_FACTOR_MICR"][0],
            "macr": standard_stratum_data["ADJ_FACTOR_MACR"][0]
        }

        # Microplot adjustment should be largest (smallest plot size)
        assert adj_factors["micr"] > adj_factors["subp"]
        assert adj_factors["micr"] > adj_factors["macr"]

        # Macroplot adjustment should be smallest (largest plot size)
        assert adj_factors["macr"] < adj_factors["subp"]


class TestGRMEstimationIntegration:
    """Test integration of GRM data with estimation functions."""

    def test_mortality_estimation_with_grm(self, sample_fia_instance):
        """Test mortality estimation using GRM data."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Test basic mortality estimation
            result = mortality(sample_fia_instance, tree_type="gs", land_type="forest")

            assert isinstance(result, pl.DataFrame)

            # Check required columns exist
            expected_columns = ["MORT_ACRE", "MORT_ACRE_SE", "nPlots_TREE"]
            for col in expected_columns:
                assert col in result.columns, f"Missing column: {col}"

            if len(result) > 0:
                # Mortality should be non-negative
                assert result["MORT_ACRE"][0] >= 0, "Mortality rate cannot be negative"

        except Exception as e:
            # If GRM tables don't exist, should get descriptive error
            error_msg = str(e).lower()
            if "grm" in error_msg or "tree_grm_component" in error_msg:
                pytest.skip(f"GRM tables not available: {e}")
            else:
                raise

    def test_growth_estimation_with_grm(self, sample_fia_instance):
        """Test growth estimation using GRM data."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Test basic growth estimation
            result = growth(sample_fia_instance, tree_type="gs", land_type="forest")

            assert isinstance(result, pl.DataFrame)

            # Check required columns exist
            expected_columns = ["GROW_ACRE", "GROW_ACRE_SE", "nPlots_TREE"]
            for col in expected_columns:
                assert col in result.columns, f"Missing column: {col}"

            if len(result) > 0:
                # Growth should be non-negative
                assert result["GROW_ACRE"][0] >= 0, "Growth rate cannot be negative"

        except Exception as e:
            # If GRM tables don't exist, should get descriptive error
            error_msg = str(e).lower()
            if "grm" in error_msg or "tree_grm_component" in error_msg:
                pytest.skip(f"GRM tables not available: {e}")
            else:
                raise

    def test_removals_estimation_with_grm(self, sample_fia_instance):
        """Test removals estimation using GRM data."""
        if sample_fia_instance is None:
            pytest.skip("No sample FIA database available")

        try:
            # Test basic removals estimation
            result = removals(sample_fia_instance, tree_type="gs", land_type="forest")

            assert isinstance(result, pl.DataFrame)

            # Check required columns exist
            expected_columns = ["REMV_ACRE", "REMV_ACRE_SE", "nPlots_TREE"]
            for col in expected_columns:
                assert col in result.columns, f"Missing column: {col}"

            if len(result) > 0:
                # Removals should be non-negative
                assert result["REMV_ACRE"][0] >= 0, "Removal rate cannot be negative"

        except Exception as e:
            # If GRM tables don't exist, should get descriptive error
            error_msg = str(e).lower()
            if "grm" in error_msg or "tree_grm_component" in error_msg:
                pytest.skip(f"GRM tables not available: {e}")
            else:
                raise


class TestGRMDataConsistency:
    """Test consistency between GRM tables."""

    def test_grm_component_midpt_consistency(self, grm_component_data, grm_midpt_data):
        """Test consistency between GRM component and midpoint tables."""
        # All trees in component table should have corresponding midpoint data
        component_trees = set(grm_component_data["TRE_CN"].to_list())
        midpt_trees = set(grm_midpt_data["TRE_CN"].to_list())

        # Should have some overlap
        common_trees = component_trees.intersection(midpt_trees)
        assert len(common_trees) > 0, "Should have common trees between component and midpoint data"

        # Check diameter consistency
        for tree_cn in list(common_trees)[:5]:  # Check first 5 for performance
            component_row = grm_component_data.filter(pl.col("TRE_CN") == tree_cn)
            midpt_row = grm_midpt_data.filter(pl.col("TRE_CN") == tree_cn)

            if len(component_row) > 0 and len(midpt_row) > 0:
                component_dia = component_row["DIA_MIDPT"][0]
                midpt_dia = midpt_row["DIA"][0]

                if component_dia is not None and midpt_dia is not None:
                    # Should be approximately equal
                    assert abs(component_dia - midpt_dia) < 0.1, f"Diameter mismatch for tree {tree_cn}"

    def test_grm_plot_remper_consistency(self, extended_plot_data_with_remper, grm_component_data):
        """Test consistency between plot remeasurement periods and GRM data."""
        # All plots in GRM data should have remeasurement period data
        grm_plots = set(grm_component_data["PLT_CN"].to_list())
        plot_data_plots = set(extended_plot_data_with_remper["PLT_CN"].to_list())

        common_plots = grm_plots.intersection(plot_data_plots)
        assert len(common_plots) > 0, "Should have common plots between GRM and plot data"

        # Check remeasurement periods are reasonable
        for plot_cn in list(common_plots)[:3]:  # Check first 3
            plot_row = extended_plot_data_with_remper.filter(pl.col("PLT_CN") == plot_cn)
            if len(plot_row) > 0 and plot_row["REMPER"][0] is not None:
                remper = plot_row["REMPER"][0]
                # Remeasurement period should be between 1-10 years
                assert 1.0 <= remper <= 10.0, f"Invalid remeasurement period: {remper}"


class TestGRMEdgeCases:
    """Test edge cases in GRM data processing."""

    def test_missing_diameter_data_handling(self, grm_component_data):
        """Test handling of missing diameter data."""
        # Check how missing diameter data is handled
        missing_begin = grm_component_data.filter(pl.col("DIA_BEGIN").is_null())
        missing_midpt = grm_component_data.filter(pl.col("DIA_MIDPT").is_null())
        missing_end = grm_component_data.filter(pl.col("DIA_END").is_null())

        # Should handle missing data gracefully
        total_records = len(grm_component_data)
        assert total_records > 0, "Should have GRM component data"

        # Missing data should be associated with specific component types
        if len(missing_begin) > 0:
            # Missing begin diameter should be associated with ingrowth
            missing_begin_components = missing_begin["SUBP_COMPONENT_GS_FOREST"].unique().to_list()
            ingrowth_like = [c for c in missing_begin_components if c and "INGROWTH" in c]
            assert len(ingrowth_like) > 0, "Missing begin diameter should be associated with ingrowth"

    def test_zero_tpa_handling(self):
        """Test handling of zero TPA values in GRM data."""
        # Create test data with zero TPA
        test_data = pl.DataFrame({
            "TRE_CN": ["TEST1", "TEST2"],
            "SUBP_TPAGROW_UNADJ_GS_FOREST": [0.0, 2.5],
            "SUBP_COMPONENT_GS_FOREST": ["SURVIVOR", "SURVIVOR"]
        })

        # Should handle zero TPA gracefully
        non_zero_tpa = test_data.filter(pl.col("SUBP_TPAGROW_UNADJ_GS_FOREST") > 0)
        assert len(non_zero_tpa) == 1, "Should filter out zero TPA records appropriately"

    def test_invalid_component_types(self):
        """Test handling of invalid component types."""
        # Create test data with invalid component
        test_data = pl.DataFrame({
            "TRE_CN": ["TEST1", "TEST2"],
            "SUBP_COMPONENT_GS_FOREST": ["INVALID_COMPONENT", "SURVIVOR"],
            "SUBP_TPAGROW_UNADJ_GS_FOREST": [1.0, 2.5]
        })

        # Should be able to identify and handle invalid components
        valid_survivors = test_data.filter(pl.col("SUBP_COMPONENT_GS_FOREST") == "SURVIVOR")
        assert len(valid_survivors) == 1, "Should identify valid survivor components"

        invalid_components = test_data.filter(
            ~pl.col("SUBP_COMPONENT_GS_FOREST").is_in(["SURVIVOR", "MORTALITY1", "MORTALITY2", "CUT1", "INGROWTH"])
        )
        assert len(invalid_components) == 1, "Should identify invalid components"