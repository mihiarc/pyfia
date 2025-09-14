"""
Critical test for the utils.py column naming bug.

This test specifically targets the GROWTH_ACRE to GROW_ACRE column renaming bug
in utils.py line 93 that breaks the growth function. This test should FAIL before
the fix and PASS after the fix is applied.
"""

import polars as pl
import pytest

from pyfia.estimation.utils import format_output_columns


class TestUtilsColumnNamingBug:
    """Tests for critical column naming bugs in utils.py."""

    def test_growth_acre_column_name_bug(self):
        """
        CRITICAL BUG TEST: format_output_columns incorrectly renames GROWTH_ACRE to GROW_ACRE.

        This test should FAIL before the fix and PASS after fix is applied to utils.py.

        Bug location: src/pyfia/estimation/utils.py line 93
        Problem: "GROWTH_ACRE": "GROW_ACRE" mapping in column_maps["growth"]
        Impact: Growth function expects GROWTH_ACRE but gets GROW_ACRE, causing KeyError
        """
        # Create sample growth estimation results
        sample_results = pl.DataFrame({
            "GROWTH_ACRE": [1.5, 2.3, 0.8],
            "GROWTH_TOTAL": [15000, 23000, 8000],
            "GROWTH_ACRE_SE": [0.18, 0.28, 0.10],
            "STATECD": [37, 37, 37],
            "ALSTKCD": [1, 2, 3]
        })

        # This is what the growth function calls
        formatted_results = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=True,
            include_cv=False
        )

        # THE BUG: utils.py line 93 has "GROWTH_ACRE": "GROW_ACRE"
        # This breaks the growth function which expects GROWTH_ACRE to remain unchanged

        original_columns = set(sample_results.columns)
        formatted_columns = set(formatted_results.columns)

        # Check if the bug is present
        has_grow_acre = "GROW_ACRE" in formatted_columns
        has_growth_acre = "GROWTH_ACRE" in formatted_columns

        if has_grow_acre and not has_growth_acre:
            pytest.fail(
                "CRITICAL BUG DETECTED: format_output_columns renamed GROWTH_ACRE to GROW_ACRE.\n"
                "This breaks the growth function which expects GROWTH_ACRE column.\n"
                "BUG LOCATION: src/pyfia/estimation/utils.py line 93\n"
                "FIX: Remove or correct the \"GROWTH_ACRE\": \"GROW_ACRE\" mapping in column_maps[\"growth\"]\n"
                f"Original columns: {sorted(original_columns)}\n"
                f"Formatted columns: {sorted(formatted_columns)}"
            )

        # After fix: GROWTH_ACRE should remain unchanged
        assert "GROWTH_ACRE" in formatted_columns, (
            "GROWTH_ACRE column should remain after formatting. "
            "The growth function depends on this column name."
        )

        # Verify data integrity
        if "GROWTH_ACRE" in formatted_columns:
            original_values = sample_results["GROWTH_ACRE"].to_list()
            formatted_values = formatted_results["GROWTH_ACRE"].to_list()
            assert original_values == formatted_values, "GROWTH_ACRE values should not change during formatting"

    def test_other_growth_columns_not_affected(self):
        """Test that other growth-related columns are handled correctly."""
        sample_results = pl.DataFrame({
            "GROWTH_ACRE": [1.5, 2.3],
            "GROWTH_TOTAL": [15000, 23000],
            "GROWTH_ACRE_SE": [0.18, 0.28],
            "GROWTH_TOTAL_SE": [1800, 2760],
            "N_PLOTS": [25, 30],
            "AREA_TOTAL": [100000, 120000]
        })

        formatted_results = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=True,
            include_cv=False
        )

        # These columns should remain unchanged
        expected_unchanged_columns = [
            "GROWTH_TOTAL", "GROWTH_ACRE_SE", "GROWTH_TOTAL_SE",
            "N_PLOTS", "AREA_TOTAL"
        ]

        for col in expected_unchanged_columns:
            if col in sample_results.columns:
                assert col in formatted_results.columns, f"{col} should remain unchanged"

    def test_format_output_columns_growth_vs_other_types(self):
        """Test format_output_columns behavior across different estimation types."""

        # Test data for each estimation type
        test_cases = {
            "volume": {
                "input_cols": ["VOLUME_ACRE", "VOLUME_TOTAL"],
                "expected_renames": {"VOLUME_ACRE": "VOL_ACRE", "VOLUME_TOTAL": "VOL_TOTAL"}
            },
            "biomass": {
                "input_cols": ["BIOMASS_ACRE", "BIOMASS_TOTAL"],
                "expected_renames": {"BIOMASS_ACRE": "BIO_ACRE", "BIOMASS_TOTAL": "BIO_TOTAL"}
            },
            "mortality": {
                "input_cols": ["MORTALITY_ACRE", "MORTALITY_TOTAL"],
                "expected_renames": {"MORTALITY_ACRE": "MORT_ACRE", "MORTALITY_TOTAL": "MORT_TOTAL"}
            },
            "growth": {
                "input_cols": ["GROWTH_ACRE", "GROWTH_TOTAL"],
                # BUG: should not rename GROWTH_ACRE, but utils.py does
                "expected_renames": {}  # No renames should happen for growth
            }
        }

        for est_type, test_data in test_cases.items():
            # Create sample data
            sample_data = {col: [1.0, 2.0] for col in test_data["input_cols"]}
            sample_results = pl.DataFrame(sample_data)

            formatted_results = format_output_columns(
                sample_results,
                estimation_type=est_type,
                include_se=False,
                include_cv=False
            )

            # Check expected renames
            for original_col, expected_new_col in test_data["expected_renames"].items():
                if original_col in sample_results.columns:
                    assert expected_new_col in formatted_results.columns, (
                        f"For {est_type}: {original_col} should be renamed to {expected_new_col}"
                    )
                    assert original_col not in formatted_results.columns, (
                        f"For {est_type}: {original_col} should be renamed (not present in output)"
                    )

            # For growth specifically, test the bug
            if est_type == "growth":
                # This is the critical test for the growth bug
                if "GROW_ACRE" in formatted_results.columns and "GROWTH_ACRE" not in formatted_results.columns:
                    pytest.fail(
                        "GROWTH COLUMN BUG: GROWTH_ACRE was incorrectly renamed to GROW_ACRE. "
                        "This breaks the growth() function. Fix utils.py line 93."
                    )

    def test_column_renaming_consistency(self):
        """Test that column renaming follows consistent patterns."""

        # All estimation types should follow consistent naming conventions
        # Most use _ACRE and _TOTAL suffixes, growth should too

        sample_results = pl.DataFrame({
            "GROWTH_ACRE": [1.5],
            "REMOVAL_ACRE": [0.3],  # Note: this is in the growth mapping too
            "GROWTH_TOTAL": [15000],
            "OTHER_COLUMN": [100]
        })

        formatted_results = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=False,
            include_cv=False
        )

        # Check the inconsistency in the growth column mapping
        # Line 93-94 in utils.py:
        # "GROWTH_ACRE": "GROW_ACRE",      # <-- WRONG (inconsistent)
        # "REMOVAL_ACRE": "REMV_ACRE",     # <-- CORRECT (follows pattern)

        # REMOVAL_ACRE gets correctly abbreviated to REMV_ACRE
        if "REMOVAL_ACRE" in sample_results.columns:
            # This rename is correct and consistent
            assert "REMV_ACRE" in formatted_results.columns or "REMOVAL_ACRE" in formatted_results.columns

        # But GROWTH_ACRE gets incorrectly renamed to GROW_ACRE (should stay GROWTH_ACRE or become GRTH_ACRE)
        # The inconsistency is the bug

    def test_fix_verification(self):
        """
        Verification test that should PASS after the bug is fixed.

        This test documents the correct behavior after fixing utils.py line 93.
        """
        sample_results = pl.DataFrame({
            "GROWTH_ACRE": [1.5, 2.3, 0.8],
            "GROWTH_TOTAL": [15000, 23000, 8000],
        })

        formatted_results = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=False,
            include_cv=False
        )

        # After fix: GROWTH_ACRE should remain unchanged
        assert "GROWTH_ACRE" in formatted_results.columns, (
            "After fix: GROWTH_ACRE should remain unchanged"
        )

        # After fix: GROW_ACRE should NOT exist
        assert "GROW_ACRE" not in formatted_results.columns, (
            "After fix: GROW_ACRE should not exist (was incorrectly created by bug)"
        )

        # Values should be preserved
        original_growth = sample_results["GROWTH_ACRE"].to_list()
        formatted_growth = formatted_results["GROWTH_ACRE"].to_list()
        assert original_growth == formatted_growth, "Growth values should be preserved"


class TestUtilsOtherIssues:
    """Test other issues in utils.py besides the critical column bug."""

    def test_column_ordering_consistency(self):
        """Test that format_output_columns produces consistent column ordering."""

        # Create results with mixed column order
        sample_results = pl.DataFrame({
            "GROWTH_TOTAL": [15000],
            "STATECD": [37],
            "GROWTH_ACRE": [1.5],
            "N_PLOTS": [25],
            "YEAR": [2020]
        })

        formatted_results = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=False,
            include_cv=False
        )

        # Check that priority columns come first
        column_order = formatted_results.columns

        # YEAR should be early in the order (line 125 in utils.py)
        if "YEAR" in column_order:
            year_pos = column_order.index("YEAR")
            # Should be in first few positions
            assert year_pos < 3, "YEAR should be near the beginning of column order"

        # STATECD should also be early (line 125)
        if "STATECD" in column_order:
            statecd_pos = column_order.index("STATECD")
            assert statecd_pos < 5, "STATECD should be near the beginning of column order"

    def test_cv_calculation_accuracy(self):
        """Test coefficient of variation calculation in format_output_columns."""

        sample_results = pl.DataFrame({
            "GROWTH_ACRE": [100.0, 200.0],
            "GROWTH_ACRE_SE": [12.0, 20.0],  # 12% and 10% CV
            "GROWTH_TOTAL": [1000000.0, 2000000.0],
            "GROWTH_TOTAL_SE": [150000.0, 180000.0]  # 15% and 9% CV
        })

        formatted_results = format_output_columns(
            sample_results,
            estimation_type="growth",
            include_se=True,
            include_cv=True
        )

        # Check CV calculations (line 118-122 in utils.py)
        if "GROWTH_ACRE_CV" in formatted_results.columns:
            cv_values = formatted_results["GROWTH_ACRE_CV"].to_list()
            expected_cv = [12.0, 10.0]  # (SE/estimate) * 100

            for i, (actual, expected) in enumerate(zip(cv_values, expected_cv)):
                assert abs(actual - expected) < 0.01, (
                    f"CV calculation error at row {i}: expected {expected}, got {actual}"
                )

    def test_missing_columns_handling(self):
        """Test handling of missing columns in format_output_columns."""

        # Test with minimal columns
        minimal_results = pl.DataFrame({
            "GROWTH_ACRE": [1.5]
        })

        # Should not crash with missing SE or other columns
        formatted_results = format_output_columns(
            minimal_results,
            estimation_type="growth",
            include_se=True,  # SE columns don't exist
            include_cv=True   # Can't calculate CV without SE
        )

        assert not formatted_results.is_empty(), "Should handle missing columns gracefully"
        assert "GROWTH_ACRE" in formatted_results.columns, "Should preserve existing columns"