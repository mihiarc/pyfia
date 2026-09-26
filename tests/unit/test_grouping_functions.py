"""
Tests for grouping_functions.py module.

This module provides utilities for grouping FIA data by various attributes
like size class, species, forest type, and ownership.
"""

import polars as pl
import pytest

from pyfia.constants.status_codes import LandStatus
from pyfia.filtering.utils import (
    add_forest_type_group,
    add_forest_type_group_code,
    add_land_type_column,
    add_ownership_group_name,
    add_species_info,
    auto_enhance_grouping_data,
    create_size_class_expr,
    get_forest_type_group,
    get_forest_type_group_code,
    get_ownership_group_name,
    get_size_class_bounds,
    prepare_plot_groups,
    setup_grouping_columns,
    validate_grouping_columns,
)


class TestCreateSizeClassExpr:
    """Tests for create_size_class_expr function."""

    def test_standard_size_classes(self):
        """Test standard numeric size class labels."""
        df = pl.DataFrame({"DIA": [2.0, 7.0, 15.0, 25.0, 35.0]})
        expr = create_size_class_expr("DIA", "standard")
        result = df.with_columns(expr)

        assert "SIZE_CLASS" in result.columns
        size_classes = result["SIZE_CLASS"].to_list()
        assert size_classes == ["1.0-4.9", "5.0-9.9", "10.0-19.9", "20.0-29.9", "30.0+"]

    def test_descriptive_size_classes(self):
        """Test descriptive size class labels."""
        df = pl.DataFrame({"DIA": [2.0, 7.0, 15.0, 25.0]})
        expr = create_size_class_expr("DIA", "descriptive")
        result = df.with_columns(expr)

        size_classes = result["SIZE_CLASS"].to_list()
        assert size_classes == ["Saplings", "Small", "Medium", "Large"]

    def test_custom_diameter_column(self):
        """Test using a custom diameter column name."""
        df = pl.DataFrame({"TREE_DIA": [3.0, 12.0]})
        expr = create_size_class_expr("TREE_DIA", "standard")
        result = df.with_columns(expr)

        assert result["SIZE_CLASS"].to_list() == ["1.0-4.9", "10.0-19.9"]

    def test_invalid_size_class_type(self):
        """Test that invalid size class type raises error."""
        with pytest.raises(ValueError, match="Invalid size_class_type"):
            create_size_class_expr("DIA", "invalid")

    def test_boundary_values_standard(self):
        """Test boundary values for standard size classes."""
        # Test exact boundary values
        df = pl.DataFrame({"DIA": [4.9, 5.0, 9.9, 10.0, 19.9, 20.0, 29.9, 30.0]})
        expr = create_size_class_expr("DIA", "standard")
        result = df.with_columns(expr)

        size_classes = result["SIZE_CLASS"].to_list()
        # 4.9 < 5.0, so it's in 1.0-4.9
        assert size_classes[0] == "1.0-4.9"
        # 5.0 >= 5.0 and < 10.0, so it's in 5.0-9.9
        assert size_classes[1] == "5.0-9.9"


class TestGetSizeClassBounds:
    """Tests for get_size_class_bounds function."""

    def test_standard_bounds(self):
        """Test standard size class bounds."""
        bounds = get_size_class_bounds("standard")
        assert "1.0-4.9" in bounds
        assert "5.0-9.9" in bounds
        assert "10.0-19.9" in bounds
        assert "20.0-29.9" in bounds
        assert "30.0+" in bounds

    def test_descriptive_bounds(self):
        """Test descriptive size class bounds."""
        bounds = get_size_class_bounds("descriptive")
        assert "Saplings" in bounds
        assert "Small" in bounds
        assert "Medium" in bounds
        assert "Large" in bounds

    def test_invalid_type_raises_error(self):
        """Test that invalid type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid size_class_type"):
            get_size_class_bounds("invalid")

    def test_returns_copy(self):
        """Test that function returns a copy, not original dict."""
        bounds1 = get_size_class_bounds("standard")
        bounds2 = get_size_class_bounds("standard")
        bounds1["test"] = (0, 0)
        assert "test" not in bounds2


class TestAddLandTypeColumn:
    """Tests for add_land_type_column function."""

    def test_forest_timber(self):
        """Test timber classification (productive, unreserved forest)."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [LandStatus.FOREST],
                "SITECLCD": [3],  # Productive
                "RESERVCD": [0],  # Not reserved
            }
        )
        result = add_land_type_column(df)
        assert result["LAND_TYPE"][0] == "Timber"

    def test_forest_non_timber(self):
        """Test non-timber forest (reserved)."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [LandStatus.FOREST],
                "SITECLCD": [3],
                "RESERVCD": [1],  # Reserved
            }
        )
        result = add_land_type_column(df)
        assert result["LAND_TYPE"][0] == "Non-timber forest"

    def test_nonforest(self):
        """Test non-forest land."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [LandStatus.NONFOREST],
                "SITECLCD": [0],
                "RESERVCD": [0],
            }
        )
        result = add_land_type_column(df)
        assert result["LAND_TYPE"][0] == "Non-forest"

    def test_water(self):
        """Test water classification."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [LandStatus.WATER],
                "SITECLCD": [0],
                "RESERVCD": [0],
            }
        )
        result = add_land_type_column(df)
        assert result["LAND_TYPE"][0] == "Water"

    def test_other_status(self):
        """Test other/unknown status codes."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [5],  # Some other code
                "SITECLCD": [0],
                "RESERVCD": [0],
            }
        )
        result = add_land_type_column(df)
        assert result["LAND_TYPE"][0] == "Other"

    def test_missing_columns_raises_error(self):
        """Test that missing columns raise ValueError."""
        df = pl.DataFrame({"COND_STATUS_CD": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            add_land_type_column(df)


class TestPreparePlotGroups:
    """Tests for prepare_plot_groups function."""

    def test_basic_groups(self):
        """Test basic grouping with default PLT_CN."""
        result = prepare_plot_groups(["SPCD"])
        assert result == ["PLT_CN", "SPCD"]

    def test_with_additional_groups(self):
        """Test adding additional groups."""
        result = prepare_plot_groups(["SPCD"], additional_groups=["YEAR"])
        assert result == ["PLT_CN", "SPCD", "YEAR"]

    def test_custom_always_include(self):
        """Test custom always_include columns."""
        result = prepare_plot_groups(["SPCD"], always_include=["EVALID", "PLT_CN"])
        assert result[0] == "EVALID"
        assert result[1] == "PLT_CN"
        assert "SPCD" in result

    def test_deduplication(self):
        """Test that duplicate columns are removed."""
        result = prepare_plot_groups(
            ["PLT_CN", "SPCD"],  # PLT_CN duplicated
            additional_groups=["SPCD"],  # SPCD duplicated
        )
        assert result.count("PLT_CN") == 1
        assert result.count("SPCD") == 1

    def test_preserves_order(self):
        """Test that order is preserved."""
        result = prepare_plot_groups(["A", "B", "C"], additional_groups=["D", "E"])
        assert result == ["PLT_CN", "A", "B", "C", "D", "E"]


class TestAddSpeciesInfo:
    """Tests for add_species_info function."""

    def test_no_species_df(self):
        """Test that function returns unchanged df when no species_df."""
        df = pl.DataFrame({"SPCD": [131, 316], "VALUE": [100, 200]})
        result = add_species_info(df, species_df=None)
        assert result.columns == ["SPCD", "VALUE"]

    def test_with_species_df(self):
        """Test joining species information."""
        df = pl.DataFrame({"SPCD": [131, 316], "VALUE": [100, 200]})
        species_df = pl.DataFrame(
            {
                "SPCD": [131, 316],
                "COMMON_NAME": ["Loblolly pine", "Red maple"],
                "GENUS": ["Pinus", "Acer"],
            }
        )
        result = add_species_info(df, species_df)
        assert "COMMON_NAME" in result.columns
        assert result["COMMON_NAME"].to_list() == ["Loblolly pine", "Red maple"]

    def test_include_genus(self):
        """Test including genus information."""
        df = pl.DataFrame({"SPCD": [131]})
        species_df = pl.DataFrame(
            {
                "SPCD": [131],
                "COMMON_NAME": ["Loblolly pine"],
                "GENUS": ["Pinus"],
            }
        )
        result = add_species_info(df, species_df, include_genus=True)
        assert "GENUS" in result.columns
        assert result["GENUS"][0] == "Pinus"

    def test_missing_spcd_raises_error(self):
        """Test that missing SPCD column raises error."""
        df = pl.DataFrame({"VALUE": [100]})
        with pytest.raises(ValueError, match="SPCD column not found"):
            add_species_info(df)


class TestValidateGroupingColumns:
    """Tests for validate_grouping_columns function."""

    def test_valid_columns(self):
        """Test validation passes with valid columns."""
        df = pl.DataFrame({"A": [1], "B": [2], "C": [3]})
        # Should not raise
        validate_grouping_columns(df, ["A", "B"])

    def test_missing_columns_raises_error(self):
        """Test that missing columns raise error."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError):
            validate_grouping_columns(df, ["A", "B", "C"])


class TestGetForestTypeGroup:
    """Tests for get_forest_type_group (REF equality is in test_forest_type_groups)."""

    def test_none_returns_unknown(self):
        assert get_forest_type_group(None) == "Unknown"

    @pytest.mark.parametrize(
        "fortypcd, name",
        [
            (161, "Loblolly / shortleaf pine group"),
            (162, "Loblolly / shortleaf pine group"),
            (141, "Longleaf / slash pine group"),
            (142, "Longleaf / slash pine group"),
            (103, "White / red / jack pine group"),
            (201, "Douglas-fir group"),
            (503, "Oak / hickory group"),
            (999, "Nonstocked"),
        ],
    )
    def test_ref_names(self, fortypcd, name):
        assert get_forest_type_group(fortypcd) == name

    def test_unknown_code_warns(self):
        with pytest.warns(UserWarning, match=r"FORTYPCD \[12345\]"):
            assert get_forest_type_group(12345) == "Unknown"


class TestAddForestTypeGroup:
    """Tests for add_forest_type_group function."""

    def test_adds_column(self):
        """Test that function adds FOREST_TYPE_GROUP column."""
        df = pl.DataFrame({"FORTYPCD": [161, 503, 700]})
        result = add_forest_type_group(df)
        assert result["FOREST_TYPE_GROUP"].to_list() == [
            "Loblolly / shortleaf pine group",
            "Oak / hickory group",
            "Elm / ash / cottonwood group",
        ]

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        df = pl.DataFrame({"MY_FORTYP": [500]})
        result = add_forest_type_group(
            df, fortypcd_col="MY_FORTYP", output_col="FORTYP_NAME"
        )
        assert result["FORTYP_NAME"][0] == "Oak / hickory group"

    def test_null_and_unknown_codes(self):
        """Nulls and unmatched codes get "Unknown"; one warning lists the codes."""
        df = pl.DataFrame({"FORTYPCD": [161, None, 12345, 12345, 998]})
        with pytest.warns(UserWarning, match=r"\[998, 12345\]") as record:
            result = add_forest_type_group(df)
        assert len(record) == 1
        assert result["FOREST_TYPE_GROUP"].to_list()[1:] == ["Unknown"] * 4


class TestGetOwnershipGroupName:
    """Tests for get_ownership_group_name function."""

    def test_forest_service(self):
        """Test Forest Service code."""
        assert get_ownership_group_name(10) == "Forest Service"

    def test_other_federal(self):
        """Test Other Federal code."""
        assert get_ownership_group_name(20) == "Other Federal"

    def test_state_local(self):
        """Test State and Local Government code."""
        assert get_ownership_group_name(30) == "State and Local Government"

    def test_private(self):
        """Test Private code."""
        assert get_ownership_group_name(40) == "Private"

    def test_unknown_code(self):
        """Test unknown code returns formatted string."""
        result = get_ownership_group_name(99)
        assert "Unknown" in result
        assert "99" in result

    def test_none_returns_unknown(self):
        """Test None returns unknown message."""
        result = get_ownership_group_name(None)
        assert "Unknown" in result


class TestAddOwnershipGroupName:
    """Tests for add_ownership_group_name function."""

    def test_adds_column(self):
        """Test that function adds OWNERSHIP_GROUP column."""
        df = pl.DataFrame({"OWNGRPCD": [10, 20, 30, 40]})
        result = add_ownership_group_name(df)
        assert "OWNERSHIP_GROUP" in result.columns
        assert result["OWNERSHIP_GROUP"].to_list() == [
            "Forest Service",
            "Other Federal",
            "State and Local Government",
            "Private",
        ]

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        df = pl.DataFrame({"MY_OWN": [40]})
        result = add_ownership_group_name(
            df, owngrpcd_col="MY_OWN", output_col="OWN_NAME"
        )
        assert "OWN_NAME" in result.columns
        assert result["OWN_NAME"][0] == "Private"


class TestGetForestTypeGroupCode:
    """Tests for get_forest_type_group_code (REF equality is in test_forest_type_groups)."""

    def test_none_returns_none(self):
        assert get_forest_type_group_code(None) is None

    @pytest.mark.parametrize(
        "fortypcd, group",
        [(161, 160), (162, 160), (141, 140), (142, 140), (221, 220), (503, 500)],
    )
    def test_ref_group_codes(self, fortypcd, group):
        assert get_forest_type_group_code(fortypcd) == group

    def test_unknown_code_warns(self):
        with pytest.warns(UserWarning, match="REF_FOREST_TYPE"):
            assert get_forest_type_group_code(12345) is None


class TestAddForestTypeGroupCode:
    """Tests for add_forest_type_group_code function."""

    def test_adds_column(self):
        """Test that function adds FORTYPGRP column."""
        df = pl.DataFrame({"FORTYPCD": [200, 221, 161, None]})
        result = add_forest_type_group_code(df)
        assert result["FORTYPGRP"].to_list() == [200, 220, 160, None]

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        df = pl.DataFrame({"MY_FORTYP": [200]})
        result = add_forest_type_group_code(
            df, fortypcd_col="MY_FORTYP", output_col="MY_GROUP"
        )
        assert "MY_GROUP" in result.columns


class TestSetupGroupingColumns:
    """Tests for setup_grouping_columns function."""

    def test_no_grouping(self):
        """Test with no grouping specified."""
        df = pl.DataFrame({"VALUE": [1, 2, 3]})
        result_df, group_cols = setup_grouping_columns(df)
        assert group_cols == []
        assert result_df.equals(df)

    def test_single_grp_by(self):
        """Test with single grp_by column."""
        df = pl.DataFrame({"FORTYPCD": [161], "VALUE": [100]})
        result_df, group_cols = setup_grouping_columns(df, grp_by="FORTYPCD")
        assert group_cols == ["FORTYPCD"]

    def test_multiple_grp_by(self):
        """Test with multiple grp_by columns."""
        df = pl.DataFrame({"FORTYPCD": [161], "OWNGRPCD": [40], "VALUE": [100]})
        result_df, group_cols = setup_grouping_columns(
            df, grp_by=["FORTYPCD", "OWNGRPCD"]
        )
        assert group_cols == ["FORTYPCD", "OWNGRPCD"]

    def test_by_species(self):
        """Test grouping by species."""
        df = pl.DataFrame({"SPCD": [131], "VALUE": [100]})
        result_df, group_cols = setup_grouping_columns(df, by_species=True)
        assert "SPCD" in group_cols

    def test_by_species_missing_column(self):
        """Test by_species raises error when SPCD missing."""
        df = pl.DataFrame({"VALUE": [100]})
        with pytest.raises(ValueError):
            setup_grouping_columns(df, by_species=True)

    def test_by_size_class_standard(self):
        """Test grouping by size class with standard labels."""
        df = pl.DataFrame({"DIA": [3.0, 7.0, 15.0], "VALUE": [100, 200, 300]})
        result_df, group_cols = setup_grouping_columns(df, by_size_class=True)
        assert "SIZE_CLASS" in group_cols
        assert "SIZE_CLASS" in result_df.columns
        assert result_df["SIZE_CLASS"].to_list() == ["1.0-4.9", "5.0-9.9", "10.0-19.9"]

    def test_by_size_class_descriptive(self):
        """Test grouping by size class with descriptive labels."""
        df = pl.DataFrame({"DIA": [3.0, 7.0, 15.0]})
        result_df, group_cols = setup_grouping_columns(
            df, by_size_class=True, size_class_type="descriptive"
        )
        assert result_df["SIZE_CLASS"].to_list() == ["Saplings", "Small", "Medium"]

    def test_by_size_class_custom_dia_col(self):
        """Test size class with custom diameter column."""
        df = pl.DataFrame({"TREE_DIA": [3.0, 15.0]})
        result_df, group_cols = setup_grouping_columns(
            df, by_size_class=True, dia_col="TREE_DIA"
        )
        assert "SIZE_CLASS" in result_df.columns

    def test_by_size_class_missing_dia(self):
        """Test by_size_class raises error when DIA missing."""
        df = pl.DataFrame({"VALUE": [100]})
        with pytest.raises(ValueError):
            setup_grouping_columns(df, by_size_class=True)

    def test_by_land_type(self):
        """Test grouping by land type."""
        df = pl.DataFrame({"LAND_TYPE": ["Timber", "Non-forest"], "VALUE": [100, 200]})
        result_df, group_cols = setup_grouping_columns(df, by_land_type=True)
        assert "LAND_TYPE" in group_cols

    def test_by_land_type_missing_column(self):
        """Test by_land_type raises error when LAND_TYPE missing."""
        df = pl.DataFrame({"VALUE": [100]})
        with pytest.raises(ValueError):
            setup_grouping_columns(df, by_land_type=True)

    def test_combined_groupings(self):
        """Test combining multiple grouping options."""
        df = pl.DataFrame(
            {
                "SPCD": [131],
                "DIA": [15.0],
                "FORTYPCD": [161],
            }
        )
        result_df, group_cols = setup_grouping_columns(
            df, grp_by="FORTYPCD", by_species=True, by_size_class=True
        )
        assert "FORTYPCD" in group_cols
        assert "SPCD" in group_cols
        assert "SIZE_CLASS" in group_cols

    def test_deduplication(self):
        """Test that duplicate columns are removed."""
        df = pl.DataFrame({"SPCD": [131], "VALUE": [100]})
        result_df, group_cols = setup_grouping_columns(
            df, grp_by=["SPCD", "SPCD"], by_species=True
        )
        assert group_cols.count("SPCD") == 1


class TestAutoEnhanceGroupingData:
    """Tests for auto_enhance_grouping_data function."""

    def test_enhance_fortypcd(self):
        """Test FORTYPCD enhancement."""
        df = pl.DataFrame({"FORTYPCD": [161, 503], "VALUE": [100, 200]})
        result_df, result_cols = auto_enhance_grouping_data(df, ["FORTYPCD"])

        assert "FOREST_TYPE_GROUP" in result_df.columns
        assert "FORTYPGRP" in result_df.columns
        assert "FORTYPCD" in result_df.columns  # Original preserved

    def test_enhance_owngrpcd(self):
        """Test OWNGRPCD enhancement."""
        df = pl.DataFrame({"OWNGRPCD": [10, 40], "VALUE": [100, 200]})
        result_df, result_cols = auto_enhance_grouping_data(df, ["OWNGRPCD"])

        assert "OWNERSHIP_GROUP" in result_df.columns
        assert "OWNGRPCD" in result_df.columns

    def test_enhance_both(self):
        """Test enhancing both FORTYPCD and OWNGRPCD."""
        df = pl.DataFrame(
            {
                "FORTYPCD": [161],
                "OWNGRPCD": [40],
                "VALUE": [100],
            }
        )
        result_df, result_cols = auto_enhance_grouping_data(
            df, ["FORTYPCD", "OWNGRPCD"]
        )

        assert "FOREST_TYPE_GROUP" in result_df.columns
        assert "OWNERSHIP_GROUP" in result_df.columns

    def test_no_preservation(self):
        """Test without preserving reference columns."""
        df = pl.DataFrame({"FORTYPCD": [161], "VALUE": [100]})
        result_df, result_cols = auto_enhance_grouping_data(
            df, ["FORTYPCD"], preserve_reference_columns=False
        )

        # FORTYPCD should be replaced with FORTYPGRP in group_cols
        assert "FORTYPGRP" in result_cols
        # But FORTYPCD might not be in result_cols anymore
        assert "FORTYPCD" not in result_cols or "FORTYPGRP" in result_cols

    def test_column_not_in_df(self):
        """Test when group column not in dataframe."""
        df = pl.DataFrame({"VALUE": [100]})
        result_df, result_cols = auto_enhance_grouping_data(df, ["FORTYPCD"])

        # Should not add enhancement columns when source column missing
        assert "FOREST_TYPE_GROUP" not in result_df.columns

    def test_preserves_other_columns(self):
        """Test that other columns are preserved."""
        df = pl.DataFrame(
            {
                "FORTYPCD": [161],
                "OTHER_COL": ["test"],
                "VALUE": [100],
            }
        )
        result_df, result_cols = auto_enhance_grouping_data(df, ["FORTYPCD"])

        assert "OTHER_COL" in result_df.columns
        assert result_df["OTHER_COL"][0] == "test"
