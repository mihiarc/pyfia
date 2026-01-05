"""
Tests for classification.py module.

This module provides functions for classifying trees and plots based on
FIA specifications such as tree basis assignment, size classes, and
forest type groupings.
"""

import polars as pl
import pytest

from pyfia.filtering.utils import (
    assign_forest_type_group,
    assign_size_class,
    assign_species_group,
    assign_tree_basis,
    validate_classification_columns,
)
from pyfia.constants.plot_design import PlotBasis


class TestAssignTreeBasis:
    """Tests for assign_tree_basis function."""

    def test_microplot_trees(self):
        """Test trees < 5.0 DIA are assigned to microplot."""
        tree_df = pl.DataFrame({"DIA": [1.0, 2.5, 4.9], "PLT_CN": [1, 1, 1]})
        result = assign_tree_basis(tree_df, include_macro=False)

        assert "TREE_BASIS" in result.columns
        assert all(b == PlotBasis.MICROPLOT for b in result["TREE_BASIS"].to_list())

    def test_subplot_trees_no_macro(self):
        """Test trees >= 5.0 DIA are assigned to subplot when no macro."""
        tree_df = pl.DataFrame({"DIA": [5.0, 10.0, 25.0], "PLT_CN": [1, 1, 1]})
        result = assign_tree_basis(tree_df, include_macro=False)

        assert all(b == PlotBasis.SUBPLOT for b in result["TREE_BASIS"].to_list())

    def test_with_macroplot_breakpoint(self):
        """Test macroplot assignment when tree DIA >= breakpoint."""
        tree_df = pl.DataFrame({
            "DIA": [5.0, 20.0, 25.0, 30.0],
            "PLT_CN": [1, 1, 1, 1],
        })
        plot_df = pl.DataFrame({
            "PLT_CN": [1],
            "MACRO_BREAKPOINT_DIA": [24.0],
        })
        result = assign_tree_basis(tree_df, plot_df, include_macro=True)

        basis_list = result["TREE_BASIS"].to_list()
        # 5.0 < 24.0 → SUBP
        assert basis_list[0] == PlotBasis.SUBPLOT
        # 20.0 < 24.0 → SUBP
        assert basis_list[1] == PlotBasis.SUBPLOT
        # 25.0 >= 24.0 → MACR
        assert basis_list[2] == PlotBasis.MACROPLOT
        # 30.0 >= 24.0 → MACR
        assert basis_list[3] == PlotBasis.MACROPLOT

    def test_macroplot_breakpoint_zero(self):
        """Test that zero breakpoint means no macroplot."""
        tree_df = pl.DataFrame({
            "DIA": [30.0],
            "PLT_CN": [1],
        })
        plot_df = pl.DataFrame({
            "PLT_CN": [1],
            "MACRO_BREAKPOINT_DIA": [0],
        })
        result = assign_tree_basis(tree_df, plot_df, include_macro=True)

        assert result["TREE_BASIS"][0] == PlotBasis.SUBPLOT

    def test_macroplot_breakpoint_null(self):
        """Test that null breakpoint means no macroplot."""
        tree_df = pl.DataFrame({
            "DIA": [30.0],
            "PLT_CN": [1],
        })
        plot_df = pl.DataFrame({
            "PLT_CN": [1],
            "MACRO_BREAKPOINT_DIA": [None],
        })
        result = assign_tree_basis(tree_df, plot_df, include_macro=True)

        assert result["TREE_BASIS"][0] == PlotBasis.SUBPLOT

    def test_null_diameter_returns_null(self):
        """Test that null diameter returns null tree basis."""
        tree_df = pl.DataFrame({
            "DIA": [None, 10.0],
            "PLT_CN": [1, 1],
        })
        plot_df = pl.DataFrame({
            "PLT_CN": [1],
            "MACRO_BREAKPOINT_DIA": [24.0],
        })
        result = assign_tree_basis(tree_df, plot_df, include_macro=True)

        assert result["TREE_BASIS"][0] is None
        assert result["TREE_BASIS"][1] == PlotBasis.SUBPLOT

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        tree_df = pl.DataFrame({
            "TREE_DIA": [3.0, 15.0],
            "PLT_CN": [1, 1],
        })
        result = assign_tree_basis(
            tree_df,
            include_macro=False,
            dia_column="TREE_DIA",
            output_column="MY_BASIS",
        )

        assert "MY_BASIS" in result.columns
        assert result["MY_BASIS"][0] == PlotBasis.MICROPLOT
        assert result["MY_BASIS"][1] == PlotBasis.SUBPLOT

    def test_plot_with_cn_column(self):
        """Test joining plot data when plot has CN instead of PLT_CN."""
        tree_df = pl.DataFrame({
            "DIA": [25.0],
            "PLT_CN": [1],
        })
        plot_df = pl.DataFrame({
            "CN": [1],
            "MACRO_BREAKPOINT_DIA": [20.0],
        })
        result = assign_tree_basis(tree_df, plot_df, include_macro=True)

        assert result["TREE_BASIS"][0] == PlotBasis.MACROPLOT

    def test_macro_breakpoint_already_in_tree_df(self):
        """Test when MACRO_BREAKPOINT_DIA already in tree dataframe."""
        tree_df = pl.DataFrame({
            "DIA": [25.0],
            "PLT_CN": [1],
            "MACRO_BREAKPOINT_DIA": [20.0],
        })
        # Need to provide plot_df for macroplot logic to be used
        # When plot_df is None, falls back to simple MICR/SUBP assignment
        plot_df = pl.DataFrame({
            "PLT_CN": [1],
            "MACRO_BREAKPOINT_DIA": [20.0],  # Will be skipped since already in tree_df
        })
        result = assign_tree_basis(tree_df, plot_df, include_macro=True)

        assert result["TREE_BASIS"][0] == PlotBasis.MACROPLOT


class TestAssignSizeClass:
    """Tests for assign_size_class function."""

    def test_standard_size_classes(self):
        """Test standard size class assignment."""
        tree_df = pl.DataFrame({"DIA": [2.0, 7.0, 15.0, 25.0]})
        result = assign_size_class(tree_df, class_system="standard")

        assert "SIZE_CLASS" in result.columns
        classes = result["SIZE_CLASS"].to_list()
        assert classes == ["Saplings", "Small", "Medium", "Large"]

    def test_detailed_size_classes(self):
        """Test detailed size class assignment."""
        tree_df = pl.DataFrame({"DIA": [0.5, 3.0, 7.0, 12.0, 20.0, 30.0]})
        result = assign_size_class(tree_df, class_system="detailed")

        classes = result["SIZE_CLASS"].to_list()
        assert classes == ["Seedlings", "Saplings", "Small", "Medium", "Large", "Very Large"]

    def test_simple_size_classes(self):
        """Test simple size class assignment."""
        tree_df = pl.DataFrame({"DIA": [5.0, 15.0]})
        result = assign_size_class(tree_df, class_system="simple")

        classes = result["SIZE_CLASS"].to_list()
        assert classes == ["Small", "Large"]

    def test_invalid_class_system(self):
        """Test that invalid class system raises error."""
        tree_df = pl.DataFrame({"DIA": [10.0]})
        with pytest.raises(ValueError, match="Unknown class_system"):
            assign_size_class(tree_df, class_system="invalid")

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        tree_df = pl.DataFrame({"TREE_DIA": [7.0]})
        result = assign_size_class(
            tree_df,
            dia_column="TREE_DIA",
            output_column="MY_SIZE",
        )

        assert "MY_SIZE" in result.columns
        assert result["MY_SIZE"][0] == "Small"

    def test_boundary_values(self):
        """Test boundary values for size classes."""
        tree_df = pl.DataFrame({"DIA": [4.9, 5.0, 9.9, 10.0, 19.9, 20.0]})
        result = assign_size_class(tree_df, class_system="standard")

        classes = result["SIZE_CLASS"].to_list()
        # 4.9 < 5.0 → Saplings
        assert classes[0] == "Saplings"
        # 5.0 >= 5.0 and < 10.0 → Small
        assert classes[1] == "Small"
        # 9.9 < 10.0 → Small
        assert classes[2] == "Small"
        # 10.0 >= 10.0 and < 20.0 → Medium
        assert classes[3] == "Medium"
        # 19.9 < 20.0 → Medium
        assert classes[4] == "Medium"
        # 20.0 >= 20.0 → Large
        assert classes[5] == "Large"


class TestAssignForestTypeGroup:
    """Tests for assign_forest_type_group function.

    Note: This function is deprecated and now delegates to add_forest_type_group
    from grouping_functions, which has more accurate western forest type handling.
    """

    def test_deprecation_warning(self):
        """Test that deprecation warning is raised."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [100]})

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assign_forest_type_group(cond_df)

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()

    def test_white_red_jack_pine(self):
        """Test 100-199 range returns White/Red/Jack Pine."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [100, 150, 199]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "White/Red/Jack Pine" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_spruce_fir_and_western_types(self):
        """Test 200-299 range returns Spruce/Fir or western forest type variants.

        Note: The new implementation has more granular western forest type handling.
        Code 200 returns 'Douglas-fir', 250/290 return 'Spruce/Fir'.
        """
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [250, 290]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        # These non-special codes should still return Spruce/Fir
        assert all(g == "Spruce/Fir" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_douglas_fir_specific(self):
        """Test code 200 returns Douglas-fir specifically."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [200]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert result["FOREST_TYPE_GROUP"][0] == "Douglas-fir"

    def test_longleaf_slash_pine(self):
        """Test 300-399 range returns Longleaf/Slash Pine or western variants."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [350, 399]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "Longleaf/Slash Pine" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_oak_pine(self):
        """Test 400-499 range returns Oak/Pine."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [400, 450, 499]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "Oak/Pine" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_oak_hickory(self):
        """Test 500-599 range returns Oak/Hickory."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [500, 550, 599]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "Oak/Hickory" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_oak_gum_cypress(self):
        """Test 600-699 range returns Oak/Gum/Cypress."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [600, 650, 699]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "Oak/Gum/Cypress" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_elm_ash_cottonwood(self):
        """Test 700-799 range returns Elm/Ash/Cottonwood."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [700, 750, 799]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "Elm/Ash/Cottonwood" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_maple_beech_birch(self):
        """Test 800-899 range returns Maple/Beech/Birch."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [800, 850, 899]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        assert all(g == "Maple/Beech/Birch" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_900_range_has_variants(self):
        """Test 900-999 range has various western hardwood types.

        Note: The new implementation distinguishes between Aspen/Birch,
        Alder/Maple, Western Oak, etc. in the 900 range.
        """
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [900, 950, 999]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        groups = result["FOREST_TYPE_GROUP"].to_list()
        # 900 should be Aspen/Birch, 950 Other Western Hardwoods, 999 Nonstocked
        assert groups[0] == "Aspen/Birch"
        assert groups[1] == "Other Western Hardwoods"
        assert groups[2] == "Nonstocked"

    def test_other_unknown(self):
        """Test out-of-range codes return Other."""
        import warnings
        cond_df = pl.DataFrame({"FORTYPCD": [50, 1000, 0]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(cond_df)

        # The new implementation returns "Other" for out-of-range codes
        assert all(g == "Other" for g in result["FOREST_TYPE_GROUP"].to_list())

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        import warnings
        cond_df = pl.DataFrame({"MY_FORTYP": [500]})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = assign_forest_type_group(
                cond_df,
                fortypcd_column="MY_FORTYP",
                output_column="MY_GROUP",
            )

        assert "MY_GROUP" in result.columns
        assert result["MY_GROUP"][0] == "Oak/Hickory"


class TestAssignSpeciesGroup:
    """Tests for assign_species_group function."""

    def test_major_species_southern_pines(self):
        """Test major species grouping for southern pines."""
        tree_df = pl.DataFrame({"SPCD": [131, 132, 133]})
        species_df = pl.DataFrame({
            "SPCD": [131, 132, 133],
            "GENUS": ["Pinus", "Pinus", "Pinus"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert "SPECIES_GROUP" in result.columns
        assert all(g == "Southern Pines" for g in result["SPECIES_GROUP"].to_list())

    def test_major_species_maples(self):
        """Test major species grouping for maples."""
        tree_df = pl.DataFrame({"SPCD": [316, 318, 319]})
        species_df = pl.DataFrame({
            "SPCD": [316, 318, 319],
            "GENUS": ["Acer", "Acer", "Acer"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert all(g == "Maples" for g in result["SPECIES_GROUP"].to_list())

    def test_major_species_oaks_by_code(self):
        """Test major species grouping for oaks by SPCD."""
        tree_df = pl.DataFrame({"SPCD": [800, 801, 802]})
        species_df = pl.DataFrame({
            "SPCD": [800, 801, 802],
            "GENUS": ["Quercus", "Quercus", "Quercus"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert all(g == "Oaks" for g in result["SPECIES_GROUP"].to_list())

    def test_major_species_oaks_by_genus(self):
        """Test major species grouping for oaks by genus."""
        tree_df = pl.DataFrame({"SPCD": [812]})  # Not in 800-804 range
        species_df = pl.DataFrame({
            "SPCD": [812],
            "GENUS": ["Quercus"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert result["SPECIES_GROUP"][0] == "Oaks"

    def test_major_species_pines_by_genus(self):
        """Test major species grouping for pines by genus."""
        tree_df = pl.DataFrame({"SPCD": [110]})  # Not in 131-133 range
        species_df = pl.DataFrame({
            "SPCD": [110],
            "GENUS": ["Pinus"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert result["SPECIES_GROUP"][0] == "Pines"

    def test_major_species_maples_by_genus(self):
        """Test major species grouping for maples by genus."""
        tree_df = pl.DataFrame({"SPCD": [310]})  # Not in 316-319 range
        species_df = pl.DataFrame({
            "SPCD": [310],
            "GENUS": ["Acer"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert result["SPECIES_GROUP"][0] == "Maples"

    def test_major_species_other_genus(self):
        """Test major species grouping falls back to genus for others."""
        tree_df = pl.DataFrame({"SPCD": [611]})
        species_df = pl.DataFrame({
            "SPCD": [611],
            "GENUS": ["Liquidambar"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="major_species")

        assert result["SPECIES_GROUP"][0] == "Liquidambar"

    def test_genus_grouping(self):
        """Test grouping by genus."""
        tree_df = pl.DataFrame({"SPCD": [131, 316]})
        species_df = pl.DataFrame({
            "SPCD": [131, 316],
            "GENUS": ["Pinus", "Acer"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="genus")

        assert result["SPECIES_GROUP"].to_list() == ["Pinus", "Acer"]

    def test_family_grouping(self):
        """Test grouping by family."""
        tree_df = pl.DataFrame({"SPCD": [131, 316]})
        species_df = pl.DataFrame({
            "SPCD": [131, 316],
            "GENUS": ["Pinus", "Acer"],
            "FAMILY": ["Pinaceae", "Sapindaceae"],
        })
        result = assign_species_group(tree_df, species_df, grouping_system="family")

        assert result["SPECIES_GROUP"].to_list() == ["Pinaceae", "Sapindaceae"]

    def test_invalid_grouping_system(self):
        """Test that invalid grouping system raises error."""
        tree_df = pl.DataFrame({"SPCD": [131]})
        species_df = pl.DataFrame({"SPCD": [131], "GENUS": ["Pinus"]})

        with pytest.raises(ValueError, match="Unknown grouping_system"):
            assign_species_group(tree_df, species_df, grouping_system="invalid")

    def test_custom_column_names(self):
        """Test custom input and output column names."""
        tree_df = pl.DataFrame({"MY_SPCD": [131]})
        species_df = pl.DataFrame({
            "MY_SPCD": [131],
            "GENUS": ["Pinus"],
        })
        result = assign_species_group(
            tree_df,
            species_df,
            spcd_column="MY_SPCD",
            output_column="MY_GROUP",
            grouping_system="genus",
        )

        assert "MY_GROUP" in result.columns
        assert result["MY_GROUP"][0] == "Pinus"


class TestValidateClassificationColumns:
    """Tests for validate_classification_columns function."""

    def test_tree_basis_valid(self):
        """Test validation for tree_basis with valid columns."""
        df = pl.DataFrame({"DIA": [10.0], "OTHER": [1]})
        assert validate_classification_columns(df, "tree_basis") is True

    def test_tree_basis_missing(self):
        """Test validation for tree_basis with missing columns."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="Missing required columns.*tree_basis"):
            validate_classification_columns(df, "tree_basis")

    def test_size_class_valid(self):
        """Test validation for size_class with valid columns."""
        df = pl.DataFrame({"DIA": [10.0]})
        assert validate_classification_columns(df, "size_class") is True

    def test_size_class_missing(self):
        """Test validation for size_class with missing columns."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="Missing required columns.*size_class"):
            validate_classification_columns(df, "size_class")

    def test_prop_basis_valid(self):
        """Test validation for prop_basis with valid columns."""
        df = pl.DataFrame({"MACRO_BREAKPOINT_DIA": [24.0]})
        assert validate_classification_columns(df, "prop_basis") is True

    def test_prop_basis_missing(self):
        """Test validation for prop_basis with missing columns."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="Missing required columns.*prop_basis"):
            validate_classification_columns(df, "prop_basis")

    def test_forest_type_valid(self):
        """Test validation for forest_type with valid columns."""
        df = pl.DataFrame({"FORTYPCD": [500]})
        assert validate_classification_columns(df, "forest_type") is True

    def test_forest_type_missing(self):
        """Test validation for forest_type with missing columns."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="Missing required columns.*forest_type"):
            validate_classification_columns(df, "forest_type")

    def test_land_use_valid(self):
        """Test validation for land_use with valid columns."""
        df = pl.DataFrame({"COND_STATUS_CD": [1], "RESERVCD": [0]})
        assert validate_classification_columns(df, "land_use") is True

    def test_land_use_missing(self):
        """Test validation for land_use with missing columns."""
        df = pl.DataFrame({"COND_STATUS_CD": [1]})
        with pytest.raises(ValueError, match="Missing required columns.*land_use"):
            validate_classification_columns(df, "land_use")

    def test_species_group_valid(self):
        """Test validation for species_group with valid columns."""
        df = pl.DataFrame({"SPCD": [131]})
        assert validate_classification_columns(df, "species_group") is True

    def test_species_group_missing(self):
        """Test validation for species_group with missing columns."""
        df = pl.DataFrame({"OTHER": [1]})
        with pytest.raises(ValueError, match="Missing required columns.*species_group"):
            validate_classification_columns(df, "species_group")

    def test_unknown_classification_type(self):
        """Test that unknown classification type raises error."""
        df = pl.DataFrame({"DIA": [10.0]})
        with pytest.raises(ValueError, match="Unknown classification_type"):
            validate_classification_columns(df, "unknown_type")

    def test_custom_required_columns(self):
        """Test validation with custom required columns."""
        df = pl.DataFrame({"A": [1], "B": [2]})
        assert validate_classification_columns(
            df, "tree_basis", required_columns=["A", "B"]
        ) is True

    def test_custom_required_columns_missing(self):
        """Test validation with custom required columns missing."""
        df = pl.DataFrame({"A": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_classification_columns(
                df, "tree_basis", required_columns=["A", "B", "C"]
            )
