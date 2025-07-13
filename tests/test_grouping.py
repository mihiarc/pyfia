"""Tests for the grouping module."""

import polars as pl
import pytest

from pyfia.filters.grouping import (
    DESCRIPTIVE_SIZE_CLASSES,
    STANDARD_SIZE_CLASSES,
    add_land_type_column,
    add_species_info,
    create_size_class_expr,
    get_size_class_bounds,
    prepare_plot_groups,
    setup_grouping_columns,
    validate_grouping_columns,
)


@pytest.fixture
def sample_tree_df():
    """Create sample tree data for testing."""
    return pl.DataFrame({
        "CN": ["1", "2", "3", "4", "5", "6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "SPCD": [110, 121, 110, 202, 316, 121],
        "DIA": [3.5, 6.2, 12.5, 25.0, 4.8, 32.5],
        "TPA_UNADJ": [6.018, 1.234, 0.616, 0.308, 1.234, 0.154],
    })


@pytest.fixture
def sample_cond_df():
    """Create sample condition data for testing."""
    return pl.DataFrame({
        "PLT_CN": ["P1", "P2", "P3", "P4"],
        "COND_STATUS_CD": [1, 1, 2, 3],
        "SITECLCD": [3, 7, 3, None],
        "RESERVCD": [0, 0, 0, 0],
    })


@pytest.fixture
def sample_species_df():
    """Create sample species reference data."""
    return pl.DataFrame({
        "SPCD": [110, 121, 202, 316],
        "COMMON_NAME": ["shortleaf pine", "loblolly pine", "black cherry", "red maple"],
        "GENUS": ["Pinus", "Pinus", "Prunus", "Acer"],
    })


class TestSetupGroupingColumns:
    """Test the main setup_grouping_columns function."""

    def test_no_grouping(self, sample_tree_df):
        """Test with no grouping specified."""
        result_df, group_cols = setup_grouping_columns(sample_tree_df)

        assert group_cols == []
        assert result_df.columns == sample_tree_df.columns

    def test_custom_grouping_string(self, sample_tree_df):
        """Test with custom grouping as string."""
        result_df, group_cols = setup_grouping_columns(
            sample_tree_df,
            grp_by="PLT_CN"
        )

        assert group_cols == ["PLT_CN"]

    def test_custom_grouping_list(self, sample_tree_df):
        """Test with custom grouping as list."""
        result_df, group_cols = setup_grouping_columns(
            sample_tree_df,
            grp_by=["PLT_CN", "SPCD"]
        )

        assert group_cols == ["PLT_CN", "SPCD"]

    def test_species_grouping(self, sample_tree_df):
        """Test species grouping."""
        result_df, group_cols = setup_grouping_columns(
            sample_tree_df,
            by_species=True
        )

        assert "SPCD" in group_cols

    def test_size_class_grouping(self, sample_tree_df):
        """Test size class grouping."""
        result_df, group_cols = setup_grouping_columns(
            sample_tree_df,
            by_size_class=True
        )

        assert "sizeClass" in group_cols
        assert "sizeClass" in result_df.columns

        # Verify size classes
        size_classes = result_df["sizeClass"].unique().sort()
        expected = ["1.0-4.9", "5.0-9.9", "10.0-19.9", "20.0-29.9", "30.0+"]
        assert set(size_classes) == set(expected)

    def test_combined_grouping(self, sample_tree_df):
        """Test combining multiple grouping options."""
        result_df, group_cols = setup_grouping_columns(
            sample_tree_df,
            grp_by="PLT_CN",
            by_species=True,
            by_size_class=True
        )

        assert group_cols == ["PLT_CN", "SPCD", "sizeClass"]
        assert "sizeClass" in result_df.columns

    def test_duplicate_removal(self, sample_tree_df):
        """Test that duplicates are removed from group columns."""
        result_df, group_cols = setup_grouping_columns(
            sample_tree_df,
            grp_by=["SPCD", "PLT_CN", "SPCD"],  # SPCD duplicated
            by_species=True  # Also adds SPCD
        )

        # Should only have unique columns
        assert group_cols == ["SPCD", "PLT_CN"]

    def test_missing_column_error(self, sample_tree_df):
        """Test error when required column is missing."""
        df_no_spcd = sample_tree_df.drop("SPCD")

        with pytest.raises(ValueError, match="SPCD column not found"):
            setup_grouping_columns(df_no_spcd, by_species=True)


class TestSizeClassExpressions:
    """Test size class expression creation."""

    def test_standard_size_classes(self, sample_tree_df):
        """Test standard numeric size class labels."""
        expr = create_size_class_expr("DIA", "standard")
        result = sample_tree_df.with_columns(expr)

        # Check specific trees
        assert result.filter(pl.col("DIA") == 3.5)["sizeClass"][0] == "1.0-4.9"
        assert result.filter(pl.col("DIA") == 6.2)["sizeClass"][0] == "5.0-9.9"
        assert result.filter(pl.col("DIA") == 12.5)["sizeClass"][0] == "10.0-19.9"
        assert result.filter(pl.col("DIA") == 25.0)["sizeClass"][0] == "20.0-29.9"
        assert result.filter(pl.col("DIA") == 32.5)["sizeClass"][0] == "30.0+"

    def test_descriptive_size_classes(self, sample_tree_df):
        """Test descriptive size class labels."""
        expr = create_size_class_expr("DIA", "descriptive")
        result = sample_tree_df.with_columns(expr)

        assert result.filter(pl.col("DIA") == 3.5)["sizeClass"][0] == "Saplings"
        assert result.filter(pl.col("DIA") == 6.2)["sizeClass"][0] == "Small"
        assert result.filter(pl.col("DIA") == 12.5)["sizeClass"][0] == "Medium"
        assert result.filter(pl.col("DIA") == 25.0)["sizeClass"][0] == "Large"

    def test_custom_diameter_column(self):
        """Test using a different diameter column name."""
        df = pl.DataFrame({"DIA_BEGIN": [3.5, 15.0, 25.0]})
        expr = create_size_class_expr("DIA_BEGIN", "standard")
        result = df.with_columns(expr)

        assert "sizeClass" in result.columns
        assert result["sizeClass"].to_list() == ["1.0-4.9", "10.0-19.9", "20.0-29.9"]

    def test_invalid_size_class_type(self):
        """Test error with invalid size class type."""
        with pytest.raises(ValueError, match="Invalid size_class_type"):
            create_size_class_expr("DIA", "invalid")


class TestLandTypeColumn:
    """Test land type categorization."""

    def test_add_land_type_column(self, sample_cond_df):
        """Test adding land type column."""
        result = add_land_type_column(sample_cond_df)

        assert "landType" in result.columns

        # Check specific categorizations
        land_types = result["landType"].to_list()
        assert land_types[0] == "Timber"  # Forest, productive, unreserved
        assert land_types[1] == "Non-timber forest"  # Forest but not timber
        assert land_types[2] == "Non-forest"  # Status code 2
        assert land_types[3] == "Water"  # Status code 3

    def test_missing_required_columns(self):
        """Test error when required columns are missing."""
        df = pl.DataFrame({"PLT_CN": ["P1", "P2"]})

        with pytest.raises(ValueError, match="Missing required columns"):
            add_land_type_column(df)


class TestPlotGroups:
    """Test plot grouping preparation."""

    def test_prepare_plot_groups_default(self):
        """Test default behavior with PLT_CN always included."""
        base_groups = ["SPCD", "sizeClass"]
        result = prepare_plot_groups(base_groups)

        assert result == ["PLT_CN", "SPCD", "sizeClass"]

    def test_prepare_plot_groups_additional(self):
        """Test with additional groups."""
        base_groups = ["SPCD"]
        additional = ["STATECD", "INVYR"]
        result = prepare_plot_groups(base_groups, additional_groups=additional)

        assert result == ["PLT_CN", "SPCD", "STATECD", "INVYR"]

    def test_prepare_plot_groups_custom_always_include(self):
        """Test with custom always_include columns."""
        base_groups = ["SPCD"]
        result = prepare_plot_groups(
            base_groups,
            always_include=["PLT_CN", "EVALID"]
        )

        assert result == ["PLT_CN", "EVALID", "SPCD"]

    def test_prepare_plot_groups_duplicates(self):
        """Test that duplicates are handled correctly."""
        base_groups = ["PLT_CN", "SPCD"]  # PLT_CN duplicated
        result = prepare_plot_groups(base_groups)

        assert result == ["PLT_CN", "SPCD"]  # No duplicates


class TestSpeciesInfo:
    """Test species information joining."""

    def test_add_species_info_basic(self, sample_tree_df, sample_species_df):
        """Test basic species info joining."""
        result = add_species_info(
            sample_tree_df,
            sample_species_df,
            include_common_name=True,
            include_genus=False
        )

        assert "COMMON_NAME" in result.columns
        assert "GENUS" not in result.columns

        # Check specific species
        pine_rows = result.filter(pl.col("SPCD").is_in([110, 121]))
        assert all("pine" in name for name in pine_rows["COMMON_NAME"])

    def test_add_species_info_all_columns(self, sample_tree_df, sample_species_df):
        """Test including all species columns."""
        result = add_species_info(
            sample_tree_df,
            sample_species_df,
            include_common_name=True,
            include_genus=True
        )

        assert "COMMON_NAME" in result.columns
        assert "GENUS" in result.columns

    def test_add_species_info_no_ref_table(self, sample_tree_df):
        """Test behavior when no species reference table provided."""
        result = add_species_info(sample_tree_df, species_df=None)

        # Should return unchanged
        assert result.columns == sample_tree_df.columns

    def test_add_species_info_missing_spcd(self):
        """Test error when SPCD column missing."""
        df = pl.DataFrame({"PLT_CN": ["P1", "P2"]})

        with pytest.raises(ValueError, match="SPCD column not found"):
            add_species_info(df)


# TestStandardization class removed - standardize_group_names no longer needed
class TestValidation:
    """Test validation functions."""

    def test_validate_columns_success(self, sample_tree_df):
        """Test successful validation."""
        # Should not raise
        validate_grouping_columns(sample_tree_df, ["SPCD", "DIA"])

    def test_validate_columns_missing(self, sample_tree_df):
        """Test validation with missing columns."""
        with pytest.raises(ValueError, match="Missing required grouping columns"):
            validate_grouping_columns(sample_tree_df, ["SPCD", "MISSING_COL"])


class TestSizeClassBounds:
    """Test size class bounds retrieval."""

    def test_get_standard_bounds(self):
        """Test getting standard size class bounds."""
        bounds = get_size_class_bounds("standard")

        assert bounds == STANDARD_SIZE_CLASSES
        assert bounds["1.0-4.9"] == (1.0, 5.0)
        assert bounds["30.0+"] == (30.0, float("inf"))

    def test_get_descriptive_bounds(self):
        """Test getting descriptive size class bounds."""
        bounds = get_size_class_bounds("descriptive")

        assert bounds == DESCRIPTIVE_SIZE_CLASSES
        assert bounds["Saplings"] == (1.0, 5.0)
        assert bounds["Large"] == (20.0, float("inf"))

    def test_invalid_bounds_type(self):
        """Test error with invalid bounds type."""
        with pytest.raises(ValueError, match="Invalid size_class_type"):
            get_size_class_bounds("invalid")
