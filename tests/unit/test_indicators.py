"""Unit tests for land type indicator functions.

Tests the filtering/indicators.py module which provides land type
classification functions using named constants instead of magic numbers.
"""

import polars as pl
import pytest

from pyfia.constants.status_codes import LandStatus, ReserveStatus, SiteClass
from pyfia.filtering.indicators import (
    LandTypeCategory,
    add_land_type_categories,
    classify_land_types,
    get_land_domain_indicator,
)


class TestGetLandDomainIndicator:
    """Test get_land_domain_indicator function."""

    def test_forest_indicator_matches_status(self):
        """Forest indicator should match COND_STATUS_CD == 1."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [1, 2, 3, 1, 5],
            }
        )
        expr = get_land_domain_indicator("forest")
        result = df.filter(expr)
        assert len(result) == 2
        assert all(result["COND_STATUS_CD"] == 1)

    def test_timber_indicator_requires_all_conditions(self):
        """Timber indicator requires forest + productive + not reserved."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [1, 1, 1, 1, 2],
                "SITECLCD": [1, 7, 3, 2, 1],  # 7 is unproductive
                "RESERVCD": [0, 0, 1, 0, 0],  # 1 is reserved
            }
        )
        expr = get_land_domain_indicator("timber")
        result = df.filter(expr)
        # Only rows 0 and 3 should match (forest, productive, not reserved)
        assert len(result) == 2
        # Verify the matching conditions
        assert all(result["COND_STATUS_CD"] == 1)
        assert all(result["RESERVCD"] == 0)
        assert all(result["SITECLCD"].is_in([1, 2, 3, 4, 5, 6]))

    def test_all_indicator_includes_everything(self):
        """All indicator should include all rows."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [1, 2, 3, 4, 5],
            }
        )
        expr = get_land_domain_indicator("all")
        result = df.filter(expr)
        assert len(result) == 5

    def test_indicator_works_with_lazyframe(self):
        """Indicator should work with LazyFrames for server-side execution."""
        lf = pl.LazyFrame(
            {
                "COND_STATUS_CD": [1, 2, 1],
            }
        )
        expr = get_land_domain_indicator("forest")
        result = lf.filter(expr).collect()
        assert len(result) == 2


class TestConstantsRegression:
    """Regression tests for status code constants."""

    def test_land_status_forest(self):
        """LandStatus.FOREST should be 1."""
        assert LandStatus.FOREST == 1

    def test_productive_classes(self):
        """SiteClass.PRODUCTIVE_CLASSES should be [1,2,3,4,5,6]."""
        assert SiteClass.PRODUCTIVE_CLASSES == [1, 2, 3, 4, 5, 6]

    def test_not_reserved(self):
        """ReserveStatus.NOT_RESERVED should be 0."""
        assert ReserveStatus.NOT_RESERVED == 0


class TestLandTypeCategory:
    """Test LandTypeCategory enum."""

    def test_timber_category(self):
        """Test TIMBER category value."""
        assert LandTypeCategory.TIMBER == "Timber"

    def test_non_timber_forest_category(self):
        """Test NON_TIMBER_FOREST category value."""
        assert LandTypeCategory.NON_TIMBER_FOREST == "Non-Timber Forest"

    def test_non_forest_category(self):
        """Test NON_FOREST category value."""
        assert LandTypeCategory.NON_FOREST == "Non-Forest"


class TestAddLandTypeCategories:
    """Test add_land_type_categories function."""

    def test_adds_land_type_column(self):
        """Should add LAND_TYPE column with correct categories."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [1, 1, 2, 3],
                "SITECLCD": [1, 7, 1, 1],  # 7 is unproductive
                "RESERVCD": [0, 0, 0, 0],
            }
        )
        result = add_land_type_categories(df)
        assert "LAND_TYPE" in result.columns

        # Row 0: Forest + productive + unreserved = Timber
        assert result["LAND_TYPE"][0] == "Timber"

        # Row 1: Forest but unproductive = Non-Timber Forest
        assert result["LAND_TYPE"][1] == "Non-Timber Forest"

        # Row 2: Non-forest
        assert result["LAND_TYPE"][2] == "Non-Forest"


class TestClassifyLandTypes:
    """Test classify_land_types function."""

    def test_adds_land_indicator(self):
        """Should add landD domain indicator column."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [1, 2],
            }
        )
        result = classify_land_types(df, land_type="forest")
        assert "landD" in result.columns
        assert result["landD"][0] == 1  # Forest
        assert result["landD"][1] == 0  # Non-forest

    def test_by_land_type_adds_categories(self):
        """When by_land_type=True, should add LAND_TYPE column."""
        df = pl.DataFrame(
            {
                "COND_STATUS_CD": [1, 1],
                "SITECLCD": [1, 7],
                "RESERVCD": [0, 0],
            }
        )
        result = classify_land_types(df, land_type="forest", by_land_type=True)
        assert "LAND_TYPE" in result.columns
        assert "landD" in result.columns
