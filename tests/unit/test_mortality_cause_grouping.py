"""Tests for AGENTCD and DSTRBCD grouping in mortality estimation.

This module tests the feature that enables grouping mortality estimates by:
- AGENTCD: Mortality agent code (cause of death at tree level)
- DSTRBCD1/2/3: Disturbance codes (condition-level disturbances)

Use case: Timber casualty loss analysis for tax purposes, where losses
must be classified by cause (fire, insects, disease, weather, etc.).
"""

import polars as pl
import pytest

from pyfia.estimation.columns import (
    TREE_GROUPING_COLUMNS,
    COND_GROUPING_COLUMNS,
    get_tree_columns,
    get_cond_columns,
)
from pyfia.estimation.grm import aggregate_cond_to_plot


class TestColumnWhitelist:
    """Test that AGENTCD and DSTRBCD columns are in grouping whitelists."""

    def test_agentcd_in_tree_grouping_columns(self):
        """AGENTCD should be available for tree-level grouping."""
        assert "AGENTCD" in TREE_GROUPING_COLUMNS, (
            "AGENTCD must be in TREE_GROUPING_COLUMNS to enable "
            "grouping mortality by cause of death"
        )

    def test_dstrbcd_in_cond_grouping_columns(self):
        """DSTRBCD columns should be available for condition-level grouping."""
        assert "DSTRBCD1" in COND_GROUPING_COLUMNS, (
            "DSTRBCD1 must be in COND_GROUPING_COLUMNS to enable "
            "grouping by primary disturbance"
        )
        assert "DSTRBCD2" in COND_GROUPING_COLUMNS
        assert "DSTRBCD3" in COND_GROUPING_COLUMNS

    def test_get_cond_columns_includes_dstrbcd_when_requested(self):
        """get_cond_columns should include DSTRBCD1 when specified in grp_by."""
        cols = get_cond_columns(grp_by="DSTRBCD1")
        assert "DSTRBCD1" in cols

    def test_get_cond_columns_includes_multiple_dstrbcd(self):
        """get_cond_columns should include multiple DSTRBCD columns."""
        cols = get_cond_columns(grp_by=["DSTRBCD1", "DSTRBCD2"])
        assert "DSTRBCD1" in cols
        assert "DSTRBCD2" in cols


class TestConditionAggregation:
    """Test that DSTRBCD columns are preserved in condition aggregation."""

    def test_aggregate_cond_preserves_dstrbcd1(self, condition_data_with_dstrbcd):
        """DSTRBCD1 should be preserved when aggregating COND to plot level."""
        cond = condition_data_with_dstrbcd.lazy()
        result = aggregate_cond_to_plot(cond).collect()

        assert "DSTRBCD1" in result.columns, (
            "DSTRBCD1 must be preserved in aggregate_cond_to_plot() "
            "to enable grouping by disturbance"
        )

    def test_aggregate_cond_preserves_all_dstrbcd(self, condition_data_with_dstrbcd):
        """All DSTRBCD columns should be preserved in aggregation."""
        cond = condition_data_with_dstrbcd.lazy()
        result = aggregate_cond_to_plot(cond).collect()

        for col in ["DSTRBCD1", "DSTRBCD2", "DSTRBCD3"]:
            assert col in result.columns, f"{col} must be preserved in aggregation"

    def test_aggregate_cond_dstrbcd_values_correct(self, condition_data_with_dstrbcd):
        """DSTRBCD values should be correctly preserved (first value per plot)."""
        cond = condition_data_with_dstrbcd.lazy()
        result = aggregate_cond_to_plot(cond).collect()

        # Check that DSTRBCD1 values are preserved correctly
        p1_row = result.filter(pl.col("PLT_CN") == "P1")
        assert p1_row["DSTRBCD1"][0] == 30, "P1 should have DSTRBCD1=30 (Fire)"

        p2_row = result.filter(pl.col("PLT_CN") == "P2")
        assert p2_row["DSTRBCD1"][0] == 10, "P2 should have DSTRBCD1=10 (Insect)"


class TestAGENTCDMapping:
    """Test AGENTCD code mappings for mortality cause classification."""

    @pytest.fixture
    def agentcd_codes(self):
        """Standard AGENTCD codes and their meanings."""
        return {
            0: "No agent recorded",
            10: "Insect",
            20: "Disease",
            30: "Fire",
            40: "Animal",
            50: "Weather",
            60: "Vegetation (competition)",
            70: "Unknown/other",
            80: "Silvicultural/land clearing",
        }

    @pytest.fixture
    def casualty_classification(self):
        """Tax classification of mortality causes.

        Based on Forest Landowner's Guide to Federal Income Tax (Ag Handbook 731).
        """
        return {
            "casualty": [30, 50],  # Fire, Weather (sudden events)
            "non_casualty": [10, 20],  # Insect, Disease (gradual)
            "non_deductible": [40, 60, 80],  # Animal, Vegetation, Silvicultural
            "unknown": [0, 70],  # No agent, Unknown
        }

    def test_fire_is_casualty(self, casualty_classification):
        """Fire (AGENTCD=30) should be classified as casualty loss."""
        assert 30 in casualty_classification["casualty"]

    def test_weather_is_casualty(self, casualty_classification):
        """Weather damage (AGENTCD=50) should be classified as casualty loss."""
        assert 50 in casualty_classification["casualty"]

    def test_insect_is_non_casualty(self, casualty_classification):
        """Insect damage (AGENTCD=10) should be classified as non-casualty."""
        assert 10 in casualty_classification["non_casualty"]

    def test_disease_is_non_casualty(self, casualty_classification):
        """Disease (AGENTCD=20) should be classified as non-casualty."""
        assert 20 in casualty_classification["non_casualty"]


class TestDSTRBCDMapping:
    """Test DSTRBCD code mappings for disturbance classification."""

    @pytest.fixture
    def weather_dstrbcd_codes(self):
        """Weather-related DSTRBCD codes that qualify as casualty loss."""
        return {
            50: "Weather - general",
            51: "Ice/frost",
            52: "Hurricane/tornado/wind",
            53: "Flood",
            54: "Drought",  # Note: Drought may be gradual, classification varies
        }

    def test_hurricane_code_recognized(self, weather_dstrbcd_codes):
        """Hurricane/wind damage (DSTRBCD=52) should be in weather codes."""
        assert 52 in weather_dstrbcd_codes
