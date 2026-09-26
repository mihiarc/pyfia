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
    COND_GROUPING_COLUMNS,
    TREE_GROUPING_COLUMNS,
    get_cond_columns,
)
from pyfia.estimation.grm import attach_tree_conditions


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


class TestTreeConditionAttachment:
    """Each GRM tree gets its own condition's attributes (#138)."""

    @pytest.fixture
    def two_condition_plot(self):
        """Plot P1 has two conditions with different disturbances; P2 has one."""
        cond = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P1", "P2"],
                "CONDID": [1, 2, 1],
                "CONDPROP_UNADJ": [0.6, 0.4, 1.0],
                "COND_STATUS_CD": [1, 1, 1],
                "DSTRBCD1": [30, 10, 0],
                "OWNGRPCD": [40, 10, 40],
            }
        ).lazy()
        tree = pl.DataFrame(
            {"CN": ["T1", "T2", "T3", "T4"], "CONDID": [1, 2, 2, 1]}
        ).lazy()
        grm = pl.DataFrame(
            {
                "TRE_CN": ["T1", "T2", "T3", "T4"],
                "PLT_CN": ["P1", "P1", "P1", "P2"],
                "TPA_UNADJ": [1.0, 2.0, 3.0, 4.0],
            }
        ).lazy()
        return grm, tree, cond

    def test_trees_get_their_own_condition(self, two_condition_plot):
        result = attach_tree_conditions(*two_condition_plot).collect().sort("TRE_CN")
        assert result["DSTRBCD1"].to_list() == [30, 10, 10, 0]
        assert result["OWNGRPCD"].to_list() == [40, 10, 10, 40]

    def test_area_keys_stay_at_plot_level(self, two_condition_plot):
        """CONDPROP_UNADJ is the plot's total and CONDID is 1, as the GRM
        aggregation and variance expect."""
        result = attach_tree_conditions(*two_condition_plot).collect().sort("TRE_CN")
        assert result["CONDPROP_UNADJ"].to_list() == [1.0, 1.0, 1.0, 1.0]
        assert result["CONDID"].to_list() == [1, 1, 1, 1]

    def test_group_splits_sum_to_plot_total(self, two_condition_plot):
        result = attach_tree_conditions(*two_condition_plot).collect()
        p1 = result.filter(pl.col("PLT_CN") == "P1")
        by_group = p1.group_by("DSTRBCD1").agg(pl.col("TPA_UNADJ").sum())
        assert dict(by_group.iter_rows()) == {30: 1.0, 10: 5.0}
        assert by_group["TPA_UNADJ"].sum() == p1["TPA_UNADJ"].sum()

    def test_row_count_unchanged(self, two_condition_plot):
        grm = two_condition_plot[0]
        result = attach_tree_conditions(*two_condition_plot).collect()
        assert result.height == grm.collect().height


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


class TestPlotGroupingColumns:
    """Test that plot-level grouping columns are properly supported."""

    def test_unitcd_in_plot_grouping_columns(self):
        """UNITCD should be available for plot-level grouping."""
        from pyfia.estimation.columns import PLOT_GROUPING_COLUMNS

        assert "UNITCD" in PLOT_GROUPING_COLUMNS, (
            "UNITCD must be in PLOT_GROUPING_COLUMNS to enable "
            "grouping by FIA survey unit"
        )

    def test_statecd_in_plot_grouping_columns(self):
        """STATECD should be available for plot-level grouping."""
        from pyfia.estimation.columns import PLOT_GROUPING_COLUMNS

        assert "STATECD" in PLOT_GROUPING_COLUMNS

    def test_countycd_in_plot_grouping_columns(self):
        """COUNTYCD should be available for plot-level grouping."""
        from pyfia.estimation.columns import PLOT_GROUPING_COLUMNS

        assert "COUNTYCD" in PLOT_GROUPING_COLUMNS

    def test_plot_grouping_columns_complete(self):
        """PLOT_GROUPING_COLUMNS should include all common geographic identifiers."""
        from pyfia.estimation.columns import PLOT_GROUPING_COLUMNS

        expected = ["STATECD", "COUNTYCD", "UNITCD", "INVYR"]
        for col in expected:
            assert col in PLOT_GROUPING_COLUMNS, (
                f"{col} should be in PLOT_GROUPING_COLUMNS"
            )
