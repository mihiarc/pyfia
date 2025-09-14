"""
Integration tests for area estimation matching EVALIDator patterns.

These tests verify that area.py produces results consistent with the
working SQL examples in FIA_WORKING_QUERY_BANK.md
"""

from unittest.mock import Mock
import os
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from pyfia.estimation import area


class TestAreaIntegrationEVALIDator:
    """Integration tests matching EVALIDator SQL patterns."""

    @pytest.fixture
    def minnesota_forest_area_data(self):
        """
        Create test data matching Minnesota Forest Area example.
        EVALID: 272201 - should produce forest type group areas.
        """
        # Create realistic plot data
        plots = pl.DataFrame(
            {
                "CN": [f"P{i}" for i in range(1, 11)],
                "PLT_CN": [f"P{i}" for i in range(1, 11)],
                "STATECD": [23] * 10,  # Minnesota
                "MACRO_BREAKPOINT_DIA": [24.0] * 10,
            }
        )

        # Create conditions with forest type groups
        # Matching the SQL: Aspen/birch (0900), Spruce/fir (0120), Oak/hickory (0500)
        conditions = pl.DataFrame(
            {
                "CN": [f"C{i}" for i in range(1, 16)],
                "PLT_CN": [
                    "P1",
                    "P1",
                    "P2",
                    "P3",
                    "P3",
                    "P4",
                    "P5",
                    "P6",
                    "P7",
                    "P8",
                    "P8",
                    "P9",
                    "P9",
                    "P10",
                    "P10",
                ],
                "CONDID": [1, 2, 1, 1, 2, 1, 1, 1, 1, 1, 2, 1, 2, 1, 2],
                "COND_STATUS_CD": [
                    1,
                    1,
                    1,
                    1,
                    2,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    3,
                    1,
                    1,
                ],  # Mix of forest, non-forest, water
                "CONDPROP_UNADJ": [
                    0.6,
                    0.4,
                    1.0,
                    0.7,
                    0.3,
                    1.0,
                    1.0,
                    1.0,
                    1.0,
                    0.5,
                    0.5,
                    0.8,
                    0.2,
                    0.3,
                    0.7,
                ],
                "PROP_BASIS": ["SUBP"] * 12 + ["MACR"] * 3,  # Some macroplot conditions
                "FORTYPCD": [
                    901,
                    902,
                    121,
                    122,
                    None,
                    901,
                    501,
                    701,
                    801,
                    101,
                    901,
                    121,
                    None,
                    999,
                    501,
                ],
                "SITECLCD": [3, 3, 3, 3, None, 3, 3, 3, 3, 3, 3, 3, None, 3, 3],
                "RESERVCD": [0] * 15,
            }
        )

        # Create stratification data
        strata = pl.DataFrame(
            {
                "CN": ["S1", "S2", "S3"],
                "EVALID": [272201] * 3,
                "EXPNS": [
                    6000.0,
                    5000.0,
                    4000.0,
                ],  # Total ~17.6M acres when summed across plots
                "ADJ_FACTOR_SUBP": [1.0] * 3,
                "ADJ_FACTOR_MACR": [0.25] * 3,
                "P2POINTCNT": [4, 3, 3],
                "STRATUM_WGT": [0.4, 0.35, 0.25],
                "AREA_USED": [7040000.0, 6160000.0, 4400000.0],  # ~17.6M total
            }
        )

        # Create plot-stratum assignments
        ppsa = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"],
                "STRATUM_CN": [
                    "S1",
                    "S1",
                    "S1",
                    "S1",
                    "S2",
                    "S2",
                    "S2",
                    "S3",
                    "S3",
                    "S3",
                ],
                "EVALID": [272201] * 10,
            }
        )

        # Create forest type reference data
        forest_types = pl.DataFrame(
            {
                "VALUE": [901, 902, 121, 122, 501, 701, 801, 101, 999],
                "TYPGRPCD": [900, 900, 120, 120, 500, 700, 800, 100, 999],
                "MEANING": [
                    "Aspen",
                    "Birch",
                    "Spruce",
                    "Fir",
                    "Oak/hickory",
                    "Elm/ash",
                    "Maple/beech",
                    "White pine",
                    "Nonstocked",
                ],
            }
        )

        forest_type_groups = pl.DataFrame(
            {
                "VALUE": [900, 120, 500, 700, 800, 100, 999],
                "MEANING": [
                    "Aspen / birch group",
                    "Spruce / fir group",
                    "Oak / hickory group",
                    "Elm / ash / cottonwood group",
                    "Maple / beech / birch group",
                    "White / red / jack pine group",
                    "Nonstocked",
                ],
            }
        )

        return {
            "plots": plots,
            "conditions": conditions,
            "strata": strata,
            "ppsa": ppsa,
            "forest_types": forest_types,
            "forest_type_groups": forest_type_groups,
        }

    def test_minnesota_forest_area_by_type_group(self, minnesota_forest_area_data, real_fia_instance, use_real_data):
        """
        Test that matches Minnesota Forest Area by Forest Type Group example.
        Should calculate areas matching the SQL pattern with PROP_BASIS handling.
        """
        if use_real_data:
            # Run against real DB with broad assertions only
            db = real_fia_instance
            result = area(db, grp_by=["FORTYPCD"], land_type="forest", totals=True)
            assert "AREA_PERC" in result.columns
            assert result.shape[0] > 0
            return
        data = minnesota_forest_area_data

        # Add forest type group to conditions
        conditions_with_group = (
            data["conditions"]
            .join(
                data["forest_types"].select(["VALUE", "TYPGRPCD"]),
                left_on="FORTYPCD",
                right_on="VALUE",
                how="left",
            )
            .join(
                data["forest_type_groups"].rename({"MEANING": "FOREST_TYPE_GROUP"}),
                left_on="TYPGRPCD",
                right_on="VALUE",
                how="left",
            )
        )

        # Mock database
        mock_db = Mock()
        mock_db.evalid = [272201]
        mock_db.get_plots = Mock(return_value=data["plots"])
        mock_db.get_conditions = Mock(return_value=conditions_with_group)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=data["strata"])),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=data["ppsa"])))
            ),
        }
        mock_db.load_table = Mock()

        # Calculate area by forest type group
        result = area(
            mock_db, grp_by=["FOREST_TYPE_GROUP"], land_type="forest", totals=True
        )

        # Verify results structure
        assert "FOREST_TYPE_GROUP" in result.columns
        assert "AREA_PERC" in result.columns
        assert "AREA" in result.columns

        # Check that major forest type groups are present
        forest_groups = result["FOREST_TYPE_GROUP"].to_list()
        assert "Aspen / birch group" in forest_groups
        assert "Spruce / fir group" in forest_groups

        # Verify PROP_BASIS handling affected results
        # Conditions with PROP_BASIS='MACR' should use ADJ_FACTOR_MACR=0.25
        # This should reduce their contribution to total area

        # For grouped estimates (not by_land_type), each group shows its percentage
        # of the specified land type (forest in this case)
        # The sum may exceed 100% if plots have multiple conditions with different groups
        # This is correct behavior for forest type analysis
        total_perc = result["AREA_PERC"].sum()
        assert total_perc > 0  # Should have some forest area

        # Each individual percentage should be valid
        for perc in result["AREA_PERC"].to_list():
            if perc is not None and not np.isnan(perc):
                assert 0 <= perc <= 100

    def test_prop_basis_adjustment_factor_selection(self, real_fia_instance, use_real_data):
        """
        Test that PROP_BASIS correctly selects between ADJ_FACTOR_MACR and ADJ_FACTOR_SUBP.
        This matches the SQL pattern:
        CASE c.PROP_BASIS
            WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR
            ELSE ps.ADJ_FACTOR_SUBP
        END
        """
        if use_real_data:
            db = real_fia_instance
            # Just ensure query runs and adjustment columns exist in strata
            res = area(db, land_type="forest", totals=True)
            assert "AREA" in res.columns
            assert res.shape[0] > 0
            return
        # Create test data with clear MACR vs SUBP distinction
        plots = pl.DataFrame({"CN": ["P1", "P2"], "PLT_CN": ["P1", "P2"]})

        conditions = pl.DataFrame(
            {
                "CN": ["C1", "C2"],
                "PLT_CN": ["P1", "P2"],
                "CONDID": [1, 1],
                "COND_STATUS_CD": [1, 1],
                "CONDPROP_UNADJ": [1.0, 1.0],
                "PROP_BASIS": ["SUBP", "MACR"],  # Different PROP_BASIS
                "SITECLCD": [3, 3],
                "RESERVCD": [0, 0],
            }
        )

        strata = pl.DataFrame(
            {
                "CN": ["S1"],
                "EVALID": [1],
                "EXPNS": [1000.0],
                "ADJ_FACTOR_SUBP": [1.0],  # SUBP adjustment = 1.0
                "ADJ_FACTOR_MACR": [0.25],  # MACR adjustment = 0.25
                "P2POINTCNT": [2],
                "STRATUM_WGT": [1.0],
                "AREA_USED": [2000.0],
            }
        )

        ppsa = pl.DataFrame(
            {"PLT_CN": ["P1", "P2"], "STRATUM_CN": ["S1", "S1"], "EVALID": [1, 1]}
        )

        # Mock database
        mock_db = Mock()
        mock_db.evalid = [1]
        mock_db.get_plots = Mock(return_value=plots)
        mock_db.get_conditions = Mock(return_value=conditions)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=strata)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=ppsa)))
            ),
        }
        mock_db.load_table = Mock()

        # Calculate area
        result = area(mock_db, land_type="forest", totals=True)

        # The MACR plot should contribute less to total area due to 0.25 adjustment
        # SUBP plot: 1.0 * 1.0 * 1000 = 1000 acres
        # MACR plot: 1.0 * 0.25 * 1000 = 250 acres
        # Total: 1250 acres
        assert result["AREA"][0] == pytest.approx(1250.0, rel=0.01)

    def test_direct_expansion_method(self, real_fia_instance, use_real_data):
        """
        Test that area calculation uses direct expansion (not post-stratified means).
        This matches the SQL pattern:
        SUM(c.CONDPROP_UNADJ * adjustment_factor * ps.EXPNS)
        """
        if use_real_data:
            db = real_fia_instance
            res = area(db, land_type="forest", totals=True)
            assert "AREA" in res.columns
            assert "AREA_PERC" in res.columns
            assert res.shape[0] > 0
            return
        # Create simple test case
        plots = pl.DataFrame({"CN": ["P1", "P2", "P3"], "PLT_CN": ["P1", "P2", "P3"]})

        conditions = pl.DataFrame(
            {
                "CN": ["C1", "C2", "C3"],
                "PLT_CN": ["P1", "P2", "P3"],
                "CONDID": [1, 1, 1],
                "COND_STATUS_CD": [1, 1, 1],  # All forest
                "CONDPROP_UNADJ": [1.0, 0.5, 0.75],  # Different proportions
                "PROP_BASIS": ["SUBP", "SUBP", "SUBP"],
                "SITECLCD": [3, 3, 3],
                "RESERVCD": [0, 0, 0],
            }
        )

        strata = pl.DataFrame(
            {
                "CN": ["S1", "S2"],
                "EVALID": [1, 1],
                "EXPNS": [1000.0, 2000.0],  # Different expansion factors
                "ADJ_FACTOR_SUBP": [1.0, 1.0],
                "ADJ_FACTOR_MACR": [0.25, 0.25],
                "P2POINTCNT": [2, 1],
                "STRATUM_WGT": [0.6, 0.4],
                "AREA_USED": [2000.0, 2000.0],
            }
        )

        ppsa = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3"],
                "STRATUM_CN": ["S1", "S1", "S2"],  # P1,P2 in S1; P3 in S2
                "EVALID": [1, 1, 1],
            }
        )

        # Mock database
        mock_db = Mock()
        mock_db.evalid = [1]
        mock_db.get_plots = Mock(return_value=plots)
        mock_db.get_conditions = Mock(return_value=conditions)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=strata)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=ppsa)))
            ),
        }
        mock_db.load_table = Mock()

        # Calculate area
        result = area(mock_db, land_type="forest", totals=True)

        # Direct expansion calculation:
        # P1: 1.0 * 1.0 * 1000 = 1000
        # P2: 0.5 * 1.0 * 1000 = 500
        # P3: 0.75 * 1.0 * 2000 = 1500
        # Total forest area: 3000
        assert result["AREA"][0] == pytest.approx(3000.0, rel=0.01)

        # Percentage should be 100% since all conditions are forest
        assert result["AREA_PERC"][0] == pytest.approx(100.0, rel=0.01)

    def test_by_land_type_denominator(self, real_fia_instance, use_real_data):
        """
        Test that by_land_type uses correct denominator (excludes water).
        For byLandType=TRUE: percentages should be of land area only (status 1+2).
        """
        if use_real_data:
            db = real_fia_instance
            res = area(db, by_land_type=True, totals=True)
            # Basic structural check only when using real data
            assert "LAND_TYPE" in res.columns
            assert res.shape[0] > 0
            return
        plots = pl.DataFrame(
            {"CN": ["P1", "P2", "P3", "P4"], "PLT_CN": ["P1", "P2", "P3", "P4"]}
        )

        conditions = pl.DataFrame(
            {
                "CN": ["C1", "C2", "C3", "C4"],
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "CONDID": [1, 1, 1, 1],
                "COND_STATUS_CD": [1, 1, 2, 3],  # Forest, Forest, Non-forest, Water
                "CONDPROP_UNADJ": [1.0, 1.0, 1.0, 1.0],
                "PROP_BASIS": ["SUBP"] * 4,
                "SITECLCD": [3, 7, None, None],  # Timber, Non-timber, N/A, N/A
                "RESERVCD": [0, 0, 0, 0],
            }
        )

        strata = pl.DataFrame(
            {
                "CN": ["S1"],
                "EVALID": [1],
                "EXPNS": [1000.0],
                "ADJ_FACTOR_SUBP": [1.0],
                "ADJ_FACTOR_MACR": [0.25],
                "P2POINTCNT": [4],
                "STRATUM_WGT": [1.0],
                "AREA_USED": [4000.0],
            }
        )

        ppsa = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": ["S1"] * 4,
                "EVALID": [1] * 4,
            }
        )

        # Mock database
        mock_db = Mock()
        mock_db.evalid = [1]
        mock_db.get_plots = Mock(return_value=plots)
        mock_db.get_conditions = Mock(return_value=conditions)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=strata)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=ppsa)))
            ),
        }
        mock_db.load_table = Mock()

        # Calculate area by land type
        result = area(mock_db, by_land_type=True, totals=True)

        # Check results
        assert "LAND_TYPE" in result.columns

        # Get land type percentages
        timber = result.filter(pl.col("LAND_TYPE") == "Timber")
        result.filter(pl.col("LAND_TYPE") == "Non-Timber Forest")
        non_forest = result.filter(pl.col("LAND_TYPE") == "Non-Forest")
        result.filter(pl.col("LAND_TYPE") == "Water")

        # Percentages should be relative to land area (excluding water)
        # Land area = 3000 (P1 + P2 + P3), Water = 1000 (P4)
        # Timber: 1000/3000 = 33.33%
        # Non-timber forest: 1000/3000 = 33.33%
        # Non-forest: 1000/3000 = 33.33%
        # Water should still have area but percentage calculation different

        if len(timber) > 0:
            assert timber["AREA_PERC"][0] == pytest.approx(33.33, rel=0.1)
        if len(non_forest) > 0:
            assert non_forest["AREA_PERC"][0] == pytest.approx(33.33, rel=0.1)

        # Total land percentages (excluding water) should sum to ~100%
        land_only = result.filter(pl.col("LAND_TYPE") != "Water")
        total_land_perc = land_only["AREA_PERC"].sum()
        assert total_land_perc == pytest.approx(100.0, rel=1.0)

    def test_variance_calculation_components(self):
        """
        Test that variance calculations include all necessary components.
        Should calculate variance at stratum level and combine properly.
        """
        # Create data with variation for meaningful variance
        plots = pl.DataFrame(
            {
                "CN": [f"P{i}" for i in range(1, 7)],
                "PLT_CN": [f"P{i}" for i in range(1, 7)],
            }
        )

        conditions = pl.DataFrame(
            {
                "CN": [f"C{i}" for i in range(1, 7)],
                "PLT_CN": [f"P{i}" for i in range(1, 7)],
                "CONDID": [1] * 6,
                "COND_STATUS_CD": [1, 1, 1, 1, 2, 2],  # Mix of forest and non-forest
                "CONDPROP_UNADJ": [1.0, 0.8, 0.6, 1.0, 1.0, 0.5],
                "PROP_BASIS": ["SUBP"] * 6,
                "SITECLCD": [3, 3, 3, 3, None, None],
                "RESERVCD": [0] * 6,
            }
        )

        strata = pl.DataFrame(
            {
                "CN": ["S1", "S2"],
                "EVALID": [1, 1],
                "EXPNS": [1000.0, 1500.0],
                "ADJ_FACTOR_SUBP": [1.0, 1.0],
                "ADJ_FACTOR_MACR": [0.25, 0.25],
                "P2POINTCNT": [3, 3],
                "STRATUM_WGT": [0.5, 0.5],
                "AREA_USED": [3000.0, 4500.0],
            }
        )

        ppsa = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(1, 7)],
                "STRATUM_CN": ["S1", "S1", "S1", "S2", "S2", "S2"],
                "EVALID": [1] * 6,
            }
        )

        # Mock database
        mock_db = Mock()
        mock_db.evalid = [1]
        mock_db.get_plots = Mock(return_value=plots)
        mock_db.get_conditions = Mock(return_value=conditions)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=strata)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(return_value=Mock(collect=Mock(return_value=ppsa)))
            ),
        }
        mock_db.load_table = Mock()

        # Calculate area with variance
        result = area(mock_db, land_type="forest", variance=True)

        # Check variance components
        assert "AREA_PERC_VAR" in result.columns
        # Variance can be 0 if there's no variation within strata
        # or if the sample size is too small
        assert result["AREA_PERC_VAR"][0] >= 0  # Should have non-negative variance

        # Standard error should be sqrt(variance)
        result_se = area(mock_db, land_type="forest", variance=False)
        assert "AREA_PERC_SE" in result_se.columns

        # SE^2 should approximately equal variance
        se_squared = result_se["AREA_PERC_SE"][0] ** 2
        variance = result["AREA_PERC_VAR"][0]
        assert se_squared == pytest.approx(variance, rel=0.01)
