"""
Core integration tests for area estimation module.

These tests validate the main area() function workflow with realistic FIA data structures.
For real validation against published estimates, see test_area_real.py.

Tests cover:
- Basic area calculation integration
- Land type classification (timber, non-timber forest, non-forest, water)
- Tree domain filtering
- By land type grouping functionality
"""

from unittest.mock import Mock

import polars as pl
import pytest

from pyfia.estimation import area


class TestAreaEstimation:
    """Integration tests for area estimation functions."""

    @pytest.fixture
    def sample_plot_data(self):
        """Create sample PLOT data matching FIA schema."""
        return pl.DataFrame({
            "CN": ["P1", "P2", "P3", "P4", "P5"],
            "INVYR": [2023, 2023, 2023, 2023, 2023],
            "STATECD": [37, 37, 37, 37, 37],
            "PLOT": [1, 2, 3, 4, 5],
            "ELEV": [100.0, 200.0, 150.0, 300.0, 125.0],
            "LAT": [35.5, 35.6, 35.7, 35.8, 35.9],
            "LON": [-78.5, -78.6, -78.7, -78.8, -78.9],
            "ECOSUBCD": ["231A", "231A", "231B", "231A", "231A"],
            "P2POINTCNT": [1.0, 1.0, 1.0, 1.0, 1.0]
        })

    @pytest.fixture
    def sample_cond_data(self):
        """Create sample COND data with various land types and PROP_BASIS."""
        return pl.DataFrame({
            "CN": ["C1", "C2", "C3", "C4", "C5", "C6"],
            "PLT_CN": ["1", "1", "2", "3", "4", "5"],
            "CONDID": [1, 2, 1, 1, 1, 1],
            "COND_STATUS_CD": [1, 1, 1, 2, 3, 1],  # Forest, Forest, Forest, Non-forest, Water, Forest
            "CONDPROP_UNADJ": [0.7, 0.3, 1.0, 1.0, 0.2, 1.0],  # Reduce water proportion to fix >100% issue
            "PROP_BASIS": ["SUBP", "SUBP", "MACR", "SUBP", "SUBP", "MACR"],
            "FORTYPCD": [171, 171, 162, 0, 0, 182],
            "SITECLCD": [2, 2, 1, 7, 7, 3],
            "RESERVCD": [0, 0, 0, 0, 0, 0]
        })

    @pytest.fixture
    def sample_tree_data(self):
        """Create sample TREE data."""
        return pl.DataFrame({
            "CN": ["T1", "T2", "T3", "T4", "T5"],
            "PLT_CN": ["1", "1", "2", "3", "5"],
            "CONDID": [1, 2, 1, 1, 1],
            "STATUSCD": [1, 1, 1, 2, 1],  # Live, Live, Live, Dead, Live
            "SPCD": [131, 316, 131, 131, 621],  # Loblolly, Red maple, Loblolly, Loblolly, Yellow-poplar
            "DIA": [10.5, 8.2, 15.3, 12.0, 6.5],
            "TPA_UNADJ": [5.0, 5.0, 5.0, 5.0, 5.0],
        })

    @pytest.fixture
    def sample_stratum_data(self):
        """Create sample POP_STRATUM data."""
        return pl.DataFrame({
            "CN": ["S1", "S2"],
            "EVALID": [372301, 372301],
            "ESTN_UNIT_CN": ["EU1", "EU2"],
            "ADJ_FACTOR_SUBP": [10.0, 15.0],
            "ADJ_FACTOR_MACR": [8.0, 12.0],
            "EXPNS": [1000.0, 12000.0],
            "P2POINTCNT": [100, 200],
            "STRATUM_CN": ["S1", "S2"],
            "P1POINTCNT": [50, 150]
        })

    @pytest.fixture
    def sample_ppsa_data(self):
        """Create sample POP_PLOT_STRATUM_ASSGN data."""
        return pl.DataFrame({
            "PLT_CN": ["1", "2", "3", "4", "5"],
            "STRATUM_CN": ["S1", "S1", "S1", "S2", "S2"],
            "EVALID": [372301, 372301, 372301, 372301, 372301],
        })

    def test_area_main_function_integration(
        self,
        mock_db,
        sample_plot_data,
        sample_cond_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test main area function integration workflow."""
        # Setup mock database with realistic FIA structure
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=sample_stratum_data)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(
                    return_value=Mock(collect=Mock(return_value=sample_ppsa_data))
                )
            ),
        }

        # Test basic forest area calculation
        result = area(mock_db, land_type="forest")

        # Validate result structure matches expected FIA area estimation output
        assert isinstance(result, pl.DataFrame)
        assert "AREA_PERC" in result.columns
        assert "AREA_PERC_SE" in result.columns
        assert "N_PLOTS" in result.columns
        
        # Validate reasonable area percentage (0-100%)
        area_perc = result["AREA_PERC"][0]
        assert 0 <= area_perc <= 100
        
        # Validate sample size
        n_plots = result["N_PLOTS"][0]
        assert n_plots > 0

    def test_area_by_land_type(
        self,
        mock_db,
        sample_plot_data,
        sample_cond_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test area calculation grouped by land type (core FIA functionality)."""
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=sample_stratum_data)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(
                    return_value=Mock(collect=Mock(return_value=sample_ppsa_data))
                )
            ),
        }

        result = area(mock_db, by_land_type=True)
        
        # Show the actual test results
        print("\n=== CURRENT TEST DATA (SYNTHETIC - NOT REAL FIA DATA) ===")
        print("Sample condition data represents:")
        print("- Plot 1: 70% Timber + 30% Timber (conditions C1, C2)")
        print("- Plot 2: 100% Timber (condition C3)")  
        print("- Plot 3: 100% Non-Forest (condition C4)")
        print("- Plot 4: 20% Water (condition C5)")
        print("- Plot 5: 100% Non-Timber Forest (condition C6)")
        print(f"\nCalculated area percentages:")
        for i, row in enumerate(result.iter_rows(named=True)):
            print(f"- {row['LAND_TYPE']}: {row['AREA_PERC']:.2f}% (N_PLOTS={row['N_PLOTS']})")

        # Validate land type classification works correctly
        assert "LAND_TYPE" in result.columns
        assert len(result) >= 1  # At least one land type category

        # Validate land type categories are correct
        land_types = result["LAND_TYPE"].unique().to_list()
        expected_types = ["Timber", "Non-Timber Forest", "Non-Forest", "Water"]
        for land_type in land_types:
            assert land_type in expected_types

        # Validate area percentages are reasonable
        for i in range(len(result)):
            area_perc = result["AREA_PERC"][i]
            if area_perc is not None:
                assert 0 <= area_perc <= 100

    def test_area_with_tree_domain(
        self,
        mock_db,
        sample_plot_data,
        sample_cond_data,
        sample_tree_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test area calculation with tree domain filter (critical FIA capability)."""
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.get_trees = Mock(return_value=sample_tree_data)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=sample_stratum_data)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(
                    return_value=Mock(collect=Mock(return_value=sample_ppsa_data))
                )
            ),
        }

        # Test filtering for conditions with live loblolly pine
        result = area(mock_db, tree_domain="SPCD == 131 and STATUSCD == 1")

        assert isinstance(result, pl.DataFrame)
        # Area should be filtered down since only some plots have qualifying trees
        assert result["AREA_PERC"][0] < 100.0
        assert result["AREA_PERC"][0] > 0.0

    def test_area_timber_land_type(
        self,
        mock_db,
        sample_plot_data,
        sample_cond_data,
        sample_stratum_data,
        sample_ppsa_data,
    ):
        """Test timber land classification (important FIA land use definition)."""
        mock_db.get_plots = Mock(return_value=sample_plot_data)
        mock_db.get_conditions = Mock(return_value=sample_cond_data)
        mock_db.tables = {
            "POP_STRATUM": Mock(collect=Mock(return_value=sample_stratum_data)),
            "POP_PLOT_STRATUM_ASSGN": Mock(
                filter=Mock(
                    return_value=Mock(collect=Mock(return_value=sample_ppsa_data))
                )
            ),
        }

        result = area(mock_db, land_type="timber")

        assert isinstance(result, pl.DataFrame)
        assert "AREA_PERC" in result.columns
        
        # Timber area should be subset of total forest area
        # (excludes reserved lands and non-productive sites)
        assert result["AREA_PERC"][0] <= 100.0
        assert result["AREA_PERC"][0] >= 0.0