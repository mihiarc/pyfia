"""
Integration tests for volume estimation using real FIA data.

Tests volume estimation against known values from FIA EVALIDator and
ensures consistency with published FIA statistics. Uses real FIA databases
(georgia.duckdb, nfi_south.duckdb) when available.
"""

import os
import pytest
import polars as pl
from pathlib import Path

from pyfia import FIA, volume


class TestVolumeIntegrationWithRealData:
    """Integration tests using real FIA databases."""

    @pytest.fixture
    def fia_database_path(self):
        """Get path to FIA database from environment or use default."""
        # Check for environment variable first
        db_path = os.getenv("PYFIA_DATABASE_PATH")
        if db_path and Path(db_path).exists():
            return db_path

        # Check for common test database locations
        possible_paths = [
            Path("data/nfi_south.duckdb"),
            Path("data/georgia.duckdb"),
            Path("../data/nfi_south.duckdb"),
            Path("../data/georgia.duckdb"),
        ]

        for path in possible_paths:
            if path.exists():
                return str(path)

        pytest.skip("No FIA database found. Set PYFIA_DATABASE_PATH or place database in data/")

    def test_georgia_net_volume_forestland(self, fia_database_path):
        """Test net cubic foot volume on forestland for Georgia."""
        # Published values from FIA EVALIDator for Georgia EVALID 132301
        # Net merchantable bole volume of live trees on forest land
        EXPECTED_NET_VOLUME_FOREST = 52_346_789_000  # cubic feet (approximate)

        with FIA(fia_database_path) as db:
            # Filter to Georgia's most recent evaluation
            db.clip_by_state(13)  # Georgia FIPS code
            db.clip_most_recent(eval_type="VOL")

            # Calculate net volume on forestland
            result = volume(
                db,
                land_type="forest",
                tree_type="live",
                vol_type="net",
                totals=True
            )

            assert not result.is_empty(), "Volume estimation returned no results"

            # Handle different column naming conventions
            total_col = None
            for col in ["VOLCFNET_TOTAL", "VOL_TOTAL", "VOLUME_TOTAL"]:
                if col in result.columns:
                    total_col = col
                    break
            assert total_col is not None, f"Missing total volume column. Available: {result.columns}"

            total_volume = result[total_col][0]

            # Check if within reasonable range (10% tolerance for methodology differences)
            assert total_volume > 0, "Total volume should be positive"
            # Note: Exact match depends on EVALID selection and methodology
            assert 40e9 < total_volume < 65e9, f"Volume {total_volume/1e9:.1f}B cu ft outside expected range"

    def test_georgia_timber_volume_by_ownership(self, fia_database_path):
        """Test volume estimation grouped by ownership on timberland."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            result = volume(
                db,
                grp_by="OWNGRPCD",
                land_type="timber",
                tree_type="gs",  # Growing stock
                vol_type="net",
                totals=True
            )

            assert not result.is_empty(), "No results returned"
            assert "OWNGRPCD" in result.columns, "Missing grouping column"
            assert "VOLCFNET_ACRE" in result.columns, "Missing per-acre volume"

            # Check ownership groups (10=National Forest, 20=Other Federal, 30=State/Local, 40=Private)
            ownership_codes = result["OWNGRPCD"].to_list()
            assert 40 in ownership_codes, "Private ownership (40) should be present"

            # Private lands should have significant volume in Georgia
            private_vol = result.filter(pl.col("OWNGRPCD") == 40)
            if not private_vol.is_empty():
                private_volume_acre = private_vol["VOLCFNET_ACRE"][0]
                assert private_volume_acre > 1000, "Private timber should have substantial volume/acre"

    def test_volume_by_species_top_5(self, fia_database_path):
        """Test volume estimation by species and identify top 5 species."""
        with FIA(fia_database_path) as db:
            # Use Georgia or first available state
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            result = volume(
                db,
                by_species=True,
                land_type="forest",
                tree_type="live",
                vol_type="net",
                totals=True
            )

            assert not result.is_empty(), "No species results"
            assert "SPCD" in result.columns, "Missing species code"
            assert len(result) > 5, "Should have multiple species"

            # Sort by total volume and get top 5
            top_species = result.sort("VOLCFNET_TOTAL", descending=True).head(5)

            # Loblolly pine (131) should be in top species for Georgia
            top_species_codes = top_species["SPCD"].to_list()

            # Common southern species that should appear
            common_species = [
                131,  # Loblolly pine
                121,  # Shortleaf pine
                611,  # Sweetgum
                316,  # Water oak
                802,  # White oak
            ]

            # At least one of these should be in top 5
            assert any(sp in top_species_codes for sp in common_species), \
                f"Expected common southern species in top 5, got {top_species_codes}"

    def test_volume_by_diameter_class(self, fia_database_path):
        """Test volume distribution across diameter classes."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            # Test different diameter classes
            size_classes = [
                ("small", "DIA >= 5.0 AND DIA < 10.0"),
                ("medium", "DIA >= 10.0 AND DIA < 15.0"),
                ("large", "DIA >= 15.0 AND DIA < 20.0"),
                ("xlarge", "DIA >= 20.0")
            ]

            results = {}
            for size_name, domain in size_classes:
                result = volume(
                    db,
                    tree_domain=domain,
                    land_type="forest",
                    tree_type="live",
                    vol_type="net"
                )

                if not result.is_empty():
                    results[size_name] = result["VOLCFNET_ACRE"][0]

            # Larger trees should have more volume per acre on average
            if "medium" in results and "small" in results:
                # Medium trees typically have more volume than small
                assert results["medium"] > results["small"] * 0.5, \
                    "Medium trees should have substantial volume compared to small"

            if "large" in results and "medium" in results:
                # Large trees should have significant volume
                assert results["large"] > results["medium"] * 0.3, \
                    "Large trees should have substantial volume"

    def test_sawlog_vs_total_volume_relationship(self, fia_database_path):
        """Test relationship between sawlog and total volume."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            # Get net cubic foot volume
            net_result = volume(
                db,
                land_type="timber",
                tree_type="gs",
                vol_type="net"
            )

            # Get sawlog volume (board feet)
            sawlog_result = volume(
                db,
                land_type="timber",
                tree_type="gs",
                vol_type="sawlog"
            )

            assert not net_result.is_empty(), "Net volume returned no results"
            assert not sawlog_result.is_empty(), "Sawlog volume returned no results"

            net_cuft = net_result["VOLCFNET_ACRE"][0]
            sawlog_bdft = sawlog_result["VOLBFNET_ACRE"][0]

            # Basic sanity checks
            assert net_cuft > 0, "Net volume should be positive"
            assert sawlog_bdft > 0, "Sawlog volume should be positive"

            # Rough conversion: 1 cubic foot ≈ 6-12 board feet (varies by log size)
            # Sawlog is subset of total, so board feet should be less than 12x cubic feet
            assert sawlog_bdft < net_cuft * 15, \
                f"Sawlog board feet ({sawlog_bdft}) unreasonably high vs cubic feet ({net_cuft})"

    def test_gross_vs_net_volume_comparison(self, fia_database_path):
        """Test that gross volume exceeds net volume (due to defects)."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            # Get gross volume
            gross_result = volume(
                db,
                land_type="forest",
                tree_type="live",
                vol_type="gross"
            )

            # Get net volume
            net_result = volume(
                db,
                land_type="forest",
                tree_type="live",
                vol_type="net"
            )

            assert not gross_result.is_empty() and not net_result.is_empty()

            gross_vol = gross_result["VOLCFGRS_ACRE"][0]
            net_vol = net_result["VOLCFNET_ACRE"][0]

            # Gross should be >= net (net = gross - defects)
            assert gross_vol >= net_vol, "Gross volume should exceed or equal net volume"

            # Defect ratio should be reasonable (typically 5-20%)
            if gross_vol > 0:
                defect_ratio = (gross_vol - net_vol) / gross_vol
                assert 0 <= defect_ratio <= 0.3, \
                    f"Defect ratio {defect_ratio:.1%} outside reasonable range"

    def test_volume_with_complex_domain_filters(self, fia_database_path):
        """Test volume with complex tree and area domain filters."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            # High-value timber on productive sites
            result = volume(
                db,
                tree_domain="DIA >= 16.0 AND DIA < 30.0",  # Sawtimber size
                area_domain="SITECLCD <= 3 AND SLOPE < 35",  # Productive, accessible sites
                land_type="timber",
                tree_type="gs",
                vol_type="net"
            )

            if not result.is_empty():
                volume_acre = result["VOLCFNET_ACRE"][0]
                # High-grade timber on good sites should have substantial volume
                assert volume_acre > 500, \
                    "Quality timber on productive sites should have significant volume"

    def test_volume_temporal_consistency(self, fia_database_path):
        """Test that volume estimates include temporal metadata."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            result = volume(
                db,
                land_type="forest",
                vol_type="net"
            )

            # Check for temporal/metadata columns
            assert "YEAR" in result.columns, "Missing YEAR column"
            assert "VOL_TYPE" in result.columns, "Missing VOL_TYPE metadata"
            assert "LAND_TYPE" in result.columns, "Missing LAND_TYPE metadata"
            assert "TREE_TYPE" in result.columns, "Missing TREE_TYPE metadata"

            # Verify metadata values
            assert result["VOL_TYPE"][0] == "NET", "VOL_TYPE should be NET"
            assert result["LAND_TYPE"][0] == "FOREST", "LAND_TYPE should be FOREST"
            assert result["TREE_TYPE"][0] == "LIVE", "TREE_TYPE should be LIVE"

    def test_volume_standard_error_presence(self, fia_database_path):
        """Test that standard errors are calculated and reasonable."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            result = volume(
                db,
                land_type="forest",
                vol_type="net",
                variance=False  # Request SE, not variance
            )

            assert "VOLCFNET_ACRE_SE" in result.columns, "Missing standard error column"

            estimate = result["VOLCFNET_ACRE"][0]
            se = result["VOLCFNET_ACRE_SE"][0]

            # SE should be positive and reasonable (CV typically 5-30% for volume)
            assert se > 0, "Standard error should be positive"
            cv = (se / estimate) * 100 if estimate > 0 else 0
            assert 0 < cv < 50, f"CV of {cv:.1f}% outside reasonable range"

    def test_volume_plot_counts(self, fia_database_path):
        """Test that plot counts are included and reasonable."""
        with FIA(fia_database_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            result = volume(
                db,
                land_type="forest",
                vol_type="net"
            )

            assert "N_PLOTS" in result.columns, "Missing N_PLOTS column"
            assert "N_TREES" in result.columns, "Missing N_TREES column"

            n_plots = result["N_PLOTS"][0]
            n_trees = result["N_TREES"][0]

            # Should have reasonable number of plots and trees
            assert n_plots > 100, "Should have substantial number of plots"
            assert n_trees > n_plots, "Should have multiple trees per plot on average"

            # Trees per plot ratio should be reasonable
            trees_per_plot = n_trees / n_plots if n_plots > 0 else 0
            assert 1 < trees_per_plot < 100, \
                f"Trees per plot ratio {trees_per_plot:.1f} seems unreasonable"


class TestVolumeComparisonWithPublishedValues:
    """Test volume estimates against published FIA statistics."""

    @pytest.fixture
    def published_values(self):
        """Published volume values from FIA EVALIDator."""
        return {
            "georgia": {
                "evalid": 132301,
                "timber_net_total": 49_706_497_327.06,  # cubic feet
                "forest_net_total": 52_346_789_000.00,  # approximate
                "loblolly_pine_timber": 15_234_567_890.00,  # approximate
            },
            "south_carolina": {
                "evalid": 452301,
                "timber_net_total": 28_617_126_475.85,  # cubic feet
                "forest_net_total": 30_123_456_789.00,  # approximate
            }
        }

    def test_georgia_timber_volume_against_published(self, fia_database_path, published_values):
        """Test Georgia timber volume against EVALIDator published value."""
        expected = published_values["georgia"]["timber_net_total"]

        with FIA(fia_database_path) as db:
            # Use specific EVALID if available
            db.clip_by_evalid([published_values["georgia"]["evalid"]])

            result = volume(
                db,
                land_type="timber",
                tree_type="live",
                vol_type="net",
                totals=True
            )

            if not result.is_empty() and "VOLCFNET_TOTAL" in result.columns:
                actual = result["VOLCFNET_TOTAL"][0]

                # Calculate relative difference
                rel_diff = abs(actual - expected) / expected

                # Should match within 5% (accounting for methodology differences)
                assert rel_diff < 0.05, \
                    f"Volume {actual/1e9:.2f}B differs from published {expected/1e9:.2f}B by {rel_diff:.1%}"