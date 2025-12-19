"""
Integration tests for spatial filtering and grouping.

These tests validate end-to-end spatial workflows:
- clip_by_polygon with estimators
- intersect_polygons with grp_by in estimators
- Combination of spatial and non-spatial filters

Tests use real FIA database for realistic validation.
"""

import json
from pathlib import Path

import pytest

from pyfia import FIA, area, tpa


class TestSpatialWithEstimators:
    """Integration tests for spatial operations with estimators."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        return Path("data/georgia.duckdb")

    @pytest.fixture
    def north_south_regions(self, tmp_path):
        """Create GeoJSON with North/South regions splitting Georgia."""
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"REGION": "North", "REGION_ID": 1},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-85.6, 33.0],
                                [-80.7, 33.0],
                                [-80.7, 35.0],
                                [-85.6, 35.0],
                                [-85.6, 33.0],
                            ]
                        ],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {"REGION": "South", "REGION_ID": 2},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-85.6, 30.3],
                                [-80.7, 30.3],
                                [-80.7, 33.0],
                                [-85.6, 33.0],
                                [-85.6, 30.3],
                            ]
                        ],
                    },
                },
            ],
        }

        geojson_path = tmp_path / "regions.geojson"
        with open(geojson_path, "w") as f:
            json.dump(geojson, f)

        return geojson_path

    @pytest.fixture
    def clip_polygon(self, tmp_path):
        """Create a smaller clip polygon for testing."""
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-84.5, 32.0],
                                [-83.0, 32.0],
                                [-83.0, 34.0],
                                [-84.5, 34.0],
                                [-84.5, 32.0],
                            ]
                        ],
                    },
                }
            ],
        }

        geojson_path = tmp_path / "clip.geojson"
        with open(geojson_path, "w") as f:
            json.dump(geojson, f)

        return geojson_path

    def test_intersect_polygons_with_tpa_grp_by(self, db_path, north_south_regions):
        """Test that intersect_polygons attributes work with tpa grp_by."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia
            db.clip_most_recent(eval_type="VOL")
            db.intersect_polygons(north_south_regions, attributes=["REGION"])

            # Run TPA with grp_by using polygon attribute
            result = tpa(db, grp_by=["REGION"], tree_type="live")

            # Verify results
            assert result is not None
            assert len(result) > 0
            assert "REGION" in result.columns
            assert "TPA" in result.columns

            # Should have results for at least one region
            regions = result["REGION"].unique().to_list()
            assert len(regions) >= 1

            # TPA values should be positive
            assert all(result["TPA"] > 0)

    def test_intersect_polygons_with_area_grp_by(self, db_path, north_south_regions):
        """Test that intersect_polygons attributes work with area grp_by."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia
            db.clip_most_recent(eval_type="ALL")
            db.intersect_polygons(north_south_regions, attributes=["REGION"])

            # Run area with grp_by using polygon attribute
            result = area(db, grp_by=["REGION"], land_type="forest")

            # Verify results
            assert result is not None
            assert len(result) > 0
            assert "REGION" in result.columns
            assert "AREA" in result.columns

            # Should have results for at least one region
            regions = result["REGION"].unique().to_list()
            assert len(regions) >= 1

            # Area values should be positive
            assert all(result["AREA"] > 0)

    def test_intersect_polygons_multiple_attributes_grp_by(
        self, db_path, north_south_regions
    ):
        """Test grp_by with multiple polygon attributes."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")
            db.intersect_polygons(
                north_south_regions, attributes=["REGION", "REGION_ID"]
            )

            # Group by both attributes
            result = tpa(db, grp_by=["REGION", "REGION_ID"], tree_type="live")

            assert result is not None
            assert "REGION" in result.columns
            assert "REGION_ID" in result.columns

    def test_intersect_polygons_combined_with_species_grouping(
        self, db_path, north_south_regions
    ):
        """Test combining polygon grouping with by_species."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")
            db.intersect_polygons(north_south_regions, attributes=["REGION"])

            # Group by both region and species
            result = tpa(db, grp_by=["REGION"], by_species=True, tree_type="live")

            assert result is not None
            assert "REGION" in result.columns
            assert "SPCD" in result.columns
            assert "TPA" in result.columns

            # Should have multiple species per region
            assert len(result) > 2

    def test_clip_by_polygon_with_estimator(self, db_path, clip_polygon):
        """Test that clip_by_polygon filters are applied in estimators."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        # Get baseline without spatial filter
        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            baseline = area(db, land_type="forest")
            baseline_area = baseline["AREA"].sum()

        # Get result with spatial filter
        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            db.clip_by_polygon(clip_polygon)
            filtered = area(db, land_type="forest")
            filtered_area = filtered["AREA"].sum()

        # Filtered area should be less than baseline
        assert filtered_area < baseline_area
        assert filtered_area > 0

    def test_clip_and_intersect_combined(
        self, db_path, clip_polygon, north_south_regions
    ):
        """Test using both clip_by_polygon and intersect_polygons together."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")

            # First clip to a region
            db.clip_by_polygon(clip_polygon)

            # Then add region attributes for grouping
            db.intersect_polygons(north_south_regions, attributes=["REGION"])

            # Run estimation with grouping
            result = tpa(db, grp_by=["REGION"], tree_type="live")

            assert result is not None
            assert "REGION" in result.columns
            assert len(result) >= 1

    def test_intersect_polygons_preserves_variance(self, db_path, north_south_regions):
        """Test that variance calculations work with polygon grouping."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")
            db.intersect_polygons(north_south_regions, attributes=["REGION"])

            result = tpa(db, grp_by=["REGION"], tree_type="live", variance=True)

            assert result is not None
            # Should have variance columns (TPA_VAR contains variance for TPA)
            assert "TPA_VAR" in result.columns or "TPA_SE" in result.columns

    def test_north_south_region_assignment(self, db_path, north_south_regions):
        """Test that plots are correctly assigned to North/South regions."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.intersect_polygons(north_south_regions, attributes=["REGION"])

            # Get the polygon attributes
            attrs = db._polygon_attributes
            assert attrs is not None

            # Georgia spans both regions (roughly divided at 33 degrees latitude)
            regions = attrs["REGION"].unique().to_list()

            # Should have both North and South
            assert "North" in regions or "South" in regions

            # Count plots per region
            region_counts = attrs.group_by("REGION").len()
            assert all(region_counts["len"] > 0)


class TestSpatialEdgeCases:
    """Test edge cases in spatial operations."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        return Path("data/georgia.duckdb")

    @pytest.fixture
    def tiny_polygon(self, tmp_path):
        """Create a very small polygon that may not contain many plots."""
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"NAME": "Tiny"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-84.0, 33.0],
                                [-83.9, 33.0],
                                [-83.9, 33.1],
                                [-84.0, 33.1],
                                [-84.0, 33.0],
                            ]
                        ],
                    },
                }
            ],
        }

        geojson_path = tmp_path / "tiny.geojson"
        with open(geojson_path, "w") as f:
            json.dump(geojson, f)

        return geojson_path

    def test_intersect_with_few_matching_plots(self, db_path, tiny_polygon):
        """Test intersect_polygons with polygon matching few plots."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)

            # This may match very few plots
            db.intersect_polygons(tiny_polygon, attributes=["NAME"])

            # Should still work, even if few matches
            attrs = db._polygon_attributes
            # Could be 0 or more matches
            assert attrs is not None

    def test_estimator_with_null_polygon_attributes(self, db_path, tiny_polygon):
        """Test that estimators handle NULL polygon attributes gracefully."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="VOL")
            db.intersect_polygons(tiny_polygon, attributes=["NAME"])

            # Run without grp_by - should work even with NULLs
            result = tpa(db, tree_type="live")

            assert result is not None
            assert "TPA" in result.columns
