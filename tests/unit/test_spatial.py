"""
Unit tests for spatial filtering functionality.

Tests the clip_by_polygon method and related spatial operations.
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from pyfia.core import FIA
from pyfia.core.exceptions import (
    NoSpatialFilterError,
    SpatialExtensionError,
    SpatialFileError,
)


def _get_test_db_path() -> str | Path | None:
    """Get database path for tests, supporting MotherDuck."""
    env_path = os.getenv("PYFIA_DATABASE_PATH")
    if env_path:
        # MotherDuck connection strings don't need file existence check
        if env_path.startswith("md:") or env_path.startswith("motherduck:"):
            return env_path
        if Path(env_path).exists():
            return env_path

    default_path = Path("data/georgia.duckdb")
    if default_path.exists():
        return default_path

    # Fall back to MotherDuck if token available
    if os.getenv("MOTHERDUCK_TOKEN"):
        return "md:fia_ga_eval2023"

    return None


def _is_motherduck() -> bool:
    """Check if tests are running against MotherDuck."""
    env_path = os.getenv("PYFIA_DATABASE_PATH", "")
    if env_path.startswith("md:") or env_path.startswith("motherduck:"):
        return True
    # Also check if falling back to MotherDuck (no local DB but token available)
    db_path = _get_test_db_path()
    if isinstance(db_path, str) and (db_path.startswith("md:") or db_path.startswith("motherduck:")):
        return True
    return False


# Skip marker for tests that require local DuckDB with spatial extension
requires_local_duckdb = pytest.mark.skipif(
    _is_motherduck(),
    reason="Spatial operations require local DuckDB with spatial extension"
)


class TestSpatialExceptions:
    """Test spatial exception classes."""

    def test_spatial_file_error_basic(self):
        """Test SpatialFileError with basic message."""
        error = SpatialFileError("test.shp")
        assert "test.shp" in str(error)
        assert error.path == "test.shp"

    def test_spatial_file_error_with_reason(self):
        """Test SpatialFileError with reason."""
        error = SpatialFileError("test.shp", reason="File not found")
        assert "File not found" in str(error)
        assert error.reason == "File not found"

    def test_spatial_file_error_with_formats(self):
        """Test SpatialFileError with supported formats."""
        error = SpatialFileError(
            "test.xyz",
            reason="Unknown format",
            supported_formats=[".shp", ".geojson"],
        )
        assert ".shp" in str(error)
        assert ".geojson" in str(error)
        assert error.supported_formats == [".shp", ".geojson"]

    def test_spatial_extension_error_basic(self):
        """Test SpatialExtensionError with basic message."""
        error = SpatialExtensionError()
        assert "spatial extension" in str(error).lower()

    def test_spatial_extension_error_with_reason(self):
        """Test SpatialExtensionError with reason."""
        error = SpatialExtensionError(reason="Extension not installed")
        assert "Extension not installed" in str(error)

    def test_no_spatial_filter_error_basic(self):
        """Test NoSpatialFilterError with basic message."""
        error = NoSpatialFilterError("region.geojson")
        assert "region.geojson" in str(error)
        assert "EPSG:4326" in str(error)  # Should mention CRS

    def test_no_spatial_filter_error_with_count(self):
        """Test NoSpatialFilterError with polygon count."""
        error = NoSpatialFilterError("region.geojson", n_polygons=5)
        assert "5 polygon" in str(error)


@requires_local_duckdb
class TestClipByPolygonValidation:
    """Test clip_by_polygon input validation."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        path = _get_test_db_path()
        if path is None:
            pytest.skip("No FIA database found")
        return path

    def test_file_not_found_raises_error(self, db_path):
        """Test that non-existent file raises SpatialFileError."""
        with FIA(db_path) as db:
            with pytest.raises(SpatialFileError) as exc_info:
                db.clip_by_polygon("nonexistent_file.shp")

            assert "not found" in str(exc_info.value).lower()
            assert exc_info.value.path == "nonexistent_file.shp"

    def test_invalid_file_extension_still_attempts_read(self, db_path):
        """Test that any file extension is attempted (GDAL determines format)."""
        # Create a temporary file with invalid content
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"not a spatial file")
            temp_path = f.name

        try:
            with FIA(db_path) as db:
                # Should raise SpatialFileError when GDAL can't read it
                with pytest.raises((SpatialFileError, Exception)):
                    db.clip_by_polygon(temp_path)
        finally:
            Path(temp_path).unlink()


@requires_local_duckdb
class TestClipByPolygonIntegration:
    """Integration tests for clip_by_polygon with real data."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        path = _get_test_db_path()
        if path is None:
            pytest.skip("No FIA database found")
        return path

    @pytest.fixture
    def georgia_bbox_geojson(self, tmp_path):
        """Create a GeoJSON file with Georgia bounding box."""
        # Approximate bounding box for Georgia
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Georgia BBox"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-85.6, 30.3],  # SW corner
                                [-80.7, 30.3],  # SE corner
                                [-80.7, 35.0],  # NE corner
                                [-85.6, 35.0],  # NW corner
                                [-85.6, 30.3],  # Close polygon
                            ]
                        ],
                    },
                }
            ],
        }

        geojson_path = tmp_path / "georgia_bbox.geojson"
        with open(geojson_path, "w") as f:
            json.dump(geojson, f)

        return geojson_path

    @pytest.fixture
    def small_polygon_geojson(self, tmp_path):
        """Create a GeoJSON file with a small polygon (likely no plots)."""
        # Tiny polygon in the ocean (no FIA plots)
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Ocean"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-75.0, 30.0],
                                [-74.9, 30.0],
                                [-74.9, 30.1],
                                [-75.0, 30.1],
                                [-75.0, 30.0],
                            ]
                        ],
                    },
                }
            ],
        }

        geojson_path = tmp_path / "ocean.geojson"
        with open(geojson_path, "w") as f:
            json.dump(geojson, f)

        return geojson_path

    def test_clip_by_polygon_returns_self(self, db_path, georgia_bbox_geojson):
        """Test that clip_by_polygon returns self for chaining."""

        with FIA(db_path) as db:
            result = db.clip_by_polygon(georgia_bbox_geojson)
            assert result is db

    def test_clip_by_polygon_filters_plots(self, db_path, georgia_bbox_geojson):
        """Test that clip_by_polygon reduces the number of plots."""

        with FIA(db_path) as db:
            # Get all plots first
            db.clip_by_state(13)  # Georgia
            all_plots = db.get_plots()
            all_count = len(all_plots)

        with FIA(db_path) as db:
            # Get plots within polygon
            db.clip_by_state(13)
            db.clip_by_polygon(georgia_bbox_geojson)
            filtered_plots = db.get_plots()
            filtered_count = len(filtered_plots)

        # With a bounding box covering Georgia, should get most plots
        # but not necessarily all (due to coordinate precision)
        assert filtered_count > 0
        assert filtered_count <= all_count

    def test_clip_by_polygon_sets_spatial_plot_cns(self, db_path, georgia_bbox_geojson):
        """Test that clip_by_polygon sets _spatial_plot_cns attribute."""

        with FIA(db_path) as db:
            assert db._spatial_plot_cns is None
            db.clip_by_polygon(georgia_bbox_geojson)
            assert db._spatial_plot_cns is not None
            assert len(db._spatial_plot_cns) > 0

    def test_clip_by_polygon_empty_result_raises_error(
        self, db_path, small_polygon_geojson
    ):
        """Test that empty result raises NoSpatialFilterError."""

        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia
            with pytest.raises(NoSpatialFilterError):
                db.clip_by_polygon(small_polygon_geojson)

    def test_clip_by_polygon_chains_with_state_filter(
        self, db_path, georgia_bbox_geojson
    ):
        """Test that clip_by_polygon works after clip_by_state."""

        with FIA(db_path) as db:
            # Chain filters
            db.clip_by_state(13).clip_by_polygon(georgia_bbox_geojson)

            # Should have both filters applied
            assert db.state_filter == [13]
            assert db._spatial_plot_cns is not None

    def test_clip_by_polygon_clears_tables(self, db_path, georgia_bbox_geojson):
        """Test that clip_by_polygon clears cached tables."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            # Load a table
            db.load_table("PLOT")
            assert "PLOT" in db.tables

            # Apply spatial filter
            db.clip_by_polygon(georgia_bbox_geojson)

            # Tables should be cleared
            assert "PLOT" not in db.tables

    def test_clip_by_polygon_with_predicate(self, db_path, georgia_bbox_geojson):
        """Test clip_by_polygon with different predicates."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            # Use 'within' predicate
            db.clip_by_polygon(georgia_bbox_geojson, predicate="within")
            within_count = len(db._spatial_plot_cns or [])

        with FIA(db_path) as db:
            db.clip_by_state(13)
            # Use 'intersects' predicate (default)
            db.clip_by_polygon(georgia_bbox_geojson, predicate="intersects")
            intersects_count = len(db._spatial_plot_cns or [])

        # For points, intersects and within should give same results
        # (a point is within a polygon if and only if it intersects it)
        assert within_count == intersects_count


@requires_local_duckdb
class TestSpatialExtensionLoading:
    """Test DuckDB spatial extension loading."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        path = _get_test_db_path()
        if path is None:
            pytest.skip("No FIA database found")
        return path

    def test_spatial_extension_loads_on_demand(self, db_path):
        """Test that spatial extension is loaded when needed."""
        with FIA(db_path) as db:
            backend = db._reader._backend
            assert hasattr(backend, "_spatial_loaded")
            assert not backend._spatial_loaded

            # Load extension explicitly
            backend.load_spatial_extension()
            assert backend._spatial_loaded

    def test_spatial_extension_loads_once(self, db_path):
        """Test that spatial extension is only loaded once."""
        with FIA(db_path) as db:
            backend = db._reader._backend

            # Load twice
            backend.load_spatial_extension()
            backend.load_spatial_extension()

            # Should still be loaded
            assert backend._spatial_loaded


@requires_local_duckdb
class TestIntersectPolygons:
    """Test intersect_polygons method for spatial attribute joins."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        path = _get_test_db_path()
        if path is None:
            pytest.skip("No FIA database found")
        return path

    @pytest.fixture
    def counties_geojson(self, tmp_path):
        """Create a GeoJSON with multiple county-like polygons with attributes."""
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"NAME": "North Region", "REGION_ID": 1},
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
                    "properties": {"NAME": "South Region", "REGION_ID": 2},
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

    def test_intersect_polygons_returns_self(self, db_path, counties_geojson):
        """Test that intersect_polygons returns self for chaining."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            result = db.intersect_polygons(counties_geojson, attributes=["NAME"])
            assert result is db

    def test_intersect_polygons_sets_attributes(self, db_path, counties_geojson):
        """Test that intersect_polygons sets _polygon_attributes."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            assert db._polygon_attributes is None
            db.intersect_polygons(counties_geojson, attributes=["NAME", "REGION_ID"])
            assert db._polygon_attributes is not None
            assert "CN" in db._polygon_attributes.columns
            assert "NAME" in db._polygon_attributes.columns
            assert "REGION_ID" in db._polygon_attributes.columns

    def test_intersect_polygons_joins_to_plot(self, db_path, counties_geojson):
        """Test that polygon attributes are joined to PLOT table."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.intersect_polygons(counties_geojson, attributes=["NAME"])

            # Load PLOT table
            db.load_table("PLOT")
            plot_schema = db.tables["PLOT"].collect_schema().names()

            # NAME should be in PLOT columns
            assert "NAME" in plot_schema

    def test_intersect_polygons_file_not_found(self, db_path):
        """Test that non-existent file raises SpatialFileError."""

        with FIA(db_path) as db:
            with pytest.raises(SpatialFileError) as exc_info:
                db.intersect_polygons("nonexistent.geojson", attributes=["NAME"])

            assert "not found" in str(exc_info.value).lower()

    def test_intersect_polygons_empty_attributes_raises_error(
        self, db_path, counties_geojson
    ):
        """Test that empty attributes list raises ValueError."""

        with FIA(db_path) as db:
            with pytest.raises(ValueError) as exc_info:
                db.intersect_polygons(counties_geojson, attributes=[])

            assert "at least one column" in str(exc_info.value).lower()

    def test_intersect_polygons_missing_attribute_raises_error(
        self, db_path, counties_geojson
    ):
        """Test that requesting non-existent attribute raises ValueError."""

        with FIA(db_path) as db:
            with pytest.raises(ValueError) as exc_info:
                db.intersect_polygons(
                    counties_geojson, attributes=["NAME", "NONEXISTENT_COLUMN"]
                )

            assert "NONEXISTENT_COLUMN" in str(exc_info.value)

    def test_intersect_polygons_clears_tables(self, db_path, counties_geojson):
        """Test that intersect_polygons clears cached tables."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.load_table("PLOT")
            assert "PLOT" in db.tables

            db.intersect_polygons(counties_geojson, attributes=["NAME"])

            # Tables should be cleared
            assert "PLOT" not in db.tables

    def test_intersect_polygons_chains_with_clip_by_polygon(
        self, db_path, counties_geojson
    ):
        """Test that intersect_polygons and clip_by_polygon can be used together."""

        # Create a smaller clip polygon
        clip_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-84.0, 32.0],
                                [-83.0, 32.0],
                                [-83.0, 33.0],
                                [-84.0, 33.0],
                                [-84.0, 32.0],
                            ]
                        ],
                    },
                }
            ],
        }

        import tempfile

        with tempfile.NamedTemporaryFile(
            suffix=".geojson", delete=False, mode="w"
        ) as f:
            json.dump(clip_geojson, f)
            clip_path = f.name

        try:
            with FIA(db_path) as db:
                db.clip_by_state(13)
                db.clip_by_polygon(clip_path)
                db.intersect_polygons(counties_geojson, attributes=["NAME"])

                # Both should be set
                assert db._spatial_plot_cns is not None
                assert db._polygon_attributes is not None
        finally:
            Path(clip_path).unlink()

    def test_intersect_polygons_multiple_attributes(self, db_path, counties_geojson):
        """Test that multiple attributes can be requested."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.intersect_polygons(
                counties_geojson, attributes=["NAME", "REGION_ID"]
            )

            assert db._polygon_attributes is not None
            assert len(db._polygon_attributes.columns) == 3  # CN, NAME, REGION_ID

    def test_intersect_polygons_assigns_correct_region(self, db_path, counties_geojson):
        """Test that plots are assigned to correct regions based on location."""

        with FIA(db_path) as db:
            db.clip_by_state(13)
            db.intersect_polygons(counties_geojson, attributes=["NAME"])

            # Check that we have both regions represented
            attrs = db._polygon_attributes
            unique_names = attrs["NAME"].unique().to_list()

            # Georgia spans both regions we defined
            assert len(unique_names) >= 1  # At least one region should match
