"""
Unit tests for spatial filtering functionality.

Tests the clip_by_polygon method and related spatial operations.
"""

import json
import tempfile
from pathlib import Path

import pytest

from pyfia.core import FIA
from pyfia.core.exceptions import (
    NoSpatialFilterError,
    SpatialExtensionError,
    SpatialFileError,
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


class TestClipByPolygonValidation:
    """Test clip_by_polygon input validation."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        return Path("data/georgia.duckdb")

    def test_file_not_found_raises_error(self, db_path):
        """Test that non-existent file raises SpatialFileError."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            with pytest.raises(SpatialFileError) as exc_info:
                db.clip_by_polygon("nonexistent_file.shp")

            assert "not found" in str(exc_info.value).lower()
            assert exc_info.value.path == "nonexistent_file.shp"

    def test_invalid_file_extension_still_attempts_read(self, db_path):
        """Test that any file extension is attempted (GDAL determines format)."""
        if not db_path.exists():
            pytest.skip("Test database not found")

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


class TestClipByPolygonIntegration:
    """Integration tests for clip_by_polygon with real data."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        return Path("data/georgia.duckdb")

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
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            result = db.clip_by_polygon(georgia_bbox_geojson)
            assert result is db

    def test_clip_by_polygon_filters_plots(self, db_path, georgia_bbox_geojson):
        """Test that clip_by_polygon reduces the number of plots."""
        if not db_path.exists():
            pytest.skip("Test database not found")

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
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            assert db._spatial_plot_cns is None
            db.clip_by_polygon(georgia_bbox_geojson)
            assert db._spatial_plot_cns is not None
            assert len(db._spatial_plot_cns) > 0

    def test_clip_by_polygon_empty_result_raises_error(
        self, db_path, small_polygon_geojson
    ):
        """Test that empty result raises NoSpatialFilterError."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            db.clip_by_state(13)  # Georgia
            with pytest.raises(NoSpatialFilterError):
                db.clip_by_polygon(small_polygon_geojson)

    def test_clip_by_polygon_chains_with_state_filter(
        self, db_path, georgia_bbox_geojson
    ):
        """Test that clip_by_polygon works after clip_by_state."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            # Chain filters
            db.clip_by_state(13).clip_by_polygon(georgia_bbox_geojson)

            # Should have both filters applied
            assert db.state_filter == [13]
            assert db._spatial_plot_cns is not None

    def test_clip_by_polygon_clears_tables(self, db_path, georgia_bbox_geojson):
        """Test that clip_by_polygon clears cached tables."""
        if not db_path.exists():
            pytest.skip("Test database not found")

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
        if not db_path.exists():
            pytest.skip("Test database not found")

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


class TestSpatialExtensionLoading:
    """Test DuckDB spatial extension loading."""

    @pytest.fixture
    def db_path(self):
        """Return path to test database."""
        return Path("data/georgia.duckdb")

    def test_spatial_extension_loads_on_demand(self, db_path):
        """Test that spatial extension is loaded when needed."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            backend = db._reader._backend
            assert hasattr(backend, "_spatial_loaded")
            assert not backend._spatial_loaded

            # Load extension explicitly
            backend.load_spatial_extension()
            assert backend._spatial_loaded

    def test_spatial_extension_loads_once(self, db_path):
        """Test that spatial extension is only loaded once."""
        if not db_path.exists():
            pytest.skip("Test database not found")

        with FIA(db_path) as db:
            backend = db._reader._backend

            # Load twice
            backend.load_spatial_extension()
            backend.load_spatial_extension()

            # Should still be loaded
            assert backend._spatial_loaded
