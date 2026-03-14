"""Unit tests for load_table() EVALID and state filtering.

Verifies that load_table() applies PLT_CN-based EVALID filtering and
STATECD-based state filtering to any table that has those columns,
not just a hardcoded allowlist of table names.
"""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest


@pytest.fixture
def mock_fia():
    """Create a mock FIA instance with the real load_table method."""
    from pyfia.core.fia import FIA

    with patch.object(FIA, "__init__", lambda self: None):
        db = FIA()
        db.tables = {}
        db.evalid = None
        db.state_filter = None
        db._polygon_attributes = None
        db._spatial_plot_cns = None
        db._valid_plot_cns = None
        db._reader = MagicMock()
        return db


class TestEVALIDFilteringByColumn:
    """EVALID filtering should apply to any table with PLT_CN, not just TREE/COND."""

    def test_tree_grm_component_gets_evalid_filtered(self, mock_fia):
        """TREE_GRM_COMPONENT has PLT_CN and should be EVALID-filtered."""
        mock_fia.evalid = [132303]
        mock_fia._valid_plot_cns = ["100", "200", "300"]
        mock_fia._reader.get_table_schema.return_value = {
            "CN": "VARCHAR",
            "TRE_CN": "VARCHAR",
            "PLT_CN": "VARCHAR",
            "COMPONENT": "VARCHAR",
            "TPA_UNADJ": "DOUBLE",
        }
        mock_fia._reader.read_table.return_value = pl.DataFrame(
            {"TRE_CN": ["1"], "PLT_CN": ["100"], "TPA_UNADJ": [1.0]}
        ).lazy()

        mock_fia.load_table("TREE_GRM_COMPONENT")

        # Verify read_table was called with a PLT_CN IN (...) WHERE clause
        call_args = mock_fia._reader.read_table.call_args
        where_clause = call_args.kwargs.get("where", "") or ""
        assert "PLT_CN IN" in where_clause

    def test_table_without_plt_cn_skips_evalid_filter(self, mock_fia):
        """Tables without PLT_CN (e.g. POP_EVAL) should not get EVALID filtering."""
        mock_fia.evalid = [132303]
        mock_fia._valid_plot_cns = ["100", "200"]
        mock_fia._reader.get_table_schema.return_value = {
            "CN": "VARCHAR",
            "EVALID": "INTEGER",
            "EVAL_DESCR": "VARCHAR",
        }
        mock_fia._reader.read_table.return_value = pl.DataFrame(
            {"CN": ["1"], "EVALID": [132303], "EVAL_DESCR": ["test"]}
        ).lazy()

        mock_fia.load_table("POP_EVAL")

        # Should use default path without PLT_CN filtering
        call_args = mock_fia._reader.read_table.call_args
        where_clause = call_args.kwargs.get("where", "") or ""
        assert "PLT_CN IN" not in where_clause

    def test_no_evalid_set_skips_filter(self, mock_fia):
        """When no EVALID is set, PLT_CN filtering should be skipped."""
        mock_fia.evalid = None
        mock_fia._reader.get_table_schema.return_value = {
            "CN": "VARCHAR",
            "PLT_CN": "VARCHAR",
            "TPA_UNADJ": "DOUBLE",
        }
        mock_fia._reader.read_table.return_value = pl.DataFrame(
            {"CN": ["1"], "PLT_CN": ["100"], "TPA_UNADJ": [1.0]}
        ).lazy()

        mock_fia.load_table("TREE_GRM_COMPONENT")

        # Should use default path
        call_args = mock_fia._reader.read_table.call_args
        where_clause = call_args.kwargs.get("where", "") or ""
        assert "PLT_CN IN" not in where_clause


class TestStateFilteringByColumn:
    """State filtering should apply to any table with STATECD."""

    def test_table_with_statecd_gets_filtered(self, mock_fia):
        """Any table with STATECD should get state filtering."""
        mock_fia.state_filter = [13]  # Georgia
        mock_fia._reader.get_table_schema.return_value = {
            "CN": "VARCHAR",
            "PLT_CN": "VARCHAR",
            "STATECD": "INTEGER",
        }
        mock_fia._reader.read_table.return_value = pl.DataFrame(
            {"CN": ["1"], "PLT_CN": ["100"], "STATECD": [13]}
        ).lazy()

        mock_fia.load_table("SEEDLING")

        call_args = mock_fia._reader.read_table.call_args
        where_clause = call_args.kwargs.get("where", "") or ""
        assert "STATECD IN (13)" in where_clause

    def test_table_without_statecd_skips_filter(self, mock_fia):
        """Tables without STATECD should not get state filtering."""
        mock_fia.state_filter = [13]
        mock_fia._reader.get_table_schema.return_value = {
            "CN": "VARCHAR",
            "EVALID": "INTEGER",
        }
        mock_fia._reader.read_table.return_value = pl.DataFrame(
            {"CN": ["1"], "EVALID": [132303]}
        ).lazy()

        mock_fia.load_table("POP_EVAL")

        call_args = mock_fia._reader.read_table.call_args
        where_clause = call_args.kwargs.get("where", "") or ""
        assert "STATECD" not in where_clause


class TestSpatialFilteringByColumn:
    """Spatial filtering should apply to any table with PLT_CN."""

    def test_spatial_filter_applies_to_grm_table(self, mock_fia):
        """Tables with PLT_CN should get spatial filtering when active."""
        mock_fia._spatial_plot_cns = ["100", "200"]
        mock_fia._reader.get_table_schema.return_value = {
            "CN": "VARCHAR",
            "PLT_CN": "VARCHAR",
            "TPA_UNADJ": "DOUBLE",
        }
        data = pl.DataFrame(
            {
                "CN": ["1", "2", "3"],
                "PLT_CN": ["100", "200", "999"],
                "TPA_UNADJ": [1.0, 2.0, 3.0],
            }
        ).lazy()
        mock_fia._reader.read_table.return_value = data

        result = mock_fia.load_table("TREE_GRM_COMPONENT")

        # Should filter to only spatial plot CNs
        collected = result.collect()
        assert collected.shape[0] == 2
        assert set(collected["PLT_CN"].to_list()) == {"100", "200"}
