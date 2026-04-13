"""Unit tests for total_ecosystem function.

Tests the pool-summing logic in isolation using monkeypatched pool functions.
No database connection required.
"""

from unittest.mock import patch

import polars as pl
import pytest

from pyfia.carbon.total_ecosystem import total_ecosystem


class MockDB:
    """Mock database for testing."""

    def __init__(self):
        self.db_path = "/fake/path"
        self.tables = {}
        self.evalid = [132301]
        self.evalids = None
        self._state_filter = None


def _make_pool_result(
    carbon_acre: float, carbon_total: float, n_plots: int = 100, year: int = 2023
) -> pl.DataFrame:
    """Build a minimal pool result DataFrame."""
    return pl.DataFrame(
        {
            "YEAR": [year],
            "POOL": ["TOTAL"],
            "CARBON_ACRE": [carbon_acre],
            "CARBON_TOTAL": [carbon_total],
            "N_PLOTS": [n_plots],
            "N_TREES": [0],
        }
    )


def _make_empty_result() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "YEAR": pl.Series([], dtype=pl.Int64),
            "POOL": pl.Series([], dtype=pl.Utf8),
            "CARBON_ACRE": pl.Series([], dtype=pl.Float64),
            "CARBON_TOTAL": pl.Series([], dtype=pl.Float64),
            "N_PLOTS": pl.Series([], dtype=pl.Int64),
            "N_TREES": pl.Series([], dtype=pl.Int64),
        }
    )


# Patch at the source modules where the functions are defined
_POOL_PATCHES = {
    "live_tree": "pyfia.carbon.live_tree.live_tree",
    "standing_dead": "pyfia.carbon.standing_dead.standing_dead",
    "understory": "pyfia.carbon.understory.understory",
    "downed_dead": "pyfia.carbon.downed_dead.downed_dead",
    "litter": "pyfia.carbon.litter.litter",
    "soil_organic": "pyfia.carbon.soil_organic.soil_organic",
}


def _start_pool_patches(pool_results: dict) -> list:
    """Start patches for all six pool functions. Returns list of patchers."""
    patchers = []
    for name, target in _POOL_PATCHES.items():
        p = patch(target, return_value=pool_results.get(name, _make_pool_result(1.0, 1000.0)))
        p.start()
        patchers.append(p)
    return patchers


def _stop_pool_patches(patchers: list):
    for p in patchers:
        p.stop()


@pytest.fixture
def mock_infra():
    """Patch ensure_fia_instance and ensure_evalid_set."""
    with patch(
        "pyfia.carbon.total_ecosystem.ensure_fia_instance",
        return_value=(MockDB(), False),
    ), patch("pyfia.carbon.total_ecosystem.ensure_evalid_set"):
        yield


class TestTotalEcosystemSumming:
    """Tests for the pool-summing logic."""

    def test_sums_carbon_acre_correctly(self, mock_infra):
        results = {
            "live_tree": _make_pool_result(10.0, 100000.0),
            "standing_dead": _make_pool_result(2.0, 20000.0),
            "understory": _make_pool_result(1.0, 10000.0),
            "downed_dead": _make_pool_result(3.0, 30000.0),
            "litter": _make_pool_result(4.0, 40000.0),
            "soil_organic": _make_pool_result(20.0, 200000.0),
        }
        patchers = _start_pool_patches(results)
        try:
            result = total_ecosystem(MockDB())
            total_row = result.filter(pl.col("POOL") == "TOTAL_ECOSYSTEM")
            assert abs(float(total_row["CARBON_ACRE"][0]) - 40.0) < 1e-10
            assert abs(float(total_row["CARBON_TOTAL"][0]) - 400000.0) < 1e-10
        finally:
            _stop_pool_patches(patchers)

    def test_result_has_seven_rows(self, mock_infra):
        """6 pool rows + 1 TOTAL_ECOSYSTEM row."""
        results = {
            name: _make_pool_result(1.0, 1000.0)
            for name in _POOL_PATCHES
        }
        patchers = _start_pool_patches(results)
        try:
            result = total_ecosystem(MockDB())
            assert len(result) == 7
            pools = result["POOL"].to_list()
            for expected in [
                "TOTAL_ECOSYSTEM", "LIVE_TREE", "STANDING_DEAD",
                "UNDERSTORY", "DOWNED_DEAD", "LITTER", "SOIL_ORGANIC",
            ]:
                assert expected in pools
        finally:
            _stop_pool_patches(patchers)

    def test_handles_all_empty_results(self, mock_infra):
        """If all pools return empty, total_ecosystem returns a fallback row."""
        results = {name: _make_empty_result() for name in _POOL_PATCHES}
        patchers = _start_pool_patches(results)
        try:
            result = total_ecosystem(MockDB())
            assert len(result) > 0
            assert result["CARBON_ACRE"][0] == 0.0
        finally:
            _stop_pool_patches(patchers)

    def test_year_propagates(self, mock_infra):
        results = {
            name: _make_pool_result(1.0, 1000.0, year=2023)
            for name in _POOL_PATCHES
        }
        patchers = _start_pool_patches(results)
        try:
            result = total_ecosystem(MockDB())
            total_row = result.filter(pl.col("POOL") == "TOTAL_ECOSYSTEM")
            assert int(total_row["YEAR"][0]) == 2023
        finally:
            _stop_pool_patches(patchers)


class TestTotalEcosystemSignature:
    def test_no_pool_parameter(self):
        """total_ecosystem has no pool parameter."""
        import inspect
        sig = inspect.signature(total_ecosystem)
        assert "pool" not in sig.parameters
