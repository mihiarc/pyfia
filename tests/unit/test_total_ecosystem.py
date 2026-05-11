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

    def test_has_grp_by_parameter(self):
        """total_ecosystem accepts grp_by, matching sibling estimators."""
        import inspect
        sig = inspect.signature(total_ecosystem)
        assert "grp_by" in sig.parameters
        assert sig.parameters["grp_by"].default is None


def _make_grouped_pool_result(
    rows: list[tuple],
    group_col: str = "FORTYPCD",
    year: int = 2023,
) -> pl.DataFrame:
    """Build a multi-row pool result keyed by a grouping column.

    Each ``rows`` entry is ``(group_value, carbon_acre, carbon_total)``.
    """
    return pl.DataFrame(
        {
            "YEAR": [year] * len(rows),
            group_col: [r[0] for r in rows],
            "POOL": ["TOTAL"] * len(rows),
            "CARBON_ACRE": [r[1] for r in rows],
            "CARBON_TOTAL": [r[2] for r in rows],
            "N_PLOTS": [100] * len(rows),
            "N_TREES": [0] * len(rows),
        }
    )


class TestTotalEcosystemGrpBy:
    """Tests for grp_by forwarding and per-group summing."""

    def test_grp_by_forwarded_to_each_pool(self, mock_infra):
        """grp_by must be passed through to every pool estimator."""
        result_df = _make_grouped_pool_result([("A", 1.0, 100.0)])
        mocks: dict[str, object] = {}
        patchers = []
        try:
            for name, target in _POOL_PATCHES.items():
                p = patch(target, return_value=result_df)
                mocks[name] = p.start()
                patchers.append(p)
            total_ecosystem(MockDB(), grp_by="FORTYPCD")
            for name, mock in mocks.items():
                assert mock.call_args is not None, f"pool {name} was not called"
                kwargs = mock.call_args.kwargs
                assert kwargs.get("grp_by") == "FORTYPCD", (
                    f"pool {name} did not receive grp_by; got {kwargs}"
                )
        finally:
            for p in patchers:
                p.stop()

    def test_grp_by_sums_per_group(self, mock_infra):
        """TOTAL_ECOSYSTEM row is summed within each group, not collapsed."""
        # Two forest type codes, each pool reports both.
        results = {
            name: _make_grouped_pool_result(
                [("A", 1.0, 100.0), ("B", 2.0, 200.0)]
            )
            for name in _POOL_PATCHES
        }
        patchers = _start_pool_patches(results)
        try:
            result = total_ecosystem(MockDB(), grp_by="FORTYPCD")
            totals = result.filter(pl.col("POOL") == "TOTAL_ECOSYSTEM").sort(
                "FORTYPCD"
            )
            # 2 groups × 1 TOTAL row each
            assert len(totals) == 2
            assert totals["FORTYPCD"].to_list() == ["A", "B"]
            # 6 pools × 1.0 = 6.0 for group A, 6 × 2.0 = 12.0 for group B
            assert abs(float(totals["CARBON_ACRE"][0]) - 6.0) < 1e-10
            assert abs(float(totals["CARBON_ACRE"][1]) - 12.0) < 1e-10
            # 6 × 100 = 600, 6 × 200 = 1200
            assert abs(float(totals["CARBON_TOTAL"][0]) - 600.0) < 1e-10
            assert abs(float(totals["CARBON_TOTAL"][1]) - 1200.0) < 1e-10
        finally:
            _stop_pool_patches(patchers)

    def test_grp_by_preserves_pool_rows(self, mock_infra):
        """Pool rows survive grp_by — output has pool rows + TOTAL rows per group."""
        results = {
            name: _make_grouped_pool_result(
                [("A", 1.0, 100.0), ("B", 2.0, 200.0)]
            )
            for name in _POOL_PATCHES
        }
        patchers = _start_pool_patches(results)
        try:
            result = total_ecosystem(MockDB(), grp_by="FORTYPCD")
            # 6 pools × 2 groups + 2 TOTAL rows = 14 rows
            assert len(result) == 14
            assert "FORTYPCD" in result.columns
            # Each pool appears for each group
            pool_a = result.filter(pl.col("FORTYPCD") == "A")
            assert len(pool_a) == 7  # 6 pools + 1 TOTAL
        finally:
            _stop_pool_patches(patchers)
