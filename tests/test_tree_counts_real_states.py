import os
import pytest
import polars as pl

from pyfia import FIA
from pyfia.estimation.tree import tree_count


pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="module")
def db_path_env() -> str:
    # Support either PYFIA_DUCKDB_PATH or PYFIA_DATABASE_PATH
    path = os.environ.get("PYFIA_DUCKDB_PATH") or os.environ.get("PYFIA_DATABASE_PATH")
    if not path:
        pytest.skip("No database path set (PYFIA_DUCKDB_PATH or PYFIA_DATABASE_PATH)")
    # Expand and check absolute/relative
    candidate = os.path.abspath(os.path.expanduser(path))
    if not os.path.exists(candidate):
        # Try relative to repo root (cwd under pytest)
        candidate2 = os.path.abspath(os.path.join(os.getcwd(), path))
        if os.path.exists(candidate2):
            candidate = candidate2
        else:
            pytest.skip(f"Database file not found at '{candidate}' or '{candidate2}'")
    return candidate


def _approx_equal(actual: float, expected: float, rel_tol: float = 0.03) -> bool:
    if expected == 0:
        return abs(actual) < 1e-6
    return abs(actual - expected) / expected <= rel_tol


def test_georgia_total_and_loblolly(db_path_env):
    db = FIA(db_path_env)
    # Georgia (STATECD=13), live trees on timberland
    total_df = tree_count(db, area_domain="STATECD == 13", land_type="timber", tree_type="live", totals=True)
    assert len(total_df) > 0
    total = float(total_df.select(pl.col("TREE_COUNT")).item())
    # Published target: 13.8 billion
    assert _approx_equal(total, 13_800_000_000.0, rel_tol=0.05)

    # Loblolly/shortleaf forest type group approx via FORTYPCD grouping
    loblolly_df = tree_count(
        db,
        area_domain="STATECD == 13 AND FORTYPCD BETWEEN 160 AND 169",
        land_type="timber",
        tree_type="live",
        totals=True,
    )
    loblolly = float(loblolly_df.select(pl.col("TREE_COUNT")).item())
    # Published target: 4.6 billion
    assert _approx_equal(loblolly, 4_600_000_000.0, rel_tol=0.08)


def test_south_carolina_total_and_loblolly(db_path_env):
    db = FIA(db_path_env)
    # South Carolina (STATECD=45), live trees on timberland
    total_df = tree_count(db, area_domain="STATECD == 45", land_type="timber", tree_type="live", totals=True)
    assert len(total_df) > 0
    total = float(total_df.select(pl.col("TREE_COUNT")).item())
    # Published target: 8.8 billion
    assert _approx_equal(total, 8_800_000_000.0, rel_tol=0.05)

    loblolly_df = tree_count(
        db,
        area_domain="STATECD == 45 AND FORTYPCD BETWEEN 160 AND 169",
        land_type="timber",
        tree_type="live",
        totals=True,
    )
    loblolly = float(loblolly_df.select(pl.col("TREE_COUNT")).item())
    # Published target: 3.8 billion
    assert _approx_equal(loblolly, 3_800_000_000.0, rel_tol=0.08)


