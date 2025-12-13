"""
Configuration for EVALIDator validation tests.

Validation tests compare pyFIA estimates against official EVALIDator
results to ensure statistical accuracy. These tests require:
1. Network access to query the EVALIDator API
2. A FIA database (set PYFIA_DATABASE_PATH or place in data/)

Run validation tests:
    uv run pytest tests/validation/ -v -s

Run for specific state:
    uv run pytest tests/validation/ -v -s -k "georgia"
    uv run pytest tests/validation/ -v -s -k "oregon"

Skip validation tests in CI:
    uv run pytest -m "not validation"
"""

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest


# =============================================================================
# State Configuration
# =============================================================================


@dataclass
class StateConfig:
    """Configuration for a state's validation testing."""

    name: str
    abbrev: str
    state_code: int
    evalid: int  # For inventory estimates (EXPCURR, EXPVOL)
    evalid_grm: int  # For GRM estimates (EXPGROW, EXPMORT, EXPREMV)
    year: int
    db_path: str


# Available states for validation testing
# Add new states here after downloading with: pyfia.download(states="XX", dir="data/")
STATES = {
    "georgia": StateConfig(
        name="Georgia",
        abbrev="GA",
        state_code=13,
        evalid=132301,
        evalid_grm=132303,
        year=2023,
        db_path="data/georgia.duckdb",
    ),
    "oregon": StateConfig(
        name="Oregon",
        abbrev="OR",
        state_code=41,
        evalid=412201,
        evalid_grm=412203,
        year=2022,
        db_path="data/or/or.duckdb",
    ),
}

# Default state for single-state tests
DEFAULT_STATE = "georgia"

# Legacy constants for backward compatibility
GEORGIA_STATE_CODE = STATES["georgia"].state_code
GEORGIA_EVALID = STATES["georgia"].evalid
GEORGIA_EVALID_GRM = STATES["georgia"].evalid_grm
GEORGIA_YEAR = STATES["georgia"].year

# Tolerance thresholds
FLOAT_TOLERANCE = 1e-6  # Relative tolerance for floating point comparison
DATA_SYNC_TOLERANCE = 0.01  # 1% tolerance for database version differences
SE_TOLERANCE = 0.03  # 3% tolerance for SE values
EXACT_MATCH_TOLERANCE_PCT = 0.001  # For compare_estimates reporting


# =============================================================================
# Pytest Configuration
# =============================================================================


def pytest_collection_modifyitems(items):
    """Mark all tests in this directory as validation tests."""
    for item in items:
        item.add_marker(pytest.mark.validation)
        item.add_marker(pytest.mark.network)
        item.add_marker(pytest.mark.slow)


# =============================================================================
# Fixtures
# =============================================================================


def _find_database(state_key: str | None = None) -> Path | None:
    """Find FIA database for validation tests."""
    # If state specified, look for that state's database
    if state_key and state_key in STATES:
        state_path = Path.cwd() / STATES[state_key].db_path
        if state_path.exists():
            return state_path

    # Try environment variable
    env_path = os.getenv("PYFIA_DATABASE_PATH")
    if env_path and Path(env_path).exists():
        return Path(env_path)

    # Try default locations
    paths_to_try = [
        Path.cwd() / "data" / "georgia.duckdb",
        Path.cwd() / "fia.duckdb",
        Path.home() / "fia.duckdb",
    ]

    for path in paths_to_try:
        if path.exists():
            return path

    return None


@pytest.fixture(scope="module")
def fia_db():
    """Get default FIA database path (Georgia) for validation tests.

    Skips tests gracefully if no database is available, making CI-friendly.
    Set PYFIA_DATABASE_PATH environment variable or place database in data/.
    """
    db_path = _find_database(DEFAULT_STATE)

    if db_path is None:
        pytest.skip(
            "No FIA database found for validation tests. "
            "Set PYFIA_DATABASE_PATH or place database in data/georgia.duckdb"
        )

    return str(db_path)


@pytest.fixture(scope="module")
def evalidator_client():
    """Create EVALIDator client with extended timeout for validation tests."""
    from pyfia.evalidator import EVALIDatorClient

    return EVALIDatorClient(timeout=120)


@pytest.fixture(scope="module", params=list(STATES.keys()))
def state_config(request):
    """Parameterized fixture providing state configuration.

    This fixture runs tests for each configured state.
    Use with tests that should validate across multiple states.
    """
    state_key = request.param
    config = STATES[state_key]

    # Check if database exists for this state
    db_path = Path.cwd() / config.db_path
    if not db_path.exists():
        pytest.skip(f"Database not found for {config.name}: {config.db_path}")

    return config


# =============================================================================
# Helper Functions
# =============================================================================


def values_match(pyfia_val: float, ev_val: float, rel_tol: float = FLOAT_TOLERANCE) -> bool:
    """Check if two values match within floating point tolerance."""
    if ev_val == 0:
        return pyfia_val == 0
    return abs(pyfia_val - ev_val) / abs(ev_val) < rel_tol


def se_values_match(pyfia_se: float, ev_se: float, rel_tol: float = SE_TOLERANCE) -> bool:
    """Check if SE values match within tolerance."""
    if ev_se == 0:
        return pyfia_se == 0
    return abs(pyfia_se - ev_se) / abs(ev_se) < rel_tol


def extract_grm_estimate(result: pl.DataFrame, estimator_name: str) -> tuple[float, float]:
    """Extract estimate and SE from GRM estimator results.

    Parameters
    ----------
    result : pl.DataFrame
        Result from growth(), mortality(), or removals()
    estimator_name : str
        Name of the estimator for error messages

    Returns
    -------
    tuple[float, float]
        (estimate, standard_error)
    """
    estimate_col = None
    for col in result.columns:
        if "TOTAL" in col.upper() and "SE" not in col.upper():
            estimate_col = col
            break

    se_col = None
    for col in result.columns:
        if "TOTAL_SE" in col.upper():
            se_col = col
            break

    if estimate_col is None:
        raise ValueError(
            f"{estimator_name} result missing estimate column. "
            f"Available columns: {result.columns}"
        )

    estimate = result[estimate_col][0]
    se = result[se_col][0] if se_col else 0.0

    return float(estimate), float(se)
