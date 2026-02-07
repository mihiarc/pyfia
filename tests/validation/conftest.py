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
    db_path: str  # Local file path
    motherduck_db: str  # MotherDuck database name


# Available states for validation testing
# Add new states here after downloading with: pyfia.download(states="XX", dir="data/")
# MotherDuck databases follow pattern: fia_{state}_eval{year}
STATES = {
    "georgia": StateConfig(
        name="Georgia",
        abbrev="GA",
        state_code=13,
        evalid=132301,
        evalid_grm=132303,
        year=2023,
        db_path="data/georgia.duckdb",
        motherduck_db="fia_ga_eval2023",
    ),
    "oregon": StateConfig(
        name="Oregon",
        abbrev="OR",
        state_code=41,
        evalid=412201,
        evalid_grm=412203,
        year=2022,
        db_path="data/or/or.duckdb",
        motherduck_db="fia_or_eval2022",
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
EXACT_MATCH_TOLERANCE_PCT = 0.001  # For compare_estimates reporting

# SE (Standard Error) tolerance thresholds
#
# pyFIA implements the stratified domain total variance formula from
# Bechtold & Patterson (2005): V(Ŷ) = Σ_h w_h² × s²_yh × n_h
#
# This matches EVALIDator's methodology and typically produces SE estimates
# within 1-3% of official USFS values. The 5% tolerance accommodates:
#
# 1. Tree-based estimates (volume, biomass, TPA, GRM): ~0.1-2.5% difference
#    due to minor floating-point precision differences and rounding
#
# 2. Area estimates: ~2-3% difference due to post-stratification variance
#    components (V1/V2) that EVALIDator includes but pyFIA approximates
#
# Validation results for Georgia (EVALID 132301, 132303):
# - Volume SE: 0.67% difference (pyFIA 545.6M vs EVALIDator 549.3M)
# - Biomass SE: 0.37% difference (pyFIA 14.2M vs EVALIDator 14.3M)
# - TPA SE: 2.4% difference (pyFIA 204.2M vs EVALIDator 199.4M)
# - Growth SE: 0.11% difference (pyFIA 35.4M vs EVALIDator 35.4M)
# - Area SE: 2.1% difference (pyFIA 138.9K vs EVALIDator 136.0K)
#
# Reference: https://doi.org/10.2737/SRS-GTR-80
# See also: docs/fia_technical_context.md
SE_TOLERANCE = 0.05  # 5% tolerance for area estimates
SE_TOLERANCE_TREE = 0.05  # 5% tolerance for tree-based estimates (volume, biomass, tpa)
SE_TOLERANCE_GRM = 0.05  # 5% tolerance for GRM estimates (growth, mortality, removals)


# =============================================================================
# Pytest Configuration
# =============================================================================


def pytest_collection_modifyitems(items):
    """Mark tests in this directory as validation tests."""
    validation_dir = Path(__file__).parent
    for item in items:
        if validation_dir in item.path.parents:
            item.add_marker(pytest.mark.validation)
            item.add_marker(pytest.mark.network)
            item.add_marker(pytest.mark.slow)


# =============================================================================
# Fixtures
# =============================================================================


def _find_database(state_key: str | None = None) -> str | None:
    """Find FIA database for validation tests.

    Checks in order:
    1. Local file for specified state
    2. PYFIA_DATABASE_PATH environment variable
    3. Default local file locations
    4. MotherDuck (if MOTHERDUCK_TOKEN is set)
    """
    # If state specified, look for that state's database
    if state_key and state_key in STATES:
        state_config = STATES[state_key]

        # Try local file first
        state_path = Path.cwd() / state_config.db_path
        if state_path.exists():
            return str(state_path)

        # Try MotherDuck if token is available
        if os.getenv("MOTHERDUCK_TOKEN"):
            return f"md:{state_config.motherduck_db}"

    # Try environment variable
    env_path = os.getenv("PYFIA_DATABASE_PATH")
    if env_path:
        # Could be a local path or MotherDuck connection string
        if env_path.startswith("md:") or env_path.startswith("motherduck:"):
            return env_path
        if Path(env_path).exists():
            return env_path

    # Try default local locations
    paths_to_try = [
        Path.cwd() / "data" / "georgia.duckdb",
        Path.cwd() / "fia.duckdb",
        Path.home() / "fia.duckdb",
    ]

    for path in paths_to_try:
        if path.exists():
            return str(path)

    # Fallback to MotherDuck for default state if token available
    if os.getenv("MOTHERDUCK_TOKEN"):
        return f"md:{STATES[DEFAULT_STATE].motherduck_db}"

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
    """Create EVALIDator client with extended timeout and retry for validation tests.

    The EVALIDator API is occasionally unstable, returning empty responses,
    connection resets, or timeouts. This fixture configures aggressive retry
    settings to handle transient failures gracefully.
    """
    from pyfia.evalidator.client import EVALIDatorClient

    return EVALIDatorClient(timeout=120, max_retries=5, retry_delay=3.0)


@pytest.fixture(scope="module", params=list(STATES.keys()))
def state_config(request):
    """Parameterized fixture providing state configuration.

    This fixture runs tests for each configured state.
    Use with tests that should validate across multiple states.
    """
    state_key = request.param
    config = STATES[state_key]

    # Check if database exists for this state (local file or MotherDuck)
    db_path = Path.cwd() / config.db_path
    has_local = db_path.exists()
    has_motherduck = bool(os.getenv("MOTHERDUCK_TOKEN"))

    if not has_local and not has_motherduck:
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


def plot_counts_match(pyfia_count: int, ev_count: int) -> bool:
    """Check if plot counts match exactly.

    Plot counts should always match exactly if the same EVALID and filters
    are being used. Differences indicate data sync or filtering issues.
    """
    return pyfia_count == ev_count


def extract_grm_estimate(
    result: pl.DataFrame, estimator_name: str
) -> tuple[float, float, int | None]:
    """Extract estimate, SE, and plot count from GRM estimator results.

    Parameters
    ----------
    result : pl.DataFrame
        Result from growth(), mortality(), or removals()
    estimator_name : str
        Name of the estimator for error messages

    Returns
    -------
    tuple[float, float, int | None]
        (estimate, standard_error, plot_count)
        plot_count will be None if not available in results
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

    # Extract plot count if available
    plot_count = None
    if "N_PLOTS" in result.columns:
        plot_count = int(result["N_PLOTS"][0])

    return float(estimate), float(se), plot_count
