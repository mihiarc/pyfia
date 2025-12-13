"""
Pytest configuration and shared fixtures for pyFIA tests.

Test Structure:
- unit/: Fast, isolated tests (no database)
- integration/: Tests with database interactions
- e2e/: End-to-end workflow tests
- validation/: EVALIDator comparison tests
- property/: Property-based tests with Hypothesis

Run specific categories:
    pytest tests/unit/           # Fast unit tests only
    pytest tests/integration/    # Integration tests
    pytest -m "not slow"         # Skip slow tests
    pytest -m "not network"      # Skip network tests
"""

import os
from pathlib import Path

import pytest

from pyfia import FIA

# Register fixtures from modular fixture files
pytest_plugins = [
    "tests.fixtures.data",
    "tests.fixtures.mocks",
    "tests.fixtures.grm",
]


# =============================================================================
# Database Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def georgia_db_path():
    """Path to the Georgia DuckDB database for testing.

    Uses PYFIA_DATABASE_PATH env var if set, otherwise looks for
    data/georgia.duckdb relative to project root.
    """
    # Check environment variable first
    env_path = os.getenv("PYFIA_DATABASE_PATH")
    if env_path and Path(env_path).exists():
        return Path(env_path)

    # Look relative to project root
    project_root = Path(__file__).parent.parent
    candidates = [
        project_root / "data" / "georgia.duckdb",
        project_root / "fia.duckdb",
    ]

    for path in candidates:
        if path.exists():
            return path

    pytest.skip("No FIA database found for testing")


@pytest.fixture(scope="session")
def georgia_db(georgia_db_path):
    """Session-scoped FIA database connection for Georgia data."""
    db = FIA(str(georgia_db_path))
    yield db
    db.close()


@pytest.fixture
def fia_db(georgia_db_path):
    """Function-scoped FIA database for tests that modify state."""
    db = FIA(str(georgia_db_path))
    yield db
    db.close()


@pytest.fixture
def georgia_fia(georgia_db_path):
    """FIA instance clipped to Georgia most recent evaluation."""
    db = FIA(str(georgia_db_path))
    db.clip_by_state(13, most_recent=True)
    yield db
    db.close()


# =============================================================================
# Test Configuration
# =============================================================================

@pytest.fixture
def sample_evaluation():
    """Sample evaluation metadata for testing."""
    return {
        "evalid": 132301,
        "statecd": 13,
        "eval_typ": "EXPALL",
        "start_invyr": 2018,
        "end_invyr": 2023,
    }


# =============================================================================
# Hypothesis Configuration
# =============================================================================

def pytest_configure(config):
    """Configure hypothesis profiles for different test scenarios."""
    from hypothesis import settings, Verbosity, Phase

    # Development profile - fast iteration
    settings.register_profile(
        "dev",
        max_examples=10,
        verbosity=Verbosity.normal,
        phases=[Phase.generate, Phase.target],
    )

    # CI profile - thorough testing
    settings.register_profile(
        "ci",
        max_examples=100,
        verbosity=Verbosity.normal,
    )

    # Nightly profile - exhaustive testing
    settings.register_profile(
        "nightly",
        max_examples=1000,
        verbosity=Verbosity.verbose,
    )

    # Load profile from environment or default to dev
    profile = os.getenv("HYPOTHESIS_PROFILE", "dev")
    settings.load_profile(profile)
