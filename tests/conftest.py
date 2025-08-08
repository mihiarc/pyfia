"""
Pytest configuration and shared fixtures for pyFIA tests.

This module provides reusable test fixtures for consistent testing
across all pyFIA modules.
"""

import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from pyfia import FIA
from pyfia.models import EvaluationInfo

# Import centralized fixtures to make them available globally
from fixtures import *


@pytest.fixture(scope="session")
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        yield Path(tmp.name)
    # Cleanup happens automatically


@pytest.fixture(scope="session")
def use_real_data() -> bool:
    """Return True if PYFIA_DATABASE_PATH is set and exists."""
    db_path = os.getenv("PYFIA_DATABASE_PATH")
    return bool(db_path and Path(db_path).exists())


@pytest.fixture(scope="session")
def real_fia_instance(use_real_data):
    """Create a FIA instance pointing to the real DuckDB if available."""
    if not use_real_data:
        return None
    return FIA(os.getenv("PYFIA_DATABASE_PATH"))


@pytest.fixture(scope="session")
def sample_fia_db():
    """Use the development DuckDB for tests instead of constructing a SQLite DB."""
    # Always prefer the local development DuckDB `fia.duckdb` in repo root
    db_path = Path.cwd() / "fia.duckdb"
    if not db_path.exists():
        pytest.skip("Development database 'fia.duckdb' not found; skip DB-backed tests")
    yield db_path

    # Note: By using `fia.duckdb`, all population tables including POP_EVAL_TYP
    # are available and align with production. This avoids divergence between
    # synthetic schemas and real converter output.


@pytest.fixture
def sample_fia_instance(sample_fia_db, use_real_data):
    """Create a FIA instance backing tests; prefer real DB if configured.

    When using the real database, clip to a manageable scope to keep tests fast.
    """
    if use_real_data:
        db = FIA(os.getenv("PYFIA_DATABASE_PATH"))
        try:
            # Default to Georgia (13), most recent evaluation for bounded size
            db.clip_by_state(13, most_recent=True)
        except Exception:
            pass
        return db
    return FIA(str(sample_fia_db))


@pytest.fixture
def sample_evaluation():
    """Create a sample evaluation info object."""
    return EvaluationInfo(
        evalid=372301,
        statecd=37,
        eval_typ="VOL",
        start_invyr=2018,
        end_invyr=2023,
        nplots=10
    )


# Legacy fixtures removed - use centralized fixtures from fixtures.py instead


# Legacy sample_estimation_data fixture removed - use standard_estimation_dataset or simple_estimation_dataset instead


