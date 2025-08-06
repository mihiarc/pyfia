"""
Configuration settings for pyFIA.

This module provides default settings and paths for pyFIA operations.
Note: For new code, prefer using settings.py with Pydantic Settings.
"""

import os
from pathlib import Path

# Import new settings for migration path

# Default database paths (kept for backwards compatibility)
DEFAULT_DUCKDB_PATH = "fia.duckdb"
DEFAULT_ENGINE = "duckdb"

# Environment variable override (kept for backwards compatibility)
FIA_DB_PATH = os.environ.get("FIA_DB_PATH", DEFAULT_DUCKDB_PATH)
FIA_DB_ENGINE = os.environ.get("FIA_DB_ENGINE", DEFAULT_ENGINE)


def get_default_db_path() -> Path:
    """
    Get the default database path.

    Checks in order:
    1. FIA_DB_PATH environment variable
    2. Default DuckDB path

    Returns:
        Path to the default database
    """
    db_path = Path(FIA_DB_PATH)
    if db_path.exists():
        return db_path
    return Path(DEFAULT_DUCKDB_PATH)


def get_default_engine() -> str:
    """
    Get the default database engine.

    Returns:
        Default engine type ("sqlite" or "duckdb")
    """
    return FIA_DB_ENGINE.lower()


class Config:
    """Configuration container for pyFIA settings."""

    def __init__(self):
        self.db_path = get_default_db_path()
        self.engine = get_default_engine()
        self.cache_dir = Path.home() / ".pyfia" / "cache"
        self.log_dir = Path.home() / ".pyfia" / "logs"

        # Create directories if they don't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def set_db_path(self, path: str) -> None:
        """Set the database path."""
        self.db_path = Path(path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {path}")

    def set_engine(self, engine: str) -> None:
        """Set the database engine."""
        if engine.lower() not in ["sqlite", "duckdb"]:
            raise ValueError(f"Invalid engine: {engine}. Use 'sqlite' or 'duckdb'")
        self.engine = engine.lower()


# Global configuration instance
config = Config()
