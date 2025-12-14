"""
Settings management for pyFIA using Pydantic Settings.

This module provides centralized configuration with environment variable support.
It is the canonical source for all pyFIA configuration.
"""

import os
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PyFIASettings(BaseSettings):
    """
    Central settings for pyFIA with environment variable support.

    Environment variables are prefixed with PYFIA_.
    For example: PYFIA_DATABASE_PATH, PYFIA_LOG_LEVEL

    Also supports legacy environment variables FIA_DB_PATH and FIA_DB_ENGINE
    for backwards compatibility.
    """

    model_config = SettingsConfigDict(
        env_prefix="PYFIA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database settings
    database_path: Path = Field(
        default=Path("fia.duckdb"), description="Path to FIA database"
    )
    database_engine: str = Field(
        default="duckdb", description="Database engine (duckdb or sqlite)"
    )

    # Performance settings
    max_threads: int = Field(
        default=4, ge=1, le=32, description="Maximum threads for parallel processing"
    )
    chunk_size: int = Field(
        default=10000, ge=1000, description="Chunk size for batch processing"
    )

    # Cache settings
    cache_enabled: bool = Field(default=True, description="Enable caching")
    cache_dir: Path = Field(
        default=Path.home() / ".pyfia" / "cache", description="Cache directory"
    )

    # Download settings
    download_dir: Path = Field(
        default=Path.home() / ".pyfia" / "data",
        description="Directory for downloaded FIA data",
    )
    download_timeout: int = Field(
        default=300, ge=30, le=3600, description="Download timeout in seconds"
    )
    download_chunk_size: int = Field(
        default=1024 * 1024,
        ge=1024,
        description="Download chunk size in bytes (default 1MB)",
    )
    download_max_retries: int = Field(
        default=3, ge=1, le=10, description="Maximum download retry attempts"
    )
    download_use_cache: bool = Field(
        default=True, description="Use cached downloads when available"
    )

    # Logging settings
    log_level: str = Field(default="CRITICAL", description="Logging level")
    log_dir: Path = Field(
        default=Path.home() / ".pyfia" / "logs", description="Log directory"
    )

    # CLI settings
    cli_page_size: int = Field(
        default=20, ge=5, le=100, description="Number of rows to display in CLI"
    )
    cli_max_width: int = Field(
        default=120, ge=80, description="Maximum width for CLI output"
    )

    # Type checking settings
    type_check_on_load: bool = Field(
        default=False, description="Run type checks when loading data"
    )

    @field_validator("database_engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        """
        Validate database engine choice.

        Parameters
        ----------
        v : str
            Database engine to validate

        Returns
        -------
        str
            Validated and lowercased engine name
        """
        valid_engines = ["duckdb", "sqlite"]
        if v.lower() not in valid_engines:
            raise ValueError(f"Engine must be one of {valid_engines}")
        return v.lower()

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """
        Validate log level.

        Parameters
        ----------
        v : str
            Log level to validate

        Returns
        -------
        str
            Validated and uppercased log level
        """
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()

    @field_validator("database_path")
    @classmethod
    def validate_database_path(cls, v: Path) -> Path:
        """
        Validate database path exists.

        Parameters
        ----------
        v : Path
            Database path to validate

        Returns
        -------
        Path
            Validated database path
        """
        if v.exists() and not v.is_file():
            raise ValueError(f"Database path {v} exists but is not a file")
        return v

    def create_directories(self) -> None:
        """
        Create necessary directories if they don't exist.

        Returns
        -------
        None
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def get_connection_string(self) -> str:
        """
        Get database connection string.

        Returns
        -------
        str
            Database connection string for the configured engine
        """
        if self.database_engine == "duckdb":
            return f"duckdb:///{self.database_path}"
        else:
            return f"sqlite:///{self.database_path}"


# Check for legacy environment variables and use them if new ones aren't set
def _create_settings_with_legacy_support() -> PyFIASettings:
    """
    Create settings with support for legacy environment variable names.

    Returns
    -------
    PyFIASettings
        Configured settings instance with legacy environment variable support
    """
    # Check for legacy FIA_DB_PATH
    legacy_db_path = os.environ.get("FIA_DB_PATH")
    new_db_path = os.environ.get("PYFIA_DATABASE_PATH")

    # Check for legacy FIA_DB_ENGINE
    legacy_engine = os.environ.get("FIA_DB_ENGINE")
    new_engine = os.environ.get("PYFIA_DATABASE_ENGINE")

    # Build kwargs for settings - legacy takes precedence if new isn't set
    kwargs: dict[str, Any] = {}
    if legacy_db_path and not new_db_path:
        kwargs["database_path"] = Path(legacy_db_path)
    if legacy_engine and not new_engine:
        kwargs["database_engine"] = legacy_engine

    return PyFIASettings(**kwargs)


# Global settings instance
settings = _create_settings_with_legacy_support()

# Create directories on import
settings.create_directories()


# Backwards compatibility functions (delegate to settings)
def get_default_db_path() -> Path:
    """
    Get the default database path.

    Checks environment variables and settings for database path.

    Returns
    -------
    Path
        Path to the default database
    """
    return settings.database_path


def get_default_engine() -> str:
    """
    Get the default database engine.

    Returns
    -------
    str
        Default engine type ("sqlite" or "duckdb")
    """
    return settings.database_engine
