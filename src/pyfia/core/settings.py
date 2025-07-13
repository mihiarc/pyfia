"""
Settings management for pyFIA using Pydantic Settings.

This module provides centralized configuration with environment variable support.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PyFIASettings(BaseSettings):
    """
    Central settings for pyFIA with environment variable support.
    
    Environment variables are prefixed with PYFIA_.
    For example: PYFIA_DATABASE_PATH, PYFIA_LOG_LEVEL
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
        default=Path("fia.duckdb"),
        description="Path to FIA database"
    )
    database_engine: str = Field(
        default="duckdb",
        description="Database engine (duckdb or sqlite)"
    )

    # API settings
    openai_api_key: Optional[str] = Field(
        default=None,
        description="OpenAI API key for AI features"
    )
    openai_model: str = Field(
        default="gpt-4o",
        description="OpenAI model to use"
    )

    # Performance settings
    max_threads: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Maximum threads for parallel processing"
    )
    chunk_size: int = Field(
        default=10000,
        ge=1000,
        description="Chunk size for batch processing"
    )

    # Cache settings
    cache_enabled: bool = Field(
        default=True,
        description="Enable caching"
    )
    cache_dir: Path = Field(
        default=Path.home() / ".pyfia" / "cache",
        description="Cache directory"
    )

    # Logging settings
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    log_dir: Path = Field(
        default=Path.home() / ".pyfia" / "logs",
        description="Log directory"
    )

    # CLI settings
    cli_page_size: int = Field(
        default=20,
        ge=5,
        le=100,
        description="Number of rows to display in CLI"
    )
    cli_max_width: int = Field(
        default=120,
        ge=80,
        description="Maximum width for CLI output"
    )

    # Type checking settings
    type_check_on_load: bool = Field(
        default=False,
        description="Run type checks when loading data"
    )

    @field_validator("database_engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        """Validate database engine choice."""
        valid_engines = ["duckdb", "sqlite"]
        if v.lower() not in valid_engines:
            raise ValueError(f"Engine must be one of {valid_engines}")
        return v.lower()

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()

    @field_validator("database_path")
    @classmethod
    def validate_database_path(cls, v: Path) -> Path:
        """Validate database path exists."""
        if v.exists() and not v.is_file():
            raise ValueError(f"Database path {v} exists but is not a file")
        return v

    def create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def get_connection_string(self) -> str:
        """Get database connection string."""
        if self.database_engine == "duckdb":
            return f"duckdb:///{self.database_path}"
        else:
            return f"sqlite:///{self.database_path}"


# Global settings instance
settings = PyFIASettings()

# Create directories on import
settings.create_directories()
