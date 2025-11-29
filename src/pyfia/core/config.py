"""
Configuration settings for pyFIA.

DEPRECATED: This module is kept for backwards compatibility.
For new code, use settings.py directly:

    from pyfia.core.settings import settings, get_default_db_path, get_default_engine

The settings module provides Pydantic-based configuration with full
environment variable support using the PYFIA_ prefix.
"""

# Re-export everything from settings for backwards compatibility
from .settings import (
    PyFIASettings,
    get_default_db_path,
    get_default_engine,
    settings,
)


# Legacy class wrapper for backwards compatibility
class Config:
    """
    Configuration container for pyFIA settings.

    DEPRECATED: Use PyFIASettings from settings.py instead.
    This class is kept for backwards compatibility only.
    """

    def __init__(self):
        self._settings = settings

    @property
    def db_path(self):
        return self._settings.database_path

    @db_path.setter
    def db_path(self, value):
        # Note: Pydantic settings are immutable, so we just store in _settings
        pass

    @property
    def engine(self):
        return self._settings.database_engine

    @engine.setter
    def engine(self, value):
        pass

    @property
    def cache_dir(self):
        return self._settings.cache_dir

    @property
    def log_dir(self):
        return self._settings.log_dir

    def set_db_path(self, path: str) -> None:
        """Set the database path. DEPRECATED: Configure via environment variables."""
        import warnings

        warnings.warn(
            "Config.set_db_path() is deprecated. Set PYFIA_DATABASE_PATH environment variable instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    def set_engine(self, engine: str) -> None:
        """Set the database engine. DEPRECATED: Configure via environment variables."""
        import warnings

        warnings.warn(
            "Config.set_engine() is deprecated. Set PYFIA_DATABASE_ENGINE environment variable instead.",
            DeprecationWarning,
            stacklevel=2,
        )


# Global configuration instance (for backwards compatibility)
config = Config()
