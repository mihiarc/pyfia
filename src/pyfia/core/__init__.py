"""
Core functionality for pyFIA.

This module contains the fundamental classes and functions for working with FIA data:
- Main FIA class for database interaction
- Data reader for loading FIA tables
- Configuration and settings management
- Custom exception classes
"""

from .data_reader import FIADataReader
from .exceptions import (
    ConfigurationError,
    ConnectionError,
    DatabaseError,
    EstimationError,
    FilterError,
    InsufficientDataError,
    InvalidDomainError,
    InvalidEVALIDError,
    MissingColumnError,
    NoEVALIDError,
    NoSpatialFilterError,
    PyFIAError,
    SpatialError,
    SpatialExtensionError,
    SpatialFileError,
    StratificationError,
    TableNotFoundError,
)
from .fia import FIA
from .settings import PyFIASettings, get_default_db_path, get_default_engine, settings

__all__ = [
    # Main classes
    "FIA",
    "FIADataReader",
    "PyFIASettings",
    # Settings helpers
    "get_default_db_path",
    "get_default_engine",
    "settings",
    # Exceptions
    "PyFIAError",
    "DatabaseError",
    "TableNotFoundError",
    "ConnectionError",
    "EstimationError",
    "InsufficientDataError",
    "StratificationError",
    "MissingColumnError",
    "FilterError",
    "InvalidDomainError",
    "InvalidEVALIDError",
    "NoEVALIDError",
    "ConfigurationError",
    # Spatial exceptions
    "SpatialError",
    "SpatialFileError",
    "SpatialExtensionError",
    "NoSpatialFilterError",
]
