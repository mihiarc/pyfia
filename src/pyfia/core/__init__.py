"""
Core functionality for pyFIA.

This module contains the fundamental classes and functions for working with FIA data:
- Main FIA class for database interaction
- Data reader for loading FIA tables
- Configuration and settings management
"""

from .data_reader import FIADataReader
from .fia import FIA
from .settings import PyFIASettings, get_default_db_path, get_default_engine, settings

__all__ = [
    "FIA",
    "FIADataReader",
    "get_default_db_path",
    "get_default_engine",
    "PyFIASettings",
    "settings",
]
