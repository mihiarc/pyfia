"""
Grouping utilities for FIA estimation.

This module re-exports the grouping functions from grouping_functions.py
for backward compatibility. All functionality has been consolidated into
the grouping_functions module.
"""

# Re-export from the consolidated module
from .grouping_functions import setup_grouping_columns

__all__ = ["setup_grouping_columns"]