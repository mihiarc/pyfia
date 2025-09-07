"""
Data processing utilities for estimation.

This module contains join optimization, aggregation workflows, and statistical calculations.
"""

from .join import JoinManager, get_join_manager

__all__ = [
    "JoinManager",
    "get_join_manager",
]