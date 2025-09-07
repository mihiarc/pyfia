"""
Core estimation framework components.

This module contains the base classes and configuration for all estimators.
"""

from .base import BaseEstimator
from .config import EstimatorConfig
from .builder import CompositeQueryBuilder

__all__ = [
    "BaseEstimator",
    "EstimatorConfig",
    "CompositeQueryBuilder",
]