"""
Infrastructure components for estimation.

This module contains supporting infrastructure like caching, evaluation, progress tracking, and formatting.
"""

from .caching import cached_operation, LazyFrameCache
from .evaluation import LazyEstimatorMixin, FrameWrapper, operation, CollectionStrategy
from .progress import OperationType, EstimatorProgressMixin
from .formatters import OutputFormatter

__all__ = [
    # Caching
    "cached_operation",
    "LazyFrameCache",
    
    # Evaluation
    "LazyEstimatorMixin",
    "FrameWrapper",
    "operation",
    "CollectionStrategy",
    
    # Progress
    "OperationType",
    "EstimatorProgressMixin",
    
    # Formatting
    "OutputFormatter",
]