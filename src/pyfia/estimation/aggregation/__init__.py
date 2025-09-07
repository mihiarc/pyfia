"""
Unified aggregation system for pyFIA estimation.

This module provides a modern, unified approach to handling complex aggregation
workflows for all FIA estimation types including area, volume, biomass, TPA,
mortality, and growth estimation.

The system uses a configurable, strategy-based approach with proper interfaces
for extensibility and maintainability.
"""

from .interfaces import (
    AggregationStrategy,
    EstimationType,
    IAggregationBuilder,
    IEstimationWorkflow,
    IPercentageCalculator,
    IVarianceCalculator,
    UnifiedAggregationConfig,
)
from .unified_builder import UnifiedAggregationBuilder
from .unified_workflow import UnifiedEstimationWorkflow

__all__ = [
    "EstimationType",
    "AggregationStrategy",
    "UnifiedAggregationConfig",
    "IAggregationBuilder",
    "IEstimationWorkflow",
    "IPercentageCalculator",
    "IVarianceCalculator",
    "UnifiedAggregationBuilder",
    "UnifiedEstimationWorkflow",
]
