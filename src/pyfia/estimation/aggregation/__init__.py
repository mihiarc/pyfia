"""
Aggregation workflow components for pyFIA estimation.

This module provides components for handling complex aggregation workflows
including population-level estimation, stratum-level statistics, and
batch processing for memory efficiency.
"""

from .workflow import PopulationEstimationWorkflow, AreaAggregationBuilder, AggregationConfig

__all__ = [
    "PopulationEstimationWorkflow",
    "AreaAggregationBuilder",
    "AggregationConfig",
]