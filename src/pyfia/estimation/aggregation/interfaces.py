"""
Interfaces and contracts for the unified aggregation system.

This module defines the core interfaces that all aggregation components
must implement, ensuring consistency and extensibility across the estimation system.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
from pydantic import BaseModel


class EstimationType(Enum):
    """Types of FIA estimations supported by the aggregation system."""
    AREA = "area"
    VOLUME = "volume"
    BIOMASS = "biomass"
    TPA = "tpa"
    MORTALITY = "mortality"
    GROWTH = "growth"


class AggregationStrategy(Enum):
    """Aggregation strategies for different estimation types."""
    RATIO_OF_MEANS = "ratio_of_means"
    MEAN_OF_RATIOS = "mean_of_ratios"
    SIMPLE_EXPANSION = "simple_expansion"
    POST_STRATIFIED = "post_stratified"


class UnifiedAggregationConfig(BaseModel):
    """Unified configuration for all aggregation workflows."""
    estimation_type: EstimationType
    strategy: AggregationStrategy = AggregationStrategy.RATIO_OF_MEANS
    group_cols: Optional[List[str]] = None
    include_totals: bool = False
    include_variance: bool = True
    by_land_type: bool = False
    batch_size: Optional[int] = None

    # Estimation-specific response columns
    response_columns: Optional[Dict[str, str]] = None

    # Variance calculation options
    use_rfia_variance: bool = True
    precision: int = 10


class IAggregationBuilder(ABC):
    """Interface for building aggregation expressions."""

    @abstractmethod
    def with_totals(self, numerator_col: str, denominator_col: str) -> 'IAggregationBuilder':
        """Add total aggregations for numerator and denominator."""
        pass

    @abstractmethod
    def with_variance_statistics(self, **columns) -> 'IAggregationBuilder':
        """Add variance calculation statistics."""
        pass

    @abstractmethod
    def with_response_variables(self, response_cols: Dict[str, str]) -> 'IAggregationBuilder':
        """Add response variable aggregations."""
        pass

    @abstractmethod
    def group_by(self, *columns: str) -> 'IAggregationBuilder':
        """Set grouping columns."""
        pass

    @abstractmethod
    def build_stratum_aggregation(self) -> Tuple[List[str], List[pl.Expr]]:
        """Build stratum-level aggregation specification."""
        pass

    @abstractmethod
    def build_population_aggregation(self) -> Tuple[Optional[List[str]], List[pl.Expr]]:
        """Build population-level aggregation specification."""
        pass

    @abstractmethod
    def reset(self) -> 'IAggregationBuilder':
        """Reset builder to initial state."""
        pass


class IEstimationWorkflow(ABC):
    """Interface for estimation workflows."""

    @abstractmethod
    def calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """Calculate population-level estimates."""
        pass

    @abstractmethod
    def validate_input_data(self, data: pl.DataFrame) -> Dict[str, Any]:
        """Validate input data for estimation."""
        pass


class IPercentageCalculator(ABC):
    """Interface for percentage calculations."""

    @abstractmethod
    def calculate_standard_percentages(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate standard percentages."""
        pass

    @abstractmethod
    def calculate_land_type_percentages(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate land type specific percentages."""
        pass


class IVarianceCalculator(ABC):
    """Interface for variance calculations."""

    @abstractmethod
    def variance_component(self, var_name: str) -> pl.Expr:
        """Calculate variance component for stratified sampling."""
        pass

    @abstractmethod
    def covariance_component(self) -> pl.Expr:
        """Calculate covariance component for ratio variance."""
        pass

    @abstractmethod
    def calculate_ratio_variance(
        self,
        numerator_col: str,
        denominator_col: str,
        numerator_var_col: str,
        denominator_var_col: str,
        covariance_col: str,
    ) -> pl.Expr:
        """Calculate ratio variance using the delta method."""
        pass
