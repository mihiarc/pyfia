"""
Simplified FIA estimation module.

This module provides straightforward statistical estimation functions
for FIA data without unnecessary abstraction layers.

Main API Functions:
- area(): Estimate forest area
- volume(): Estimate tree volume  
- biomass(): Estimate tree biomass and carbon
- tpa(): Estimate trees per acre and basal area
- mortality(): Estimate tree mortality
- growth(): Estimate growth, removals, and net change

All functions follow a consistent pattern:
1. Simple parameter interface
2. Clear calculation logic
3. Standard output format
"""

# Import base components
from .base import BaseEstimator
from .config import EstimatorConfig, VolumeConfig, BiomassConfig, MortalityConfig, create_config

# Import utilities
from .aggregation import (
    aggregate_to_population,
    aggregate_by_domain,
    aggregate_plot_level,
    merge_stratification,
    apply_adjustment_factors
)

from .statistics import (
    VarianceCalculator,
    calculate_ratio_of_means_variance,
    calculate_post_stratified_variance,
    calculate_confidence_interval,
    calculate_cv
)

from .utils import (
    join_tables,
    format_output_columns,
    check_required_columns,
    filter_most_recent_evalid
)

# Import estimator functions - THE MAIN PUBLIC API
from .estimators import (
    area,
    biomass,
    growth,
    mortality,
    tpa,
    volume
)

# Import estimator classes for advanced usage
from .estimators import (
    AreaEstimator,
    BiomassEstimator,
    GrowthEstimator,
    MortalityEstimator,
    TPAEstimator,
    VolumeEstimator
)

__version__ = "2.0.0"  # Major version bump for simplified architecture

__all__ = [
    # Main API functions
    "area",
    "biomass",
    "growth",
    "mortality",
    "tpa",
    "volume",
    
    # Estimator classes
    "AreaEstimator",
    "BiomassEstimator",
    "GrowthEstimator",
    "MortalityEstimator",
    "TPAEstimator",
    "VolumeEstimator",
    
    # Base components
    "BaseEstimator",
    "EstimatorConfig",
    "VolumeConfig",
    "BiomassConfig",
    "MortalityConfig",
    "create_config",
    
    # Utilities (for advanced users)
    "aggregate_to_population",
    "aggregate_by_domain",
    "aggregate_plot_level",
    "merge_stratification",
    "apply_adjustment_factors",
    "VarianceCalculator",
    "calculate_ratio_of_means_variance",
    "calculate_post_stratified_variance",
    "calculate_confidence_interval",
    "calculate_cv",
    "join_tables",
    "format_output_columns",
    "check_required_columns",
    "filter_most_recent_evalid",
]