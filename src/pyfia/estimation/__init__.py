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
from .config import (
    BiomassConfig,
    EstimatorConfig,
    MortalityConfig,
    VolumeConfig,
    create_config,
)

# Import estimator functions - THE MAIN PUBLIC API
# Import estimator classes for advanced usage
from .estimators import (
    AreaEstimator,
    AreaChangeEstimator,
    BiomassEstimator,
    CarbonPoolEstimator,
    GrowthEstimator,
    MortalityEstimator,
    RemovalsEstimator,
    TPAEstimator,
    VolumeEstimator,
    area,
    area_change,
    biomass,
    carbon,
    carbon_flux,
    carbon_pool,
    growth,
    mortality,
    removals,
    tpa,
    volume,
)
from .statistics import (
    VarianceCalculator,
    calculate_confidence_interval,
    calculate_cv,
    calculate_post_stratified_variance,
    calculate_ratio_of_means_variance,
)
from .utils import format_output_columns

__version__ = "2.0.0"  # Major version bump for simplified architecture

__all__ = [
    # Main API functions
    "area",
    "area_change",
    "biomass",
    "carbon",
    "carbon_flux",
    "carbon_pool",
    "growth",
    "mortality",
    "removals",
    "tpa",
    "volume",
    # Estimator classes
    "AreaEstimator",
    "AreaChangeEstimator",
    "BiomassEstimator",
    "CarbonPoolEstimator",
    "GrowthEstimator",
    "MortalityEstimator",
    "RemovalsEstimator",
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
    "VarianceCalculator",
    "calculate_ratio_of_means_variance",
    "calculate_post_stratified_variance",
    "calculate_confidence_interval",
    "calculate_cv",
    "format_output_columns",
]
