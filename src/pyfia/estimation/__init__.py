"""
FIA estimation module.

This module provides statistical estimation functions for FIA data
following Bechtold & Patterson (2005) methodology.

Public API Functions:
    area(): Estimate forest area
    area_change(): Estimate forest area change between inventories
    biomass(): Estimate tree biomass
    carbon(): Estimate tree carbon (alias for biomass with carbon output)
    carbon_flux(): Estimate carbon flux between inventories
    carbon_pool(): Estimate carbon pools
    growth(): Estimate tree growth
    mortality(): Estimate tree mortality
    removals(): Estimate tree removals
    site_index(): Estimate area-weighted mean site index
    tpa(): Estimate trees per acre and basal area
    volume(): Estimate tree volume

All functions follow a consistent pattern:
1. Accept a FIADatabase and optional filtering/grouping parameters
2. Return a polars DataFrame with estimates and uncertainty measures
3. Include standard error and confidence intervals

For internal implementation details (estimator classes, column constants,
variance calculations), import directly from submodules:
    - pyfia.estimation.estimators.*
    - pyfia.estimation.base
    - pyfia.estimation.columns
    - pyfia.estimation.variance
"""

from __future__ import annotations

# Import estimator functions - THE PUBLIC API
from .estimators import (
    area,
    area_change,
    biomass,
    carbon,
    carbon_flux,
    carbon_pool,
    growth,
    mortality,
    removals,
    site_index,
    tpa,
    volume,
)

__version__ = "2.0.0"

# Only expose user-facing estimator functions
__all__ = [
    "area",
    "area_change",
    "biomass",
    "carbon",
    "carbon_flux",
    "carbon_pool",
    "growth",
    "mortality",
    "removals",
    "site_index",
    "tpa",
    "volume",
]
