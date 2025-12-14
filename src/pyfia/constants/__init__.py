"""
Constants and enumerations for pyFIA.

This module provides:
- FIA-specific codes and lookup tables
- Mathematical constants
- Default values and configuration

All constants are re-exported here for backward compatibility.
"""

# Plot design and diameter constants
from .plot_design import (
    DESCRIPTIVE_SIZE_CLASSES,
    STANDARD_SIZE_CLASSES,
    DiameterBreakpoints,
    PlotBasis,
    PlotDesign,
)

# Status and classification codes
from .status_codes import (
    DamageAgent,
    EstimatorType,
    EvaluationType,
    LandStatus,
    OwnershipGroup,
    ReserveStatus,
    SiteClass,
    TreeClass,
    TreeComponent,
    TreeStatus,
)

# State FIPS codes
from .states import StateCodes

# FIA table names
from .tables import TableNames

# Defaults, validation, and math constants
from .defaults import (
    Defaults,
    ErrorMessages,
    MathConstants,
    ValidationRanges,
)

__all__ = [
    # Plot design
    "PlotDesign",
    "DiameterBreakpoints",
    "PlotBasis",
    "STANDARD_SIZE_CLASSES",
    "DESCRIPTIVE_SIZE_CLASSES",
    # Status codes
    "TreeStatus",
    "TreeClass",
    "LandStatus",
    "SiteClass",
    "ReserveStatus",
    "OwnershipGroup",
    "DamageAgent",
    "TreeComponent",
    "EvaluationType",
    "EstimatorType",
    # States
    "StateCodes",
    # Tables
    "TableNames",
    # Defaults and validation
    "MathConstants",
    "Defaults",
    "ValidationRanges",
    "ErrorMessages",
]
