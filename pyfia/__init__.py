"""
pyFIA - Python implementation of rFIA for Forest Inventory Analysis

A Python package for working with USDA Forest Inventory and Analysis (FIA) data.
"""

__version__ = "0.1.0"
__author__ = "Chris Mihiar"

# Import core classes
from .core import FIA
from .data_reader import FIADataReader
from .estimation_utils import (
    merge_estimation_data,
    calculate_adjustment_factors,
    calculate_stratum_estimates,
    calculate_population_estimates,
    apply_domain_filter,
    calculate_ratio_estimates,
    summarize_by_groups
)

# Import estimation functions
from .tpa import tpa
from .volume import volume
from .mortality import mortality
from .biomass import biomass
from .area import area

# Define what's available when using "from pyfia import *"
__all__ = [
    # Core classes
    'FIA',
    'FIADataReader',
    # Estimation functions
    'tpa',
    'volume',
    'mortality',
    'biomass',
    'area',
    # Utility functions
    'merge_estimation_data',
    'calculate_adjustment_factors',
    'calculate_stratum_estimates',
    'calculate_population_estimates',
    'apply_domain_filter',
    'calculate_ratio_estimates',
    'summarize_by_groups'
]