"""
pyFIA - Python implementation of rFIA for Forest Inventory Analysis

A Python package for working with USDA Forest Inventory and Analysis (FIA) data.
"""

__version__ = "0.1.0"
__author__ = "Chris Mihiar"

# Import core classes
from .core import FIA
from .data_reader import FIADataReader
from .config import config, get_default_db_path, get_default_engine
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
    # Configuration
    'config',
    'get_default_db_path',
    'get_default_engine',
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


# Convenience function to get default FIA database
def get_fia(db_path=None, engine=None):
    """
    Get FIA database instance with default settings.
    
    Args:
        db_path: Optional database path (uses default if None)
        engine: Optional engine type (uses default if None)
        
    Returns:
        FIA instance
    """
    if db_path is None:
        db_path = get_default_db_path()
    if engine is None:
        engine = get_default_engine()
    
    return FIA(db_path, engine=engine)