"""
pyFIA - A modern Python package for working with USDA Forest Inventory and Analysis (FIA) data.

Built on Polars and DuckDB for high-performance forest analytics.
"""

__version__ = "0.2.0"
__author__ = "Chris Mihiar"

# Core exports - Main functionality
from pyfia.core.config import config, get_default_db_path, get_default_engine
from pyfia.core.data_reader import FIADataReader
from pyfia.core.fia import FIA
from pyfia.core.settings import PyFIASettings, settings

# Estimation functions - High-level API
from pyfia.estimation.area import area
from pyfia.estimation.biomass import biomass
from pyfia.estimation.growth import growth
from pyfia.estimation.mortality import mortality
from pyfia.estimation.tpa import tpa
from pyfia.estimation.tree import tree_count

# Utility functions
from pyfia.estimation.utils import (
    apply_domain_filter,
    calculate_adjustment_factors,
    calculate_population_estimates,
    calculate_ratio_estimates,
    calculate_stratum_estimates,
    merge_estimation_data,
    summarize_by_groups,
)
from pyfia.estimation.volume import volume

# Define public API
__all__ = [
    # Core classes
    "FIA",
    "FIADataReader",
    # Configuration
    "config",
    "get_default_db_path",
    "get_default_engine",
    "settings",
    "PyFIASettings",
    # Estimation functions
    "area",
    "biomass",
    "volume",
    "tpa",
    "mortality",
    "growth",
    "tree_count",
    # Utility functions
    "merge_estimation_data",
    "calculate_adjustment_factors",
    "calculate_stratum_estimates",
    "calculate_population_estimates",
    "apply_domain_filter",
    "calculate_ratio_estimates",
    "summarize_by_groups",
]


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
