"""
PyFIA - A Python library for USDA Forest Inventory and Analysis (FIA) data analysis.

A high-performance Python library for analyzing USDA Forest Inventory and Analysis (FIA) data
using modern data science tools like Polars and DuckDB.

Part of the FIA Python Ecosystem:
- PyFIA: Survey/plot data analysis (https://github.com/mihiarc/pyfia)
- GridFIA: Spatial raster analysis (https://github.com/mihiarc/gridfia)
- PyFVS: Growth/yield simulation (https://github.com/mihiarc/pyfvs)
- AskFIA: AI conversational interface (https://github.com/mihiarc/askfia)
"""

__version__ = "1.1.0b6"
__author__ = "Chris Mihiar"

# Core exports - Main functionality
from pyfia.core.data_reader import FIADataReader
from pyfia.core.exceptions import (
    ConfigurationError,
    DatabaseError,
    EstimationError,
    FilterError,
    InsufficientDataError,
    InvalidDomainError,
    InvalidEVALIDError,
    MissingColumnError,
    NoEVALIDError,
    PyFIAError,
    StratificationError,
    TableNotFoundError,
)
from pyfia.core.fia import FIA, MotherDuckFIA
from pyfia.core.settings import (
    PyFIASettings,
    get_default_db_path,
    get_default_engine,
    settings,
)

# Data download - Download FIA data directly from DataMart
from pyfia.downloader import (
    COMMON_TABLES,
    VALID_STATE_CODES,
    DataMartClient,
    DownloadCache,
    cache_info,
    clear_cache,
    download,
)

# Estimation functions - High-level API
from pyfia.estimation.estimators.area import area
from pyfia.estimation.estimators.area_change import area_change
from pyfia.estimation.estimators.biomass import biomass
from pyfia.estimation.estimators.growth import growth
from pyfia.estimation.estimators.mortality import mortality
from pyfia.estimation.estimators.removals import removals
from pyfia.estimation.estimators.tpa import tpa
from pyfia.estimation.estimators.volume import volume

# EVALIDator API client - For validation against official USFS estimates
from pyfia.evalidator.client import EVALIDatorClient, EVALIDatorEstimate
from pyfia.evalidator.estimate_types import EstimateType
from pyfia.evalidator.validation import (
    ValidationResult,
    compare_estimates,
    validate_pyfia_estimate,
)

# Reference table utilities - Useful for adding descriptive names to results
from pyfia.utils.reference_tables import (
    join_forest_type_names,
    join_multiple_references,
    join_species_names,
    join_state_names,
)

# Note: Statistical utility functions (merge_estimation_data, calculate_stratum_estimates, etc.)
# are internal to the estimators. Users should use the high-level estimation functions
# (area, volume, tpa, etc.) which handle all statistical calculations internally.

# Define public API
__all__ = [
    # Core classes
    "FIA",
    "MotherDuckFIA",
    "FIADataReader",
    # Configuration
    "get_default_db_path",
    "get_default_engine",
    "settings",
    "PyFIASettings",
    # Exceptions
    "PyFIAError",
    "DatabaseError",
    "TableNotFoundError",
    "EstimationError",
    "InsufficientDataError",
    "StratificationError",
    "MissingColumnError",
    "FilterError",
    "InvalidDomainError",
    "InvalidEVALIDError",
    "NoEVALIDError",
    "ConfigurationError",
    # Estimation functions
    "area",
    "area_change",
    "biomass",
    "volume",
    "tpa",
    "mortality",
    "growth",
    "removals",
    # Reference table utilities
    "join_forest_type_names",
    "join_species_names",
    "join_state_names",
    "join_multiple_references",
    # EVALIDator validation
    "EVALIDatorClient",
    "EVALIDatorEstimate",
    "EstimateType",
    "ValidationResult",
    "compare_estimates",
    "validate_pyfia_estimate",
    # Data download
    "download",
    "DataMartClient",
    "DownloadCache",
    "COMMON_TABLES",
    "VALID_STATE_CODES",
    "clear_cache",
    "cache_info",
]


def get_fia(db_path=None, engine=None):
    """
    Get FIA database instance with default settings.

    Parameters
    ----------
    db_path : str or Path, optional
        Path to FIA database. Uses default from settings if None.
    engine : str, optional
        Database engine type ('duckdb' or 'sqlite'). Uses default if None.

    Returns
    -------
    FIA
        Configured FIA database instance.
    """
    if db_path is None:
        db_path = get_default_db_path()
    if engine is None:
        engine = get_default_engine()

    return FIA(db_path, engine=engine)
