"""
pyFIA - A Python library for USDA Forest Inventory and Analysis (FIA) data analysis.

A high-performance Python library for analyzing USDA Forest Inventory and Analysis (FIA) data
using modern data science tools like Polars and DuckDB.
"""

__version__ = "0.2.0"
__author__ = "Chris Mihiar"

# Core exports - Main functionality
from pyfia.core.data_reader import FIADataReader
from pyfia.core.fia import FIA
from pyfia.core.settings import (
    PyFIASettings,
    get_default_db_path,
    get_default_engine,
    settings,
)

# Estimation functions - High-level API
from pyfia.estimation.estimators.area import area
from pyfia.estimation.estimators.biomass import biomass
from pyfia.estimation.estimators.growth import growth
from pyfia.estimation.estimators.mortality import mortality
from pyfia.estimation.estimators.removals import removals
from pyfia.estimation.estimators.tpa import tpa
from pyfia.estimation.estimators.volume import volume

# Reference table utilities - Useful for adding descriptive names to results
from pyfia.utils.reference_tables import (
    join_forest_type_names,
    join_multiple_references,
    join_species_names,
    join_state_names,
)

# EVALIDator API client - For validation against official USFS estimates
from pyfia.evalidator import (
    EVALIDatorClient,
    EVALIDatorEstimate,
    EstimateType,
    ValidationResult,
    compare_estimates,
    validate_pyfia_estimate,
)

# Note: Statistical utility functions (merge_estimation_data, calculate_stratum_estimates, etc.)
# are internal to the estimators. Users should use the high-level estimation functions
# (area, volume, tpa, etc.) which handle all statistical calculations internally.

# Converter functionality - Check availability without importing unused names
try:
    from importlib.util import find_spec

    _CONVERTER_AVAILABLE = find_spec("pyfia.converter") is not None
except ImportError:
    _CONVERTER_AVAILABLE = False

# Define public API
__all__ = [
    # Core classes
    "FIA",
    "FIADataReader",
    # Configuration
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
    # Conversion functions
    "convert_sqlite_to_duckdb",
    "merge_state_databases",
    "append_to_database",
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


# High-level conversion API functions
def convert_sqlite_to_duckdb(
    source_path, target_path, state_code=None, config=None, **kwargs
):
    """
    Convert a SQLite FIA database to DuckDB format.

    Parameters
    ----------
    source_path : str or Path
        Path to source SQLite database.
    target_path : str or Path
        Path to target DuckDB database.
    state_code : int, optional
        FIPS state code. Auto-detected from filename if not provided.
    config : dict, optional
        Configuration parameters for conversion.
    **kwargs : dict
        Additional configuration parameters passed to converter.

    Returns
    -------
    Dict[str, int]
        Conversion results with row counts per table.

    Examples
    --------
    Simple conversion:

    >>> convert_sqlite_to_duckdb("OR_FIA.db", "oregon.duckdb")

    With configuration:

    >>> convert_sqlite_to_duckdb(
    ...     "OR_FIA.db",
    ...     "oregon.duckdb",
    ...     compression_level="high",
    ...     validation_level="comprehensive"
    ... )
    """
    return FIA.convert_from_sqlite(
        source_path, target_path, state_code, config, **kwargs
    )


def merge_state_databases(
    source_paths, target_path, state_codes=None, config=None, **kwargs
):
    """
    Merge multiple state SQLite databases into a single DuckDB database.

    Parameters
    ----------
    source_paths : list of str or Path
        Paths to source SQLite databases.
    target_path : str or Path
        Path to target DuckDB database.
    state_codes : list of int, optional
        FIPS state codes to include. Processes all if not provided.
    config : dict, optional
        Configuration parameters for conversion.
    **kwargs : dict
        Additional configuration parameters passed to converter.

    Returns
    -------
    Dict[str, Dict[str, int]]
        Merge results with row counts per state and table.

    Examples
    --------
    >>> merge_state_databases(
    ...     ["OR_FIA.db", "WA_FIA.db", "CA_FIA.db"],
    ...     "pacific_states.duckdb"
    ... )
    """
    return FIA.merge_states(source_paths, target_path, state_codes, config, **kwargs)


def append_to_database(
    target_db, source_path, state_code=None, dedupe=False, dedupe_keys=None, **kwargs
):
    """
    Append data from a SQLite database to an existing DuckDB database.

    Parameters
    ----------
    target_db : str, Path, or FIA
        Path to target DuckDB database or existing FIA instance.
    source_path : str or Path
        Path to source SQLite database.
    state_code : int, optional
        FIPS state code. Auto-detected from filename if not provided.
    dedupe : bool, default False
        Whether to remove duplicate records after append.
    dedupe_keys : list of str, optional
        Column names to use for deduplication. Uses primary keys if None.
    **kwargs : dict
        Additional configuration parameters passed to converter.

    Returns
    -------
    Dict[str, int]
        Append results with row counts per table.

    Examples
    --------
    Append to existing database:

    >>> append_to_database("oregon.duckdb", "OR_FIA_update.db", dedupe=True)

    Or use with FIA instance:

    >>> db = FIA("oregon.duckdb")
    >>> append_to_database(db, "OR_FIA_update.db", dedupe=True)
    """
    from pyfia.core.fia import FIA as FIAClass

    if isinstance(target_db, FIAClass):
        return target_db.append_data(
            source_path, state_code, dedupe, dedupe_keys, **kwargs
        )
    else:
        # Create FIA instance for the target database
        db = FIA(target_db)
        return db.append_data(source_path, state_code, dedupe, dedupe_keys, **kwargs)
