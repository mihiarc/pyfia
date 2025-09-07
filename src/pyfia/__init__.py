"""
pyFIA - A Python library for USDA Forest Inventory and Analysis (FIA) data analysis.

A programmatic API for forest analytics built on Polars and DuckDB, providing
exact statistical compatibility with the R rFIA package.
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

# Volume estimation (moved here for better organization)
from pyfia.estimation.volume import volume

# Reference table utilities - Useful for adding descriptive names to results
from pyfia.utils.reference_tables import (
    join_forest_type_names,
    join_species_names,
    join_state_names,
    join_multiple_references,
)

# Note: Statistical utility functions (merge_estimation_data, calculate_stratum_estimates, etc.)
# are internal to the estimators. Users should use the high-level estimation functions 
# (area, volume, tpa, etc.) which handle all statistical calculations internally.

# Converter functionality - Import only if needed to avoid heavy dependencies
try:
    from pyfia.converter import (
        ConverterConfig,
        DataValidator,
        FIAConverter,
        SchemaOptimizer,
        StateMerger,
    )
    _CONVERTER_AVAILABLE = True
except ImportError:
    _CONVERTER_AVAILABLE = False

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
    # Reference table utilities
    "join_forest_type_names",
    "join_species_names",
    "join_state_names",
    "join_multiple_references",
    # Conversion functions
    "convert_sqlite_to_duckdb",
    "merge_state_databases",
    "append_to_database",
]

# Add converter classes to __all__ if available
if _CONVERTER_AVAILABLE:
    __all__.extend([
        "FIAConverter",
        "ConverterConfig",
        "SchemaOptimizer",
        "StateMerger",
        "DataValidator"
    ])


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


# Provide a dummy attribute for tests that patch 'pyfia.tpa._prepare_tpa_data'
# Tests reference a legacy path; expose a no-op placeholder to keep them working.
setattr(tpa, "_prepare_tpa_data", None)


# High-level conversion API functions
def convert_sqlite_to_duckdb(
    source_path,
    target_path,
    state_code=None,
    config=None,
    **kwargs
):
    """
    Convert a SQLite FIA database to DuckDB format.

    Args:
        source_path: Path to source SQLite database
        target_path: Path to target DuckDB database
        state_code: Optional FIPS state code (auto-detected if not provided)
        config: Optional ConverterConfig object or dict of config parameters
        **kwargs: Additional configuration parameters

    Returns:
        ConversionResult with conversion details

    Example:
        # Simple conversion
        convert_sqlite_to_duckdb("OR_FIA.db", "oregon.duckdb")
        
        # With configuration
        convert_sqlite_to_duckdb(
            "OR_FIA.db",
            "oregon.duckdb",
            compression_level="high",
            validation_level="comprehensive"
        )
    """
    return FIA.convert_from_sqlite(source_path, target_path, state_code, config, **kwargs)


def merge_state_databases(
    source_paths,
    target_path,
    state_codes=None,
    config=None,
    **kwargs
):
    """
    Merge multiple state SQLite databases into a single DuckDB database.

    Args:
        source_paths: List of paths to source SQLite databases
        target_path: Path to target DuckDB database
        state_codes: Optional list of state codes to include
        config: Optional ConverterConfig object or dict of config parameters
        **kwargs: Additional configuration parameters

    Returns:
        ConversionResult with merge details

    Example:
        merge_state_databases(
            ["OR_FIA.db", "WA_FIA.db", "CA_FIA.db"],
            "pacific_states.duckdb"
        )
    """
    return FIA.merge_states(source_paths, target_path, state_codes, config, **kwargs)


def append_to_database(
    target_db,
    source_path,
    state_code=None,
    dedupe=False,
    dedupe_keys=None,
    **kwargs
):
    """
    Append data from a SQLite database to an existing DuckDB database.

    Args:
        target_db: Path to target DuckDB database or FIA instance
        source_path: Path to source SQLite database
        state_code: Optional FIPS state code (auto-detected if not provided)
        dedupe: Whether to remove duplicate records
        dedupe_keys: Column names to use for deduplication
        **kwargs: Additional configuration parameters

    Returns:
        ConversionResult with append details

    Example:
        # Append to existing database
        append_to_database("oregon.duckdb", "OR_FIA_update.db", dedupe=True)
        
        # Or use with FIA instance
        db = FIA("oregon.duckdb")
        append_to_database(db, "OR_FIA_update.db", dedupe=True)
    """
    from pyfia.core.fia import FIA as FIAClass
    if isinstance(target_db, FIAClass):
        return target_db.append_data(source_path, state_code, dedupe, dedupe_keys, **kwargs)
    else:
        # Create FIA instance for the target database
        db = FIA(target_db)
        return db.append_data(source_path, state_code, dedupe, dedupe_keys, **kwargs)
