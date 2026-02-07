"""
Accessing Raw Plot-Level Data from FIA
======================================

This example demonstrates how to retrieve raw plot-level data from pyFIA
for custom analyses beyond the standard estimation functions.

When to Use This Approach
-------------------------
Use direct data access when you need to:

- Build predictive models (ML/statistics) with FIA plot data
- Create maps or perform spatial analysis
- Link FIA data with external datasets (climate, soils, remote sensing)
- Perform custom analyses not covered by estimation functions
- Export data for use in other tools (R, GIS, etc.)

Understanding FIA Data Structure
--------------------------------
FIA uses a hierarchical data model:

    PLOT (1 per location)
      |
      +-- COND (1+ per plot, represents different "conditions" or stands)
      |     |
      |     +-- Forest type, ownership, site index, etc.
      |
      +-- TREE (many per plot)
            |
            +-- Species, diameter, height, volume, etc.

Key insight: A single plot can have MULTIPLE conditions. For example,
a plot might be 60% pine plantation and 40% hardwood. Each condition
has its own site index, forest type, and ownership.

Key Tables and Columns
----------------------
PLOT table (location information):
    - CN: Unique plot identifier
    - LAT, LON: Coordinates (fuzzed for privacy)
    - ELEV: Elevation in feet
    - INVYR: Inventory year
    - COUNTYCD: County FIPS code

COND table (stand attributes - one row per condition):
    - PLT_CN: Links to PLOT.CN
    - CONDID: Condition number (1, 2, 3...)
    - SICOND: Site index (height at base age)
    - SIBASE: Base age for site index (25 or 50 years)
    - SISP: Species code used for site index
    - FORTYPCD: Forest type code
    - STDAGE: Stand age
    - OWNGRPCD: Ownership group code
    - CONDPROP_UNADJ: Proportion of plot in this condition

TREE table (individual trees):
    - PLT_CN: Links to PLOT.CN
    - SPCD: Species code
    - DIA: Diameter at breast height (inches)
    - HT: Total height (feet)
    - VOLCFNET: Net cubic foot volume
    - DRYBIO_AG: Aboveground dry biomass (pounds)

Usage
-----
    # Default: uses NC data from ~/.pyfia/
    uv run python examples/plot_data_access.py

    # With custom database
    uv run python examples/plot_data_access.py /path/to/state.duckdb

Functions Provided
------------------
- get_plot_locations(): Basic plot coordinates
- get_site_index_by_condition(): Site index at condition level
- get_site_index_with_location(): Conditions joined with plot locations
- get_weighted_site_index_by_plot(): Area-weighted site index per plot
- get_tree_data_by_plot(): Tree data with plot locations
"""

from pyfia import FIA
import polars as pl


def get_plot_locations(db_path: str, state_fips: int) -> pl.DataFrame:
    """
    Retrieve plot locations with basic attributes.

    This is the simplest form of data access - just plot coordinates
    and identifiers. Useful for mapping or as a starting point for joins.

    Parameters
    ----------
    db_path : str
        Path to FIA DuckDB database.
    state_fips : int
        State FIPS code (e.g., 37 for NC, 13 for GA).

    Returns
    -------
    pl.DataFrame
        Plot data with CN, LAT, LON, ELEV, INVYR, COUNTYCD.

    Example
    -------
    >>> plots = get_plot_locations("data/nc.duckdb", 37)
    >>> print(plots.head())
    """
    db = FIA(db_path)
    db.clip_by_state(state_fips)
    db.clip_most_recent(eval_type="VOL")

    plots = db.get_plots(columns=[
        "CN", "LAT", "LON", "ELEV", "INVYR", "STATECD", "COUNTYCD"
    ])

    return plots


def get_site_index_by_condition(db_path: str, state_fips: int) -> pl.DataFrame:
    """
    Retrieve site index data at the condition (stand) level.

    Site index is a measure of site productivity - the expected height
    of dominant trees at a base age (typically 25 or 50 years). Higher
    site index = more productive site.

    IMPORTANT: Site index is measured per CONDITION, not per plot.
    A plot can have multiple conditions with different site indices.

    Parameters
    ----------
    db_path : str
        Path to FIA DuckDB database.
    state_fips : int
        State FIPS code.

    Returns
    -------
    pl.DataFrame
        Condition data with site index and related attributes.

    Key Columns
    -----------
    - SICOND: Site index for the condition (feet at base age)
    - SIBASE: Base age for site index (typically 25 or 50 years)
    - SISP: Species code used for site index determination
    - CONDPROP_UNADJ: Proportion of plot area in this condition

    Example
    -------
    >>> conds = get_site_index_by_condition("data/nc.duckdb", 37)
    >>> # Filter to valid site index values
    >>> valid = conds.filter(pl.col("SICOND").is_not_null())
    """
    db = FIA(db_path)
    db.clip_by_state(state_fips)
    db.clip_most_recent(eval_type="VOL")

    # Load PLOT table first (required for EVALID filtering in get_conditions)
    db.load_table("PLOT")

    conds = db.get_conditions(columns=[
        "CN", "PLT_CN", "CONDID",
        "SICOND", "SIBASE", "SISP",
        "FORTYPCD", "STDAGE", "STDSZCD",
        "SITECLCD", "OWNGRPCD",
        "CONDPROP_UNADJ"
    ])

    return conds


def get_site_index_with_location(db_path: str, state_fips: int) -> pl.DataFrame:
    """
    Join site index data with plot locations.

    Returns one row per condition, with both stand attributes and
    plot location information. Useful for spatial analysis of site
    productivity.

    Parameters
    ----------
    db_path : str
        Path to FIA DuckDB database.
    state_fips : int
        State FIPS code.

    Returns
    -------
    pl.DataFrame
        Combined plot location and condition site index data.

    Example
    -------
    >>> data = get_site_index_with_location("data/nc.duckdb", 37)
    >>> # Export for GIS analysis
    >>> data.write_csv("site_index_locations.csv")
    """
    db = FIA(db_path)
    db.clip_by_state(state_fips)
    db.clip_most_recent(eval_type="VOL")

    plots = db.get_plots(columns=[
        "CN", "LAT", "LON", "ELEV", "INVYR", "COUNTYCD"
    ])

    conds = db.get_conditions(columns=[
        "PLT_CN", "CONDID",
        "SICOND", "SIBASE", "SISP",
        "FORTYPCD", "STDAGE",
        "CONDPROP_UNADJ"
    ])

    # Join condition data with plot locations
    result = conds.join(
        plots,
        left_on="PLT_CN",
        right_on="CN",
        how="inner"
    ).select([
        "PLT_CN", "CONDID",
        "LAT", "LON", "ELEV", "INVYR", "COUNTYCD",
        "SICOND", "SIBASE", "SISP",
        "FORTYPCD", "STDAGE",
        "CONDPROP_UNADJ"
    ])

    return result


def get_weighted_site_index_by_plot(db_path: str, state_fips: int) -> pl.DataFrame:
    """
    Calculate area-weighted site index per plot.

    When a plot has multiple conditions with different site indices,
    this function computes a weighted average based on condition proportions.
    This provides a single representative site index per plot location.

    The Formula
    -----------
    weighted_SI = sum(SICOND * CONDPROP_UNADJ) / sum(CONDPROP_UNADJ)

    For example, if a plot is:
    - 60% pine plantation with SI=80
    - 40% hardwood with SI=70

    Then: weighted_SI = (80*0.6 + 70*0.4) / (0.6 + 0.4) = 76

    Parameters
    ----------
    db_path : str
        Path to FIA DuckDB database.
    state_fips : int
        State FIPS code.

    Returns
    -------
    pl.DataFrame
        One row per plot with:
        - weighted_SICOND: Area-weighted site index
        - dominant_SISP: Site index species from largest condition
        - dominant_FORTYPCD: Forest type from largest condition
        - n_conditions: Number of conditions contributing to average

    Example
    -------
    >>> weighted = get_weighted_site_index_by_plot("data/nc.duckdb", 37)
    >>> # Find high-productivity sites
    >>> high_si = weighted.filter(pl.col("weighted_SICOND") > 90)
    """
    db = FIA(db_path)
    db.clip_by_state(state_fips)
    db.clip_most_recent(eval_type="VOL")

    plots = db.get_plots(columns=[
        "CN", "LAT", "LON", "ELEV", "INVYR", "COUNTYCD"
    ])

    conds = db.get_conditions(columns=[
        "PLT_CN", "CONDID",
        "SICOND", "SIBASE", "SISP",
        "FORTYPCD", "STDAGE",
        "CONDPROP_UNADJ"
    ])

    # Calculate area-weighted site index
    weighted_si = (
        conds
        .filter(pl.col("SICOND").is_not_null())
        .group_by("PLT_CN")
        .agg([
            # Weighted average: sum(SI * proportion) / sum(proportion)
            (pl.col("SICOND") * pl.col("CONDPROP_UNADJ")).sum().alias("si_x_prop"),
            pl.col("CONDPROP_UNADJ").sum().alias("total_prop"),
            # Track number of conditions contributing to the average
            pl.len().alias("n_conditions"),
            # Base age (assume consistent within plot)
            pl.col("SIBASE").first().alias("SIBASE"),
            # Get SI species from largest condition
            pl.col("SISP").sort_by("CONDPROP_UNADJ", descending=True).first().alias("dominant_SISP"),
            # Get dominant forest type
            pl.col("FORTYPCD").sort_by("CONDPROP_UNADJ", descending=True).first().alias("dominant_FORTYPCD"),
        ])
        .with_columns(
            (pl.col("si_x_prop") / pl.col("total_prop")).round(1).alias("weighted_SICOND")
        )
        .select([
            "PLT_CN", "weighted_SICOND", "SIBASE",
            "dominant_SISP", "dominant_FORTYPCD", "n_conditions"
        ])
    )

    # Join with plot locations
    result = weighted_si.join(
        plots,
        left_on="PLT_CN",
        right_on="CN",
        how="inner"
    ).select([
        "PLT_CN",
        "LAT", "LON", "ELEV",
        "INVYR", "COUNTYCD",
        "weighted_SICOND", "SIBASE",
        "dominant_SISP", "dominant_FORTYPCD",
        "n_conditions"
    ])

    return result


def get_tree_data_by_plot(db_path: str, state_fips: int) -> pl.DataFrame:
    """
    Retrieve tree-level data with plot locations.

    Returns individual tree measurements joined with their plot
    coordinates. Useful for species distribution modeling or
    tree-level analyses.

    Parameters
    ----------
    db_path : str
        Path to FIA DuckDB database.
    state_fips : int
        State FIPS code.

    Returns
    -------
    pl.DataFrame
        Tree data joined with plot locations.

    Key Tree Columns
    ----------------
    - SPCD: Species code
    - DIA: Diameter at breast height (inches)
    - HT: Total height (feet)
    - STATUSCD: 1=live, 2=dead
    - VOLCFNET: Net cubic foot volume
    - DRYBIO_AG: Aboveground dry biomass (pounds)

    Example
    -------
    >>> trees = get_tree_data_by_plot("data/nc.duckdb", 37)
    >>> # Filter to live loblolly pine
    >>> loblolly = trees.filter(
    ...     (pl.col("SPCD") == 131) & (pl.col("STATUSCD") == 1)
    ... )
    """
    db = FIA(db_path)
    db.clip_by_state(state_fips)
    db.clip_most_recent(eval_type="VOL")

    plots = db.get_plots(columns=["CN", "LAT", "LON", "ELEV", "INVYR"])

    trees = db.get_trees(columns=[
        "CN", "PLT_CN", "SUBP", "TREE",
        "SPCD", "DIA", "HT", "ACTUALHT",
        "STATUSCD", "DRYBIO_AG", "VOLCFNET"
    ])

    # Join trees with plot locations
    result = trees.join(
        plots.rename({"CN": "PLOT_CN"}),
        left_on="PLT_CN",
        right_on="PLOT_CN",
        how="inner"
    )

    return result


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == "__main__":
    import sys

    # Default to NC if no argument provided
    DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "~/.pyfia/data/nc/nc.duckdb"
    DB_PATH = str(__import__("pathlib").Path(DB_PATH).expanduser())
    STATE = 37  # North Carolina

    print(__doc__)
    print("=" * 70)
    print(f"Database: {DB_PATH}")
    print(f"State FIPS: {STATE}")
    print("=" * 70)

    # Example 1: Plot locations
    print("\n1. PLOT LOCATIONS")
    print("-" * 70)
    plots = get_plot_locations(DB_PATH, STATE)
    print(plots.head(5))
    print(f"Total plots: {len(plots)}")

    # Example 2: Site index by condition
    print("\n2. SITE INDEX BY CONDITION (STAND)")
    print("-" * 70)
    conds = get_site_index_by_condition(DB_PATH, STATE)
    print(conds.filter(pl.col("SICOND").is_not_null()).head(5))
    valid_si = conds.filter(pl.col("SICOND").is_not_null())
    print(f"Conditions with valid site index: {len(valid_si)} / {len(conds)}")

    # Example 3: Site index with location
    print("\n3. SITE INDEX WITH PLOT LOCATION")
    print("-" * 70)
    si_with_loc = get_site_index_with_location(DB_PATH, STATE)
    print(si_with_loc.filter(pl.col("SICOND").is_not_null()).head(5))

    # Example 4: Weighted site index per plot
    print("\n4. AREA-WEIGHTED SITE INDEX PER PLOT")
    print("-" * 70)
    weighted = get_weighted_site_index_by_plot(DB_PATH, STATE)
    print(weighted.head(10))
    print(f"\nPlots with weighted site index: {len(weighted)}")

    # Summary statistics
    print("\n5. SITE INDEX SUMMARY STATISTICS")
    print("-" * 70)
    print(weighted.select("weighted_SICOND").describe())

    # Example: Filter to high-productivity sites
    print("\n6. EXAMPLE: HIGH-PRODUCTIVITY SITES (SI > 90)")
    print("-" * 70)
    high_productivity = weighted.filter(pl.col("weighted_SICOND") > 90)
    print(high_productivity.head(10))
    print(f"Plots with SI > 90: {len(high_productivity)}")
