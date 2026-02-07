"""
Filtering Estimates by Plot Attributes (plot_domain)
=====================================================

This example documents the `plot_domain` parameter, which allows filtering
FIA estimates by PLOT-level attributes like county, coordinates, and elevation.

The Problem
-----------
FIA data is stored in multiple tables:

    PLOT table: Location info (LAT, LON, COUNTYCD, ELEV, INVYR)
    COND table: Stand attributes (ownership, forest type, site class)
    TREE table: Individual tree measurements

The standard `area_domain` and `tree_domain` parameters only filter COND
and TREE attributes. Before `plot_domain`, filtering by county or geographic
bounds required custom SQL or post-processing.

The Solution
------------
The `plot_domain` parameter accepts SQL-like expressions that filter on
PLOT table columns:

    area(db, plot_domain="COUNTYCD == 183")  # Single county
    area(db, plot_domain="LAT >= 35.0 AND LAT <= 36.0")  # Lat range
    area(db, plot_domain="ELEV > 2000")  # High elevation

Available PLOT Columns
----------------------
Location:
    - LAT: Latitude (decimal degrees, fuzzed for privacy)
    - LON: Longitude (decimal degrees, fuzzed for privacy)
    - ELEV: Elevation (feet)

Administrative:
    - STATECD: State FIPS code
    - COUNTYCD: County FIPS code
    - UNITCD: FIA survey unit code

Temporal:
    - INVYR: Inventory year (when plot was assigned to panel)
    - MEASYEAR: Measurement year (when plot was actually measured)
    - MEASMON: Measurement month (1-12)

Identifiers:
    - PLOT: Plot number within county
    - CN: Unique plot identifier

Combining with Other Filters
----------------------------
You can use plot_domain together with area_domain and tree_domain:

    # County AND ownership filter
    area(db,
         plot_domain="COUNTYCD == 183",      # PLOT-level: Wake County
         area_domain="OWNGRPCD == 40")       # COND-level: Private land

    # Geographic bounds AND species filter
    volume(db,
           plot_domain="LAT >= 35 AND LAT <= 36",  # PLOT-level
           tree_domain="SPCD == 131")              # TREE-level: Loblolly pine

Common Use Cases
----------------
1. County-level estimates for local planning
2. Geographic subsets for regional analysis
3. Elevation bands for mountain/lowland comparisons
4. Temporal subsets for trend analysis
5. Survey unit summaries for FIA reporting

Note: This file contains example code patterns, not a runnable script.
Replace 'path/to/fia.duckdb' with your actual database path.
"""

from pyfia import FIA, area, volume, biomass, tpa


# =============================================================================
# Example 1: Single County Filter
# =============================================================================

def example_county_filter():
    """
    Estimate forest area for a specific county.

    Use Case: A county forester needs forest area statistics for their
    jurisdiction. Previously this required custom SQL; now it's one line.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)  # North Carolina

        # Get area for Wake County (COUNTYCD == 183)
        results = area(
            db,
            plot_domain="COUNTYCD == 183",
            land_type="forest"
        )
        print("Forest area in Wake County, NC:")
        print(results)


# =============================================================================
# Example 2: Multiple Counties with Grouping
# =============================================================================

def example_multiple_counties():
    """
    Estimate forest area for multiple counties, grouped by county.

    Use Case: Compare forest resources across a multi-county region,
    such as a metropolitan statistical area or watershed.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)  # North Carolina

        # Get area for multiple counties, grouped by county
        results = area(
            db,
            plot_domain="COUNTYCD IN (183, 185, 187)",  # Wake, Warren, Washington
            grp_by="COUNTYCD",  # Separate estimate per county
            land_type="forest"
        )
        print("Forest area by county:")
        print(results)


# =============================================================================
# Example 3: Survey Unit Filter
# =============================================================================

def example_survey_unit():
    """
    Estimate volume by FIA survey unit.

    Use Case: FIA reports are often organized by survey unit (groups of
    counties). This enables matching pyFIA output to official FIA reports.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Get volume for a specific survey unit
        results = volume(
            db,
            plot_domain="UNITCD == 1",
            land_type="forest",
            tree_type="live"
        )
        print("Live tree volume in survey unit 1:")
        print(results)


# =============================================================================
# Example 4: Geographic Bounding Box
# =============================================================================

def example_geographic_filter():
    """
    Estimate biomass within a geographic bounding box.

    Use Case: Analyze forest resources within a specific geographic area,
    such as a national forest boundary approximation or study area.

    Note: FIA coordinates are fuzzed up to 1 mile for privacy, so precise
    boundary matching is not possible. Use spatial filtering (clip_by_polygon)
    for accurate boundary analysis.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Filter to plots within a lat/lon box
        results = biomass(
            db,
            plot_domain="LAT >= 35.0 AND LAT <= 36.0 AND LON >= -80.0 AND LON <= -79.0",
            land_type="forest",
            tree_type="live"
        )
        print("Biomass within geographic bounds:")
        print(results)


# =============================================================================
# Example 5: Elevation Filter
# =============================================================================

def example_elevation_filter():
    """
    Estimate trees per acre at high elevations.

    Use Case: Compare forest characteristics between mountain and lowland
    forests, or focus analysis on specific elevation zones.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Get TPA for high-elevation forests (> 2000 feet)
        results = tpa(
            db,
            plot_domain="ELEV > 2000",
            land_type="forest",
            tree_type="live"
        )
        print("Trees per acre above 2000 feet elevation:")
        print(results)


# =============================================================================
# Example 6: Combining PLOT and COND Filters
# =============================================================================

def example_combined_filters():
    """
    Combine PLOT-level and COND-level filters.

    Use Case: Answer questions like "What is the private forest area
    in Wake County by forest type?" - requires filtering on both
    PLOT (county) and COND (ownership, forest type) attributes.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Filter by county (PLOT) AND ownership (COND)
        results = area(
            db,
            plot_domain="COUNTYCD == 183",   # PLOT-level: Wake County
            area_domain="OWNGRPCD == 40",    # COND-level: Private land
            grp_by="FORTYPCD",               # Group by forest type
            land_type="forest"
        )
        print("Private forest area in Wake County by forest type:")
        print(results)


# =============================================================================
# Example 7: Temporal Filter (Measurement Year)
# =============================================================================

def example_temporal_filter():
    """
    Estimate area from specific measurement years.

    Use Case: Focus on recently measured plots for more current data,
    or analyze temporal patterns in forest attributes.

    Note: MEASYEAR is when the plot was actually visited. INVYR is when
    it was assigned to the panel (may differ by 1-2 years).
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Get area from plots measured since 2015
        results = area(
            db,
            plot_domain="MEASYEAR >= 2015",
            land_type="forest"
        )
        print("Forest area from plots measured since 2015:")
        print(results)


# =============================================================================
# Example 8: Complex Multi-Condition Filter
# =============================================================================

def example_complex_plot_filter():
    """
    Use complex plot filtering with multiple conditions.

    Use Case: Highly specific analyses combining geographic, temporal,
    and administrative constraints.
    """
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Complex filter: specific counties + elevation range + recent measurements
        results = volume(
            db,
            plot_domain=(
                "COUNTYCD IN (183, 185) AND "    # Wake or Warren county
                "ELEV >= 100 AND ELEV <= 500 AND "  # Piedmont elevation
                "MEASYEAR >= 2015"               # Recent measurements
            ),
            land_type="forest",
            tree_type="live",
            grp_by="COUNTYCD"  # Separate results per county
        )
        print("Volume in specific counties, elevation range, and time period:")
        print(results)


# =============================================================================
# Quick Reference
# =============================================================================

if __name__ == "__main__":
    print(__doc__)
    print("\n" + "=" * 70)
    print("QUICK REFERENCE: plot_domain vs area_domain vs tree_domain")
    print("=" * 70)
    print("""
    plot_domain  - Filters PLOT table (location, county, elevation)
                   Example: plot_domain="COUNTYCD == 183"

    area_domain  - Filters COND table (ownership, forest type, site class)
                   Example: area_domain="OWNGRPCD == 40"

    tree_domain  - Filters TREE table (species, diameter, status)
                   Example: tree_domain="SPCD == 131 AND DIA >= 10"

    All three can be combined in a single call:

        volume(db,
               plot_domain="COUNTYCD == 183",
               area_domain="OWNGRPCD == 40",
               tree_domain="SPCD == 131")
    """)

    print("\n" + "=" * 70)
    print("AVAILABLE PLOT COLUMNS FOR FILTERING")
    print("=" * 70)
    print("""
    Location:
      - LAT         Latitude (decimal degrees)
      - LON         Longitude (decimal degrees)
      - ELEV        Elevation (feet)

    Administrative:
      - STATECD     State FIPS code
      - COUNTYCD    County FIPS code
      - UNITCD      FIA survey unit code

    Temporal:
      - INVYR       Inventory year
      - MEASYEAR    Measurement year
      - MEASMON     Measurement month (1-12)

    Identifiers:
      - PLOT        Plot number within county
      - CN          Unique plot identifier
    """)
