"""
Example: Using plot_domain to filter FIA estimates by county and location.

This example demonstrates how to use the new plot_domain parameter to filter
FIA estimates by PLOT-level attributes like COUNTYCD, UNITCD, LAT, LON, and ELEV.

The plot_domain parameter is useful when you need to filter by attributes that
are stored in the PLOT table rather than the COND table.
"""

from pyfia import FIA, area, volume, biomass, tpa

# Example 1: Filter by county
# This was not possible before without custom SQL - now it's simple!
def example_county_filter():
    """Estimate forest area for a specific county."""
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


# Example 2: Filter by multiple counties
def example_multiple_counties():
    """Estimate forest area for multiple counties."""
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)  # North Carolina

        # Get area for multiple counties
        results = area(
            db,
            plot_domain="COUNTYCD IN (183, 185, 187)",  # Wake, Warren, Washington
            grp_by="COUNTYCD",
            land_type="forest"
        )
        print("Forest area by county:")
        print(results)


# Example 3: Filter by survey unit
def example_survey_unit():
    """Estimate volume by survey unit."""
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


# Example 4: Geographic filtering by latitude/longitude
def example_geographic_filter():
    """Estimate biomass within a geographic bounding box."""
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Filter to plots within a specific geographic area
        results = biomass(
            db,
            plot_domain="LAT >= 35.0 AND LAT <= 36.0 AND LON >= -80.0 AND LON <= -79.0",
            land_type="forest",
            tree_type="live"
        )
        print("Biomass within geographic bounds:")
        print(results)


# Example 5: Filter by elevation
def example_elevation_filter():
    """Estimate trees per acre at high elevations."""
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Get TPA for high-elevation forests
        results = tpa(
            db,
            plot_domain="ELEV > 2000",  # Above 2000 feet
            land_type="forest",
            tree_type="live"
        )
        print("Trees per acre above 2000 feet elevation:")
        print(results)


# Example 6: Combine plot_domain with area_domain
def example_combined_filters():
    """Combine PLOT-level and COND-level filters."""
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Filter by county (PLOT) AND ownership (COND)
        results = area(
            db,
            plot_domain="COUNTYCD == 183",  # Wake County
            area_domain="OWNGRPCD == 40",   # Private land
            grp_by="FORTYPCD",
            land_type="forest"
        )
        print("Private forest area in Wake County by forest type:")
        print(results)


# Example 7: Filter by inventory year
def example_temporal_filter():
    """Estimate area from specific inventory years."""
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Get area from recent inventory
        results = area(
            db,
            plot_domain="MEASYEAR >= 2015",  # Plots measured since 2015
            land_type="forest"
        )
        print("Forest area from plots measured since 2015:")
        print(results)


# Example 8: Complex plot filtering
def example_complex_plot_filter():
    """Use complex plot filtering with multiple conditions."""
    with FIA("path/to/fia.duckdb") as db:
        db.clip_by_state(37)

        # Complex filter: specific counties, elevation range, recent measurements
        results = volume(
            db,
            plot_domain=(
                "COUNTYCD IN (183, 185) AND "
                "ELEV >= 100 AND ELEV <= 500 AND "
                "MEASYEAR >= 2015"
            ),
            land_type="forest",
            tree_type="live",
            grp_by="COUNTYCD"
        )
        print("Volume in specific counties, elevation range, and time period:")
        print(results)


if __name__ == "__main__":
    print(__doc__)
    print("\nNote: Replace 'path/to/fia.duckdb' with actual database path.")
    print("\nAvailable PLOT columns for filtering:")
    print("  - COUNTYCD: County FIPS code")
    print("  - UNITCD: Survey unit code")
    print("  - STATECD: State FIPS code")
    print("  - LAT: Latitude (decimal degrees)")
    print("  - LON: Longitude (decimal degrees)")
    print("  - ELEV: Elevation (feet)")
    print("  - INVYR: Inventory year")
    print("  - MEASYEAR: Measurement year")
    print("  - MEASMON: Measurement month")
    print("  - PLOT: Plot number")
    print("\nFor COND-level attributes (ownership, forest type, etc.), use area_domain instead.")
