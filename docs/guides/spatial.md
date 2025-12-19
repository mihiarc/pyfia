# Spatial Filtering

PyFIA supports spatial filtering and grouping using polygon boundaries. This enables analysis of custom regions like watersheds, management units, or administrative boundaries.

## Overview

PyFIA provides two spatial methods:

| Method | Purpose | Use Case |
|--------|---------|----------|
| `clip_by_polygon()` | Filter plots to polygon boundary | Analyze custom regions |
| `intersect_polygons()` | Join polygon attributes to plots | Group estimates by polygon attributes |

## Supported Formats

Both methods accept any GDAL-supported spatial file format:

- **Shapefile** (`.shp`)
- **GeoJSON** (`.geojson`, `.json`)
- **GeoPackage** (`.gpkg`)
- **GeoParquet** (`.parquet`)

## Clipping to a Region

Use `clip_by_polygon()` to filter plots within a polygon boundary:

```python
from pyfia import FIA, tpa, area

with FIA("southeast.duckdb") as db:
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="VOL")

    # Filter to custom region
    db.clip_by_polygon("my_region.geojson")

    # Estimates now only include plots within the polygon
    result = tpa(db, tree_type="live")
```

### Multiple Format Examples

```python
# GeoJSON
db.clip_by_polygon("boundary.geojson")

# Shapefile
db.clip_by_polygon("counties.shp")

# GeoPackage
db.clip_by_polygon("regions.gpkg")
```

### How It Works

1. Loads the DuckDB spatial extension
2. Performs point-in-polygon test using plot coordinates (LAT, LON)
3. Stores matching plot CNs as a filter
4. Applies filter when loading PLOT, TREE, and COND tables

## Grouping by Polygon Attributes

Use `intersect_polygons()` to join polygon attributes to plots for use in `grp_by`:

```python
from pyfia import FIA, tpa, area

with FIA("southeast.duckdb") as db:
    db.clip_by_state(37)
    db.clip_most_recent(eval_type="VOL")

    # Join county attributes to plots
    db.intersect_polygons("counties.shp", attributes=["NAME", "FIPS"])

    # Group estimates by county name
    result = tpa(db, grp_by=["NAME"], tree_type="live")
```

### Output Example

| NAME | TPA | BAA | N_PLOTS |
|------|-----|-----|---------|
| Wake | 523.4 | 89.2 | 45 |
| Durham | 612.1 | 102.3 | 38 |
| Orange | 487.9 | 78.6 | 29 |

### Multiple Attributes

```python
# Join multiple attributes
db.intersect_polygons(
    "regions.geojson",
    attributes=["REGION", "DISTRICT", "MANAGEMENT_UNIT"]
)

# Group by any combination
result = area(db, grp_by=["REGION", "DISTRICT"], land_type="forest")
```

### How It Works

1. Performs spatial join between plots and polygons
2. Stores polygon attributes with plot CNs
3. Joins attributes when loading PLOT table
4. Attributes flow through stratification to estimators
5. Can be used in `grp_by` like any other column

## Combining Methods

Both methods can be used together:

```python
with FIA("southeast.duckdb") as db:
    db.clip_by_state(37)
    db.clip_most_recent(eval_type="VOL")

    # First, clip to a study area
    db.clip_by_polygon("study_area.geojson")

    # Then, add management unit attributes for grouping
    db.intersect_polygons("management_units.shp", attributes=["UNIT_NAME"])

    # Estimates are clipped AND grouped
    result = tpa(db, grp_by=["UNIT_NAME"], tree_type="live")
```

## Common Analysis Patterns

### County-Level Estimates

```python
# Forest area by county
db.intersect_polygons("nc_counties.shp", attributes=["NAME"])
result = area(db, grp_by=["NAME"], land_type="forest")
```

### Watershed Analysis

```python
# Biomass by watershed
db.clip_by_polygon("watershed_boundary.geojson")
db.intersect_polygons("sub_watersheds.geojson", attributes=["HUC12"])
result = biomass(db, grp_by=["HUC12"])
```

### Management Unit Analysis

```python
# Volume on national forest management units
db.clip_by_polygon("national_forest_boundary.shp")
db.intersect_polygons("management_areas.gpkg", attributes=["AREA_NAME", "AREA_TYPE"])
result = volume(db, grp_by=["AREA_NAME"], land_type="timber")
```

### Regional Comparison

```python
# Compare north vs south regions
db.intersect_polygons("regions.geojson", attributes=["REGION"])
result = tpa(db, grp_by=["REGION"], by_species=True)
```

## Important Considerations

### Coordinate Privacy

FIA public coordinates are **fuzzed up to 1 mile** for privacy protection. This means:

- Spatial precision below ~1 mile is not meaningful
- Small polygons may have few or no matching plots
- Boundary effects exist for small analysis units

### Plots Outside Polygons

When using `intersect_polygons()`:

- Plots outside all polygons get NULL attribute values
- These plots are still included in non-grouped estimates
- Use `grp_by` to filter by attribute (NULLs excluded from groups)

### Performance

- First spatial query loads the DuckDB spatial extension
- Subsequent queries are faster (extension cached)
- Large polygon files may take longer to process

## Error Handling

### Common Errors

```python
# Missing spatial extension
from pyfia.core.exceptions import SpatialExtensionError

# Invalid file path or format
from pyfia.core.exceptions import SpatialFileError

# No spatial filter set
from pyfia.core.exceptions import NoSpatialFilterError
```

### Example

```python
from pyfia.core.exceptions import SpatialFileError

try:
    db.clip_by_polygon("invalid_file.xyz")
except SpatialFileError as e:
    print(f"Could not read spatial file: {e}")
```

## API Reference

### clip_by_polygon

```python
db.clip_by_polygon(
    polygon: str | Path,  # Path to spatial file
) -> FIA  # Returns self for chaining
```

### intersect_polygons

```python
db.intersect_polygons(
    polygon: str | Path,     # Path to spatial file
    attributes: list[str],   # Attribute columns to join
) -> FIA  # Returns self for chaining
```

## See Also

- [Domain Filtering](filtering.md) - Non-spatial filtering options
- [Grouping Results](grouping.md) - Grouping by FIA attributes
- [FIA Database API](../api/fia.md) - Full FIA class reference
