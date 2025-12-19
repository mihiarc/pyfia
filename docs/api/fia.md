# FIA Database

The `FIA` class is the main entry point for working with FIA data.

## Overview

```python
import pyfia

# Basic usage
db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Use estimation functions
result = pyfia.volume(db, grp_by="SPCD")
```

## Spatial Filtering

PyFIA supports spatial filtering using polygon boundaries:

```python
with pyfia.FIA("southeast.duckdb") as db:
    db.clip_by_state(37)  # North Carolina
    db.clip_most_recent(eval_type="VOL")

    # Filter to custom region
    db.clip_by_polygon("my_region.geojson")

    # Join polygon attributes for grouping
    db.intersect_polygons("counties.shp", attributes=["NAME"])

    # Group estimates by polygon attribute
    result = pyfia.tpa(db, grp_by=["NAME"])
```

See the [Spatial Filtering Guide](../guides/spatial.md) for detailed usage.

## Class Reference

::: pyfia.FIA
    options:
      members:
        - __init__
        - load_table
        - find_evalid
        - clip_by_evalid
        - clip_by_state
        - clip_most_recent
        - clip_by_polygon
        - intersect_polygons
        - get_plots
        - get_trees
        - get_conditions
        - prepare_estimation_data
      show_root_heading: true
      show_source: true
