# Biomass Estimation

Estimate tree biomass (dry weight) and carbon content.

## Overview

The `biomass()` function calculates biomass estimates by component. Both biomass and carbon
(biomass × 0.47) are always returned.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Above-ground biomass (returns BIO_ACRE, CARB_ACRE, etc.)
result = pyfia.biomass(db, component="AG")

# Total biomass
total = pyfia.biomass(db, component="TOTAL")
```

## Function Reference

::: pyfia.biomass
    options:
      show_root_heading: true
      show_source: true

## Biomass Components

| Component | Description | FIA Column |
|-----------|-------------|------------|
| `"AG"` | Above-ground (default) | `DRYBIO_AG` |
| `"BG"` | Below-ground | `DRYBIO_BG` |
| `"TOTAL"` | Total biomass | `DRYBIO_AG + DRYBIO_BG` |
| `"STEM"` | Stem wood | `DRYBIO_STEM` |
| `"STUMP"` | Stump | `DRYBIO_STUMP` |
| `"TOP"` | Top and branches | `DRYBIO_TOP` |
| `"FOLIAGE"` | Foliage | `DRYBIO_FOLIAGE` |

## Carbon Conversion

Carbon is always calculated as biomass × 0.47 (standard carbon fraction) and returned
in the `CARB_ACRE` and `CARB_TOTAL` columns alongside the biomass columns.

## Examples

### Above-Ground Biomass by Species

```python
result = pyfia.biomass(db, component="AG", grp_by="SPCD")
result = pyfia.join_species_names(result, db)
print(result.sort("BIO_ACRE", descending=True).head(10))
```

### Total Carbon Stock

```python
result = pyfia.biomass(
    db,
    component="TOTAL",
    land_type="forest",
    totals=True
)
print(f"Carbon: {result['CARB_TOTAL'][0]:,.0f} tons")
```

### Biomass on Timberland

```python
result = pyfia.biomass(
    db,
    component="AG",
    land_type="timber",
    tree_type="gs"
)
```

### Biomass by Forest Type

```python
result = pyfia.biomass(
    db,
    component="AG",
    grp_by="FORTYPGRPCD"
)
result = pyfia.join_forest_type_names(result, db)
print(result)
```
