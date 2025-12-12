# Biomass Estimation

Estimate tree biomass (dry weight) and carbon content.

## Overview

The `biomass()` function calculates biomass estimates by component.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Above-ground biomass
result = pyfia.biomass(db, component="AG")

# Total biomass as carbon
carbon = pyfia.biomass(db, component="TOTAL", as_carbon=True)
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

When `as_carbon=True`, biomass is multiplied by 0.47 (standard carbon fraction).

## Examples

### Above-Ground Biomass by Species

```python
result = pyfia.biomass(db, component="AG", grp_by="SPCD")
result = pyfia.join_species_names(result, db)
print(result.sort("estimate", descending=True).head(10))
```

### Total Carbon Stock

```python
result = pyfia.biomass(
    db,
    component="TOTAL",
    as_carbon=True,
    land_type="forest"
)
print(f"Carbon: {result['estimate'][0]:,.0f} tons")
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
