# Growth Estimation

Estimate annual tree growth rates.

## Overview

The `growth()` function calculates annual growth estimates using GRM methodology.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Net growth volume
result = pyfia.growth(db, measure="volume")

# Growth by species
by_species = pyfia.growth(db, measure="volume", grp_by="SPCD")
```

## Function Reference

::: pyfia.growth
    options:
      show_root_heading: true
      show_source: true

## Measurement Types

| Measure | Description |
|---------|-------------|
| `"volume"` | Net cubic-foot volume growth |
| `"sawlog"` | Board-foot sawlog growth |
| `"biomass"` | Above-ground biomass growth |
| `"tpa"` | Trees per acre growth |

## Technical Notes

Growth estimation uses:

- `TREE_GRM_COMPONENT` table for growth components
- `TREE_GRM_MIDPT` table for annualized values
- `TREE_GRM_BEGIN` table for initial measurements
- `BEGINEND` table for temporal alignment

Net growth = Survivor growth + Ingrowth - Mortality

## Examples

### Total Net Growth

```python
result = pyfia.growth(
    db,
    measure="volume",
    land_type="forest"
)
print(f"Annual Growth: {result['estimate'][0]:,.0f} cu ft/year")
```

### Growth by Species

```python
result = pyfia.growth(
    db,
    measure="volume",
    grp_by="SPCD"
)
result = pyfia.join_species_names(result, db)
print(result.sort("estimate", descending=True).head(10))
```

### Growing Stock Growth on Timberland

```python
result = pyfia.growth(
    db,
    measure="volume",
    land_type="timber",
    tree_type="gs"
)
```

### Biomass Growth

```python
result = pyfia.growth(
    db,
    measure="biomass",
    land_type="forest"
)
print(f"Annual Biomass Growth: {result['estimate'][0]:,.0f} tons/year")
```

### Growth by Size Class

Group growth by diameter size classes:

```python
# Standard FIA size classes (1.0-4.9, 5.0-9.9, 10.0-19.9, etc.)
result = pyfia.growth(db, by_size_class=True)
print(result)

# Descriptive labels (Saplings, Small, Medium, Large)
result = pyfia.growth(db, by_size_class=True, size_class_type="descriptive")
print(result)

# Timber market classes (Pulpwood, Chip-n-Saw, Sawtimber)
result = pyfia.growth(db, by_size_class=True, size_class_type="market")
print(result)
```

See [Mortality - Size Class Types](mortality.md#size-class-types) for details on size class options.
