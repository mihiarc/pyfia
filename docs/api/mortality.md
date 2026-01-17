# Mortality Estimation

Estimate annual tree mortality rates and volumes.

## Overview

The `mortality()` function calculates annual mortality estimates using GRM (Growth, Removals, Mortality) methodology.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Mortality volume
result = pyfia.mortality(db, measure="volume")

# Mortality by cause
by_agent = pyfia.mortality(db, measure="volume", grp_by="AGENTCD")
```

## Function Reference

::: pyfia.mortality
    options:
      show_root_heading: true
      show_source: true

## Measurement Types

| Measure | Description |
|---------|-------------|
| `"volume"` | Net cubic-foot volume mortality |
| `"sawlog"` | Board-foot sawlog mortality |
| `"biomass"` | Above-ground biomass mortality |
| `"tpa"` | Trees per acre mortality |
| `"count"` | Tree count mortality |
| `"basal_area"` | Basal area mortality |

## Technical Notes

Mortality estimation uses:

- `TREE_GRM_COMPONENT` table for mortality attributes
- `TREE_GRM_MIDPT` table for annualized values
- Trees with `TPAMORT_UNADJ > 0` are mortality trees
- Annual rates calculated using measurement interval

## Examples

### Total Mortality Volume

```python
result = pyfia.mortality(
    db,
    measure="volume",
    land_type="forest"
)
print(f"Annual Mortality: {result['estimate'][0]:,.0f} cu ft/year")
```

### Mortality by Agent (Tree-Level)

```python
result = pyfia.mortality(
    db,
    measure="volume",
    grp_by="AGENTCD"
)
# AGENTCD: 10=Insect, 20=Disease, 30=Fire, 50=Weather, etc.
print(result)
```

### Mortality by Disturbance (Condition-Level)

```python
result = pyfia.mortality(
    db,
    measure="volume",
    grp_by="DSTRBCD1"
)
# DSTRBCD1: 30=Fire, 52=Hurricane/wind, 54=Drought, etc.
print(result)
```

### Mortality by Species

```python
result = pyfia.mortality(
    db,
    measure="volume",
    grp_by="SPCD"
)
result = pyfia.join_species_names(result, db)
print(result.sort("estimate", descending=True).head(10))
```

### Growing Stock Mortality on Timberland

```python
result = pyfia.mortality(
    db,
    measure="volume",
    land_type="timber",
    tree_type="gs"
)
```

### Biomass Mortality

```python
result = pyfia.mortality(
    db,
    measure="biomass",
    land_type="forest"
)
print(f"Annual Biomass Mortality: {result['estimate'][0]:,.0f} tons/year")
```

### Mortality by Size Class

Group mortality by diameter size classes:

```python
# Standard FIA size classes (1.0-4.9, 5.0-9.9, 10.0-19.9, etc.)
result = pyfia.mortality(db, by_size_class=True)
print(result)

# Descriptive labels (Saplings, Small, Medium, Large)
result = pyfia.mortality(db, by_size_class=True, size_class_type="descriptive")
print(result)

# Timber market classes (Pulpwood, Chip-n-Saw, Sawtimber)
# Based on TimberMart-South categories
result = pyfia.mortality(db, by_size_class=True, size_class_type="market")
print(result)
```

### Size Class Types

| Type | Description | Categories |
|------|-------------|------------|
| `"standard"` | FIA numeric ranges | 1.0-4.9, 5.0-9.9, 10.0-19.9, 20.0-29.9, 30.0+ |
| `"descriptive"` | Text labels | Saplings, Small, Medium, Large |
| `"market"` | Timber market categories | Pulpwood, Chip-n-Saw (pine only), Sawtimber |

!!! note "Market Size Classes"
    Market size classes use species-aware thresholds based on TimberMart-South:

    - **Pine/Softwood (SPCD < 300)**: Pulpwood (5-8.9"), Chip-n-Saw (9-11.9"), Sawtimber (12"+)
    - **Hardwood (SPCD >= 300)**: Pulpwood (5-10.9"), Sawtimber (11"+)
