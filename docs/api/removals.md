# Removals Estimation

Estimate average annual timber removals.

## Overview

The `removals()` function calculates annual removals estimates (harvested timber).

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Total removals
result = pyfia.removals(db, measure="volume")

# Removals by species
by_species = pyfia.removals(db, measure="volume", grp_by="SPCD")
```

## Function Reference

::: pyfia.removals
    options:
      show_root_heading: true
      show_source: true

## Measurement Types

| Measure | Description |
|---------|-------------|
| `"volume"` | Net cubic-foot volume removals |
| `"biomass"` | Above-ground biomass removals |

## Technical Notes

Removals estimation uses:

- `TREE_GRM_COMPONENT` table for removal attributes
- `TREE_GRM_MIDPT` table for annualized values
- Trees with `TPAREMV_UNADJ > 0` are removal trees
- Calculated as: `TPAREMV_UNADJ × VOLCFNET × ADJ × EXPNS`

!!! note
    PyFIA calculates removals from raw components rather than using pre-calculated `REMVCFGS` columns for consistency with EVALIDator methodology.

## Examples

### Total Removals Volume

```python
result = pyfia.removals(
    db,
    measure="volume",
    land_type="forest"
)
print(f"Annual Removals: {result['estimate'][0]:,.0f} cu ft/year")
```

### Removals by Species

```python
result = pyfia.removals(
    db,
    measure="volume",
    grp_by="SPCD"
)
result = pyfia.join_species_names(result, db)
print(result.sort("estimate", descending=True).head(10))
```

### Growing Stock Removals

```python
result = pyfia.removals(
    db,
    measure="volume",
    land_type="timber",
    tree_type="gs"
)
```

### Biomass Removals

```python
result = pyfia.removals(
    db,
    measure="biomass",
    land_type="forest"
)
print(f"Annual Biomass Removals: {result['estimate'][0]:,.0f} tons/year")
```

### Growth-Drain Analysis

```python
# Compare growth to removals
growth = pyfia.growth(db, measure="volume")
removals = pyfia.removals(db, measure="volume")
mortality = pyfia.mortality(db, measure="volume")

print(f"Growth: {growth['estimate'][0]:,.0f} cu ft/year")
print(f"Removals: {removals['estimate'][0]:,.0f} cu ft/year")
print(f"Mortality: {mortality['estimate'][0]:,.0f} cu ft/year")

net_change = growth['estimate'][0] - removals['estimate'][0] - mortality['estimate'][0]
print(f"Net Change: {net_change:,.0f} cu ft/year")
```
