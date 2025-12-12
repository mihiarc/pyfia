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

### Mortality by Agent

```python
result = pyfia.mortality(
    db,
    measure="volume",
    grp_by="AGENTCD"
)
# AGENTCD: 10=Insect, 20=Disease, 30=Fire, etc.
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
