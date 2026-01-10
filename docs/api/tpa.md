# Trees Per Acre

Estimate tree density (TPA) and basal area (BAA).

## Overview

The `tpa()` function calculates trees per acre and basal area estimates.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Total TPA
result = pyfia.tpa(db, land_type="forest")

# TPA by size class
by_size = pyfia.tpa(db, by_size_class=True)
```

## Function Reference

::: pyfia.tpa
    options:
      show_root_heading: true
      show_source: true

## Output Columns

The `tpa()` function returns these columns:

| Column | Description |
|--------|-------------|
| `TPA` | Trees per acre estimate |
| `BAA` | Basal area (sq ft/acre) estimate |
| `TPA_SE` | TPA standard error |
| `BAA_SE` | BAA standard error |
| `TPA_TOTAL` | Total trees (if `totals=True`) |
| `BAA_TOTAL` | Total basal area (if `totals=True`) |

## Formulas

**Trees Per Acre:**
$$TPA = TPA\_UNADJ \times ADJ\_FACTOR$$

**Basal Area Per Acre:**
$$BAA = \pi \times (DIA/24)^2 \times TPA\_UNADJ \times ADJ\_FACTOR$$

## Examples

### Total TPA on Forest Land

```python
result = pyfia.tpa(db, land_type="forest")
print(f"TPA: {result['TPA'][0]:.1f} trees/acre")
print(f"BAA: {result['BAA'][0]:.1f} sq ft/acre")
```

### TPA by 2-Inch Diameter Classes

```python
result = pyfia.tpa(db, by_size_class=True)
print(result)
```

### TPA by Species

```python
result = pyfia.tpa(db, grp_by="SPCD")
result = pyfia.join_species_names(result, db)
print(result.sort("TPA", descending=True).head(10))
```

### Large Trees Only

```python
result = pyfia.tpa(
    db,
    tree_domain="DIA >= 12.0",
    land_type="timber"
)
```
