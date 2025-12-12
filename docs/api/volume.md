# Volume Estimation

Estimate standing tree volume with various volume types and filters.

## Overview

The `volume()` function calculates tree volume estimates following EVALIDator methodology.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Total net volume
total = pyfia.volume(db, land_type="forest")

# Volume by species
by_species = pyfia.volume(db, grp_by="SPCD")
```

## Function Reference

::: pyfia.volume
    options:
      show_root_heading: true
      show_source: true

## Volume Types

| Type | Column | Description |
|------|--------|-------------|
| `"net"` | `VOLCFNET` | Net cubic-foot volume |
| `"gross"` | `VOLCFGRS` | Gross cubic-foot volume |
| `"sawlog"` | `VOLBFNET` | Board-foot sawlog volume |
| `"sound"` | `VOLCSNET` | Sound wood volume |

## Examples

### Net Volume by Species

```python
result = pyfia.volume(db, vol_type="net", grp_by="SPCD")
result = pyfia.join_species_names(result, db)
print(result.sort("estimate", descending=True).head(10))
```

### Sawlog Volume on Timberland

```python
result = pyfia.volume(
    db,
    vol_type="sawlog",
    land_type="timber",
    tree_type="sl"  # Sawtimber only
)
print(f"Sawlog Volume: {result['estimate'][0]:,.0f} board feet")
```

### Volume by Diameter Class

```python
# Create diameter classes
result = pyfia.volume(
    db,
    grp_by="DIA_CLASS",
    tree_domain="DIA >= 5.0"
)
```

### Pine Volume Only

```python
result = pyfia.volume(
    db,
    land_type="forest",
    tree_domain="SPGRPCD = 10"  # Softwood group
)
```
