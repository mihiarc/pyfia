# Area Estimation

Estimate forest area by land type and various categories.

## Overview

The `area()` function calculates forest area estimates with proper variance estimation.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Total forest area
total = pyfia.area(db, land_type="forest")

# Area by forest type
by_type = pyfia.area(db, land_type="forest", grp_by="FORTYPGRPCD")
```

## Function Reference

::: pyfia.area
    options:
      show_root_heading: true
      show_source: true

## Examples

### Total Forest Area

```python
result = pyfia.area(db, land_type="forest")
print(f"Forest Area: {result['estimate'][0]:,.0f} acres")
print(f"SE: {result['se'][0]:,.0f} acres")
```

### Timberland Area

```python
result = pyfia.area(db, land_type="timber")
print(f"Timberland: {result['estimate'][0]:,.0f} acres")
```

### Area by Ownership

```python
result = pyfia.area(db, land_type="forest", grp_by="OWNGRPCD")
print(result)
```

### Area by Forest Type Group

```python
result = pyfia.area(db, land_type="forest", grp_by="FORTYPGRPCD")
result = pyfia.join_forest_type_names(result, db)
print(result.sort("estimate", descending=True))
```
