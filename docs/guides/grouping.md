# Grouping Results

PyFIA supports grouping estimation results by various FIA classification columns. When you group by common columns like forest type or ownership, PyFIA automatically adds descriptive name columns to make results more readable.

## Basic Grouping

Use the `grp_by` parameter to group results:

```python
from pyfia import FIA, volume, area

db = FIA("georgia.duckdb")
db.clip_by_state("GA")

# Group volume by forest type
result = volume(db, grp_by="FORTYPCD")

# Group area by ownership
result = area(db, grp_by="OWNGRPCD")

# Group by multiple columns
result = volume(db, grp_by=["FORTYPCD", "OWNGRPCD"])
```

## Auto-Enhanced Columns

PyFIA automatically adds descriptive name columns for common grouping variables:

| Grouping Column | Auto-Added Column | Example Values |
|-----------------|-------------------|----------------|
| `FORTYPCD` | `FOREST_TYPE_GROUP` | "Loblolly/Shortleaf Pine", "Oak/Hickory" |
| `OWNGRPCD` | `OWNERSHIP_GROUP` | "Private", "Forest Service", "State and Local Government" |

### Example Output

```python
result = volume(db, grp_by="FORTYPCD", totals=True)
print(result.columns)
# ['YEAR', 'FORTYPCD', 'FOREST_TYPE_GROUP', 'VOLCFNET_TOTAL', 'VOLCFNET_ACRE', ...]
```

The output includes both the original code (`FORTYPCD`) and the descriptive name (`FOREST_TYPE_GROUP`):

| FORTYPCD | FOREST_TYPE_GROUP | VOLCFNET_TOTAL |
|----------|-------------------|----------------|
| 161 | Loblolly/Shortleaf Pine | 15,913,000,000 |
| 503 | Oak/Hickory | 8,592,600,000 |
| 701 | Oak/Gum/Cypress | 2,145,000,000 |

## Ownership Groups

```python
result = area(db, grp_by="OWNGRPCD", totals=True)
```

| OWNGRPCD | OWNERSHIP_GROUP | AREA |
|----------|-----------------|------|
| 10 | Forest Service | 858,952 |
| 20 | Other Federal | 1,014,500 |
| 30 | State and Local Government | 909,473 |
| 40 | Private | 21,390,000 |

## Species Names

Species names require database access and are **not** auto-enhanced. Use `join_species_names()` after estimation:

```python
from pyfia import volume, join_species_names

# Group by species
result = volume(db, by_species=True)

# Add species names from reference table
result = join_species_names(result, db)
print(result.head())
```

| SPCD | COMMON_NAME | VOLCFNET_TOTAL |
|------|-------------|----------------|
| 131 | Loblolly pine | 8,234,000,000 |
| 316 | Red maple | 1,456,000,000 |
| 802 | White oak | 987,000,000 |

## Reference Table Functions

For columns that aren't auto-enhanced, use the reference table functions:

```python
from pyfia import (
    join_species_names,
    join_forest_type_names,
    join_state_names,
    join_multiple_references,
)

# Add species names
result = join_species_names(result, db)

# Add forest type names (if FORTYPCD present but not auto-enhanced)
result = join_forest_type_names(result, db)

# Add state names
result = join_state_names(result, db)

# Add multiple references at once
result = join_multiple_references(
    result, db,
    species=True,
    forest_type=True,
    state=True
)
```

## Convenience Flags

Some estimators support convenience flags for common groupings:

```python
# Group by species
result = volume(db, by_species=True)

# Group by size class
result = tpa(db, by_size_class=True)
```

## Combining Groupings

You can combine multiple grouping columns:

```python
# Volume by forest type and ownership
result = volume(db, grp_by=["FORTYPCD", "OWNGRPCD"])

# Both auto-enhanced columns are added
print(result.columns)
# [..., 'FORTYPCD', 'FOREST_TYPE_GROUP', 'OWNGRPCD', 'OWNERSHIP_GROUP', ...]
```

## Mortality-Specific Groupings

For mortality estimation, you can group by cause of death:

```python
from pyfia import mortality

# Group by mortality agent (tree-level cause of death)
result = mortality(db, grp_by="AGENTCD")
# AGENTCD: 10=Insect, 20=Disease, 30=Fire, 50=Weather, etc.

# Group by disturbance code (condition-level)
result = mortality(db, grp_by="DSTRBCD1")
# DSTRBCD1: 30=Fire, 52=Hurricane/wind, 54=Drought, etc.

# Combined grouping for detailed analysis
result = mortality(db, grp_by=["AGENTCD", "SPCD"])
```

This is useful for timber casualty loss analysis where losses must be classified by cause for tax purposes.

## Summary

| Column | Auto-Enhanced? | Manual Function |
|--------|----------------|-----------------|
| `FORTYPCD` | Yes | `join_forest_type_names()` |
| `OWNGRPCD` | Yes | N/A |
| `SPCD` | No | `join_species_names()` |
| `STATECD` | No | `join_state_names()` |
| `AGENTCD` | No | N/A (mortality only) |
| `DSTRBCD1` | No | N/A (mortality only) |
