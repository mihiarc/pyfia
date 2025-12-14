# Reference Table Utilities

Functions to join descriptive names to estimation results.

## Auto-Enhancement

PyFIA **automatically adds descriptive names** for common grouping columns:

| Grouping Column | Auto-Added Column |
|-----------------|-------------------|
| `FORTYPCD` | `FOREST_TYPE_GROUP` |
| `OWNGRPCD` | `OWNERSHIP_GROUP` |

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Forest type names added automatically!
result = pyfia.area(db, grp_by="FORTYPCD")
print(result.columns)
# ['YEAR', 'FORTYPCD', 'FOREST_TYPE_GROUP', 'AREA', ...]

# Ownership names added automatically!
result = pyfia.volume(db, grp_by="OWNGRPCD")
print(result.columns)
# ['YEAR', 'OWNGRPCD', 'OWNERSHIP_GROUP', 'VOLCFNET_TOTAL', ...]
```

## Manual Reference Table Joins

For columns that aren't auto-enhanced (like species), use these functions:

```python
# Get volume by species
result = pyfia.volume(db, grp_by="SPCD")

# Add species names manually
result = pyfia.join_species_names(result, db)
print(result)
```

## Functions

### join_species_names

Add common and scientific species names.

::: pyfia.join_species_names
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
# Basic usage
result = pyfia.join_species_names(volume_data, db)

# Include scientific name
result = pyfia.join_species_names(
    volume_data,
    db,
    include_scientific=True
)
```

### join_forest_type_names

Add forest type group names.

::: pyfia.join_forest_type_names
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
area_by_type = pyfia.area(db, grp_by="FORTYPGRPCD")
result = pyfia.join_forest_type_names(area_by_type, db)
print(result)
```

### join_state_names

Add state names and abbreviations.

::: pyfia.join_state_names
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
volume_by_state = pyfia.volume(db, grp_by="STATECD")
result = pyfia.join_state_names(volume_by_state, db)
print(result)
```

### join_multiple_references

Join multiple reference tables at once.

::: pyfia.join_multiple_references
    options:
      show_root_heading: true
      show_source: true

**Example:**

```python
# Add both species and forest type names
result = pyfia.join_multiple_references(
    data,
    db,
    tables=["species", "forest_type"]
)
```

## Available Reference Tables

| Code Column | Reference Table | Name Column |
|-------------|-----------------|-------------|
| `SPCD` | `REF_SPECIES` | Common name, Scientific name |
| `FORTYPCD` | `REF_FOREST_TYPE` | Forest type name |
| `FORTYPGRPCD` | `REF_FOREST_TYPE_GROUP` | Forest type group name |
| `STATECD` | `REF_STATE` | State name, abbreviation |
| `OWNGRPCD` | `REF_OWNGRPCD` | Ownership group name |
