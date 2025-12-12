# Domain Filtering

PyFIA provides flexible domain filtering to define your analysis population.

## Land Type

The `land_type` parameter controls which land is included:

| Value | Description | Filter Applied |
|-------|-------------|----------------|
| `"forest"` | All forest land | `COND_STATUS_CD = 1` |
| `"timber"` | Timberland only | `COND_STATUS_CD = 1 AND SITECLCD IN (1,2,3,4,5,6)` |
| `"sampled"` | All sampled land | No filter |

```python
# Forest land only (default)
pyfia.volume(db, land_type="forest")

# Timberland (productive forest)
pyfia.volume(db, land_type="timber")
```

## Tree Type

The `tree_type` parameter filters trees:

| Value | Description | Filter Applied |
|-------|-------------|----------------|
| `"gs"` | Growing stock | `TREECLCD = 2` |
| `"al"` | All live trees | `STATUSCD = 1` |
| `"sl"` | Sawtimber | `DIA >= sawlog minimum` |

```python
# Growing stock volume
pyfia.volume(db, tree_type="gs")

# All live tree volume
pyfia.volume(db, tree_type="al")
```

## Custom Domain Filters

For specialized analyses, use domain filters with SQL syntax:

### Tree Domain

Filter trees by attributes:

```python
# Large trees only
pyfia.volume(db, tree_domain="DIA >= 12.0")

# Specific species
pyfia.volume(db, tree_domain="SPCD IN (110, 111, 121)")

# Combined conditions
pyfia.volume(db, tree_domain="DIA >= 5.0 AND SPCD = 316")
```

### Area Domain

Filter conditions/plots by attributes:

```python
# Specific forest types
pyfia.area(db, area_domain="FORTYPCD = 201")

# Low slope areas
pyfia.volume(db, area_domain="SLOPE < 30")

# Specific ownership
pyfia.area(db, area_domain="OWNGRPCD = 40")  # Private land
```

## Grouping Results

Use `grp_by` to stratify results:

```python
# By single attribute
pyfia.volume(db, grp_by="SPCD")

# By multiple attributes
pyfia.volume(db, grp_by=["STATECD", "FORTYPGRPCD"])
```

## Common Analysis Patterns

### Species-Specific Volume

```python
# Pine species volume on timberland
pine_volume = pyfia.volume(
    db,
    land_type="timber",
    tree_type="gs",
    tree_domain="SPGRPCD = 10"  # Softwood group
)
```

### Sawtimber Analysis

```python
# Sawtimber volume by species
sawtimber = pyfia.volume(
    db,
    vol_type="sawlog",
    land_type="timber",
    tree_type="sl",
    grp_by="SPCD"
)
```

### Private Land Analysis

```python
# Volume on private forest land
private_volume = pyfia.volume(
    db,
    land_type="forest",
    area_domain="OWNGRPCD = 40"
)
```
