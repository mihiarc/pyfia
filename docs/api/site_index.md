# Site Index Estimation

Estimate area-weighted mean site index as a measure of site productivity.

## Overview

The `site_index()` function calculates area-weighted mean site index from FIA condition data. Site index represents the expected height (in feet) of dominant trees at a specified base age, indicating the inherent productivity of a forest site.

```python
import pyfia

db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Basic site index estimation
result = pyfia.site_index(db)

# Site index by county
by_county = pyfia.site_index(db, grp_by="COUNTYCD")
```

## Function Reference

::: pyfia.site_index
    options:
      show_root_heading: true
      show_source: true

## Key Concepts

### Base Age (SIBASE)

Site index values are only comparable within the same base age. Results are **always grouped by SIBASE** to ensure comparability.

| Base Age | Common Usage |
|----------|--------------|
| 25 years | Southern pines, fast-growing species |
| 50 years | Northern hardwoods, slower-growing species |

### Site Index Species (SISP)

The `SISP` column indicates which species' height-age equation was used to calculate site index. Different species may have different site index scales even at the same base age.

## Technical Notes

Site index estimation uses:

- `COND` table for site index values (`SICOND`, `SIBASE`, `SISP`)
- **Condition-level** estimation (not tree-level like volume or TPA)
- **Area-weighted mean**: Each condition's site index is weighted by its proportion of plot area
- Conditions without site index (null `SICOND`) are excluded

### Calculation Method

The area-weighted mean is calculated as:

```
SI_mean = Σ(SICOND × CONDPROP_UNADJ × ADJ_FACTOR × EXPNS) / Σ(CONDPROP_UNADJ × ADJ_FACTOR × EXPNS)
```

This ratio-of-means estimator follows Bechtold & Patterson (2005) methodology with proper variance calculation.

## Examples

### Statewide Site Index

```python
result = pyfia.site_index(db)
print(f"Mean Site Index: {result['SI_MEAN'][0]:.1f} ft at base age {result['SIBASE'][0]}")
```

### Site Index by County

```python
result = pyfia.site_index(db, grp_by="COUNTYCD")
print(result.sort("SI_MEAN", descending=True).head(10))
```

### Site Index by Ownership

```python
result = pyfia.site_index(db, grp_by="OWNGRPCD")
# OWNGRPCD: 10=National Forest, 20=Other Federal, 30=State/Local, 40=Private
print(result)
```

### Site Index by Forest Type

```python
result = pyfia.site_index(db, grp_by="FORTYPCD")
result = pyfia.join_forest_type_names(result, db)
print(result.sort("SI_MEAN", descending=True).head(10))
```

### Site Index by Site Index Species

Group by the species equation used to calculate site index:

```python
result = pyfia.site_index(db, grp_by="SISP")
result = pyfia.join_species_names(result, db, spcd_column="SISP")
print(result)
```

### Private Timberland Only

```python
result = pyfia.site_index(
    db,
    land_type="timber",
    area_domain="OWNGRPCD == 40"
)
print(result)
```

### Productive Sites Only

Filter to high productivity sites (site class 1-3):

```python
result = pyfia.site_index(
    db,
    area_domain="SITECLCD IN (1, 2, 3)"
)
```

### Multiple Grouping Variables

```python
result = pyfia.site_index(
    db,
    grp_by=["OWNGRPCD", "FORTYPCD"]
)
print(result)
```

## Interpreting Results

| Column | Description |
|--------|-------------|
| `YEAR` | Inventory year |
| `SIBASE` | Base age in years (always included) |
| `SI_MEAN` | Area-weighted mean site index (feet) |
| `SI_SE` | Standard error of the mean |
| `SI_VARIANCE` | Variance of the estimate |
| `N_PLOTS` | Number of plots contributing to estimate |
| `N_CONDITIONS` | Number of conditions with site index values |

## Comparison with Other Estimators

Unlike tree-based estimators (`volume()`, `tpa()`, `mortality()`), `site_index()`:

- Uses **condition-level** data, not tree-level
- Has no `measure` or `tree_type` parameters
- Always groups by `SIBASE` (base age)
- Returns a mean value, not a total
