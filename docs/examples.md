# Examples

Real-world examples demonstrating PyFIA's capabilities.

## Basic Volume Analysis

Calculate total net volume for a state:

```python
import pyfia

# Connect and filter
db = pyfia.FIA("georgia.duckdb")
db.clip_by_state("GA")

# Total volume on forest land
total_volume = pyfia.volume(db, land_type="forest")
print(f"Total Volume: {total_volume['estimate'][0]:,.0f} cubic feet")
print(f"Standard Error: {total_volume['se'][0]:,.0f}")
```

## Volume by Species

```python
# Volume grouped by species
volume_by_species = pyfia.volume(db, grp_by="SPCD", land_type="forest")

# Add species names
result = pyfia.join_species_names(volume_by_species, db)

# Sort by volume descending
result = result.sort("estimate", descending=True)
print(result.head(10))
```

## Forest Area by Forest Type

```python
# Area by forest type - names added automatically!
result = pyfia.area(
    db,
    land_type="forest",
    grp_by="FORTYPCD"
)

# Result includes both FORTYPCD and FOREST_TYPE_GROUP columns
print(result)
# FORTYPCD | FOREST_TYPE_GROUP       | AREA        | ...
# 161      | Loblolly/Shortleaf Pine | 15,913,000  | ...
# 503      | Oak/Hickory             | 8,592,600   | ...
```

## Trees Per Acre Analysis

```python
# TPA and basal area by diameter class
tpa_by_size = pyfia.tpa(
    db,
    land_type="forest",
    by_size_class=True
)
print(tpa_by_size)
```

## Mortality Analysis

```python
# Annual mortality volume on timberland
mortality = pyfia.mortality(
    db,
    measure="volume",
    land_type="timber",
    tree_type="gs"  # Growing stock
)
print(mortality)

# Mortality by cause (if available)
mortality_by_agent = pyfia.mortality(
    db,
    measure="volume",
    grp_by="AGENTCD"
)
print(mortality_by_agent)
```

## Growth and Removals

```python
# Annual growth
growth = pyfia.growth(
    db,
    measure="volume",
    land_type="forest"
)

# Annual removals
removals = pyfia.removals(
    db,
    measure="volume",
    land_type="forest"
)

print(f"Growth: {growth['estimate'][0]:,.0f} cu ft/year")
print(f"Removals: {removals['estimate'][0]:,.0f} cu ft/year")
```

## Harvest Panel Analysis

Create remeasurement panels for harvest probability modeling:

```python
import polars as pl

# Condition-level panel for harvest rates
cond_panel = pyfia.panel(db, level="condition", land_type="timber")

# Harvest rate analysis
harvest_rate = cond_panel["HARVEST"].mean()
avg_remper = cond_panel["REMPER"].mean()
annual_rate = 1 - (1 - harvest_rate) ** (1 / avg_remper)

print(f"Period harvest rate: {harvest_rate:.1%}")
print(f"Annualized rate: {annual_rate:.2%}/year")

# Harvest by ownership
harvest_by_owner = (
    cond_panel
    .group_by("t2_OWNGRPCD")
    .agg([pl.len().alias("n"), pl.col("HARVEST").mean().alias("rate")])
)
print(harvest_by_owner)
```

Tree-level panel for individual tree fate tracking:

```python
# Tree panel with automatic cut inference
tree_panel = pyfia.panel(db, level="tree", tree_type="all")

# Tree fate distribution
fate_dist = tree_panel.group_by("TREE_FATE").len()
print(fate_dist)
# TREE_FATE | len
# survivor  | 83,498
# mortality | 12,823
# cut       | 11,445
# ingrowth  | 23,111

# Get cut trees only
cut_trees = pyfia.panel(db, level="tree", harvest_only=True)
print(f"Cut trees: {len(cut_trees):,}")
```

## Biomass and Carbon

```python
# Above-ground biomass
biomass = pyfia.biomass(
    db,
    component="AG",
    land_type="forest"
)

# Convert to carbon (built-in)
carbon = pyfia.biomass(
    db,
    component="AG",
    land_type="forest",
    as_carbon=True
)

print(f"Biomass: {biomass['estimate'][0]:,.0f} tons")
print(f"Carbon: {carbon['estimate'][0]:,.0f} tons")
```

## Validation Against EVALIDator

```python
from pyfia import EVALIDatorClient, validate_pyfia_estimate

# Get official estimate
client = EVALIDatorClient()
official = client.get_volume("GA", 2022, volume_type="net_growingstock")

# Compare with PyFIA
pyfia_result = pyfia.volume(db, vol_type="net", tree_type="gs")

# Validate
validation = validate_pyfia_estimate(pyfia_result, official)
print(f"Difference: {validation.percent_difference:.2f}%")
print(f"Within CI: {validation.within_confidence_interval}")
```

## Working with Multiple States

```python
# Merge multiple state databases
pyfia.merge_state_databases(
    ["GA_FIA.db", "SC_FIA.db", "NC_FIA.db"],
    "southeast.duckdb"
)

# Analyze merged database
db = pyfia.FIA("southeast.duckdb")
volume_by_state = pyfia.volume(db, grp_by="STATECD")
result = pyfia.join_state_names(volume_by_state, db)
print(result)
```

## Custom Domain Filtering

```python
# Filter to specific conditions
volume = pyfia.volume(
    db,
    land_type="forest",
    tree_domain="DIA >= 5.0 AND SPCD IN (110, 111, 121)",  # Pine species
    area_domain="SLOPE < 30"  # Gentle slopes only
)
print(volume)
```

## Memory-Efficient Processing

```python
# Use lazy evaluation for large datasets
db = pyfia.FIA("large_database.duckdb")

# Tables are loaded lazily
plots = db.get_plots()  # Returns LazyFrame, not materialized
trees = db.get_trees()  # Still lazy

# Only materialized when needed
result = pyfia.volume(db, grp_by="SPCD")  # Efficient execution
```

## Example Scripts

Additional example scripts are available in the repository:

- [`examples/california_volume.py`](https://github.com/mihiarc/pyfia/tree/main/examples) - Volume by diameter class
- [`examples/minnesota_area.py`](https://github.com/mihiarc/pyfia/tree/main/examples) - Forest area analysis
- [`examples/georgia_mortality.py`](https://github.com/mihiarc/pyfia/tree/main/examples) - Mortality rate calculation
- [`examples/harvest_panel_analysis.py`](https://github.com/mihiarc/pyfia/tree/main/examples) - Harvest panel and tree fate analysis

Clone the repository to run these examples:

```bash
git clone https://github.com/mihiarc/pyfia.git
cd pyfia/examples
python california_volume.py
```
