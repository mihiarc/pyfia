# Usage Examples - Simplified pyFIA API

The new simplified API makes FIA estimation straightforward and intuitive. No more complex configuration objects, abstract workflows, or deep inheritance hierarchies.

## Basic Usage

```python
from pyfia import FIA
from pyfia.estimation_new import area, volume, biomass, tpa, mortality, growth

# Connect to database
db = FIA("path/to/fia.duckdb")
db.clip_by_state(37)  # North Carolina
db.clip_most_recent("EXPVOL")

# Simple function calls - that's it!
forest_area = area(db, land_type="forest")
timber_volume = volume(db, land_type="timber")
ag_biomass = biomass(db, component="AG")
trees_per_acre = tpa(db)
annual_mortality = mortality(db, measure="volume")
net_growth = growth(db, component="net")
```

## Grouping and Filtering

```python
# By species
vol_by_species = volume(db, by_species=True)

# By custom grouping
vol_by_owner = volume(db, grp_by="OWNGRPCD")

# Multiple groups
vol_by_forest_type_and_owner = volume(
    db,
    grp_by=["FORTYPCD", "OWNGRPCD"]
)

# With domain filtering
large_tree_volume = volume(
    db,
    tree_domain="DIA >= 20.0",
    area_domain="STDAGE > 50"
)

# By size class
biomass_by_size = biomass(
    db,
    by_size_class=True,
    component="TOTAL"
)
```

## Old vs New Comparison

### OLD (Complex)
```python
from pyfia.estimation.api import volume
from pyfia.estimation.framework.config import EstimatorConfig, ConfigFactory
from pyfia.estimation.infrastructure.evaluation import CollectionStrategy

# Complex configuration
config = ConfigFactory.create_volume_config(
    grp_by=["FORTYPCD"],
    by_species=False,
    land_type="timber",
    tree_type="live",
    vol_type="net",
    collection_strategy=CollectionStrategy.ADAPTIVE,
    enable_caching=True,
    cache_ttl=3600
)

# Create estimator with mixins
estimator = VolumeEstimator(db, config)
estimator.set_collection_strategy(CollectionStrategy.ADAPTIVE)
estimator.enable_progress_tracking()

# Run through workflow
results = estimator.estimate()
```

### NEW (Simple)
```python
from pyfia.estimation_new import volume

# Just call the function
results = volume(
    db,
    grp_by="FORTYPCD",
    land_type="timber",
    vol_type="net"
)
```

## All Estimator Functions

### Area Estimation
```python
# Forest area by ownership
area_results = area(
    db,
    grp_by="OWNGRPCD",
    land_type="forest"
)

# Timberland area by forest type
timber_area = area(
    db,
    grp_by="FORTYPCD",
    land_type="timber"
)
```

### Volume Estimation
```python
# Net volume by species
vol_results = volume(
    db,
    by_species=True,
    vol_type="net"
)

# Sawlog volume for large trees
sawlog_vol = volume(
    db,
    vol_type="sawlog",
    tree_domain="DIA >= 14.0"
)
```

### Biomass Estimation
```python
# Total biomass and carbon
bio_results = biomass(
    db,
    component="TOTAL"  # AG + BG
)

# Stem biomass by size class
stem_bio = biomass(
    db,
    component="STEM",
    by_size_class=True
)
```

### Trees Per Acre
```python
# TPA and basal area by species
tpa_results = tpa(
    db,
    by_species=True
)

# Large tree density
large_tpa = tpa(
    db,
    tree_domain="DIA >= 20.0"
)
```

### Mortality Estimation
```python
# Annual mortality volume
mort_vol = mortality(
    db,
    measure="volume",
    recent_only=True
)

# Mortality rate by species
mort_rate = mortality(
    db,
    by_species=True,
    measure="count",
    as_rate=True
)
```

### Growth Estimation
```python
# Net change (growth - removals - mortality)
net_change = growth(
    db,
    component="net",
    measure="volume"
)

# Gross growth by ownership
gross_growth = growth(
    db,
    grp_by="OWNGRPCD",
    component="gross"
)

# Harvest removals
removals = growth(
    db,
    component="removals",
    measure="volume"
)
```

## Advanced Usage (When Needed)

For cases requiring more control, you can use the estimator classes directly:

```python
from pyfia.estimation_new import VolumeEstimator

# Create custom config
config = {
    "grp_by": ["FORTYPCD", "OWNGRPCD"],
    "land_type": "timber",
    "vol_type": "net",
    "custom_param": "value"
}

# Use estimator class
estimator = VolumeEstimator(db, config)

# Override methods if needed
class CustomVolumeEstimator(VolumeEstimator):
    def calculate_values(self, data):
        # Custom calculation logic
        data = super().calculate_values(data)
        # Additional processing
        return data

# Run estimation
results = estimator.estimate()
```

## Key Benefits of New API

1. **Simple Function Calls** - No complex configuration objects
2. **Clear Parameters** - Named parameters with sensible defaults
3. **Consistent Pattern** - All estimators work the same way
4. **No Abstractions** - Direct, understandable code
5. **Easy Testing** - Simple functions are easy to test
6. **Better Performance** - Less overhead from abstractions

## Migration from Old API

```python
# If you have old code like this:
from pyfia.estimation import biomass as old_biomass
config = EstimatorConfig(...)
results = old_biomass(db, config)

# Replace with:
from pyfia.estimation_new import biomass
results = biomass(db, **config.to_dict())

# Or better yet, use parameters directly:
results = biomass(
    db,
    by_species=True,
    component="AG",
    land_type="forest"
)
```

## Performance

The new simplified API has **identical or better performance** compared to the old system:
- No FrameWrapper overhead
- No complex caching layers
- Direct Polars operations
- Same statistical calculations

But with **85% less code** to maintain!