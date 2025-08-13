# pyFIA Configuration Guide

This guide explains the configuration system in pyFIA, including the new Pydantic v2-based configuration classes that provide enhanced validation and type safety.

## Overview

pyFIA uses configuration classes to manage estimation parameters, ensuring consistency and preventing errors through validation. The system supports both legacy dataclass-based configs and new Pydantic-based configs.

## Base Configuration

### EstimatorConfig (Legacy)

The original dataclass-based configuration used throughout pyFIA:

```python
from pyfia.estimation import EstimatorConfig

config = EstimatorConfig(
    grp_by=["STATECD", "UNITCD"],
    by_species=True,
    by_size_class=False,
    land_type="forest",
    tree_type="live",
    tree_domain="DIA >= 10.0",
    area_domain=None,
    method="TI",
    lambda_=0.5,
    totals=True,
    variance=False
)
```

### EstimatorConfigV2 (New)

The enhanced Pydantic-based configuration with validation:

```python
from pyfia.estimation import EstimatorConfigV2

config = EstimatorConfigV2(
    grp_by=["STATECD", "UNITCD"],
    by_species=True,
    land_type="forest",  # Validated: must be "forest", "timber", or "all"
    tree_type="live",    # Validated: must be "live", "dead", "gs", or "all"
    method="TI",         # Validated: must be valid temporal method
    lambda_=0.5          # Validated: must be between 0 and 1
)
```

## Mortality Configuration

The `MortalityConfig` class extends the base configuration with mortality-specific parameters and validation rules.

### Basic Usage

```python
from pyfia.estimation import MortalityConfig, MortalityCalculator
from pyfia import FIA

# Create configuration
config = MortalityConfig(
    mortality_type="tpa",      # "tpa", "volume", or "both"
    by_species=True,
    group_by_ownership=True,
    variance=True,
    totals=True
)

# Use with calculator
with FIA("path/to/fia.duckdb") as db:
    db.clip_by_state(37)
    calculator = MortalityCalculator(db, config)
    results = calculator.estimate()
```

### Configuration Parameters

#### Mortality-Specific Parameters

- `mortality_type`: Type of mortality to calculate
  - `"tpa"`: Trees per acre (default)
  - `"volume"`: Volume per acre
  - `"both"`: Both TPA and volume

- `tree_class`: Tree classification for mortality
  - `"all"`: All trees (default)
  - `"timber"`: Timber trees only
  - `"growing_stock"`: Growing stock trees

- `group_by_species_group`: Group by species group (SPGRPCD)
- `group_by_ownership`: Group by ownership group (OWNGRPCD)
- `group_by_agent`: Group by mortality agent (AGENTCD)
- `group_by_disturbance`: Group by disturbance codes (DSTRBCD1-3)

- `include_components`: Include basal area components
- `include_natural`: Include natural mortality (default: True)
- `include_harvest`: Include harvest mortality (default: True)

- `variance_method`: Variance calculation method
  - `"standard"`: Standard variance
  - `"ratio"`: Ratio variance (default)
  - `"hybrid"`: Hybrid approach

### Validation Rules

The configuration enforces several validation rules:

1. **Mortality Type vs Tree Type**: Cannot calculate volume mortality on live trees
   ```python
   # This will raise ValueError
   config = MortalityConfig(
       mortality_type="volume",
       tree_type="live"  # Error: mortality requires dead trees
   )
   ```

2. **Tree Class vs Land Type**: Timber tree class requires timber land type
   ```python
   # This will raise ValueError
   config = MortalityConfig(
       tree_class="timber",
       land_type="forest"  # Error: timber class needs timber land
   )
   ```

3. **Domain Expression Safety**: SQL injection patterns are blocked
   ```python
   # This will raise ValueError
   config = MortalityConfig(
       tree_domain="DIA >= 10; DROP TABLE TREE;"  # Forbidden keyword
   )
   ```

### Complex Grouping Example

```python
config = MortalityConfig(
    # Multiple grouping levels
    grp_by=["UNITCD", "COUNTYCD"],
    by_species=True,
    group_by_species_group=True,
    group_by_ownership=True,
    group_by_agent=True,
    by_size_class=True,
    
    # Mortality options
    mortality_type="both",
    include_components=True,
    
    # Domain filters
    tree_domain="DIA >= 10.0 AND STATUSCD == 2",
    area_domain="COND_STATUS_CD == 1",
    
    # Output options
    variance=True,
    totals=True
)

# Get grouping columns
print(config.get_grouping_columns())
# Output: ['UNITCD', 'COUNTYCD', 'SPCD', 'SPGRPCD', 'OWNGRPCD', 'AGENTCD', 'SIZE_CLASS']

# Get expected output columns
print(config.get_output_columns())
# Output: ['MORTALITY_TPA', 'MORTALITY_TPA_VAR', 'MORTALITY_TPA_TOTAL', 
#          'MORTALITY_VOL', 'MORTALITY_VOL_VAR', 'MORTALITY_VOL_TOTAL',
#          'MORTALITY_BA', 'MORTALITY_BA_VAR', 'MORTALITY_BA_TOTAL']
```

## Backwards Compatibility

The new configs can be converted to legacy format for compatibility:

```python
# New Pydantic config
new_config = MortalityConfig(
    mortality_type="tpa",
    by_species=True,
    group_by_ownership=True
)

# Convert to legacy dataclass
legacy_config = new_config.to_estimator_config()

# Use with functions expecting legacy config
from pyfia.estimation.base import BaseEstimator
# estimator = SomeEstimator(db, legacy_config)
```

## Benefits of Pydantic Configuration

1. **Type Safety**: Automatic type validation and conversion
2. **Value Validation**: Range checks, enum validation, custom validators
3. **Better Error Messages**: Clear validation errors with context
4. **IDE Support**: Better autocomplete and type hints
5. **Serialization**: Easy JSON/dict conversion for saving configs
6. **Immutability Options**: Can make configs immutable after creation

## Future Configurations

The pattern established by `MortalityConfig` will be extended to other estimation modules:

- `AreaConfig`: For area estimation
- `VolumeConfig`: For volume estimation
- `BiomassConfig`: For biomass estimation
- `GrowthConfig`: For growth estimation

Each will extend the base configuration with module-specific parameters and validation rules.

## Best Practices

1. **Use the Most Specific Config**: Use `MortalityConfig` for mortality, not generic `EstimatorConfig`

2. **Let Validation Help You**: Don't disable validation - it prevents errors

3. **Group Related Parameters**: Use the config to group related parameters logically

4. **Document Custom Domains**: When using complex domain expressions, document their purpose

5. **Save Configurations**: Configs can be serialized for reproducibility
   ```python
   # Save configuration
   config_dict = config.model_dump()
   
   # Recreate later
   config = MortalityConfig(**config_dict)
   ```