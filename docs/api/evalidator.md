# EVALIDator API

Client for validating PyFIA estimates against official USFS values.

## Overview

EVALIDator is the USFS online tool for FIA estimates. PyFIA includes a client to fetch official estimates for validation.

```python
from pyfia import EVALIDatorClient, validate_pyfia_estimate

# Get official estimate
client = EVALIDatorClient()
official = client.get_forest_area("GA", 2022)

# Compare with PyFIA
validation = validate_pyfia_estimate(pyfia_result, official)
print(f"Difference: {validation.percent_difference:.2f}%")
```

## Client Class

::: pyfia.EVALIDatorClient
    options:
      show_root_heading: true
      show_source: true
      members:
        - __init__
        - get_forest_area
        - get_volume
        - get_biomass
        - get_tree_count

## Data Classes

### EVALIDatorEstimate

Container for EVALIDator API results.

::: pyfia.EVALIDatorEstimate
    options:
      show_root_heading: true

### ValidationResult

Container for validation comparison results.

::: pyfia.ValidationResult
    options:
      show_root_heading: true

## Estimate Types

### EstimateType

Predefined constants for EVALIDator estimate types.

::: pyfia.EstimateType
    options:
      show_root_heading: true
      members: false

**Categories:**

| Category | Constants |
|----------|-----------|
| Area | `AREA_FOREST`, `AREA_TIMBERLAND`, `AREA_SAMPLED` |
| Volume | `VOLUME_NET_GROWINGSTOCK`, `VOLUME_NET_ALLSPECIES`, `VOLUME_SAWLOG_*` |
| Biomass | `BIOMASS_AG_LIVE`, `BIOMASS_BG_LIVE`, `BIOMASS_AG_LIVE_5INCH` |
| Carbon | `CARBON_AG_LIVE`, `CARBON_TOTAL_LIVE`, `CARBON_POOL_*` |
| Change | `GROWTH_NET_VOLUME`, `REMOVALS_VOLUME`, `MORTALITY_VOLUME` |

## Validation Functions

### validate_pyfia_estimate

::: pyfia.validate_pyfia_estimate
    options:
      show_root_heading: true
      show_source: true

### compare_estimates

::: pyfia.compare_estimates
    options:
      show_root_heading: true
      show_source: true

## Examples

### Validate Forest Area

```python
from pyfia import EVALIDatorClient, validate_pyfia_estimate

# Get PyFIA estimate
pyfia_area = pyfia.area(db, land_type="forest")

# Get official estimate
client = EVALIDatorClient()
official = client.get_forest_area("GA", 2022)

# Validate
result = validate_pyfia_estimate(pyfia_area, official)
print(f"PyFIA: {pyfia_area['estimate'][0]:,.0f} acres")
print(f"Official: {official.estimate:,.0f} acres")
print(f"Difference: {result.percent_difference:.2f}%")
print(f"Within CI: {result.within_confidence_interval}")
```

### Validate Volume Estimate

```python
# PyFIA net volume
pyfia_vol = pyfia.volume(db, vol_type="net", tree_type="gs")

# Official growing stock volume
official = client.get_volume("GA", 2022, volume_type="net_growingstock")

result = validate_pyfia_estimate(pyfia_vol, official)
```

### Batch Validation

```python
estimates = [
    ("area", pyfia.area(db), client.get_forest_area("GA", 2022)),
    ("volume", pyfia.volume(db), client.get_volume("GA", 2022)),
    ("biomass", pyfia.biomass(db), client.get_biomass("GA", 2022)),
]

for name, pyfia_est, official in estimates:
    result = validate_pyfia_estimate(pyfia_est, official)
    print(f"{name}: {result.percent_difference:.2f}% diff")
```
