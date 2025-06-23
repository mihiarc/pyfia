# Growth and Mortality Queries

This directory contains SQL queries for estimating forest growth, mortality, and removals using FIA data.

## Available Queries

### Mortality Queries

- **[Colorado Merchantable Volume Mortality](colorado_mortality_merchantable_volume.md)** - Annual mortality of merchantable bole wood volume of growing-stock trees
- **[North Carolina Tree Mortality Rate](north_carolina_mortality_trees_per_acre.md)** - Simple mortality rate in trees per acre per year on forest land
- **[Georgia Growing-Stock Mortality by Damage Agent](georgia_growing_stock_mortality_by_agent.md)** - Annual mortality by tree-level cause of death and species
- **[Georgia Growing-Stock Mortality by Disturbance Type](georgia_growing_stock_mortality_by_disturbance.md)** - Annual mortality by condition-level disturbance and species

## Key Concepts

### Growth/Removal/Mortality (GRM) Evaluations

Unlike volume or area estimates, growth and mortality calculations require special evaluation types:

- **GRM Evaluations**: Use remeasurement data from plots visited multiple times
- **TREE_GRM Tables**: Contain component-based calculations for growth, mortality, and removals
- **Annual Rates**: Values are already annualized based on remeasurement period

### Important Tables

- **TREE_GRM_BEGIN**: Tree attributes at start of remeasurement period
- **TREE_GRM_COMPONENT**: Pre-calculated annual rates by component type
- **TREE_GRM_MIDPT**: Tree attributes at midpoint (for growth calculations)

### Component Types

- **MORTALITY1, MORTALITY2**: Different mortality events
- **SURVIVOR**: Trees that survived the remeasurement period
- **INGROWTH**: New trees entering the inventory
- **REMOVAL1, REMOVAL2**: Harvest or other removals

### Tree Classifications

- **All Live (_AL_)**: All live trees regardless of merchantability
- **Growing Stock (_GS_)**: Merchantable timber trees only

## Query Patterns

### Basic Mortality Query Structure

```sql
SELECT 
    SUM(
        tgc.SUBP_COMPONENT_GS_FOREST * t.VOLCFNET * 
        [adjustment_factors] * ps.EXPNS
    ) as annual_mortality
FROM POP_STRATUM ps
JOIN [standard FIA joins]
JOIN TREE_GRM_COMPONENT tgc ON tgc.PLT_CN = p.CN
JOIN TREE_GRM_BEGIN t ON t.TRE_CN = tgc.TRE_CN
WHERE 
    tgc.COMPONENT LIKE 'MORTALITY%'
    AND [other filters]
```

## Coming Soon

- Growth rate queries
- Removals (harvest) estimation
- Net change calculations
- Mortality by cause
- Growth by species group