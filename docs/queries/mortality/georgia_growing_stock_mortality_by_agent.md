# Georgia Growing-Stock Mortality by Damage Agent and Species

## Overview
This query calculates average annual mortality of growing-stock trees (at least 5 inches d.b.h.) on timberland in Georgia, grouped by damage agent and species. It uses the growing stock timber components from TREE_GRM_COMPONENT table to focus on merchantable timber mortality.

## Key Components

### Tables Used
- `POP_STRATUM` - Statistical strata and expansion factors
- `POP_PLOT_STRATUM_ASSGN` - Links plots to strata
- `PLOT` - Plot locations with previous plot linkages
- `PLOTGEOM` - Plot geometry (required for join)
- `COND` - Condition data
- `TREE` - Tree measurements with damage agent codes
- `TREE_GRM_BEGIN`, `TREE_GRM_MIDPT` - Growth period tree data
- `TREE_GRM_COMPONENT` - Mortality components with growing stock focus
- `REF_SPECIES` - Species reference data

### Key Fields
- `AGENTCD` - Damage agent code (0-99)
- `SUBP_TPAMORT_UNADJ_GS_TIMBER` - Unadjusted mortality for growing stock timber
- `SUBP_SUBPTYP_GRM_GS_TIMBER` - Subplot type for growing stock
- `SUBP_COMPONENT_GS_TIMBER` - Component type (filtered for MORTALITY%)

### Damage Agent Categories
- **0**: No serious damage
- **1-19**: Insect
- **20-29**: Disease  
- **30-39**: Fire
- **40-49**: Animal
- **50-59**: Weather
- **60-69**: Vegetation (competition)
- **70-79**: Unknown/other
- **80-89**: Logging/human
- **90-99**: Physical

### Filters
- Growing stock timber components only (_GS_TIMBER columns)
- Mortality components only (COMPONENT LIKE 'MORTALITY%')
- Georgia state (`rscd = 33`)
- 2023 GRM evaluation (`evalid = 132303`)

## Results Summary

### Total Annual Growing Stock Mortality by Damage Agent
| Damage Agent | Trees per Year | Species Count | % of Total |
|--------------|----------------|---------------|------------|
| Insect       | 12,456,468     | 23            | 35.1%      |
| Disease      | 8,584,680      | 49            | 24.2%      |
| Vegetation   | 7,880,108      | 50            | 22.2%      |
| Weather      | 5,516,640      | 45            | 15.6%      |
| Fire         | 1,474,835      | 26            | 4.2%       |
| Animal       | 344,485        | 10            | 1.0%       |
| Unknown/other| 74,005         | 3             | 0.2%       |

**Total**: ~36.3 million growing stock trees per year

### Key Species Mortality (Insect damage)
- Loblolly pine (SPCD 131): 7.9 million trees/year - dominant mortality
- Slash pine (SPCD 111): 2.4 million trees/year
- Shortleaf pine (SPCD 110): 601,286 trees/year
- Longleaf pine (SPCD 121): 525,138 trees/year

## Notes
- Insect damage is the leading cause of growing stock mortality (35.1%)
- Pine species dominate insect mortality, particularly southern pine beetle impacts
- Disease affects more species (49) but lower total mortality than insects
- Vegetation competition is significant (22.2%), reflecting dense forest conditions
- Weather-related mortality (15.6%) includes drought, wind, and ice damage
- The query uses growing stock specific columns (_GS_TIMBER) to focus on merchantable timber

## Query Translation Notes
### Oracle to DuckDB changes:
- `LPAD(SPCD, 4, '0')` → `printf('%04d', SPCD)`
- `LEAST()` function adapted for range comparisons
- Complex nested joins maintained for GRM table relationships
- Schema prefix `FS_FIADB.` removed for DuckDB