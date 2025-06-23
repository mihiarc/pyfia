# Georgia Growing-Stock Mortality by Disturbance Type and Species

## Overview
This query calculates average annual mortality of growing-stock trees (at least 5 inches d.b.h.) on timberland in Georgia, grouped by disturbance type and species. Unlike the damage agent query which uses tree-level AGENTCD, this uses condition-level DSTRBCD1 to identify disturbances affecting the forest condition.

## Key Components

### Tables Used
- `POP_STRATUM` - Statistical strata and expansion factors
- `POP_PLOT_STRATUM_ASSGN` - Links plots to strata
- `PLOT` - Plot locations with previous plot linkages
- `PLOTGEOM` - Plot geometry (required for join)
- `COND` - Condition data with disturbance codes
- `TREE` - Tree measurements
- `TREE_GRM_BEGIN`, `TREE_GRM_MIDPT` - Growth period tree data
- `TREE_GRM_COMPONENT` - Mortality components with growing stock focus
- `REF_SPECIES` - Species reference data

### Key Fields
- `DSTRBCD1` - Primary disturbance code at condition level (0-95)
- `SUBP_TPAMORT_UNADJ_GS_TIMBER` - Unadjusted mortality for growing stock timber
- `SUBP_SUBPTYP_GRM_GS_TIMBER` - Subplot type for growing stock
- `SUBP_COMPONENT_GS_TIMBER` - Component type (filtered for MORTALITY%)

### Disturbance Categories
The query includes 32 specific disturbance types:
- **0**: No visible disturbance
- **10-12**: Insect damage (general, understory, trees)
- **20-22**: Disease damage (general, understory, trees)
- **30-32**: Fire damage (general, ground, crown)
- **40-46**: Animal damage (general, beaver, porcupine, deer, bear, rabbit, livestock)
- **50-54**: Weather damage (general, ice, wind, flooding, drought)
- **60**: Vegetation competition
- **70**: Unknown/other
- **80**: Human-caused damage
- **90-95**: Geologic disturbances

### Filters
- Growing stock timber components only (_GS_TIMBER columns)
- Mortality components only (COMPONENT LIKE 'MORTALITY%')
- Georgia state (`rscd = 33`)
- 2023 GRM evaluation (`evalid = 132303`)

## Results Summary

### Total Annual Growing Stock Mortality by Disturbance Type
| Disturbance Type | Trees per Year | Species Count | % of Total |
|------------------|----------------|---------------|------------|
| No visible disturbance | 22,520,652 | 62 | 61.9% |
| Insect damage to trees | 2,675,919 | 26 | 7.3% |
| Wind damage | 2,637,548 | 29 | 7.2% |
| Ground fire damage | 2,454,826 | 31 | 6.7% |
| Disease damage to trees | 1,237,966 | 22 | 3.4% |
| Insect damage (general) | 1,183,199 | 14 | 3.2% |
| Fire (general) | 995,907 | 4 | 2.7% |
| Human-caused damage | 944,404 | 6 | 2.6% |
| Beaver damage | 504,575 | 10 | 1.4% |

**Total**: ~36.3 million growing stock trees per year (same as damage agent total)

### Key Differences from Damage Agent Analysis
- **No visible disturbance dominates** (61.9%) - many trees die without condition-level disturbance
- **Wind damage** appears significant at condition level (7.2%)
- **Fire damage** is more prominent (ground fire 6.7%, general fire 2.7%)
- **Insect and disease** show lower percentages than tree-level analysis

## Notes
- Condition-level disturbances (DSTRBCD1) capture area-wide impacts
- Tree-level damage agents (AGENTCD) capture individual tree mortality causes
- The difference explains why "No visible disturbance" is so high - individual trees can die from specific agents in undisturbed conditions
- Wind, fire, and beaver create visible condition-level changes
- Total mortality matches the damage agent query (36.3M trees/year)

## Query Translation Notes
### Oracle to DuckDB changes:
- `LPAD(SPCD, 4, '0')` → `printf('%04d', SPCD)`
- Extensive CASE statement for 32 disturbance types maintained
- Complex nested joins maintained for GRM table relationships
- Schema prefix `FS_FIADB.` removed for DuckDB