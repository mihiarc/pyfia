# North Carolina Merchantable Bole Biomass by Diameter Class and Species

## Overview
This query calculates merchantable bole bark and wood biomass of live trees (timber species at least 5 inches d.b.h.) on forest land in North Carolina. Results are presented in green short tons, grouped by diameter class and species.

## Key Components

### Tables Used
- `POP_STRATUM` - Statistical strata and expansion factors
- `POP_PLOT_STRATUM_ASSGN` - Links plots to strata
- `PLOT` - Plot locations and macroplot breakpoints
- `PLOTGEOM` - Plot geometry (required for join)
- `COND` - Condition data for forest land identification
- `TREE` - Tree measurements and biomass components
- `REF_SPECIES` - Species-specific wood and bark properties

### Key Fields
- `DRYBIO_BOLE` - Dry biomass of merchantable bole wood (pounds)
- `DRYBIO_BOLE_BARK` - Dry biomass of merchantable bole bark (pounds)
- `WOOD_SPGR_GREENVOL_DRYWT` - Wood specific gravity (green volume, dry weight)
- `BARK_SPGR_GREENVOL_DRYWT` - Bark specific gravity (green volume, dry weight)
- `MC_PCT_GREEN_WOOD` - Moisture content percentage for green wood
- `MC_PCT_GREEN_BARK` - Moisture content percentage for green bark
- `BARK_VOL_PCT` - Bark volume as percentage of wood volume

### Biomass Calculation
The query converts dry biomass to green weight using species-specific properties:
1. Separates wood and bark components based on bark volume percentage
2. Applies moisture content adjustments to convert dry weight to green weight
3. Combines wood and bark for total merchantable bole biomass
4. Converts from pounds to short tons (divide by 2000)

### Filters
- Live trees only (`STATUSCD = 1`)
- Forest land only (`COND_STATUS_CD = 1`)
- Trees ≥ 5 inches DBH (merchantable size)
- North Carolina (`rscd = 33`)
- 2023 volume evaluation (`evalid = 372301`)

## Results Summary

### Total Merchantable Bole Biomass
**1,560,412,205 green short tons** (1.56 billion tons)

### By Diameter Class (Top 5)
| Diameter Class | Biomass (million tons) | % of Total |
|----------------|------------------------|------------|
| 11.0-12.9"     | 202.1                  | 13.0%      |
| 13.0-14.9"     | 198.9                  | 12.7%      |
| 9.0-10.9"      | 181.9                  | 11.7%      |
| 15.0-16.9"     | 175.7                  | 11.3%      |
| 17.0-18.9"     | 154.0                  | 9.9%       |

### Key Species (Examples from 5.0-6.9" class)
- Loblolly pine (SPCD 131): 21.0 million tons - dominant species
- Virginia pine (SPCD 132): 2.9 million tons
- Longleaf pine (SPCD 121): 1.2 million tons
- Pond pine (SPCD 128): 0.8 million tons
- Eastern white pine (SPCD 129): 0.7 million tons

## Notes
- Biomass peaks in the 11-15" diameter range, reflecting the distribution of managed forests
- Loblolly pine dominates across all diameter classes as expected for North Carolina
- The green weight conversion accounts for typical moisture content in living trees
- Uses DRYBIO_BOLE + DRYBIO_BOLE_BARK for total merchantable bole biomass
- Species without specific gravity data use default factor of 1.76

## Query Translation Notes
### Oracle to DuckDB changes:
- `LPAD(SPCD, 4, '0')` → `printf('%04d', SPCD)`
- `LEAST()` function syntax maintained (both support)
- Table aliases maintained for clarity
- Schema prefix removed for DuckDB