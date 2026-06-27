# Missouri Forest Area Change by Forest Type Group

## Overview
This query calculates forest area changes by forest type group for Missouri using the 2024 change evaluation (EVALID 292403). It uses the SUBP_COND_CHNG_MTRX table to track condition changes between remeasurement periods.

## Key Components

### Tables Used
- `POP_STRATUM` - Statistical strata and expansion factors
- `POP_PLOT_STRATUM_ASSGN` - Links plots to strata
- `PLOT` - Plot locations and previous plot links
- `PLOTGEOM` - Plot geometry (required for join)
- `COND` - Current condition data
- `COND` (aliased as `pcond`) - Previous condition data
- `SUBP_COND_CHNG_MTRX` - Condition change matrix tracking changes between measurements
- `REF_FOREST_TYPE` & `REF_FOREST_TYPE_GROUP` - Forest type classification reference

### Key Fields
- `SUBPTYP_PROP_CHNG` - Proportion of condition that changed
- `SUBPTYP` - Subplot type (1=subplot, 3=macroplot)
- `PROP_BASIS` - Property basis for adjustment factors
- `FORTYPCD` - Forest type code
- `FORTYGPCD` - Forest type group code

### Statistical Adjustments
The query applies appropriate adjustment factors based on the subplot type:
- Macroplot conditions use `ADJ_FACTOR_MACR`
- Subplot conditions use `ADJ_FACTOR_SUBP`

### Filters
- Forest land only (`COND_STATUS_CD = 1` for both current and previous)
- Valid sample conditions (no non-sample reason codes)
- Missouri state (`rscd = 23`)
- 2024 change evaluation (`evalid = 292403`)

## Results

| Forest Type Group | Estimate (acres) |
|-------------------|------------------|
| White / red / jack pine group | 2,680.00 |
| Loblolly / shortleaf pine group | 259,594.55 |
| Other eastern softwoods group | 380,424.07 |
| Oak / pine group | 992,191.82 |
| Oak / hickory group | 11,834,572.21 |
| Oak / gum / cypress group | 167,871.00 |
| Elm / ash / cottonwood group | 1,088,900.36 |
| Maple / beech / birch group | 101,899.97 |
| Other hardwoods group | 15,728.00 |
| Exotic hardwoods group | 1,721.62 |
| Nonstocked | 63,797.99 |

## Notes
- The `/4` division in `SUBPTYP_PROP_CHNG / 4` converts from the 4-subplot design to per-plot basis
- Forest type groups are formatted with zero-padded codes for consistent sorting
- Oak/hickory dominates Missouri's forests with ~11.8 million acres
- This query tracks net forest area by type group, capturing both gains and losses

## Query Translation Notes
### Oracle to DuckDB changes:
- `LPAD(value, 4, '0')` → `printf('%04d', value)`
- `NVL()` → `COALESCE()`
- Table aliases maintained for clarity
- Schema prefix `FS_FIADB.` removed for DuckDB