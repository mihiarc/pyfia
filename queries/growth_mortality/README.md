# Growth, Removal, and Mortality (GRM) Queries

This section contains the most complex FIA queries that analyze forest change processes using the TREE_GRM_* tables. These queries require exact Oracle EVALIDator methodology for accurate results.

## Queries in this Section

### 1. [Georgia Mortality by Species](./georgia_mortality_by_species.sql)
**EVALID: 132303 (Georgia 2016-2023)**  
**Result: 159.26 million cubic feet total annual mortality, 152 species**

Demonstrates comprehensive mortality analysis with:
- Complex tree joins through previous plot connections
- TREE_GRM_MIDPT.VOLCFNET for volume calculations
- Plot-level aggregation before species grouping
- SUBPTYP_GRM adjustment factor mapping
- TPAMORT_UNADJ with MORTALITY% components

**Top Species by Mortality Volume:**
- **0131**: loblolly pine (Pinus taeda) - 159.26M cu ft (87.4%)
- **0110**: shortleaf pine (Pinus echinata) - 17.24M cu ft (9.5%)
- **0111**: slash pine (Pinus elliottii) - 4.15M cu ft (2.3%)

### 2. [South Carolina Harvest Removals by Species](./south_carolina_harvest_removals.sql)
**EVALID: 452303 (South Carolina 2023)**  
**Result: 594.12 million cubic feet total annual harvest removals, 17 species**

Demonstrates harvest/removal analysis with:
- TPAREMV_UNADJ for removal rates
- CUT% components from TREE_GRM_COMPONENT
- Same complex Oracle EVALIDator structure as mortality
- Validation against forest management data

**Top Species by Harvest Volume:**
- **0131**: loblolly pine (Pinus taeda) - 555.84M cu ft (93.6%)
- **0121**: longleaf pine (Pinus palustris) - 10.57M cu ft (1.8%)
- **0316**: red maple (Acer rubrum) - 8.78M cu ft (1.5%)

## Key Concepts Demonstrated

- **Growth, Removal, Mortality Tables**: Proper use of TREE_GRM_* tables
- **Previous Plot Connections**: Complex joins through PREV_PLT_CN and PREV_TRE_CN
- **Component Analysis**: Understanding MORTALITY% vs CUT% components
- **Adjustment Factor Mapping**: SUBPTYP_GRM to adjustment factor translation
- **Volume Calculations**: Using TREE_GRM_MIDPT.VOLCFNET for accurate volume
- **Oracle Translation**: Exact 1:1 translation from Oracle EVALIDator

## Critical GRM Methodology

These queries follow strict Oracle EVALIDator methodology:

### Tree Joins
```sql
-- Complex tree join with previous plot connection
JOIN (
    SELECT P.PREV_PLT_CN, T.* 
    FROM PLOT P 
    JOIN TREE T ON (P.CN = T.PLT_CN)
) TREE ON ((TREE.CONDID = COND.CONDID) AND (TREE.PLT_CN = COND.PLT_CN))
```

### GRM Component Mapping
```sql
-- Mortality components
SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
SUBP_TPAMORT_UNADJ_GS_TIMBER AS TPAMORT_UNADJ

-- Harvest/removal components  
SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
SUBP_TPAREMV_UNADJ_GS_TIMBER AS TPAREMV_UNADJ
```

### Adjustment Factor Application
```sql
CASE 
    WHEN COALESCE(GRM.SUBPTYP_GRM, 0) = 0 THEN 0
    WHEN GRM.SUBPTYP_GRM = 1 THEN POP_STRATUM.ADJ_FACTOR_SUBP 
    WHEN GRM.SUBPTYP_GRM = 2 THEN POP_STRATUM.ADJ_FACTOR_MICR 
    WHEN GRM.SUBPTYP_GRM = 3 THEN POP_STRATUM.ADJ_FACTOR_MACR 
    ELSE 0 
END
```

## Advanced Features

- **Plot-Level Aggregation**: Groups by plot characteristics before final species grouping
- **Previous Measurement Integration**: Links current and previous tree measurements
- **Component Filtering**: Separates mortality from harvest processes
- **Growing Stock Focus**: Uses growing-stock timber components only
- **No Additional Filters**: Maintains Oracle methodology without restrictive additions

## Critical Warnings

⚠️ **DO NOT**:
- Add diameter restrictions (DIA ≥5") - changes results significantly
- Mix mortality and harvest components in same query
- Add timberland-only filters - Oracle includes all land types
- Use regular TPA_UNADJ instead of GRM-specific TPA fields
- Modify the complex tree join structure

✅ **DO**:
- Follow exact Oracle EVALIDator structure
- Use separate queries for mortality vs harvest
- Include all tree sizes and land types
- Validate against Oracle EVALIDator results
- Document any deviations from Oracle methodology

## Usage Notes

- GRM queries are computationally intensive due to complex joins
- Results represent annual rates (not cumulative over measurement period)
- Volume calculations use merchantable bole wood (VOLCFNET)
- Growing-stock timber focus excludes non-commercial species/conditions
- Plot counts provide sample size context for reliability assessment

These queries demonstrate the most sophisticated FIA analysis techniques and require careful attention to methodology for accurate results. 