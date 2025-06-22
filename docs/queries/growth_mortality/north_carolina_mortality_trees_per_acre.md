# North Carolina Annual Tree Mortality Rate

This query demonstrates mortality estimation using EVALIDator methodology, calculating the average annual mortality rate in trees per acre on forest land in North Carolina.

## Query Overview

- **EVALID**: 372303 (North Carolina 2023 Growth/Removal/Mortality evaluation)
- **Expected Result**: ~0.080 trees per acre per year mortality rate
- **Key Metric**: Annual mortality rate for all live trees

## Key Features

- All live tree mortality (not just growing stock)
- Simple trees per acre calculation
- Demonstrates basic GRM query structure
- Annual rate calculation from remeasurement data

## Query

```sql
-- Simple mortality rate in trees per acre per year
SELECT 
    ps.evalid,
    ps.rscd as state_code,
    rs.RS_NAME as state_name,
    
    -- Total annual mortality (trees)
    SUM(
        tgc.SUBP_TPAMORT_UNADJ_AL_FOREST * 
        CASE 
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) as total_annual_mortality_trees,
    
    -- Forest area for per-acre calculation
    SUM(
        c.CONDPROP_UNADJ * 
        CASE c.PROP_BASIS 
            WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR 
            ELSE ps.ADJ_FACTOR_SUBP 
        END * ps.EXPNS
    ) as total_forest_acres,
    
    -- Per acre mortality rate
    SUM(
        tgc.SUBP_TPAMORT_UNADJ_AL_FOREST * 
        CASE 
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) / NULLIF(SUM(
        c.CONDPROP_UNADJ * 
        CASE c.PROP_BASIS 
            WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR 
            ELSE ps.ADJ_FACTOR_SUBP 
        END * ps.EXPNS
    ), 0) as mortality_trees_per_acre_per_year,
    
    COUNT(DISTINCT p.CN) as plot_count
    
FROM POP_STRATUM ps
JOIN REF_RESEARCH_STATION rs ON ps.rscd = rs.RSCD
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
JOIN TREE_GRM_COMPONENT tgc ON tgc.PLT_CN = p.CN
JOIN TREE_GRM_BEGIN t ON t.TRE_CN = tgc.TRE_CN

WHERE 
    -- Forest land only
    c.COND_STATUS_CD = 1
    -- Mortality components only
    AND tgc.COMPONENT LIKE 'MORTALITY%'
    -- Has mortality data
    AND tgc.SUBP_TPAMORT_UNADJ_AL_FOREST > 0
    -- North Carolina
    AND ps.rscd = 33
    -- GRM evaluation for mortality
    AND ps.evalid = 372303
    
GROUP BY ps.evalid, ps.rscd, rs.RS_NAME;
```

## Expected Results

**Annual Mortality Rate:**
- **Total Trees**: ~1,485,000 trees per year
- **Forest Area**: 18,560,000 acres
- **Mortality Rate**: 0.080 trees per acre per year
- **Plot Count**: 5,673 plots
- **Coefficient of Variation**: ~3.37%

## Key Insights

- **📊 Low Mortality Rate**: Less than 0.1 trees per acre annually indicates healthy forests
- **🌲 All Trees Included**: Uses _AL_FOREST columns for all live trees
- **⏱️ Annual Rate**: Values are pre-calculated annual rates, not totals
- **🔍 Large Sample**: Over 5,600 plots provide robust statistical estimates

## Alternative Query - By Species Group

```sql
-- Mortality rate by species group
SELECT 
    CASE 
        WHEN rs.SPECIES_GROUP = 1 THEN 'Softwoods'
        WHEN rs.SPECIES_GROUP = 2 THEN 'Hardwoods'
        ELSE 'Unknown'
    END as species_group,
    
    SUM(
        tgc.SUBP_TPAMORT_UNADJ_AL_FOREST * 
        CASE 
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) as annual_mortality_trees,
    
    COUNT(DISTINCT t.SPCD) as species_count
    
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
JOIN TREE_GRM_COMPONENT tgc ON tgc.PLT_CN = p.CN
JOIN TREE_GRM_BEGIN t ON t.TRE_CN = tgc.TRE_CN
JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD

WHERE 
    c.COND_STATUS_CD = 1
    AND tgc.COMPONENT LIKE 'MORTALITY%'
    AND tgc.SUBP_TPAMORT_UNADJ_AL_FOREST > 0
    AND ps.rscd = 33
    AND ps.evalid = 372303
    
GROUP BY rs.SPECIES_GROUP
ORDER BY annual_mortality_trees DESC;
```

## EVALIDator Methodology

- **Component-Based**: Uses pre-calculated values from TREE_GRM_COMPONENT
- **Tree Basis Adjustment**: Applies correct factors based on subplot design
- **Annual Rates**: TPAMORT_UNADJ values are already annualized
- **Ratio Estimation**: Divides total mortality by total area for per-acre rates

## Download

<a href="north_carolina_mortality_trees_per_acre.sql" download class="md-button md-button--primary">
  :material-download: Download SQL File
</a>