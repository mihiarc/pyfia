# Colorado Annual Mortality of Merchantable Volume

This query demonstrates mortality estimation using EVALIDator methodology, calculating the average annual mortality of merchantable bole wood volume of growing-stock trees on forest land in Colorado.

## Query Overview

- **EVALID**: 82003 (Colorado 2020 Growth/Removal/Mortality evaluation)
- **Expected Result**: ~9.7 million cubic feet per year of merchantable volume mortality
- **Key Metric**: Annual mortality rate of growing-stock merchantable volume

## Key Features

- Growing-stock tree mortality (merchantable timber only)
- Uses TREE_GRM_COMPONENT tables for remeasurement data
- Proper COMPONENT filtering for mortality events
- Annual rate calculation from remeasurement period

## Query

```sql
SELECT
    ps.evalid,
    SUM(
        tgc.SUBP_COMPONENT_GS_FOREST * t.VOLCFNET *
        CASE
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) as annual_mortality_cuft,

    COUNT(DISTINCT p.CN) as plot_count,
    MIN(p.REMPER) as min_remper,
    MAX(p.REMPER) as max_remper

FROM POP_STRATUM ps
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
    -- Growing stock trees only (have merchantable volume)
    AND t.VOLCFNET IS NOT NULL
    AND t.VOLCFNET > 0
    -- Colorado state
    AND ps.rscd = 8
    -- GRM evaluation for mortality
    AND ps.evalid = 82003

GROUP BY ps.evalid;
```

## Alternative Query - Per Acre Estimates

```sql
WITH mortality_totals AS (
    SELECT
        ps.evalid,
        SUM(
            tgc.SUBP_COMPONENT_GS_FOREST * t.VOLCFNET *
            CASE
                WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
                WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
                ELSE ps.ADJ_FACTOR_MACR
            END * ps.EXPNS
        ) as annual_mortality_cuft,
        COUNT(DISTINCT p.CN) as plot_count
    FROM POP_STRATUM ps
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
    JOIN PLOT p ON ppsa.PLT_CN = p.CN
    JOIN COND c ON c.PLT_CN = p.CN
    JOIN TREE_GRM_COMPONENT tgc ON tgc.PLT_CN = p.CN
    JOIN TREE_GRM_BEGIN t ON t.TRE_CN = tgc.TRE_CN
    WHERE
        c.COND_STATUS_CD = 1
        AND tgc.COMPONENT LIKE 'MORTALITY%'
        AND t.VOLCFNET IS NOT NULL
        AND t.VOLCFNET > 0
        AND ps.rscd = 8
        AND ps.evalid = 82003
    GROUP BY ps.evalid
),
forest_area AS (
    SELECT
        ps.evalid,
        SUM(
            c.CONDPROP_UNADJ *
            CASE c.PROP_BASIS
                WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR
                ELSE ps.ADJ_FACTOR_SUBP
            END * ps.EXPNS
        ) as total_forest_acres
    FROM POP_STRATUM ps
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
    JOIN PLOT p ON ppsa.PLT_CN = p.CN
    JOIN COND c ON c.PLT_CN = p.CN
    WHERE
        c.COND_STATUS_CD = 1
        AND ps.rscd = 8
        AND ps.evalid = 82003
    GROUP BY ps.evalid
)
SELECT
    m.evalid,
    m.annual_mortality_cuft as total_annual_mortality_cuft,
    f.total_forest_acres,
    m.annual_mortality_cuft / f.total_forest_acres as mortality_cuft_per_acre_per_year,
    m.plot_count
FROM mortality_totals m
JOIN forest_area f ON m.evalid = f.evalid;
```

## Expected Results

**Total Annual Mortality:**
- **Total Volume**: ~9.7 million cubic feet per year
- **Per Acre**: ~4.2 cubic feet per acre per year
- **Forest Area**: ~2.3 million acres
- **Plot Count**: ~1,200 plots

## Key Insights

- **🌲 Merchantable Loss**: Represents timber volume lost to mortality annually
- **📊 Growing Stock Focus**: Only includes trees with merchantable volume (VOLCFNET > 0)
- **⏱️ Annual Rate**: SUBP_COMPONENT_GS_FOREST values are already annualized
- **🔍 Component Filtering**: MORTALITY1 and MORTALITY2 capture different mortality events

## EVALIDator Methodology

- **GRM Evaluations**: Use Growth/Removal/Mortality evaluations (not Volume evaluations)
- **TREE_GRM Tables**: Contains remeasurement data for growth and mortality
- **Component Values**: Pre-calculated annual rates in TREE_GRM_COMPONENT
- **Growing Stock**: Uses _GS_FOREST columns for merchantable timber
- **Tree Basis**: Proper adjustment factors based on tree diameter and plot design

## Important Notes

1. **Evaluation Type**: Must use GRM (Growth/Removal/Mortality) evaluation, not VOL
2. **Annual Rates**: COMPONENT values are already annualized - do NOT divide by REMPER
3. **Growing Stock**: Only trees with VOLCFNET > 0 are included
4. **Component Filter**: Use LIKE 'MORTALITY%' to capture all mortality events

## Download

<a href="colorado_mortality_merchantable_volume.sql" download class="md-button md-button--primary">
  :material-download: Download SQL File
</a>