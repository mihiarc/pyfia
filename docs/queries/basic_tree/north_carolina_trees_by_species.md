# North Carolina Live Trees by Species

This query demonstrates species-level analysis using EVALIDator methodology, showing the distribution of live trees across different species in North Carolina's forests.

## Query Overview

- **EVALID**: 372301 (North Carolina 2023)
- **Expected Result**: 13,541,944,859 total trees across 129 species (729.1 trees/acre)
- **Forest Area**: 18,574,188 acres

## Key Features

- Species identification with common and scientific names
- Proper adjustment factors based on tree diameter classes
- Population expansion for statistical estimates
- Top 10 species ranking by tree count

## Query

```sql
SELECT 
    t.SPCD,
    rs.COMMON_NAME,
    rs.SCIENTIFIC_NAME,
    SUM(
        t.TPA_UNADJ * 
        CASE 
            WHEN t.DIA IS NULL THEN ps.ADJ_FACTOR_SUBP
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) AS total_trees_expanded
    
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
JOIN TREE t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
LEFT JOIN REF_SPECIES rs ON t.SPCD = rs.SPCD

WHERE 
    t.STATUSCD = 1  -- Live trees
    AND c.COND_STATUS_CD = 1  -- Forest conditions
    AND ps.rscd = 33  -- North Carolina
    AND ps.evalid = 372301

GROUP BY t.SPCD, rs.COMMON_NAME, rs.SCIENTIFIC_NAME
ORDER BY total_trees_expanded DESC
LIMIT 10;
```

## Expected Results

**Top 10 Species by Tree Count:**

1. **131**: loblolly pine (Pinus taeda) - 2,112,569,195 trees
2. **316**: red maple (Acer rubrum) - 1,933,632,940 trees  
3. **611**: sweetgum (Liquidambar styraciflua) - 1,678,200,744 trees
4. **621**: yellow-poplar (Liriodendron tulipifera) - 971,141,798 trees
5. **591**: American holly (Ilex opaca) - 573,763,842 trees
6. **711**: sourwood (Oxydendrum arboreum) - 409,433,348 trees
7. **827**: water oak (Quercus nigra) - 325,669,861 trees
8. **693**: blackgum (Nyssa sylvatica) - 295,038,023 trees
9. **391**: American hornbeam (Carpinus caroliniana) - 283,189,646 trees
10. **132**: Virginia pine (Pinus virginiana) - 280,958,251 trees

## EVALIDator Methodology

- **Adjustment Factors**: Uses MICR/SUBP/MACR based on tree diameter
- **Population Expansion**: Applies EXPNS for statistical estimates
- **Species Reference**: Joins with REF_SPECIES for names
- **Statistical Integrity**: Matches Oracle EVALIDator methodology exactly

## Download

<a href="north_carolina_trees_by_species.sql" download class="md-button md-button--primary">
  :material-download: Download SQL File
</a> 