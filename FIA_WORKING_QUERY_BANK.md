# FIA Working Query Bank

This document contains tested and validated SQL queries for the FIA database. All queries have been verified to work with the current database structure.

## 🌳 Basic Tree Queries

### 1. Oregon Total Live Trees (EVALIDator-Style)
**EVALID: 412101 (Oregon 2021)**
**Result: 10,481,113,490 live trees (357.8 trees/acre)**

```sql
SELECT 
    SUM(
        TREE.TPA_UNADJ * 
        CASE 
            WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP
            WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
            WHEN TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN POP_STRATUM.ADJ_FACTOR_SUBP
            ELSE POP_STRATUM.ADJ_FACTOR_MACR
        END * POP_STRATUM.EXPNS
    ) AS total_live_trees
    
FROM POP_STRATUM 
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
JOIN COND ON (COND.PLT_CN = PLOT.CN)
JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)

WHERE 
    TREE.STATUSCD = 1  -- Live trees
    AND COND.COND_STATUS_CD = 1  -- Forest conditions
    AND POP_STRATUM.EVALID = 412101;  -- Oregon 2021
```

**EVALIDator Methodology Notes:**
- Uses proper adjustment factors (MICR/SUBP/MACR) based on tree diameter
- Applies population expansion factors (EXPNS) for statistical estimates
- Matches Oracle EVALIDator query structure exactly
- Forest area: 29,292,380 acres

### 2. North Carolina Live Trees by Species (EVALIDator-Style)
**EVALID: 372301 (North Carolina 2023)**
**Result: 13,541,944,859 total trees, 129 species, 729.1 trees/acre**

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

**Top 10 Species Results:**
- **131**: loblolly pine (Pinus taeda) - 2,112,569,195 trees
- **316**: red maple (Acer rubrum) - 1,933,632,940 trees  
- **611**: sweetgum (Liquidambar styraciflua) - 1,678,200,744 trees
- **621**: yellow-poplar (Liriodendron tulipifera) - 971,141,798 trees
- **591**: American holly (Ilex opaca) - 573,763,842 trees
- **711**: sourwood (Oxydendrum arboreum) - 409,433,348 trees
- **827**: water oak (Quercus nigra) - 325,669,861 trees
- **693**: blackgum (Nyssa sylvatica) - 295,038,023 trees
- **391**: American hornbeam (Carpinus caroliniana) - 283,189,646 trees
- **132**: Virginia pine (Pinus virginiana) - 280,958,251 trees

**EVALIDator Methodology Notes:**
- Uses proper adjustment factors (MICR/SUBP/MACR) based on tree diameter
- Applies population expansion factors (EXPNS) for statistical estimates
- Matches Oracle EVALIDator query structure for species analysis
- Forest area: 18,574,188 acres

### 3. Minnesota Forest Area by Forest Type Group (EVALIDator-Style)
**EVALID: 272201 (Minnesota 2022)**
**Result: 17,599,046 total forest acres, 10 forest type groups**

```sql
SELECT 
    CASE 
        WHEN rftg.VALUE IS NULL THEN '0999 Nonstocked' 
        ELSE LPAD(CAST(rftg.VALUE AS VARCHAR), 4, '0') || ' ' || COALESCE(rftg.MEANING, 'Unknown')
    END as forest_type_group,
    SUM(
        c.CONDPROP_UNADJ * 
        CASE c.PROP_BASIS 
            WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR 
            ELSE ps.ADJ_FACTOR_SUBP 
        END * ps.EXPNS
    ) as total_area_acres
    
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
LEFT JOIN REF_FOREST_TYPE rft ON rft.VALUE = c.FORTYPCD
LEFT JOIN REF_FOREST_TYPE_GROUP rftg ON rft.TYPGRPCD = rftg.VALUE

WHERE 
    c.COND_STATUS_CD = 1  -- Forest conditions only
    AND c.CONDPROP_UNADJ IS NOT NULL
    AND ps.rscd = 23  -- Minnesota
    AND ps.evalid = 272201

GROUP BY rftg.VALUE, rftg.MEANING
ORDER BY total_area_acres DESC
LIMIT 10;
```

**Top 10 Forest Type Groups by Area:**
- **0900**: Aspen / birch group - 6,411,308 acres (36.4%)
- **0120**: Spruce / fir group - 4,312,514 acres (24.5%)
- **0500**: Oak / hickory group - 2,247,158 acres (12.8%)
- **0700**: Elm / ash / cottonwood group - 1,662,899 acres (9.4%)
- **0800**: Maple / beech / birch group - 1,196,822 acres (6.8%)
- **0100**: White / red / jack pine group - 1,059,161 acres (6.0%)
- **0400**: Oak / pine group - 286,679 acres (1.6%)
- **0960**: Other hardwoods group - 173,349 acres (1.0%)
- **0999**: Nonstocked - 169,101 acres (1.0%)
- **0990**: Exotic hardwoods group - 48,366 acres (0.3%)

**EVALIDator Methodology Notes:**
- Uses **forest type groups** (REF_FOREST_TYPE_GROUP) instead of individual forest types
- Matches Oracle EVALIDator query structure exactly with proper joins
- Uses PROP_BASIS for correct adjustment factor selection (MACR vs SUBP)
- Applies population expansion factors (EXPNS) for statistical estimates
- Top 10 groups represent 99.8% of total forest area

**Key Insights:**
- **🌲 Boreal Dominance**: Aspen/birch and spruce/fir groups dominate (60.9% combined)
- **🍁 Northern Hardwoods**: Oak/hickory and maple/beech/birch reflect transition zone
- **🌳 Forest Type Grouping**: EVALIDator uses broader forest type groups for analysis
- **📊 Area Concentration**: Top 6 groups account for 95.9% of all forest area

**Critical Correction**: This query now matches the Oracle EVALIDator methodology using forest type groups rather than individual forest types, providing the official FIA statistical framework for forest area estimation.

## 🏛️ EVALIDator-Style Ratio Estimation Queries

### 4. Trees Per Acre in Loblolly Pine Forest Types (Alabama)
```sql
-- CRITICAL INSIGHT: This calculates TPA for ALL SPECIES in loblolly pine forest types
-- NOT just loblolly pine trees (species 131)
WITH numerator AS (
    SELECT 
        SUM(
            t.TPA_UNADJ * 
            CASE 
                WHEN t.DIA IS NULL THEN ps.ADJ_FACTOR_SUBP
                WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
                WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
                ELSE ps.ADJ_FACTOR_MACR
            END * ps.EXPNS
        ) as TOTAL_TREES_EXPANDED
    FROM POP_STRATUM ps
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
    JOIN PLOT p ON ppsa.PLT_CN = p.CN
    JOIN COND c ON c.PLT_CN = p.CN
    JOIN TREE t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
    WHERE t.STATUSCD = 1
        AND c.FORTYPCD IN (161, 406)  -- Loblolly pine forest types
        AND c.COND_STATUS_CD = 1
        AND ps.RSCD = 33  -- Alabama
        AND ps.EVALID = 12401
),
denominator AS (
    SELECT 
        SUM(
            c.CONDPROP_UNADJ * 
            CASE 
                WHEN c.PROP_BASIS = 'MACR' THEN ps.ADJ_FACTOR_MACR 
                ELSE ps.ADJ_FACTOR_SUBP 
            END * ps.EXPNS
        ) as TOTAL_AREA_EXPANDED
    FROM POP_STRATUM ps
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
    JOIN PLOT p ON ppsa.PLT_CN = p.CN
    JOIN COND c ON c.PLT_CN = p.CN
    WHERE c.FORTYPCD IN (161, 406)  -- Loblolly pine forest types
        AND c.COND_STATUS_CD = 1
        AND c.CONDPROP_UNADJ IS NOT NULL
        AND ps.RSCD = 33  -- Alabama
        AND ps.EVALID = 12401
)
SELECT 
    n.TOTAL_TREES_EXPANDED,
    d.TOTAL_AREA_EXPANDED,
    n.TOTAL_TREES_EXPANDED / d.TOTAL_AREA_EXPANDED as TPA_IN_LOBLOLLY_FORESTS
FROM numerator n, denominator d;
```
**Result**: 770.3 TPA in loblolly pine forest types (FORTYPCD 161, 406)
**Key Insight**: Forest type analysis vs species analysis yields dramatically different results!

### 5. Loblolly Pine Trees by Forest Type (Oracle EVALIDator Style)
```sql
SELECT 
    CASE 
        WHEN rt.VALUE IS NULL THEN '9999 Other or Unknown'
        ELSE LPAD(CAST(rt.VALUE AS VARCHAR), 4, '0') || ' ' || COALESCE(rt.MEANING, 'Other or Unknown')
    END as FOREST_TYPE,
    SUM(
        t.TPA_UNADJ * 
        CASE 
            WHEN t.DIA IS NULL THEN ps.ADJ_FACTOR_SUBP
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) as TOTAL_TREES_EXPANDED
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
JOIN TREE t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
LEFT JOIN REF_FOREST_TYPE rt ON rt.VALUE = c.FORTYPCD
WHERE t.STATUSCD = 1
    AND t.SPCD = 131  -- Loblolly pine trees only
    AND c.COND_STATUS_CD = 1
    AND ps.RSCD = 33  -- Alabama
    AND ps.EVALID = 12401
GROUP BY rt.VALUE, rt.MEANING
ORDER BY TOTAL_TREES_EXPANDED DESC;
```
**Note**: This shows distribution of loblolly pine trees across different forest types

### 6. Alabama Land Area by Condition Status (EVALID 12400)
```sql
SELECT 
    CASE c.COND_STATUS_CD
        WHEN 1 THEN 'Forest land'
        WHEN 2 THEN 'Non-forest land'
        WHEN 3 THEN 'Water'
        WHEN 4 THEN 'Noncensus water'
        ELSE 'Other'
    END as land_type,
    SUM(c.CONDPROP_UNADJ * ps.EXPNS) as total_acres,
    COUNT(DISTINCT c.PLT_CN) as plot_count
FROM COND c
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON c.PLT_CN = ppsa.PLT_CN
JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
WHERE ps.RSCD = 33  -- Alabama
    AND ps.EVALID = 12400  -- ALL AREA evaluation
    AND c.CONDPROP_UNADJ IS NOT NULL
GROUP BY c.COND_STATUS_CD
ORDER BY total_acres DESC;
```
**Result**: Total Alabama land area = 33,548,846 acres (Forest: 22.7M, Non-forest: 9.2M, etc.)

### 7. California Net Merchantable Bole Wood Volume by Diameter Class
**EVALID: 62101 (California 2021 Current Volume)**
**Result: 67.05 billion cubic feet total volume, 4,188.7 cu ft/acre**

```sql
SELECT 
    CASE 
        WHEN tree.dia <= 6.99 THEN '5.0-6.9'
        WHEN tree.dia <= 8.99 THEN '7.0-8.9'
        WHEN tree.dia <= 10.99 THEN '9.0-10.9'
        WHEN tree.dia <= 12.99 THEN '11.0-12.9'
        WHEN tree.dia <= 14.99 THEN '13.0-14.9'
        WHEN tree.dia <= 16.99 THEN '15.0-16.9'
        WHEN tree.dia <= 18.99 THEN '17.0-18.9'
        WHEN tree.dia <= 20.99 THEN '19.0-20.9'
        WHEN tree.dia <= 28.99 THEN '21.0-28.9'
        ELSE '29.0+'
    END as diameter_class,
    
    SUM(
        TREE.TPA_UNADJ * TREE.VOLCFNET * 
        CASE 
            WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
            WHEN TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN POP_STRATUM.ADJ_FACTOR_SUBP
            ELSE POP_STRATUM.ADJ_FACTOR_MACR
        END * POP_STRATUM.EXPNS
    ) AS total_volume_cuft
    
FROM POP_STRATUM 
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
JOIN COND ON (COND.PLT_CN = PLOT.CN)
JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)
JOIN REF_SPECIES ON (TREE.SPCD = REF_SPECIES.SPCD)

WHERE 
    TREE.STATUSCD = 1  -- Live trees
    AND COND.RESERVCD = 0  -- Unreserved
    AND COND.SITECLCD IN (1, 2, 3, 4, 5, 6)  -- Timberland site classes
    AND COND.COND_STATUS_CD = 1  -- Forest conditions
    AND TREE.TPA_UNADJ IS NOT NULL
    AND TREE.VOLCFNET IS NOT NULL
    AND TREE.DIA >= 5.0  -- At least 5 inches DBH for merchantable timber
    AND REF_SPECIES.WOODLAND = 'N'  -- Non-woodland species (timber species)
    AND pop_stratum.rscd = 26  -- California (RSCD 26)
    AND pop_stratum.evalid = 62101
    
GROUP BY 
    CASE 
        WHEN tree.dia <= 6.99 THEN '5.0-6.9'
        WHEN tree.dia <= 8.99 THEN '7.0-8.9'
        WHEN tree.dia <= 10.99 THEN '9.0-10.9'
        WHEN tree.dia <= 12.99 THEN '11.0-12.9'
        WHEN tree.dia <= 14.99 THEN '13.0-14.9'
        WHEN tree.dia <= 16.99 THEN '15.0-16.9'
        WHEN tree.dia <= 18.99 THEN '17.0-18.9'
        WHEN tree.dia <= 20.99 THEN '19.0-20.9'
        WHEN tree.dia <= 28.99 THEN '21.0-28.9'
        ELSE '29.0+'
    END
    
ORDER BY 
    MIN(tree.dia);
```

**Results Summary:**
- **5.0-6.9 inches**: 1.29 billion cu ft (1.9%)
- **7.0-8.9 inches**: 2.32 billion cu ft (3.5%)
- **9.0-10.9 inches**: 3.22 billion cu ft (4.8%)
- **11.0-12.9 inches**: 3.85 billion cu ft (5.7%)
- **13.0-14.9 inches**: 4.21 billion cu ft (6.3%)
- **15.0-16.9 inches**: 4.34 billion cu ft (6.5%)
- **17.0-18.9 inches**: 4.68 billion cu ft (7.0%)
- **19.0-20.9 inches**: 4.62 billion cu ft (6.9%)
- **21.0-28.9 inches**: 14.63 billion cu ft (21.8%)
- **29.0+ inches**: 23.90 billion cu ft (35.6%)

**Key Insights:**
- Large trees (29.0+ inches) contain 35.6% of total volume
- Combined large diameter classes (21.0+ inches) contain 57.4% of total volume
- Smaller merchantable trees (5.0-12.9 inches) contain only 15.9% of volume
- This demonstrates the critical importance of large trees for timber volume

**EVALIDator Methodology Notes:**
- Uses proper adjustment factors (MICR, SUBP, MACR) based on tree diameter
- Applies expansion factors (EXPNS) for population estimates
- Filters for timber species only (WOODLAND = 'N')
- Restricts to timberland site classes (SITECLCD 1-6)
- Includes only unreserved forest conditions (RESERVCD = 0)

## 📝 Query Best Practices

1. **Always use EVALID filtering** for statistical estimates
2. **Include appropriate status codes** (STATUSCD=1 for live trees, COND_STATUS_CD=1 for forest)
3. **Handle NULL values** with IS NOT NULL checks
4. **Use proper expansion factors** (TPA_UNADJ for trees, EXPNS for area)
5. **Join through POP_PLOT_STRATUM_ASSGN** for EVALID-based queries
6. **Include plot counts** for context on sample sizes
7. **Order results meaningfully** (usually by the main metric DESC)
8. **🔥 CRITICAL: Understand Forest Type vs Species Analysis**
   - Forest type queries (FORTYPCD) analyze all species in specific forest types
   - Species queries (SPCD) analyze specific species across all forest types
   - Results can differ by 5x or more - choose the right approach for your question!

## ⚠️ Common Pitfalls to Avoid

1. **Don't filter by year alone** - use EVALID for proper statistical grouping
2. **Don't ignore expansion factors** - raw counts are not meaningful
3. **Don't mix EVALIDs** - each evaluation is statistically independent
4. **Don't forget status codes** - include appropriate filters for live/dead trees
5. **Don't ignore NULL values** - they can skew calculations
6. **🚨 CRITICAL: Don't confuse forest type vs species analysis**
   - "Loblolly pine TPA" could mean:
     - TPA of loblolly pine trees (SPCD 131) = ~150 TPA
     - TPA of all trees in loblolly pine forests (FORTYPCD 161, 406) = ~770 TPA
   - Always clarify which interpretation is needed!

 