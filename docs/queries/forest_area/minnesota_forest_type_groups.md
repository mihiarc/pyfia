# Minnesota Forest Area by Forest Type Group

This query demonstrates forest area estimation using EVALIDator methodology, showing how Minnesota's forests are distributed across different forest type groups.

## Query Overview

- **EVALID**: 272201 (Minnesota 2022)
- **Expected Result**: 17,599,046 total forest acres across 10 forest type groups
- **Key Insight**: Demonstrates boreal forest dominance in Minnesota

## Key Features

- Forest type group classification and area estimation
- Proper condition proportion and adjustment factor handling
- Population expansion for statistical estimates
- Comprehensive forest type group analysis

## Query

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

## Expected Results

**Top 10 Forest Type Groups by Area:**

1. **0900**: Aspen / birch group - 6,411,308 acres (36.4%)
2. **0120**: Spruce / fir group - 4,312,514 acres (24.5%)
3. **0500**: Oak / hickory group - 2,247,158 acres (12.8%)
4. **0700**: Elm / ash / cottonwood group - 1,662,899 acres (9.4%)
5. **0800**: Maple / beech / birch group - 1,196,822 acres (6.8%)
6. **0100**: White / red / jack pine group - 1,059,161 acres (6.0%)
7. **0400**: Oak / pine group - 286,679 acres (1.6%)
8. **0960**: Other hardwoods group - 173,349 acres (1.0%)
9. **0999**: Nonstocked - 169,101 acres (1.0%)
10. **0990**: Exotic hardwoods group - 48,366 acres (0.3%)

## Key Insights

- **🌲 Boreal Dominance**: Aspen/birch and spruce/fir groups dominate (60.9% combined)
- **🍁 Northern Hardwoods**: Oak/hickory and maple/beech/birch reflect transition zone
- **🌳 Forest Type Grouping**: EVALIDator uses broader forest type groups for analysis
- **📊 Area Concentration**: Top 6 groups account for 95.9% of all forest area

## EVALIDator Methodology

- **Forest Type Groups**: Uses REF_FOREST_TYPE_GROUP instead of individual forest types
- **PROP_BASIS Handling**: Correct adjustment factor selection (MACR vs SUBP)
- **Population Expansion**: Applies EXPNS for statistical estimates
- **Statistical Integrity**: Matches Oracle EVALIDator query structure exactly

## Download

<a href="minnesota_forest_type_groups.sql" download class="md-button md-button--primary">
  :material-download: Download SQL File
</a>