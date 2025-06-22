# California Net Merchantable Volume by Diameter Class

This query demonstrates advanced volume analysis using EVALIDator methodology, showing how merchantable timber volume is distributed across different diameter classes in California.

## Query Overview

- **EVALID**: 62101 (California 2021 Current Volume)
- **Expected Result**: 67.05 billion cubic feet total volume (4,188.7 cu ft/acre)
- **Key Insight**: Large trees (29.0+ inches) contain 35.6% of total volume

## Key Features

- Diameter class grouping for volume analysis
- Net merchantable bole wood volume calculations
- Timber species filtering (non-woodland species)
- Timberland site class restrictions

## Query

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

## Expected Results

**Volume Distribution by Diameter Class:**

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

## Key Insights

- **Large Tree Dominance**: Trees 29.0+ inches contain 35.6% of total volume
- **Combined Large Classes**: 21.0+ inch trees contain 57.4% of total volume
- **Small Tree Contribution**: 5.0-12.9 inch trees contain only 15.9% of volume
- **Critical Importance**: Demonstrates the vital role of large trees for timber volume

## EVALIDator Methodology

- **Volume Calculations**: Uses VOLCFNET (net merchantable cubic foot volume)
- **Adjustment Factors**: Proper MICR/SUBP/MACR based on tree diameter
- **Timber Focus**: Filters for non-woodland species only (WOODLAND = 'N')
- **Timberland Restriction**: Limited to timberland site classes (SITECLCD 1-6)
- **Unreserved Land**: Includes only unreserved forest conditions (RESERVCD = 0)

## Download

<a href="california_volume_by_diameter.sql" download class="md-button md-button--primary">
  :material-download: Download SQL File
</a> 