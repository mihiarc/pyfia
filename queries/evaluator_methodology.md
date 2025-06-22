# EVALIDator Methodology Guide

This guide provides comprehensive documentation for understanding and translating Oracle EVALIDator queries to DuckDB, maintaining statistical accuracy while leveraging modern SQL capabilities.

## 📋 Table of Contents

1. [What is EVALIDator?](#what-is-evaluator)
2. [Core Statistical Concepts](#core-statistical-concepts)
3. [Oracle to DuckDB Translation](#oracle-to-duckdb-translation)
4. [Key Table Relationships](#key-table-relationships)
5. [Adjustment Factors and Expansion](#adjustment-factors-and-expansion)
6. [Query Categories and Patterns](#query-categories-and-patterns)
7. [Common Translation Challenges](#common-translation-challenges)
8. [Validation and Testing](#validation-and-testing)
9. [Best Practices](#best-practices)

## 🎯 What is EVALIDator?

**EVALIDator** is the USDA Forest Service's official web application for accessing Forest Inventory and Analysis (FIA) data. It provides standardized queries that produce statistically valid estimates following rigorous sampling methodology.

### Key Characteristics:
- **Statistical Rigor**: Uses proper sampling weights and adjustment factors
- **Oracle-Based**: Original queries written for Oracle database
- **Standardized**: Consistent methodology across all analyses
- **Validated**: Results match published FIA reports
- **Complex**: Sophisticated statistical calculations and table joins

### Why Translate to DuckDB?
- **Performance**: DuckDB optimized for analytical workloads
- **Modern SQL**: Cleaner syntax and better optimization
- **Accessibility**: Easier deployment and maintenance
- **Integration**: Better integration with modern data science tools

## 📊 Core Statistical Concepts

### EVALID (Evaluation ID)
The fundamental unit of FIA statistical analysis.

```sql
-- EVALID Structure: SSYYTT
-- SS = State code (01-99)
-- YY = Year (last 2 digits)
-- TT = Evaluation type (01=current, 02=periodic, 03=change, etc.)

-- Examples:
-- 412101 = Oregon (41) 2021 (21) Current Volume (01)
-- 132303 = Georgia (13) 2023 (23) Change/GRM (03)
-- 452303 = South Carolina (45) 2023 (23) Change/GRM (03)
```

**Critical Rule**: Never mix EVALIDs in the same analysis - each represents a distinct statistical evaluation.

### Population Strata and Expansion
FIA uses stratified sampling with post-stratification for estimates.

```sql
-- Core Population Tables
POP_STRATUM              -- Statistical strata definitions
POP_PLOT_STRATUM_ASSGN   -- Links plots to strata
POP_EVAL                 -- Evaluation metadata
POP_ESTN_UNIT           -- Estimation units
```

### Adjustment Factors
Trees are measured on different subplot sizes based on diameter:

- **MICR (Microplot)**: Small trees (typically < 5" DBH)
- **SUBP (Subplot)**: Medium trees (5" to breakpoint)
- **MACR (Macroplot)**: Large trees (> breakpoint)

```sql
-- Standard Adjustment Factor Logic
CASE 
    WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP
    WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
    WHEN TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) 
        THEN POP_STRATUM.ADJ_FACTOR_SUBP
    ELSE POP_STRATUM.ADJ_FACTOR_MACR
END
```

## 🔄 Oracle to DuckDB Translation

### Function Mapping

| Oracle Function | DuckDB Equivalent | Purpose |
|----------------|-------------------|---------|
| `LPAD(field, 5, '0')` | `printf('%05d', field)` | Zero-padding numbers |
| `NVL(field, default)` | `COALESCE(field, default)` | NULL handling |
| `DECODE(field, val1, result1, val2, result2, default)` | `CASE WHEN field = val1 THEN result1 WHEN field = val2 THEN result2 ELSE default END` | Conditional logic |
| `LEAST(a, b)` | `LEAST(a, b)` | Same (both support) |
| `||` (concatenation) | `||` or `CONCAT()` | String concatenation |

### Data Type Considerations

```sql
-- Oracle: Implicit string to number conversion
WHERE TREE.DIA < PLOT.MACRO_BREAKPOINT_DIA

-- DuckDB: Explicit casting needed
WHERE TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0)
```

### String Formatting Differences

```sql
-- Oracle EVALIDator Style (Complex)
'`' || LPAD(CAST(SPGRPCD AS VARCHAR), 5, '0') || ' ' || NAME

-- DuckDB Simplified (Equivalent Results)
SPGRPCD, NAME  -- Group by base fields, format in application layer
```

## 🔗 Key Table Relationships

### Core Inventory Tables
```sql
-- Standard FIA Join Pattern
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON (ppsa.STRATUM_CN = ps.CN)
JOIN PLOT p ON (ppsa.PLT_CN = p.CN)
JOIN COND c ON (c.PLT_CN = p.CN)
JOIN TREE t ON (t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID)
```

### Reference Tables
```sql
-- Species Information
LEFT JOIN REF_SPECIES rs ON (t.SPCD = rs.SPCD)
LEFT JOIN REF_SPECIES_GROUP rsg ON (t.SPGRPCD = rsg.SPGRPCD)

-- Forest Type Information  
LEFT JOIN REF_FOREST_TYPE rft ON (c.FORTYPCD = rft.VALUE)
LEFT JOIN REF_FOREST_TYPE_GROUP rftg ON (rft.TYPGRPCD = rftg.VALUE)
```

### Growth, Removal, Mortality (GRM) Tables
```sql
-- Complex GRM Joins (Most Challenging)
-- Tree join with previous plot connection
JOIN (
    SELECT P.PREV_PLT_CN, T.* 
    FROM PLOT P 
    JOIN TREE T ON (P.CN = T.PLT_CN)
) TREE ON ((TREE.CONDID = COND.CONDID) AND (TREE.PLT_CN = COND.PLT_CN))

-- Previous measurement joins
LEFT OUTER JOIN PLOT PPLOT ON (PLOT.PREV_PLT_CN = PPLOT.CN)
LEFT OUTER JOIN COND PCOND ON ((TREE.PREVCOND = PCOND.CONDID) AND (TREE.PREV_PLT_CN = PCOND.PLT_CN))
LEFT OUTER JOIN TREE PTREE ON (TREE.PREV_TRE_CN = PTREE.CN)

-- GRM table joins
LEFT OUTER JOIN TREE_GRM_BEGIN TRE_BEGIN ON (TREE.CN = TRE_BEGIN.TRE_CN)
LEFT OUTER JOIN TREE_GRM_MIDPT TRE_MIDPT ON (TREE.CN = TRE_MIDPT.TRE_CN)
LEFT OUTER JOIN TREE_GRM_COMPONENT GRM ON (TREE.CN = GRM.TRE_CN)
```

## ⚖️ Adjustment Factors and Expansion

### Standard Tree Expansion
```sql
-- Basic Tree Count/Volume Expansion
SUM(
    TREE.TPA_UNADJ * 
    [ADJUSTMENT_FACTOR] * 
    POP_STRATUM.EXPNS
) AS expanded_estimate
```

### Area Expansion
```sql
-- Condition/Area Expansion
SUM(
    COND.CONDPROP_UNADJ * 
    CASE COND.PROP_BASIS 
        WHEN 'MACR' THEN POP_STRATUM.ADJ_FACTOR_MACR 
        ELSE POP_STRATUM.ADJ_FACTOR_SUBP 
    END * POP_STRATUM.EXPNS
) AS expanded_area
```

### GRM-Specific Expansion
```sql
-- Mortality Expansion
SUM(
    GRM.TPAMORT_UNADJ * 
    CASE GRM.SUBPTYP_GRM
        WHEN 1 THEN POP_STRATUM.ADJ_FACTOR_SUBP 
        WHEN 2 THEN POP_STRATUM.ADJ_FACTOR_MICR 
        WHEN 3 THEN POP_STRATUM.ADJ_FACTOR_MACR 
        ELSE 0 
    END * POP_STRATUM.EXPNS
) AS expanded_mortality

-- Harvest Removal Expansion  
SUM(
    GRM.TPAREMV_UNADJ * 
    [SAME_ADJUSTMENT_LOGIC] * 
    POP_STRATUM.EXPNS
) AS expanded_removals
```

## 📂 Query Categories and Patterns

### 1. Basic Tree Counts
**Pattern**: Simple aggregation with standard adjustment factors
```sql
-- Template
SELECT SUM(TPA_UNADJ * ADJ_FACTOR * EXPNS) AS total_trees
FROM [STANDARD_JOINS]
WHERE TREE.STATUSCD = 1 AND COND.COND_STATUS_CD = 1 AND EVALID = [TARGET]
```

### 2. Biomass Calculations
**Pattern**: Complex species-specific calculations
```sql
-- Template
SELECT SUM(
    TPA_UNADJ * 
    [COMPLEX_BIOMASS_CALCULATION] * 
    ADJ_FACTOR * 
    DRYBIO_AG / 2000  -- Pounds to tons
) AS total_biomass_tons
```

### 3. GRM Analysis
**Pattern**: Multi-table joins with previous measurements
```sql
-- Template
SELECT SUM(
    GRM.[TPA_FIELD] *  -- TPAMORT_UNADJ or TPAREMV_UNADJ
    [GRM_ADJUSTMENT_FACTOR] *
    TRE_MIDPT.VOLCFNET *  -- Use midpoint volume
    EXPNS
) AS grm_estimate
FROM [COMPLEX_GRM_JOINS]
WHERE GRM.COMPONENT LIKE '[MORTALITY%|CUT%]'
```

### 4. Volume Analysis
**Pattern**: Diameter-based analysis with timber filters
```sql
-- Template
SELECT SUM(
    TPA_UNADJ * 
    VOLCFNET * 
    ADJ_FACTOR * 
    EXPNS
) AS total_volume
WHERE TREE.DIA >= 5.0 AND REF_SPECIES.WOODLAND = 'N'
```

## 🚧 Common Translation Challenges

### 1. String Formatting Complexity
**Oracle Challenge**: Complex LPAD and concatenation for grouping
```sql
-- Oracle (Complex but exact)
'`' || LPAD(CAST(SPGRPCD AS VARCHAR), 5, '0') || ' ' || NAME AS GRP1

-- DuckDB Solution (Simplified, equivalent results)
SPGRPCD, NAME  -- Group by base fields
```

### 2. Implicit Type Conversions
**Oracle Challenge**: Automatic string-to-number conversion
```sql
-- Oracle (Works automatically)
WHERE TREE.DIA < PLOT.MACRO_BREAKPOINT_DIA

-- DuckDB (Explicit casting required)
WHERE TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DECIMAL), 9999)
```

### 3. NULL Handling Differences
**Oracle Challenge**: Different NULL behavior
```sql
-- Oracle NVL
NVL(field, default_value)

-- DuckDB COALESCE (standard SQL)
COALESCE(field, default_value)
```

### 4. Complex Nested CASE Logic
**Oracle Challenge**: Deeply nested diameter-based logic
```sql
-- Oracle Pattern (Preserve exactly)
CASE 
    WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP 
    ELSE 
        CASE LEAST(TREE.DIA, 5 - 0.001) 
            WHEN TREE.DIA THEN POP_STRATUM.ADJ_FACTOR_MICR 
            ELSE 
                CASE LEAST(TREE.DIA, COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DECIMAL), 9999) - 0.001) 
                    WHEN TREE.DIA THEN POP_STRATUM.ADJ_FACTOR_SUBP 
                    ELSE POP_STRATUM.ADJ_FACTOR_MACR 
                END 
        END 
END
```

## ✅ Validation and Testing

### 1. Result Comparison
```sql
-- Always include expected results in comments
/*
Expected Results:
- Oregon Total Trees: 10,481,113,490 (matches EVALIDator)
- Trees per Acre: 357.8
- Forest Area: 29,292,380 acres
*/
```

### 2. Sample Size Validation
```sql
-- Include plot counts for validation
SELECT 
    COUNT(DISTINCT p.CN) as plot_count,
    SUM([MAIN_CALCULATION]) as estimate
```

### 3. Cross-Validation Techniques
```sql
-- Compare simplified vs complex approaches
WITH simplified AS (SELECT SPGRPCD, SUM(biomass) as simple_total FROM ...),
     complex AS (SELECT extract_spgrpcd(GRP1), SUM(ESTIMATE) as complex_total FROM ...)
SELECT * FROM simplified s JOIN complex c ON s.SPGRPCD = c.spgrpcd
WHERE ABS(s.simple_total - c.complex_total) > 0.01  -- Flag differences
```

### 4. Reasonableness Checks
```sql
-- Validate against known ranges
SELECT 
    estimate,
    estimate / total_area as per_acre_rate,
    CASE 
        WHEN per_acre_rate > [REASONABLE_MAX] THEN 'SUSPECT HIGH'
        WHEN per_acre_rate < [REASONABLE_MIN] THEN 'SUSPECT LOW'
        ELSE 'REASONABLE'
    END as validation_flag
```

## 🎯 Best Practices

### 1. Maintain Statistical Integrity
- **Never modify core EVALIDator logic** without validation
- **Preserve exact Oracle calculations** for biomass and GRM
- **Use proper adjustment factors** for all estimates
- **Apply expansion factors** consistently

### 2. Simplify When Possible
- **Test simplified grouping approaches** (e.g., direct SPGRPCD vs LPAD formatting)
- **Validate simplified versions** produce identical results
- **Document equivalence** between approaches
- **Choose readability** when accuracy is maintained

### 3. Documentation Standards
```sql
/*
Query: [Purpose and Description]
EVALID: [Evaluation ID]
Expected Result: [Known validation result]
Oracle Compatibility: [Exact translation / Simplified equivalent]
Validation: [How results were verified]
*/
```

### 4. Error Handling
```sql
-- Include defensive programming
WHERE TREE.TPA_UNADJ IS NOT NULL
    AND TREE.STATUSCD IS NOT NULL
    AND POP_STRATUM.EXPNS IS NOT NULL
    AND POP_STRATUM.EXPNS > 0  -- Avoid division by zero
```

### 5. Performance Optimization
```sql
-- Use appropriate indexes
-- Consider CTEs for complex logic
-- Leverage DuckDB-specific optimizations
-- Test with full datasets, not samples
```

## 🔍 Advanced Topics

### Special Species Mappings
```sql
-- Handle special cases (e.g., eastern redcedar state variations)
REF_SPECIES_GROUP_TREE.SPGRPCD = CASE 
    WHEN TREE.STATECD IN (46, 38, 31, 20) AND TREE.SPCD = 122 THEN 11 
    ELSE TREE.SPGRPCD 
END
```

### Biomass Equation Complexity
```sql
-- Full species-specific biomass calculation
COALESCE(
    (
        (1 - (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT))) *
        REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT /
        [COMPLEX_DENOMINATOR] *
        (1.0 + REF_SPECIES.MC_PCT_GREEN_WOOD * 0.01) +
        [BARK_CALCULATION]
    ), 
    1.76  -- Default specific gravity
)
```

### GRM Component Logic
```sql
-- Mortality vs Harvest component separation
CASE 
    WHEN GRM.COMPONENT LIKE 'MORTALITY%' THEN TRE_MIDPT.VOLCFNET 
    ELSE 0 
END

-- vs

CASE 
    WHEN GRM.COMPONENT LIKE 'CUT%' THEN TRE_MIDPT.VOLCFNET 
    ELSE 0 
END
```

## 📚 Resources and References

### Official Documentation
- **FIA Database Description**: Latest field definitions and relationships
- **EVALIDator User Guide**: Official methodology documentation
- **FIA Sampling Manual**: Statistical framework and procedures

### Validation Sources
- **Published FIA Reports**: State-level forest statistics
- **EVALIDator Web Interface**: Direct result comparison
- **Research Publications**: Peer-reviewed methodology papers

### Technical References
- **Oracle SQL Documentation**: Original function behavior
- **DuckDB Documentation**: Modern SQL capabilities and optimizations
- **Statistical Sampling Theory**: Understanding FIA methodology

---

This guide provides the foundation for accurate EVALIDator translation while maintaining the statistical rigor required for official FIA analysis. Always validate results against known benchmarks and document any deviations from Oracle methodology. 