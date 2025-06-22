# Common Pitfalls to Avoid in FIA Queries

This guide highlights the most frequent mistakes made when writing FIA database queries and how to avoid them.

## ⚠️ Statistical and Methodological Pitfalls

### 1. **Don't filter by year alone** - use EVALID for proper statistical grouping
**❌ Wrong:**
```sql
WHERE INVYR = 2021
```

**✅ Correct:**
```sql
WHERE POP_STRATUM.EVALID = 412101  -- Oregon 2021 specific evaluation
```

**Why**: Years can span multiple evaluations with different methodologies. EVALIDs ensure consistent statistical treatment.

### 2. **Don't ignore expansion factors** - raw counts are not meaningful
**❌ Wrong:**
```sql
SELECT COUNT(*) FROM TREE WHERE STATUSCD = 1
```

**✅ Correct:**
```sql
SELECT SUM(TPA_UNADJ * ADJ_FACTOR * EXPNS) FROM TREE...
```

**Why**: FIA uses statistical sampling. Raw counts don't represent population estimates.

### 3. **Don't mix EVALIDs** - each evaluation is statistically independent
**❌ Wrong:**
```sql
WHERE EVALID IN (412101, 412201)  -- Mixing Oregon 2021 and 2022
```

**✅ Correct:**
```sql
WHERE EVALID = 412101  -- Single evaluation only
```

**Why**: Different EVALIDs use different sampling designs and cannot be combined.

### 4. **Don't forget status codes** - include appropriate filters
**❌ Wrong:**
```sql
SELECT * FROM TREE  -- Includes dead, cut, missing trees
```

**✅ Correct:**
```sql
SELECT * FROM TREE WHERE STATUSCD = 1  -- Live trees only
```

**Why**: Trees have different status codes (live, dead, cut, etc.). Always filter appropriately.

### 5. **Don't ignore NULL values** - they can skew calculations
**❌ Wrong:**
```sql
SELECT AVG(DIA) FROM TREE  -- NULLs excluded from average
```

**✅ Correct:**
```sql
SELECT AVG(DIA) FROM TREE WHERE DIA IS NOT NULL
-- OR use COALESCE for defaults
SELECT AVG(COALESCE(DIA, 0)) FROM TREE
```

**Why**: NULL handling varies by database. Be explicit about how to treat missing values.

## 🚨 CRITICAL: Analysis Type Confusion

### 6. **🔥 Don't confuse forest type vs species analysis**

This is the **most critical pitfall** in FIA analysis:

**❌ Wrong interpretation:**
"What's the TPA of loblolly pine?" could mean two completely different things:

**Option A - Species Analysis:**
- TPA of loblolly pine trees (SPCD 131) across all forest types
- Result: ~150 TPA

**Option B - Forest Type Analysis:**  
- TPA of all trees in loblolly pine forest types (FORTYPCD 161, 406)
- Result: ~770 TPA

**✅ Always clarify which interpretation is needed:**
```sql
-- Species analysis: Loblolly pine trees only
WHERE TREE.SPCD = 131

-- Forest type analysis: All trees in loblolly pine forests  
WHERE COND.FORTYPCD IN (161, 406)
```

**Results can differ by 5x or more!**

## 🌲 Growth, Removal, and Mortality (GRM) Pitfalls

### 7. **For GRM Queries: Don't add restrictive filters or mix components**

**❌ Wrong:**
```sql
-- Don't add diameter restrictions
WHERE TREE.DIA >= 5.0

-- Don't mix mortality and harvest components
WHERE (GRM.COMPONENT LIKE 'MORTALITY%' OR GRM.COMPONENT LIKE 'CUT%')
```

**✅ Correct:**
```sql
-- Keep original Oracle EVALIDator logic - no additional filters
-- Use separate queries for mortality vs harvest
```

**Why**: 
- Original Oracle EVALIDator queries include all tree sizes and land types
- Adding DIA ≥5" or timberland-only filters can change results significantly
- Mortality and harvest represent different processes and shouldn't be combined

### 8. **Don't use wrong TPA fields for GRM analysis**

**❌ Wrong:**
```sql
-- Using regular TPA_UNADJ for GRM analysis
SELECT SUM(TREE.TPA_UNADJ * VOLCFNET)
```

**✅ Correct:**
```sql
-- Use GRM-specific TPA fields
SELECT SUM(GRM.TPAMORT_UNADJ * VOLCFNET)  -- For mortality
SELECT SUM(GRM.TPAREMV_UNADJ * VOLCFNET)  -- For removals
```

**Why**: GRM tables have specific TPA fields that account for growth and change processes.

## 🌿 Biomass and Carbon Pitfalls

### 9. **For Biomass Queries: Don't simplify complex calculations**

**❌ Wrong:**
```sql
-- Oversimplified biomass calculation
SELECT SUM(DRYBIO_AG) FROM TREE
```

**✅ Correct:**
```sql
-- Include all species-specific adjustments
COALESCE(
    (complex_wood_calculation + complex_bark_calculation),
    1.76  -- Default specific gravity
) * DRYBIO_AG / 2000  -- Convert pounds to tons
```

**Why**: Biomass calculations require species-specific wood and bark properties, moisture content adjustments, and proper unit conversions.

### 10. **Don't omit species-specific properties**

**❌ Wrong:**
```sql
-- Ignoring species differences
SELECT SUM(DRYBIO_AG) FROM TREE
```

**✅ Correct:**
```sql
-- Include species-specific properties
JOIN REF_SPECIES ON (TREE.SPCD = REF_SPECIES.SPCD)
-- Use WOOD_SPGR_GREENVOL_DRYWT, BARK_SPGR_GREENVOL_DRYWT, etc.
```

**Why**: Different species have vastly different wood and bark properties that affect biomass calculations.

### 11. **Don't ignore moisture content adjustments**

**❌ Wrong:**
```sql
-- Using green volume without moisture adjustment
volume * specific_gravity
```

**✅ Correct:**
```sql
-- Include moisture content adjustments
volume * specific_gravity * (1.0 + MC_PCT_GREEN_WOOD * 0.01)
```

**Why**: Green volume includes moisture that must be accounted for in dry biomass calculations.

### 12. **Don't skip type casting for critical fields**

**❌ Wrong:**
```sql
CASE WHEN TREE.DIA < PLOT.MACRO_BREAKPOINT_DIA THEN...
```

**✅ Correct:**
```sql
CASE WHEN TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DECIMAL), 9999) THEN...
```

**Why**: Some fields may be stored as VARCHAR and need explicit casting for numeric comparisons.

## 🔍 Query Structure and Performance Pitfalls

### 13. **Don't skip proper table joins**

**❌ Wrong:**
```sql
-- Missing POP_PLOT_STRATUM_ASSGN join
FROM PLOT 
JOIN POP_STRATUM ON ...  -- Direct join without assignment table
```

**✅ Correct:**
```sql
FROM POP_STRATUM
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
```

**Why**: The assignment table is essential for proper statistical weighting.

### 14. **Don't forget condition-level joins**

**❌ Wrong:**
```sql
-- Missing condition linkage
JOIN TREE ON (TREE.PLT_CN = PLOT.CN)
```

**✅ Correct:**
```sql
-- Include condition linkage
JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)
```

**Why**: Trees are associated with specific conditions within plots.

### 15. **Don't use inefficient grouping strategies**

**❌ Wrong:**
```sql
-- Grouping by complex calculated fields
GROUP BY LPAD(CAST(SPGRPCD AS VARCHAR), 5, '0') || ' ' || NAME
```

**✅ Correct:**
```sql
-- Group by base fields, format in SELECT
GROUP BY SPGRPCD, NAME
```

**Why**: Grouping by calculated fields can be inefficient and harder to maintain.

## 📝 Documentation and Maintenance Pitfalls

### 16. **Don't skip query documentation**

**❌ Wrong:**
```sql
SELECT SUM(complicated_calculation) FROM multiple_tables WHERE complex_conditions;
```

**✅ Correct:**
```sql
/*
Purpose: Calculate biomass by species group for Colorado 2021
EVALID: 82101
Expected Result: 1.096 billion tons
Methodology: Oracle EVALIDator compatible with species-specific adjustments
*/
SELECT ... -- Well-documented query
```

**Why**: Complex FIA queries need documentation for maintenance and validation.

### 17. **Don't assume query portability**

**❌ Wrong:**
```sql
-- Oracle-specific syntax in DuckDB
SELECT LPAD(field, 5, '0') FROM table
```

**✅ Correct:**
```sql
-- Database-appropriate syntax
SELECT printf('%05d', field) FROM table  -- DuckDB
-- OR test and document database-specific versions
```

**Why**: Different databases have different SQL dialects and functions.

### 18. **Don't neglect validation**

**❌ Wrong:**
```sql
-- No validation or expected results
SELECT calculation FROM tables;
```

**✅ Correct:**
```sql
-- Include validation checks and expected results
SELECT calculation FROM tables;
-- Expected: 10.48 billion trees (matches EVALIDator result)
```

**Why**: FIA queries should be validated against known results to ensure accuracy.

## 🎯 Key Takeaways

1. **Always use EVALIDs**, not years, for statistical grouping
2. **Understand forest type vs species analysis** - the most critical distinction
3. **Include all expansion factors** for meaningful results
4. **Follow exact Oracle EVALIDator methodology** for GRM and biomass queries
5. **Document extensively** - FIA queries are complex and need good documentation
6. **Validate results** against known benchmarks
7. **Be explicit about NULL handling** and status code filtering
8. **Use appropriate table joins** including POP_PLOT_STRATUM_ASSGN

Following these guidelines will help you avoid the most common mistakes and produce reliable, accurate FIA analyses. 