# EVALIDator Quick Reference Card

This quick reference provides the most common patterns for translating Oracle EVALIDator queries to DuckDB.

## 🚀 Quick Start Templates

### Basic Tree Count Query
```sql
SELECT
    SUM(
        t.TPA_UNADJ *
        CASE
            WHEN t.DIA IS NULL THEN ps.ADJ_FACTOR_SUBP
            WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
            WHEN t.DIA < COALESCE(CAST(p.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0)
                THEN ps.ADJ_FACTOR_SUBP
            ELSE ps.ADJ_FACTOR_MACR
        END * ps.EXPNS
    ) AS total_trees
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
JOIN TREE t ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
WHERE t.STATUSCD = 1
    AND c.COND_STATUS_CD = 1
    AND ps.EVALID = [YOUR_EVALID];
```

### Basic Area Query
```sql
SELECT
    SUM(
        c.CONDPROP_UNADJ *
        CASE c.PROP_BASIS
            WHEN 'MACR' THEN ps.ADJ_FACTOR_MACR
            ELSE ps.ADJ_FACTOR_SUBP
        END * ps.EXPNS
    ) AS total_area_acres
FROM POP_STRATUM ps
JOIN POP_PLOT_STRATUM_ASSGN ppsa ON ppsa.STRATUM_CN = ps.CN
JOIN PLOT p ON ppsa.PLT_CN = p.CN
JOIN COND c ON c.PLT_CN = p.CN
WHERE c.COND_STATUS_CD = 1
    AND ps.EVALID = [YOUR_EVALID];
```

## 🔄 Oracle to DuckDB Function Translations

| Oracle | DuckDB | Example |
|--------|--------|---------|
| `LPAD(field, 4, '0')` | `printf('%04d', field)` | Species code formatting |
| `NVL(field, 0)` | `COALESCE(field, 0)` | NULL handling |
| `DECODE(x, 1, 'A', 2, 'B', 'C')` | `CASE WHEN x = 1 THEN 'A' WHEN x = 2 THEN 'B' ELSE 'C' END` | Conditional logic |

## 📊 Common EVALID Patterns

### Current Volume Evaluations (Type 01)
```sql
-- Examples: 412101 (OR 2021), 62101 (CA 2021), 482101 (TX 2021)
WHERE ps.EVALID = [STATE][YEAR]01
```

### Growth/Removal/Mortality (Type 03)
```sql
-- Examples: 132303 (GA 2023), 452303 (SC 2023), 372303 (NC 2023)
WHERE ps.EVALID = [STATE][YEAR]03
```

### All Area Evaluations (Type 00)
```sql
-- Examples: 412100 (OR 2021), 132300 (GA 2023)
WHERE ps.EVALID = [STATE][YEAR]00
```

## 🌲 Standard Filters

### Live Trees on Forest Land
```sql
WHERE t.STATUSCD = 1
    AND c.COND_STATUS_CD = 1
```

### Merchantable Timber
```sql
WHERE t.STATUSCD = 1
    AND c.COND_STATUS_CD = 1
    AND t.DIA >= 5.0
    AND rs.WOODLAND = 'N'
    AND c.SITECLCD IN (1,2,3,4,5,6)
    AND c.RESERVCD = 0
```

### Timberland Only
```sql
WHERE c.COND_STATUS_CD = 1
    AND c.SITECLCD IN (1,2,3,4,5,6)
    AND c.RESERVCD = 0
```

## 🎯 Species and Forest Type Grouping

### By Species
```sql
-- Simple approach (recommended)
GROUP BY t.SPCD, rs.COMMON_NAME

-- Oracle EVALIDator style (complex but exact)
GROUP BY '`' || LPAD(CAST(t.SPCD AS VARCHAR), 4, '0') || ' ' || rs.COMMON_NAME
```

### By Species Group
```sql
-- Simple approach
GROUP BY t.SPGRPCD, rsg.NAME

-- Oracle EVALIDator style
GROUP BY '`' || LPAD(CAST(t.SPGRPCD AS VARCHAR), 5, '0') || ' ' || rsg.NAME
```

### By Forest Type
```sql
GROUP BY c.FORTYPCD, rft.MEANING
```

## 📈 Volume and Biomass Calculations

### Net Cubic Foot Volume
```sql
SUM(
    t.TPA_UNADJ *
    t.VOLCFNET *
    [ADJUSTMENT_FACTOR] *
    ps.EXPNS
) AS total_volume_cuft
```

### Above-Ground Dry Biomass
```sql
SUM(
    t.TPA_UNADJ *
    [ADJUSTMENT_FACTOR] *
    COALESCE(t.DRYBIO_AG / 2000, 0) *  -- Convert pounds to tons
    ps.EXPNS
) AS total_biomass_tons
```

## 🔄 GRM Query Patterns

### Mortality Query Structure
```sql
-- Complex GRM joins required
LEFT OUTER JOIN TREE_GRM_MIDPT TRE_MIDPT ON (TREE.CN = TRE_MIDPT.TRE_CN)
LEFT OUTER JOIN (
    SELECT
        TRE_CN,
        SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
        SUBP_SUBPTYP_GRM_GS_TIMBER AS SUBPTYP_GRM,
        SUBP_TPAMORT_UNADJ_GS_TIMBER AS TPAMORT_UNADJ
    FROM TREE_GRM_COMPONENT
) GRM ON (TREE.CN = GRM.TRE_CN)

-- Mortality calculation
SUM(
    GRM.TPAMORT_UNADJ *
    CASE GRM.SUBPTYP_GRM
        WHEN 1 THEN ps.ADJ_FACTOR_SUBP
        WHEN 2 THEN ps.ADJ_FACTOR_MICR
        WHEN 3 THEN ps.ADJ_FACTOR_MACR
        ELSE 0
    END *
    CASE WHEN GRM.COMPONENT LIKE 'MORTALITY%'
        THEN TRE_MIDPT.VOLCFNET ELSE 0 END *
    ps.EXPNS
) AS annual_mortality_cuft
```

### Harvest Removal Query Structure
```sql
-- Same GRM joins but different fields
LEFT OUTER JOIN (
    SELECT
        TRE_CN,
        SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
        SUBP_SUBPTYP_GRM_GS_TIMBER AS SUBPTYP_GRM,
        SUBP_TPAREMV_UNADJ_GS_TIMBER AS TPAREMV_UNADJ  -- Note: REMV not MORT
    FROM TREE_GRM_COMPONENT
) GRM ON (TREE.CN = GRM.TRE_CN)

-- Harvest calculation
SUM(
    GRM.TPAREMV_UNADJ *  -- Note: REMV not MORTALITY
    [SAME_ADJUSTMENT_LOGIC] *
    CASE WHEN GRM.COMPONENT LIKE 'CUT%'  -- Note: CUT not MORTALITY
        THEN TRE_MIDPT.VOLCFNET ELSE 0 END *
    ps.EXPNS
) AS annual_harvest_cuft
```

## ⚡ Performance Tips

### Use Appropriate Indexes
```sql
-- Typical useful indexes
CREATE INDEX idx_pop_stratum_evalid ON POP_STRATUM(EVALID);
CREATE INDEX idx_tree_status ON TREE(STATUSCD);
CREATE INDEX idx_cond_status ON COND(COND_STATUS_CD);
```

### Leverage CTEs for Complex Logic
```sql
WITH tree_expansion AS (
    SELECT
        t.CN,
        t.TPA_UNADJ *
        CASE
            WHEN t.DIA IS NULL THEN ps.ADJ_FACTOR_SUBP
            -- ... rest of logic
        END * ps.EXPNS AS expanded_tpa
    FROM [JOINS]
)
SELECT SUM(expanded_tpa) FROM tree_expansion;
```

## ✅ Validation Checklist

- [ ] **EVALID specified**: Never mix different EVALIDs
- [ ] **Status codes included**: STATUSCD = 1, COND_STATUS_CD = 1
- [ ] **Adjustment factors applied**: Based on tree diameter
- [ ] **Expansion factors applied**: EXPNS for final estimates
- [ ] **NULL handling**: Use COALESCE or IS NOT NULL
- [ ] **Expected results documented**: Include known validation values
- [ ] **Plot counts included**: For sample size context

## 🚨 Critical Warnings

### ❌ Never Do This
```sql
-- DON'T mix EVALIDs
WHERE ps.EVALID IN (412101, 412100)  -- WRONG!

-- DON'T forget expansion
SELECT COUNT(*) FROM TREE  -- Raw counts are meaningless!

-- DON'T ignore status codes
SELECT SUM(TPA_UNADJ) FROM TREE  -- Includes dead trees!
```

### ✅ Always Do This
```sql
-- Single EVALID per query
WHERE ps.EVALID = 412101

-- Proper expansion
SELECT SUM(TPA_UNADJ * ADJ_FACTOR * EXPNS)

-- Appropriate filters
WHERE t.STATUSCD = 1 AND c.COND_STATUS_CD = 1
```

## 📚 State and EVALID Quick Reference

| State | Code | Recent EVALIDs |
|-------|------|----------------|
| Oregon | 41 | 412101 (volume), 412100 (area) |
| California | 06 | 62101 (volume), 62100 (area) |
| Georgia | 13 | 132303 (GRM), 132301 (volume) |
| North Carolina | 37 | 372303 (GRM), 372301 (volume) |
| South Carolina | 45 | 452303 (GRM), 452301 (volume) |
| Colorado | 08 | 82101 (volume), 82100 (area) |
| Minnesota | 27 | 272201 (volume), 272200 (area) |

---

For detailed methodology and advanced topics, see the [EVALIDator Methodology Guide](./evaluator_methodology.md).