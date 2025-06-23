-- North Carolina Annual Tree Mortality Rate
-- Average annual mortality rate in trees per acre on forest land
-- EVALID: 372303 (North Carolina 2023 Growth/Removal/Mortality evaluation)

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