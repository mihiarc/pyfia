-- Colorado Annual Mortality of Merchantable Volume
-- Average annual mortality of merchantable bole wood volume of growing-stock trees
-- on forest land in Colorado
-- EVALID: 82003 (Colorado 2020 Growth/Removal/Mortality evaluation)

-- Total Annual Mortality Query
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