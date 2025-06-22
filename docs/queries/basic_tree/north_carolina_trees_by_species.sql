/*
North Carolina Live Trees by Species (EVALIDator-Style)
EVALID: 372301 (North Carolina 2023)
Result: 13,541,944,859 total trees, 129 species, 729.1 trees/acre
*/

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