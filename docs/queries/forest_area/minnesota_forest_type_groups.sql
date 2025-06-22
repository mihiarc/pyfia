/*
Minnesota Forest Area by Forest Type Group (EVALIDator-Style)
EVALID: 272201 (Minnesota 2022)
Result: 17,599,046 total forest acres, 10 forest type groups
*/

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