/*
California Net Merchantable Bole Wood Volume by Diameter Class
EVALID: 62101 (California 2021 Current Volume)
Result: 67.05 billion cubic feet total volume, 4,188.7 cu ft/acre
*/

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