-- Missouri Forest Area Change by Forest Type Group
-- EVALID: 292403 (Missouri 2024 Change evaluation)
-- Translated from EVALIDator Oracle query to DuckDB

SELECT 
    GRP1,
    GRP2,
    SUM(ESTIMATED_VALUE * EXPNS) AS ESTIMATE
FROM (
    SELECT 
        pop_stratum.estn_unit_cn,
        pop_stratum.cn AS STRATACN,
        plot.cn AS plot_cn,
        plot.prev_plt_cn,
        cond.cn AS cond_cn,
        plot.lat,
        plot.lon,
        pop_stratum.expns AS EXPNS,
        -- Format forest type group code with padding and name
        '`' || printf('%04d', COALESCE(REF_FORTYGPCDSQ.FORTYGPCD, 999)) || ' ' || 
        COALESCE(REF_FORTYGPCDSQ.MEANING, 'Nonstocked') AS GRP1,
        -- Format EVALID grouping
        CASE COALESCE(pop_stratum.evalid, -1)
            WHEN 292403 THEN '`0029 292024 Missouri 2024'
            WHEN -1 THEN '`98 Unavailable'
            ELSE '`99 Unavailable'
        END AS GRP2,
        -- Calculate forest area change
        SUM(COALESCE(
            SCCM.SUBPTYP_PROP_CHNG / 4 * 
            CASE cond.PROP_BASIS 
                WHEN 'MACR' THEN pop_stratum.ADJ_FACTOR_MACR 
                ELSE pop_stratum.ADJ_FACTOR_SUBP 
            END, 
            0
        )) AS ESTIMATED_VALUE
    FROM POP_STRATUM pop_stratum
    JOIN POP_PLOT_STRATUM_ASSGN pop_plot_stratum_assgn 
        ON pop_plot_stratum_assgn.STRATUM_CN = pop_stratum.CN
    JOIN PLOT plot 
        ON pop_plot_stratum_assgn.PLT_CN = plot.CN
    JOIN PLOTGEOM plotgeom 
        ON plot.CN = plotgeom.CN
    JOIN COND cond 
        ON cond.PLT_CN = plot.CN
    JOIN COND pcond 
        ON pcond.PLT_CN = plot.PREV_PLT_CN
    JOIN SUBP_COND_CHNG_MTRX sccm 
        ON sccm.PLT_CN = cond.PLT_CN 
        AND sccm.PREV_PLT_CN = pcond.PLT_CN 
        AND sccm.CONDID = cond.CONDID 
        AND sccm.PREVCOND = pcond.CONDID
    LEFT JOIN (
        SELECT 
            ref_forest_type.VALUE AS FORTYPC,
            ref_forest_type_group.VALUE AS FORTYGPCD,
            ref_forest_type_group.MEANING
        FROM REF_FOREST_TYPE ref_forest_type
        JOIN REF_FOREST_TYPE_GROUP ref_forest_type_group 
            ON ref_forest_type.TYPGRPCD = ref_forest_type_group.VALUE
    ) REF_FORTYGPCDSQ 
        ON REF_FORTYGPCDSQ.FORTYPC = cond.fortypcd
    WHERE 
        cond.CONDPROP_UNADJ IS NOT NULL
        AND ((sccm.SUBPTYP = 3 AND cond.PROP_BASIS = 'MACR') 
             OR (sccm.SUBPTYP = 1 AND cond.PROP_BASIS = 'SUBP'))
        AND COALESCE(cond.COND_NONSAMPLE_REASN_CD, 0) = 0
        AND COALESCE(pcond.COND_NONSAMPLE_REASN_CD, 0) = 0
        AND cond.COND_STATUS_CD = 1 
        AND pcond.COND_STATUS_CD = 1
        AND pop_stratum.rscd = 23 
        AND pop_stratum.evalid = 292403
    GROUP BY 
        pop_stratum.estn_unit_cn,
        pop_stratum.cn,
        plot.cn,
        plot.prev_plt_cn,
        cond.cn,
        plot.lat,
        plot.lon,
        pop_stratum.expns,
        REF_FORTYGPCDSQ.FORTYGPCD,
        REF_FORTYGPCDSQ.MEANING,
        pop_stratum.evalid
) subquery
GROUP BY GRP1, GRP2
ORDER BY GRP1, GRP2;