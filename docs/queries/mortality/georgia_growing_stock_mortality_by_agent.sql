-- Georgia Average Annual Mortality of Growing-Stock Trees by Damage Agent and Species
-- Trees at least 5 inches d.b.h. on timberland
-- EVALID: 132303 (Georgia 2023 Growth/Removal/Mortality evaluation)

SELECT
    GRP1 as damage_agent,
    GRP2 as species,
    SUM(ESTIMATED_VALUE * EXPNS) as ESTIMATE
FROM (
    SELECT
        pop_stratum.estn_unit_cn,
        pop_stratum.cn as STRATACN,
        plot.cn as plot_cn,
        plot.prev_plt_cn,
        cond.cn as cond_cn,
        plot.lat,
        plot.lon,
        pop_stratum.expns as EXPNS,

        -- Damage agent grouping
        CASE COALESCE(tree.agentcd, -1)
            WHEN -1 THEN '`0011 Not available'
            WHEN 0 THEN '`0001 No serious damage'
            ELSE CASE
                WHEN tree.agentcd <= 19 THEN '`0002 Insect'
                WHEN tree.agentcd <= 29 THEN '`0003 Disease'
                WHEN tree.agentcd <= 39 THEN '`0004 Fire'
                WHEN tree.agentcd <= 49 THEN '`0005 Animal'
                WHEN tree.agentcd <= 59 THEN '`0006 Weather'
                WHEN tree.agentcd <= 69 THEN '`0007 Vegetation'
                WHEN tree.agentcd <= 79 THEN '`0008 Unknown/other'
                WHEN tree.agentcd <= 89 THEN '`0009 Logging/human'
                WHEN tree.agentcd <= 99 THEN '`0010 Physical'
                ELSE '`0011 Not available'
            END
        END as GRP1,

        -- Species grouping with formatting
        '`' ||
        CASE
            WHEN REF_SPECIES_SPP.SPCD IS NULL THEN '9999'
            ELSE printf('%04d', REF_SPECIES_SPP.SPCD)
        END ||
        CASE
            WHEN REF_SPECIES_SPP.SPCD IS NULL THEN 'Other or Unknown'
            ELSE ' SPCD ' || printf('%04d', REF_SPECIES_SPP.SPCD) || ' - ' ||
                 REF_SPECIES_SPP.COMMON_NAME || ' (' ||
                 REF_SPECIES_SPP.GENUS || ' ' || REF_SPECIES_SPP.SPECIES ||
                 CASE
                     WHEN REF_SPECIES_SPP.VARIETY IS NULL THEN ')'
                     ELSE ' ' || REF_SPECIES_SPP.VARIETY || ')'
                 END
        END as GRP2,

        -- Growing stock mortality calculation
        SUM(
            GRM.TPAMORT_UNADJ *
            -- Adjustment factor based on subplot type
            CASE
                WHEN COALESCE(GRM.SUBPTYP_GRM, 0) = 0 THEN 0
                WHEN GRM.SUBPTYP_GRM = 1 THEN POP_STRATUM.ADJ_FACTOR_SUBP
                WHEN GRM.SUBPTYP_GRM = 2 THEN POP_STRATUM.ADJ_FACTOR_MICR
                WHEN GRM.SUBPTYP_GRM = 3 THEN POP_STRATUM.ADJ_FACTOR_MACR
                ELSE 0
            END *
            -- Only mortality components
            CASE
                WHEN GRM.COMPONENT LIKE 'MORTALITY%' THEN 1
                ELSE 0
            END
        ) AS ESTIMATED_VALUE

    FROM POP_STRATUM pop_stratum
    JOIN POP_PLOT_STRATUM_ASSGN pop_plot_stratum_assgn
        ON pop_stratum.CN = pop_plot_stratum_assgn.STRATUM_CN
    JOIN PLOT plot
        ON pop_plot_stratum_assgn.PLT_CN = plot.CN
    JOIN PLOTGEOM plotgeom
        ON plot.CN = plotgeom.CN
    JOIN COND cond
        ON plot.CN = cond.PLT_CN
    JOIN (
        -- Join current plot to tree with previous plot info
        SELECT p.PREV_PLT_CN, t.*
        FROM PLOT p
        JOIN TREE t ON p.CN = t.PLT_CN
    ) tree
        ON tree.CONDID = cond.CONDID AND tree.PLT_CN = cond.PLT_CN
    LEFT OUTER JOIN PLOT pplot
        ON plot.PREV_PLT_CN = pplot.CN
    LEFT OUTER JOIN COND pcond
        ON tree.PREVCOND = pcond.CONDID AND tree.PREV_PLT_CN = pcond.PLT_CN
    LEFT OUTER JOIN TREE ptree
        ON tree.PREV_TRE_CN = ptree.CN
    LEFT OUTER JOIN TREE_GRM_BEGIN tre_begin
        ON tree.CN = tre_begin.TRE_CN
    LEFT OUTER JOIN TREE_GRM_MIDPT tre_midpt
        ON tree.CN = tre_midpt.TRE_CN
    LEFT OUTER JOIN (
        -- Growing stock mortality components
        SELECT
            TRE_CN,
            DIA_BEGIN,
            DIA_MIDPT,
            DIA_END,
            SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
            SUBP_SUBPTYP_GRM_GS_TIMBER AS SUBPTYP_GRM,
            SUBP_TPAMORT_UNADJ_GS_TIMBER AS TPAMORT_UNADJ
        FROM TREE_GRM_COMPONENT
    ) grm
        ON tree.CN = grm.TRE_CN
    LEFT OUTER JOIN REF_SPECIES ref_species_spp
        ON tree.SPCD = ref_species_spp.SPCD

    WHERE
        -- Georgia GRM evaluation
        pop_stratum.rscd = 33
        AND pop_stratum.evalid = 132303

    GROUP BY
        pop_stratum.estn_unit_cn,
        pop_stratum.cn,
        plot.cn,
        plot.prev_plt_cn,
        cond.cn,
        plot.lat,
        plot.lon,
        pop_stratum.expns,
        GRP1,
        GRP2,
        ref_species_spp.SPCD,
        ref_species_spp.COMMON_NAME,
        ref_species_spp.GENUS,
        ref_species_spp.SPECIES,
        ref_species_spp.VARIETY
) subquery
GROUP BY GRP1, GRP2
ORDER BY GRP1, GRP2;