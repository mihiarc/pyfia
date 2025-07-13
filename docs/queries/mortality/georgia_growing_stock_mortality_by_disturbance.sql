-- Georgia Average Annual Mortality of Growing-Stock Trees by Disturbance Type and Species
-- Trees at least 5 inches d.b.h. on timberland
-- EVALID: 132303 (Georgia 2023 Growth/Removal/Mortality evaluation)

SELECT
    GRP1 as disturbance_type,
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

        -- Disturbance type grouping (condition-level)
        CASE COALESCE(cond.dstrbcd1, -1)
            WHEN 0 THEN '`0001 No visible disturbance'
            WHEN 10 THEN '`0002 Insect damage'
            WHEN 11 THEN '`0003 Insect damage to understory vegetation'
            WHEN 12 THEN '`0004 Insect damage to trees including seedlings and saplings'
            WHEN 20 THEN '`0005 Disease damage'
            WHEN 21 THEN '`0006 Disease damage to understory vegetation'
            WHEN 22 THEN '`0007 Disease damage to trees, including seedlings and saplings'
            WHEN 30 THEN '`0008 Fire (from crown and ground fire, either prescribed or natural)'
            WHEN 31 THEN '`0009 Ground fire damage'
            WHEN 32 THEN '`0010 Crown fire damage'
            WHEN 40 THEN '`0011 Animal damage'
            WHEN 41 THEN '`0012 Beaver (includes flooding caused by beaver)'
            WHEN 42 THEN '`0013 Porcupine'
            WHEN 43 THEN '`0014 Deer/ungulate'
            WHEN 44 THEN '`0015 Bear'
            WHEN 45 THEN '`0016 Rabbit'
            WHEN 46 THEN '`0017 Domestic animal/livestock (includes grazing)'
            WHEN 50 THEN '`0018 Weather damage'
            WHEN 51 THEN '`0019 Ice'
            WHEN 52 THEN '`0020 Wind (includes hurricane, tornado)'
            WHEN 53 THEN '`0021 Flooding (weather induced)'
            WHEN 54 THEN '`0022 Drought'
            WHEN 60 THEN '`0023 Vegetation (suppression,competition,vines)'
            WHEN 70 THEN '`0024 Unknown/not sure/other'
            WHEN 80 THEN '`0025 Human-caused damage'
            WHEN 90 THEN '`0026 Geologic disturbance'
            WHEN 91 THEN '`0027 Landslide'
            WHEN 92 THEN '`0028 Avalanche track'
            WHEN 93 THEN '`0029 Volcanic blast zone'
            WHEN 94 THEN '`0030 Other geologic event'
            WHEN 95 THEN '`0031 Earth movement/avalanches'
            WHEN -1 THEN '`0032 Not available'
            ELSE '`0032 Other'
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