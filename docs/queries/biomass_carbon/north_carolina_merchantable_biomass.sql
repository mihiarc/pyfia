-- North Carolina Merchantable Bole Biomass by Diameter Class and Species
-- Live trees (at least 5 inches d.b.h.) on forest land
-- Results in green short tons
-- EVALID: 372301 (North Carolina 2023 Volume evaluation)

SELECT 
    GRP1 as diameter_class,
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
        
        -- Diameter class grouping
        CASE 
            WHEN COALESCE(tree.dia, -1) = -1 THEN '`0022 Not available'
            WHEN tree.dia = 0 OR tree.dia IS NULL THEN '`0022 not measured'
            WHEN tree.dia <= 2.99 THEN '`0001 1.0-2.9'
            WHEN tree.dia <= 4.99 THEN '`0002 3.0-4.9'
            WHEN tree.dia <= 6.99 THEN '`0003 5.0-6.9'
            WHEN tree.dia <= 8.99 THEN '`0004 7.0-8.9'
            WHEN tree.dia <= 10.99 THEN '`0005 9.0-10.9'
            WHEN tree.dia <= 12.99 THEN '`0006 11.0-12.9'
            WHEN tree.dia <= 14.99 THEN '`0007 13.0-14.9'
            WHEN tree.dia <= 16.99 THEN '`0008 15.0-16.9'
            WHEN tree.dia <= 18.99 THEN '`0009 17.0-18.9'
            WHEN tree.dia <= 20.99 THEN '`0010 19.0-20.9'
            WHEN tree.dia <= 22.99 THEN '`0011 21.0-22.9'
            WHEN tree.dia <= 24.99 THEN '`0012 23.0-24.9'
            WHEN tree.dia <= 26.99 THEN '`0013 25.0-26.9'
            WHEN tree.dia <= 28.99 THEN '`0014 27.0-28.9'
            WHEN tree.dia <= 30.99 THEN '`0015 29.0-30.9'
            WHEN tree.dia <= 32.99 THEN '`0016 31.0-32.9'
            WHEN tree.dia <= 34.99 THEN '`0017 33.0-34.9'
            WHEN tree.dia <= 36.99 THEN '`0018 35.0-36.9'
            WHEN tree.dia <= 38.99 THEN '`0019 37.0-38.9'
            WHEN tree.dia <= 40.99 THEN '`0020 39.0-40.9'
            ELSE '`0021 41.0+'
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
        
        -- Merchantable bole biomass calculation (bark + wood)
        SUM(
            TREE.TPA_UNADJ * 
            -- Green weight conversion factor
            COALESCE(
                (
                    -- Wood component
                    (1 - (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT))) *
                    REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT /
                    (
                        (1 - (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT))) *
                        REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT +
                        (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT)) *
                        REF_SPECIES.BARK_SPGR_GREENVOL_DRYWT
                    ) *
                    (1.0 + REF_SPECIES.MC_PCT_GREEN_WOOD * 0.01) +
                    -- Bark component
                    (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT)) *
                    REF_SPECIES.BARK_SPGR_GREENVOL_DRYWT /
                    (
                        (1 - (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT))) *
                        REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT +
                        (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT)) *
                        REF_SPECIES.BARK_SPGR_GREENVOL_DRYWT
                    ) *
                    (1.0 + REF_SPECIES.MC_PCT_GREEN_BARK * 0.01)
                ),
                1.76  -- Default when species data unavailable
            ) * 
            -- Adjustment factor based on plot design
            CASE 
                WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP
                WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
                WHEN TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN POP_STRATUM.ADJ_FACTOR_SUBP
                ELSE POP_STRATUM.ADJ_FACTOR_MACR
            END * 
            -- Merchantable bole biomass (using DRYBIO_BOLE + DRYBIO_BOLE_BARK)
            COALESCE((TREE.DRYBIO_BOLE + TREE.DRYBIO_BOLE_BARK) / 2000, 0)  -- Convert pounds to tons
        ) AS ESTIMATED_VALUE
        
    FROM POP_STRATUM pop_stratum
    JOIN POP_PLOT_STRATUM_ASSGN pop_plot_stratum_assgn 
        ON pop_plot_stratum_assgn.STRATUM_CN = pop_stratum.CN
    JOIN PLOT plot 
        ON pop_plot_stratum_assgn.PLT_CN = plot.CN
    JOIN PLOTGEOM plotgeom 
        ON plot.CN = plotgeom.CN
    JOIN COND cond 
        ON cond.PLT_CN = plot.CN
    JOIN TREE tree 
        ON tree.PLT_CN = cond.PLT_CN AND tree.CONDID = cond.CONDID
    JOIN REF_SPECIES ref_species 
        ON tree.SPCD = ref_species.SPCD
    LEFT OUTER JOIN REF_SPECIES ref_species_spp 
        ON tree.SPCD = ref_species_spp.SPCD
        
    WHERE 
        tree.STATUSCD = 1  -- Live trees
        AND cond.COND_STATUS_CD = 1  -- Forest land
        AND tree.DIA >= 5.0  -- At least 5 inches DBH
        AND pop_stratum.rscd = 33  -- North Carolina
        AND pop_stratum.evalid = 372301  -- 2023 evaluation
        
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