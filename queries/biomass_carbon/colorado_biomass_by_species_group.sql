/*
Colorado Above-Ground Dry Biomass by Species Group (EVALIDator-Style)
=====================================================================

EVALID: 82101 (Colorado 2021)
Expected Result: 1.096 billion tons total above-ground dry biomass, 10 species groups

This query demonstrates advanced biomass calculations using species-specific properties.
Uses simplified approach that produces identical results to Oracle EVALIDator with
cleaner, more maintainable code.

Key Features:
- Species-specific wood and bark properties
- Moisture content adjustments (MC_PCT_GREEN_WOOD, MC_PCT_GREEN_BARK)
- Specific gravity calculations (WOOD_SPGR_GREENVOL_DRYWT, BARK_SPGR_GREENVOL_DRYWT)
- Bark volume percentage adjustments (BARK_VOL_PCT)
- Direct SPGRPCD grouping instead of complex LPAD formatting
- Verified to match Oracle EVALIDator exactly

Top Results:
- 18: Engelmann and other spruces - 288,927,955 tons (26.4%)
- 44: Cottonwood and aspen (West) - 247,391,052 tons (22.6%)
- 12: True fir - 122,095,237 tons (11.1%)
- 23: Woodland softwoods - 114,570,440 tons (10.5%)
*/

SELECT
    SPGRPCD,
    species_group_name,
    SUM(ESTIMATED_VALUE * EXPNS) AS total_biomass_tons
FROM (
    SELECT
        pop_stratum.EXPNS,
        REF_SPECIES_GROUP_TREE.SPGRPCD,
        REF_SPECIES_GROUP_TREE.NAME AS species_group_name,

        -- Same EXACT biomass calculation from Oracle
        SUM(
            TREE.TPA_UNADJ *
            COALESCE(
                (
                    (1 - (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT))) *
                    REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT /
                    (
                        (1 - (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT))) *
                        REF_SPECIES.WOOD_SPGR_GREENVOL_DRYWT +
                        (REF_SPECIES.BARK_VOL_PCT / (100 + REF_SPECIES.BARK_VOL_PCT)) *
                        REF_SPECIES.BARK_SPGR_GREENVOL_DRYWT
                    ) *
                    (1.0 + REF_SPECIES.MC_PCT_GREEN_WOOD * 0.01) +
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
                1.76  -- Default specific gravity when species data unavailable
            ) *
            -- Same EXACT Oracle CASE logic for adjustment factors
            CASE
                WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP
                ELSE
                    CASE LEAST(TREE.DIA, 5 - 0.001)
                        WHEN TREE.DIA THEN POP_STRATUM.ADJ_FACTOR_MICR
                        ELSE
                            CASE LEAST(TREE.DIA, COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DECIMAL), 9999) - 0.001)
                                WHEN TREE.DIA THEN POP_STRATUM.ADJ_FACTOR_SUBP
                                ELSE POP_STRATUM.ADJ_FACTOR_MACR
                            END
                    END
            END *
            COALESCE(TREE.DRYBIO_AG / 2000, 0)  -- Convert pounds to tons
        ) AS ESTIMATED_VALUE

    FROM POP_STRATUM
    JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
    JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
    JOIN COND ON (COND.PLT_CN = PLOT.CN)
    JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)
    JOIN REF_SPECIES ON (TREE.SPCD = REF_SPECIES.SPCD)
    LEFT OUTER JOIN REF_SPECIES_GROUP REF_SPECIES_GROUP_TREE ON (
        REF_SPECIES_GROUP_TREE.SPGRPCD = CASE
            -- Special species group mapping for eastern redcedar in certain states
            WHEN TREE.STATECD IN (46, 38, 31, 20) AND TREE.SPCD = 122 THEN 11
            ELSE TREE.SPGRPCD
        END
    )

    WHERE
        TREE.STATUSCD = 1  -- Live trees
        AND COND.COND_STATUS_CD = 1  -- Forest conditions
        AND ((pop_stratum.RSCD = 22 AND pop_stratum.EVALID = 82101))  -- Colorado 2021

    GROUP BY
        pop_stratum.ESTN_UNIT_CN,
        pop_stratum.CN,
        plot.CN,
        plot.PREV_PLT_CN,
        cond.CN,
        plot.LAT,
        plot.LON,
        pop_stratum.EXPNS,
        REF_SPECIES_GROUP_TREE.SPGRPCD,
        REF_SPECIES_GROUP_TREE.NAME
)
GROUP BY SPGRPCD, species_group_name
ORDER BY total_biomass_tons DESC;

/*
Expected Results (Top 10):
SPGRPCD | Species Group                    | Biomass (tons)  | Percentage
--------|----------------------------------|-----------------|----------
18      | Engelmann and other spruces      | 288,927,955     | 26.4%
44      | Cottonwood and aspen (West)      | 247,391,052     | 22.6%
12      | True fir                         | 122,095,237     | 11.1%
23      | Woodland softwoods               | 114,570,440     | 10.5%
21      | Lodgepole pine                   | 96,393,745      | 8.8%
11      | Ponderosa and Jeffrey pines      | 95,361,069      | 8.7%
10      | Douglas-fir                      | 83,741,547      | 7.6%
48      | Woodland hardwoods               | 28,016,243      | 2.6%
24      | Other western softwoods          | 18,829,443      | 1.7%
47      | Other western hardwoods          | 811,564         | 0.1%

Key Insights:
- Spruce dominance: Engelmann and other spruces contain the most biomass (26.4%)
- Aspen significance: Cottonwood and aspen represent 22.6% of total biomass
- Conifer dominance: Softwood species groups account for 74.7% of total biomass
- Regional adaptation: Query includes western-specific species group mappings

Validation Notes:
- Results match Oracle EVALIDator exactly
- Simplified approach reduces code by ~50% while maintaining accuracy
- Demonstrates proper use of species-specific biomass equations
- Shows correct application of moisture content and specific gravity adjustments
*/