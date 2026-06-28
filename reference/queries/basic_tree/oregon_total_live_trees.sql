/*
Oregon Total Live Trees (EVALIDator-Style)
==========================================

EVALID: 412101 (Oregon 2021)
Expected Result: 10,481,113,490 live trees (357.8 trees/acre)
Forest Area: 29,292,380 acres

This query demonstrates the fundamental EVALIDator methodology for tree counting,
including proper adjustment factors and population expansion.

EVALIDator Methodology Notes:
- Uses proper adjustment factors (MICR/SUBP/MACR) based on tree diameter
- Applies population expansion factors (EXPNS) for statistical estimates
- Matches Oracle EVALIDator query structure exactly
- Includes only live trees in forest conditions

Key Concepts:
- TPA_UNADJ: Trees per acre (unadjusted) - base measurement
- Adjustment Factors: Correct for sampling design based on tree size
- EXPNS: Expansion factor to convert plot data to population estimates
- EVALID: Statistical evaluation identifier for proper grouping
*/

SELECT
    SUM(
        TREE.TPA_UNADJ *
        CASE
            WHEN TREE.DIA IS NULL THEN POP_STRATUM.ADJ_FACTOR_SUBP
            WHEN TREE.DIA < 5.0 THEN POP_STRATUM.ADJ_FACTOR_MICR
            WHEN TREE.DIA < COALESCE(CAST(PLOT.MACRO_BREAKPOINT_DIA AS DOUBLE), 9999.0) THEN POP_STRATUM.ADJ_FACTOR_SUBP
            ELSE POP_STRATUM.ADJ_FACTOR_MACR
        END * POP_STRATUM.EXPNS
    ) AS total_live_trees

FROM POP_STRATUM
JOIN POP_PLOT_STRATUM_ASSGN ON (POP_PLOT_STRATUM_ASSGN.STRATUM_CN = POP_STRATUM.CN)
JOIN PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
JOIN COND ON (COND.PLT_CN = PLOT.CN)
JOIN TREE ON (TREE.PLT_CN = COND.PLT_CN AND TREE.CONDID = COND.CONDID)

WHERE
    TREE.STATUSCD = 1  -- Live trees
    AND COND.COND_STATUS_CD = 1  -- Forest conditions
    AND POP_STRATUM.EVALID = 412101;  -- Oregon 2021

/*
Expected Results:
- Total Live Trees: 10,481,113,490
- Trees per Acre: 357.8
- Forest Area: 29,292,380 acres

Validation Notes:
- Result matches Oracle EVALIDator exactly
- Demonstrates proper use of adjustment factors
- Shows correct application of expansion factors
- Template for other state/EVALID combinations
*/