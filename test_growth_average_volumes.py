#!/usr/bin/env python
"""
Test growth using average of TREE and MIDPT volumes.

Target: 2,473,614,987 cu ft for Georgia EVALID 132303
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("GROWTH CALCULATION - AVERAGING APPROACHES")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # Theory: Maybe we should average TREE and MIDPT volumes
    # TREE represents ending inventory, MIDPT represents midpoint
    average_approach = """
    SELECT
        'Average TREE+MIDPT' as method,
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN (COALESCE(T.VOLCFNET, 0) + COALESCE(MIDPT.VOLCFNET, 0)) / 2.0 / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) -
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                THEN COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
    JOIN PLOT ON GRM.PLT_CN = PLOT.CN
    JOIN COND ON GRM.PLT_CN = COND.PLT_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
    """

    result = conn.execute(average_approach).fetchone()
    print(f"\n{result[0]}: {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"Difference from target: {diff:+.1f}%")

    # Try weighted average favoring TREE
    weighted_75_25 = """
    SELECT
        'Weighted 75% TREE, 25% MIDPT' as method,
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN (COALESCE(T.VOLCFNET, 0) * 0.75 + COALESCE(MIDPT.VOLCFNET, 0) * 0.25) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) -
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                THEN COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
    JOIN PLOT ON GRM.PLT_CN = PLOT.CN
    JOIN COND ON GRM.PLT_CN = COND.PLT_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
    """

    result = conn.execute(weighted_75_25).fetchone()
    print(f"\n{result[0]}: {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"Difference from target: {diff:+.1f}%")

    # What ratio would we need?
    tree_growth = 3069709776
    midpt_growth = 1497121950
    target = 2473614987

    # Solve: w * tree_growth + (1-w) * midpt_growth = target
    weight_tree = (target - midpt_growth) / (tree_growth - midpt_growth)
    weight_midpt = 1 - weight_tree

    print(f"\nOptimal weights to match target:")
    print(f"  TREE weight: {weight_tree:.4f} ({weight_tree*100:.1f}%)")
    print(f"  MIDPT weight: {weight_midpt:.4f} ({weight_midpt*100:.1f}%)")

    optimal_weighted = f"""
    SELECT
        'Optimal weighted' as method,
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN (COALESCE(T.VOLCFNET, 0) * {weight_tree} + COALESCE(MIDPT.VOLCFNET, 0) * {weight_midpt}) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) -
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                THEN COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
    JOIN PLOT ON GRM.PLT_CN = PLOT.CN
    JOIN COND ON GRM.PLT_CN = COND.PLT_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
    """

    result = conn.execute(optimal_weighted).fetchone()
    print(f"\n{result[0]}: {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"Difference from target: {diff:+.1f}%")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("The exact EVALIDator method needs 62.1% TREE + 37.9% MIDPT weighting.")
print("This suggests a specific interpolation method is used.")