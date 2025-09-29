#!/usr/bin/env python
"""
Test exact EVALIDator logic based on the provided SQL query.

Key insights from EVALIDator:
1. Uses BEGINEND cross-join
2. Different volumes for different components
3. Uses PTREE as fallback when TRE_BEGIN is null
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("EXACT EVALIDATOR LOGIC TEST")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # First check if we have PTREE references
    ptree_check = """
    SELECT
        COUNT(*) as total_trees,
        COUNT(DISTINCT T.PREV_TRE_CN) as has_prev_cn,
        COUNT(DISTINCT PTREE.CN) as has_ptree
    FROM TREE T
    LEFT JOIN TREE PTREE ON T.PREV_TRE_CN = PTREE.CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON T.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
    """

    result = conn.execute(ptree_check).fetchone()
    print(f"\nPrevious tree references:")
    print(f"  Total trees: {result[0]:,}")
    print(f"  Has PREV_TRE_CN: {result[1]:,}")
    print(f"  Has matching PTREE: {result[2]:,}")

    # Implement exact EVALIDator logic
    evalidator_exact = """
    WITH grm_calc AS (
        SELECT
            BE.ONEORTWO,
            GRM.SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
            SUM(
                GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
                (CASE
                    WHEN COALESCE(GRM.SUBP_SUBPTYP_GRM_GS_TIMBER, 0) = 0 THEN 0
                    WHEN GRM.SUBP_SUBPTYP_GRM_GS_TIMBER = 1 THEN PS.ADJ_FACTOR_SUBP
                    WHEN GRM.SUBP_SUBPTYP_GRM_GS_TIMBER = 2 THEN PS.ADJ_FACTOR_MICR
                    WHEN GRM.SUBP_SUBPTYP_GRM_GS_TIMBER = 3 THEN PS.ADJ_FACTOR_MACR
                    ELSE 0
                END) *
                (CASE
                    WHEN BE.ONEORTWO = 2 THEN
                        (CASE
                            WHEN (GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER = 'INGROWTH'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%')
                            THEN (T.VOLCFNET / PLOT.REMPER)
                            WHEN (GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'CUT%'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'DIVERSION%'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'MORTALITY%')
                            THEN (MIDPT.VOLCFNET / PLOT.REMPER)
                            ELSE 0
                        END)
                    ELSE -- ONEORTWO = 1
                        (CASE
                            WHEN (GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER = 'CUT1'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER = 'DIVERSION1'
                                  OR GRM.SUBP_COMPONENT_GS_TIMBER = 'MORTALITY1')
                            THEN
                                CASE
                                    WHEN TRE_BEGIN.TRE_CN IS NOT NULL
                                    THEN -(TRE_BEGIN.VOLCFNET / PLOT.REMPER)
                                    ELSE -(COALESCE(PTREE.VOLCFNET, 0) / PLOT.REMPER)
                                END
                            ELSE 0
                        END)
                END) *
                PS.EXPNS
            ) AS GROWTH_COMPONENT
        FROM BEGINEND BE
        CROSS JOIN TREE_GRM_COMPONENT GRM
        JOIN TREE T ON GRM.TRE_CN = T.CN
        LEFT JOIN TREE PTREE ON T.PREV_TRE_CN = PTREE.CN
        LEFT JOIN TREE_GRM_BEGIN TRE_BEGIN ON T.CN = TRE_BEGIN.TRE_CN
        LEFT JOIN TREE_GRM_MIDPT MIDPT ON T.CN = MIDPT.TRE_CN
        JOIN PLOT ON T.PLT_CN = PLOT.CN
        JOIN COND ON T.PLT_CN = COND.PLT_CN AND T.CONDID = COND.CONDID
        JOIN POP_PLOT_STRATUM_ASSGN PPSA ON T.PLT_CN = PPSA.PLT_CN
        JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
        WHERE PPSA.EVALID = 132303
          AND BE.ONEORTWO IN (1, 2)
          AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER IS NOT NULL
          AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
          -- Apply timberland filters (from EVALIDator rscd=33)
          AND COND.COND_STATUS_CD = 1  -- Forest land
          AND COND.RESERVCD = 0        -- Not reserved
          AND COND.SITECLCD < 7        -- Productive
        GROUP BY BE.ONEORTWO, GRM.SUBP_COMPONENT_GS_TIMBER
    )
    SELECT
        ONEORTWO,
        COMPONENT,
        GROWTH_COMPONENT
    FROM grm_calc
    ORDER BY ONEORTWO, COMPONENT
    """

    print("\n" + "-" * 80)
    print("Growth by ONEORTWO and component:")
    print(f"{'ONEORTWO':>10s} {'Component':20s} {'Growth':>20s}")
    print("-" * 80)

    results = conn.execute(evalidator_exact).fetchall()
    total = 0
    oneortwo_totals = {}

    for row in results:
        oneortwo, component, growth = row
        if component:
            print(f"{oneortwo:10.0f} {component:20s} {growth:20,.0f}")
            total += growth
            if oneortwo not in oneortwo_totals:
                oneortwo_totals[oneortwo] = 0
            oneortwo_totals[oneortwo] += growth

    print("-" * 80)
    for oneortwo, subtotal in oneortwo_totals.items():
        print(f"{'Subtotal':10s} ONEORTWO={oneortwo:.0f}: {subtotal:20,.0f}")

    print("-" * 80)
    print(f"{'TOTAL':30s} {total:20,.0f}")
    print(f"{'TARGET':30s} {2473614987:20,.0f}")
    print(f"{'DIFFERENCE':30s} {(total - 2473614987):+20,.0f} ({(total - 2473614987)/2473614987*100:+.1f}%)")

    # Test without PTREE fallback
    print("\n" + "=" * 80)
    print("Testing without PTREE fallback:")

    no_ptree = """
    SELECT SUM(
        GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
        (CASE
            WHEN COALESCE(GRM.SUBP_SUBPTYP_GRM_GS_TIMBER, 0) = 0 THEN 0
            WHEN GRM.SUBP_SUBPTYP_GRM_GS_TIMBER = 1 THEN PS.ADJ_FACTOR_SUBP
            WHEN GRM.SUBP_SUBPTYP_GRM_GS_TIMBER = 2 THEN PS.ADJ_FACTOR_MICR
            WHEN GRM.SUBP_SUBPTYP_GRM_GS_TIMBER = 3 THEN PS.ADJ_FACTOR_MACR
            ELSE 0
        END) *
        (CASE
            WHEN BE.ONEORTWO = 2 THEN
                (CASE
                    WHEN (GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                          OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%')
                    THEN COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                    ELSE 0
                END)
            WHEN BE.ONEORTWO = 1 THEN
                (CASE
                    WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                    THEN -COALESCE(TRE_BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                    ELSE 0
                END)
            ELSE 0
        END) *
        PS.EXPNS
    ) AS GROWTH
    FROM BEGINEND BE
    CROSS JOIN TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_BEGIN TRE_BEGIN ON T.CN = TRE_BEGIN.TRE_CN
    JOIN PLOT ON T.PLT_CN = PLOT.CN
    JOIN COND ON T.PLT_CN = COND.PLT_CN AND T.CONDID = COND.CONDID
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON T.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND BE.ONEORTWO IN (1, 2)
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
    """

    result = conn.execute(no_ptree).fetchone()[0]
    print(f"  Without PTREE: {result:,.0f} ({(result - 2473614987)/2473614987*100:+.1f}%)")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("The EVALIDator logic uses:")
print("1. BEGINEND cross-join for proper accounting")
print("2. Different volume sources for different component types")
print("3. PTREE fallback when TRE_BEGIN is not available")