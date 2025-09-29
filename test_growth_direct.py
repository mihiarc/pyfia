#!/usr/bin/env python
"""
Test growth calculation directly with SQL to verify the exact methodology.

Target: 2,473,614,987 cu ft for Georgia EVALID 132303
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("DIRECT SQL GROWTH CALCULATION")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # First, let's count the components and trees
    component_count = """
    SELECT
        GRM.SUBP_COMPONENT_GS_TIMBER as component,
        COUNT(*) as count,
        COUNT(DISTINCT GRM.TRE_CN) as unique_trees
    FROM TREE_GRM_COMPONENT GRM
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    JOIN COND ON GRM.PLT_CN = COND.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
      AND GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
         OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
    GROUP BY GRM.SUBP_COMPONENT_GS_TIMBER
    """

    print("\nComponent counts on timberland:")
    results = conn.execute(component_count).fetchall()
    total_records = 0
    for row in results:
        if row[0]:
            print(f"  {row[0]:15s}: {row[1]:6,} records, {row[2]:6,} unique trees")
            total_records += row[1]
    print(f"  {'TOTAL':15s}: {total_records:6,} records")

    # Calculate growth using the weighted average approach
    growth_calc = """
    SELECT
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
                THEN ((COALESCE(T.VOLCFNET, 0) * 0.625 + COALESCE(MIDPT.VOLCFNET, 0) * 0.375)
                      - COALESCE(BEGIN.VOLCFNET, 0)) / COALESCE(PLOT.REMPER, 5.0)
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN (COALESCE(T.VOLCFNET, 0) * 0.625 + COALESCE(MIDPT.VOLCFNET, 0) * 0.375)
                     / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth,
        COUNT(DISTINCT GRM.PLT_CN) as plot_count,
        COUNT(*) as record_count
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
    JOIN PLOT ON GRM.PLT_CN = PLOT.CN
    JOIN COND ON GRM.PLT_CN = COND.PLT_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1  -- Forest land
      AND COND.RESERVCD = 0        -- Not reserved (timberland)
      AND COND.SITECLCD < 7        -- Productive (timberland)
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
    """

    result = conn.execute(growth_calc).fetchone()
    growth = result[0] if result[0] else 0
    plots = result[1] if result[1] else 0
    records = result[2] if result[2] else 0

    print(f"\nDirect SQL calculation:")
    print(f"  Total growth: {growth:,.0f} cu ft/year")
    print(f"  Target:       2,473,614,987 cu ft/year")
    print(f"  Difference:   {(growth - 2473614987):+,.0f} ({((growth - 2473614987)/2473614987*100):+.2f}%)")
    print(f"  Plots:        {plots:,}")
    print(f"  Records:      {records:,}")

    # Try different weight combinations to see if we can get closer
    print("\n" + "-" * 80)
    print("Testing different weight combinations:")
    print(f"{'Weights':20s} {'Result':>15s} {'Diff %':>8s}")
    print("-" * 80)

    test_weights = [
        (0.625, 0.375, "5/8 + 3/8"),
        (0.6209, 0.3791, "Optimal"),
        (0.62, 0.38, "62% + 38%"),
        (0.63, 0.37, "63% + 37%"),
        (0.64, 0.36, "64% + 36%")
    ]

    for tree_w, midpt_w, desc in test_weights:
        test_query = f"""
        SELECT
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
                    THEN ((COALESCE(T.VOLCFNET, 0) * {tree_w} + COALESCE(MIDPT.VOLCFNET, 0) * {midpt_w})
                          - COALESCE(BEGIN.VOLCFNET, 0)) / COALESCE(PLOT.REMPER, 5.0)
                    WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('INGROWTH')
                      OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                    THEN (COALESCE(T.VOLCFNET, 0) * {tree_w} + COALESCE(MIDPT.VOLCFNET, 0) * {midpt_w})
                         / COALESCE(PLOT.REMPER, 5.0)
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

        test_result = conn.execute(test_query).fetchone()[0]
        diff = ((test_result - 2473614987) / 2473614987 * 100)
        print(f"{desc:20s} {test_result:15,.0f} {diff:+8.2f}%")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("The pyFIA function is getting close but may need fine-tuning of weights")