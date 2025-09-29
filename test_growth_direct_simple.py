#!/usr/bin/env python
"""
Test the simplest possible growth calculation to match EVALIDator.
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("SIMPLEST GROWTH CALCULATION")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # Simplest approach: Just calculate NET growth without BEGINEND
    simple_net = """
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
            CASE GRM.SUBP_COMPONENT_GS_TIMBER
                WHEN 'SURVIVOR' THEN
                    (T.VOLCFNET - COALESCE(BEGIN.VOLCFNET, 0)) / PLOT.REMPER
                WHEN 'INGROWTH' THEN
                    T.VOLCFNET / PLOT.REMPER
                ELSE
                    CASE
                        WHEN GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%' THEN
                            T.VOLCFNET / PLOT.REMPER
                        ELSE 0
                    END
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON T.CN = BEGIN.TRE_CN
    JOIN PLOT ON T.PLT_CN = PLOT.CN
    JOIN COND ON T.PLT_CN = COND.PLT_CN AND T.CONDID = COND.CONDID
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON T.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
      AND (GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
           OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%')
    """

    result = conn.execute(simple_net).fetchone()
    print(f"\nSimple NET growth (no BEGINEND):")
    print(f"  Result: {result[0]:,.0f} cu ft")
    print(f"  Target: 2,473,614,987 cu ft")
    print(f"  Diff:   {(result[0] - 2473614987)/2473614987*100:+.1f}%")

    # Check how pyFIA would calculate this
    pyfia_style = """
    SELECT
        COUNT(DISTINCT GRM.PLT_CN) as plots,
        COUNT(DISTINCT GRM.TRE_CN) as trees,
        SUM(PS.EXPNS) as total_expns,
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            CASE GRM.SUBP_COMPONENT_GS_TIMBER
                WHEN 'SURVIVOR' THEN
                    (T.VOLCFNET - COALESCE(BEGIN.VOLCFNET, 0)) / PLOT.REMPER
                WHEN 'INGROWTH' THEN
                    T.VOLCFNET / PLOT.REMPER
                ELSE
                    CASE
                        WHEN GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%' THEN
                            T.VOLCFNET / PLOT.REMPER
                        ELSE 0
                    END
            END *
            PS.EXPNS
        ) / COUNT(DISTINCT GRM.PLT_CN) as growth_per_plot
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON T.CN = BEGIN.TRE_CN
    JOIN PLOT ON T.PLT_CN = PLOT.CN
    JOIN COND ON T.PLT_CN = COND.PLT_CN AND T.CONDID = COND.CONDID
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON T.PLT_CN = PPSA.PLT_CN
    JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
      AND (GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
           OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%')
    """

    result = conn.execute(pyfia_style).fetchall()[0]
    print(f"\npyFIA-style aggregation:")
    print(f"  Plots: {result[0]:,}")
    print(f"  Trees: {result[1]:,}")
    print(f"  Total EXPNS: {result[2]:,.0f}")
    print(f"  Growth per plot: {result[3]:,.0f}")

print("\n" + "=" * 80)