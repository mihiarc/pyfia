#!/usr/bin/env python
"""
Test growth calculation by component to understand EVALIDator logic.

Target: 2,473,614,987 cu ft for Georgia EVALID 132303
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("GROWTH BY COMPONENT ANALYSIS")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # Analyze each component separately
    component_analysis = """
    WITH component_calc AS (
        SELECT
            GRM.SUBP_COMPONENT_GS_TIMBER as component,
            SUM(
                GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
                CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                    WHEN 0 THEN 0
                    WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                    WHEN 2 THEN PS.ADJ_FACTOR_MICR
                    WHEN 3 THEN PS.ADJ_FACTOR_MACR
                    ELSE 0
                END *
                COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0) *
                PS.EXPNS
            ) as volume_from_tree,
            SUM(
                GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
                CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                    WHEN 0 THEN 0
                    WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                    WHEN 2 THEN PS.ADJ_FACTOR_MICR
                    WHEN 3 THEN PS.ADJ_FACTOR_MACR
                    ELSE 0
                END *
                COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0) *
                PS.EXPNS
            ) as volume_from_midpt,
            SUM(
                GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
                CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                    WHEN 0 THEN 0
                    WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                    WHEN 2 THEN PS.ADJ_FACTOR_MICR
                    WHEN 3 THEN PS.ADJ_FACTOR_MACR
                    ELSE 0
                END *
                COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0) *
                PS.EXPNS
            ) as volume_from_begin,
            COUNT(*) as records
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
        GROUP BY GRM.SUBP_COMPONENT_GS_TIMBER
    )
    SELECT * FROM component_calc
    ORDER BY volume_from_tree DESC
    """

    print("\nComponent-by-component volume analysis:")
    print(f"{'Component':15s} {'Records':>8s} {'TREE Vol':>15s} {'MIDPT Vol':>15s} {'BEGIN Vol':>15s}")
    print("-" * 80)

    results = conn.execute(component_analysis).fetchall()
    total_tree = total_midpt = total_begin = 0
    for row in results:
        if row[0]:  # Skip NULL components
            print(f"{row[0]:15s} {row[4]:8,} {row[1]:15,.0f} {row[2]:15,.0f} {row[3]:15,.0f}")
            total_tree += row[1]
            total_midpt += row[2]
            total_begin += row[3]

    print("-" * 80)
    print(f"{'TOTAL':15s} {' ':8s} {total_tree:15,.0f} {total_midpt:15,.0f} {total_begin:15,.0f}")

    # Now test EVALIDator logic:
    # For growth, we want ending volume - beginning volume for survivors
    # Plus ending volume for ingrowth
    # Minus beginning volume for mortality and removals

    evalidator_logic = """
    SELECT
        'Net Growth' as metric,
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
                -- Survivors: ending - beginning
                WHEN 'SURVIVOR' THEN
                    (COALESCE(T.VOLCFNET, 0) - COALESCE(BEGIN.VOLCFNET, 0)) / COALESCE(PLOT.REMPER, 5.0)
                -- Ingrowth and reversions: ending volume only
                WHEN 'INGROWTH' THEN
                    COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                -- Mortality and removals: negative beginning volume
                WHEN 'MORTALITY1' THEN
                    -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN 'MORTALITY2' THEN
                    -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN 'CUT1' THEN
                    -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN 'CUT2' THEN
                    -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN 'DIVERSION1' THEN
                    -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN 'DIVERSION2' THEN
                    -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE
                    CASE
                        WHEN GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%' THEN
                            COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                        ELSE 0
                    END
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
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

    print("\n" + "=" * 80)
    print("TESTING NET GROWTH CALCULATION")
    print("=" * 80)

    result = conn.execute(evalidator_logic).fetchone()
    print(f"\nNet growth (standard logic): {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"Difference from target: {diff:+.1f}%")

    # Test gross growth (positive components only)
    gross_growth = """
    SELECT
        'Gross Growth' as metric,
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
                -- Survivors: ending volume
                WHEN 'SURVIVOR' THEN
                    COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                -- Ingrowth and reversions: ending volume
                WHEN 'INGROWTH' THEN
                    COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE
                    CASE
                        WHEN GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%' THEN
                            COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                        ELSE 0
                    END
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
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR' THEN
                    COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
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
      AND GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
         OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
    """

    result = conn.execute(gross_growth).fetchone()
    print(f"\nGross growth (no mort/remv): {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"Difference from target: {diff:+.1f}%")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("Compare the calculated values to the target to determine correct logic")