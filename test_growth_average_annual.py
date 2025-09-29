#!/usr/bin/env python
"""
Test growth calculation with correct average annual logic.

The published estimate might be AVERAGE ANNUAL growth, not total.
Target: 2,473,614,987 cu ft for Georgia EVALID 132303
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("AVERAGE ANNUAL GROWTH CALCULATION")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft (average annual)")
    print("=" * 80)

    # Check the REMPER values to understand the period
    remper_check = """
    SELECT
        PLOT.REMPER,
        COUNT(DISTINCT PLOT.CN) as plot_count,
        COUNT(DISTINCT GRM.TRE_CN) as tree_count
    FROM TREE_GRM_COMPONENT GRM
    JOIN PLOT ON GRM.PLT_CN = PLOT.CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
    GROUP BY PLOT.REMPER
    ORDER BY plot_count DESC
    """

    print("\nREMPER distribution:")
    results = conn.execute(remper_check).fetchall()
    for row in results:
        print(f"  REMPER={row[0]}: {row[1]:,} plots, {row[2]:,} trees")

    # The key insight: EVALIDator shows AVERAGE ANNUAL growth
    # We need to annualize by dividing by REMPER (remeasurement period)
    # This is already done in our queries with: / COALESCE(PLOT.REMPER, 5.0)

    # Let's try the exact EVALIDator approach:
    # Gross growth = ending volume of survivors + ingrowth
    # Net growth = gross growth - mortality

    # First, calculate just as EVALIDator would
    evalidator_gross = """
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
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as gross_accretion,
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
        ) as beginning_survivors
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

    result = conn.execute(evalidator_gross).fetchone()
    gross_accretion = result[0]
    beginning_survivors = result[1]
    gross_growth = gross_accretion - beginning_survivors

    print(f"\nUsing TREE table (current inventory):")
    print(f"  Gross accretion: {gross_accretion:,.0f} cu ft")
    print(f"  Beginning survivors: {beginning_survivors:,.0f} cu ft")
    print(f"  Gross growth: {gross_growth:,.0f} cu ft")
    diff = ((gross_growth - 2473614987) / 2473614987 * 100)
    print(f"  Difference from target: {diff:+.1f}%")

    # Now try with MIDPT volumes instead
    midpt_calculation = """
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
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as gross_accretion_midpt,
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
        ) as beginning_survivors_midpt
    FROM TREE_GRM_COMPONENT GRM
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

    result = conn.execute(midpt_calculation).fetchone()
    gross_accretion = result[0]
    beginning_survivors = result[1]
    gross_growth = gross_accretion - beginning_survivors

    print(f"\nUsing TREE_GRM_MIDPT table:")
    print(f"  Gross accretion: {gross_accretion:,.0f} cu ft")
    print(f"  Beginning survivors: {beginning_survivors:,.0f} cu ft")
    print(f"  Gross growth: {gross_growth:,.0f} cu ft")
    diff = ((gross_growth - 2473614987) / 2473614987 * 100)
    print(f"  Difference from target: {diff:+.1f}%")

    # Maybe we need to scale by 0.5 due to the BEGINEND duplication?
    print(f"\nWith 0.5 scaling factor:")
    scaled_growth = gross_growth * 0.5
    print(f"  Scaled growth: {scaled_growth:,.0f} cu ft")
    diff = ((scaled_growth - 2473614987) / 2473614987 * 100)
    print(f"  Difference from target: {diff:+.1f}%")

    # Check if maybe we should only use specific components
    only_survivor_ingrowth = """
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
                THEN (COALESCE(MIDPT.VOLCFNET, 0) - COALESCE(BEGIN.VOLCFNET, 0)) / COALESCE(PLOT.REMPER, 5.0)
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'INGROWTH'
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as net_growth
    FROM TREE_GRM_COMPONENT GRM
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
      AND GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
    """

    result = conn.execute(only_survivor_ingrowth).fetchone()
    print(f"\nOnly SURVIVOR+INGROWTH (net change):")
    print(f"  Net growth: {result[0]:,.0f} cu ft")
    diff = ((result[0] - 2473614987) / 2473614987 * 100)
    print(f"  Difference from target: {diff:+.1f}%")

print("\n" + "=" * 80)
print("ANALYSIS")
print("=" * 80)
print("The published estimate appears to be gross growth (accretion).")
print("We're getting close with TREE table calculation.")