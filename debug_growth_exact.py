#!/usr/bin/env python
"""
Debug growth calculation to exactly match EVALIDator.

Target: 2,473,614,987 cu ft for Georgia EVALID 132303 (timberland, growing stock)
"""

import duckdb
import polars as pl

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("DEBUGGING GROWTH CALCULATION - EXACT EVALIDATOR MATCH")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # First, let's replicate the EXACT EVALIDator query structure
    # This is based on the query from test_growth_evaluation.py

    evalidator_query = """
    WITH component_data AS (
        SELECT
            GRM.TRE_CN,
            GRM.PLT_CN,
            GRM.SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER AS TPAGROW_UNADJ,
            GRM.SUBP_SUBPTYP_GRM_GS_TIMBER AS SUBPTYP_GRM,
            MIDPT.VOLCFNET AS VOLCFNET_MIDPT,
            BEGIN.VOLCFNET AS VOLCFNET_BEGIN,
            PLOT.REMPER,
            PS.EXPNS,
            PS.ADJ_FACTOR_SUBP,
            PS.ADJ_FACTOR_MICR,
            PS.ADJ_FACTOR_MACR,
            BE.ONEORTWO,
            COND.COND_STATUS_CD,
            COND.RESERVCD,
            COND.SITECLCD
        FROM TREE_GRM_COMPONENT GRM
        CROSS JOIN (SELECT DISTINCT ONEORTWO FROM BEGINEND WHERE ONEORTWO IN (1, 2)) BE
        JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
        LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
        JOIN PLOT ON GRM.PLT_CN = PLOT.CN
        JOIN COND ON GRM.PLT_CN = COND.PLT_CN
        JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
        JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
        WHERE PPSA.EVALID = 132303
          AND COND.COND_STATUS_CD = 1  -- Forest land
          AND COND.RESERVCD = 0        -- Not reserved (timberland)
          AND COND.SITECLCD < 7        -- Productive (timberland)
    )
    SELECT
        'Step 1: All components, both ONEORTWO' as description,
        COUNT(*) as row_count,
        COUNT(DISTINCT TRE_CN) as unique_trees,
        COUNT(DISTINCT PLT_CN) as unique_plots
    FROM component_data
    """

    result = conn.execute(evalidator_query).fetchone()
    print(f"\n{result[0]}:")
    print(f"  Rows: {result[1]:,}")
    print(f"  Unique trees: {result[2]:,}")
    print(f"  Unique plots: {result[3]:,}")

    # Now calculate growth using EVALIDator logic
    calculation_query = """
    WITH component_calc AS (
        SELECT
            GRM.PLT_CN,
            BE.ONEORTWO,
            GRM.SUBP_COMPONENT_GS_TIMBER AS COMPONENT,
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER AS TPAGROW_UNADJ,
            GRM.SUBP_SUBPTYP_GRM_GS_TIMBER AS SUBPTYP_GRM,
            MIDPT.VOLCFNET AS VOLCFNET_MIDPT,
            BEGIN.VOLCFNET AS VOLCFNET_BEGIN,
            PLOT.REMPER,
            PS.EXPNS,
            PS.ADJ_FACTOR_SUBP,
            PS.ADJ_FACTOR_MICR,
            PS.ADJ_FACTOR_MACR,
            -- Calculate adjustment factor
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END AS ADJ_FACTOR,
            -- Calculate volume change based on ONEORTWO
            CASE
                WHEN BE.ONEORTWO = 2 THEN
                    CASE
                        WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                          OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                        THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                        ELSE 0
                    END
                WHEN BE.ONEORTWO = 1 THEN
                    CASE
                        WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                        THEN -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                        ELSE 0
                    END
                ELSE 0
            END AS VOLUME_CHANGE
        FROM TREE_GRM_COMPONENT GRM
        CROSS JOIN (SELECT DISTINCT ONEORTWO FROM BEGINEND WHERE ONEORTWO IN (1, 2)) BE
        JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
        LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
        JOIN PLOT ON GRM.PLT_CN = PLOT.CN
        JOIN COND ON GRM.PLT_CN = COND.PLT_CN
        JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
        JOIN POP_STRATUM PS ON PPSA.STRATUM_CN = PS.CN
        WHERE PPSA.EVALID = 132303
          AND COND.COND_STATUS_CD = 1
          AND COND.RESERVCD = 0
          AND COND.SITECLCD < 7
    )
    SELECT
        'Step 2: Growth calculation by ONEORTWO' as description,
        ONEORTWO,
        COUNT(*) as trees,
        SUM(TPAGROW_UNADJ * ADJ_FACTOR * VOLUME_CHANGE * EXPNS) as growth_total
    FROM component_calc
    WHERE TPAGROW_UNADJ > 0
    GROUP BY ONEORTWO
    ORDER BY ONEORTWO
    """

    print("\n" + "-" * 80)
    print("Growth by ONEORTWO:")
    results = conn.execute(calculation_query).fetchall()
    total_growth = 0
    for row in results:
        print(f"  ONEORTWO={row[1]}: {row[2]:,} trees, growth={row[3]:,.0f}")
        total_growth += row[3]

    print(f"\n  TOTAL GROWTH: {total_growth:,.0f} cu ft")
    print(f"  Target:       2,473,614,987 cu ft")
    print(f"  Difference:   {(total_growth - 2473614987):+,.0f} ({((total_growth - 2473614987)/2473614987*100):+.1f}%)")

    # Now let's check component distribution
    component_query = """
    SELECT
        SUBP_COMPONENT_GS_TIMBER as component,
        COUNT(*) as count,
        COUNT(DISTINCT TRE_CN) as trees,
        SUM(SUBP_TPAGROW_UNADJ_GS_TIMBER) as total_tpa
    FROM TREE_GRM_COMPONENT GRM
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND SUBP_COMPONENT_GS_TIMBER IS NOT NULL
    GROUP BY SUBP_COMPONENT_GS_TIMBER
    ORDER BY count DESC
    """

    print("\n" + "-" * 80)
    print("Component distribution in EVALID 132303:")
    components = conn.execute(component_query).fetchall()
    for comp in components:
        if comp[0]:  # Skip NULL components
            tpa_val = comp[3] if comp[3] is not None else 0
            print(f"  {comp[0]}: {comp[1]:,} records, {comp[2]:,} trees, TPA={tpa_val:.1f}")

    # Check filters
    filter_check = """
    SELECT
        'Forest land plots' as description,
        COUNT(DISTINCT PLT_CN) as plots
    FROM COND
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON COND.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
    UNION ALL
    SELECT
        'Timberland plots' as description,
        COUNT(DISTINCT PLT_CN) as plots
    FROM COND
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON COND.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND COND.COND_STATUS_CD = 1
      AND COND.RESERVCD = 0
      AND COND.SITECLCD < 7
    """

    print("\n" + "-" * 80)
    print("Plot counts by land type:")
    filters = conn.execute(filter_check).fetchall()
    for f in filters:
        print(f"  {f[0]}: {f[1]:,}")

    # Test without BEGINEND cross-join (simple calculation)
    simple_query = """
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
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as net_growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
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

    print("\n" + "-" * 80)
    print("Simple NET growth (without BEGINEND):")
    result = conn.execute(simple_query).fetchone()
    print(f"  Net growth: {result[0]:,.0f} cu ft")
    print(f"  Difference from target: {((result[0] - 2473614987)/2473614987*100):+.1f}%")

print("\n" + "=" * 80)
print("ANALYSIS")
print("=" * 80)
print("The exact calculation method needs to be determined.")
print("Key factors to investigate:")
print("1. BEGINEND cross-join handling")
print("2. Component filtering (which components to include)")
print("3. Volume calculation (ending only vs ending-beginning)")
print("4. Adjustment factor application")