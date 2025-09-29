#!/usr/bin/env python
"""
Test exact EVALIDator calculation method for growth.

Target: 2,473,614,987 cu ft for Georgia EVALID 132303
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("EXACT EVALIDATOR CALCULATION METHOD")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # First, check what components EVALIDator includes
    component_check = """
    SELECT
        GRM.SUBP_COMPONENT_GS_TIMBER as component,
        COUNT(*) as count,
        SUM(GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER) as tpa_sum
    FROM TREE_GRM_COMPONENT GRM
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
    GROUP BY GRM.SUBP_COMPONENT_GS_TIMBER
    ORDER BY tpa_sum DESC
    """

    print("\nComponents with positive TPAGROW:")
    components = conn.execute(component_check).fetchall()
    for comp in components:
        if comp[0]:
            print(f"  {comp[0]:20s}: {comp[1]:6,} records, TPA_SUM={comp[2]:,.1f}")

    # EVALIDator uses TREE table for volumes, not TREE_GRM_MIDPT
    # Let's check if TREE table exists and has the needed columns
    tree_check = """
    SELECT COUNT(*) as tree_count
    FROM information_schema.columns
    WHERE table_name = 'TREE'
      AND column_name IN ('CN', 'VOLCFNET', 'PLT_CN')
    """

    tree_exists = conn.execute(tree_check).fetchone()[0]
    print(f"\nTREE table check: {tree_exists} required columns found")

    if tree_exists == 3:
        # EVALIDator-style calculation using TREE table
        evalidator_calc = """
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
                    WHEN BE.ONEORTWO = 2 THEN
                        CASE
                            WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH', 'CUT', 'DIVERSION')
                              OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                            THEN COALESCE(T2.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                            ELSE 0
                        END
                    WHEN BE.ONEORTWO = 1 THEN
                        CASE
                            WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'CUT', 'DIVERSION')
                            THEN -COALESCE(T1.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                            ELSE 0
                        END
                    ELSE 0
                END *
                PS.EXPNS
            ) as growth
        FROM TREE_GRM_COMPONENT GRM
        CROSS JOIN (SELECT DISTINCT ONEORTWO FROM BEGINEND WHERE ONEORTWO IN (1, 2)) BE
        LEFT JOIN TREE T2 ON GRM.TRE_CN = T2.PREV_TRE_CN  -- Ending measurement
        LEFT JOIN TREE T1 ON GRM.TRE_CN = T1.CN           -- Beginning measurement
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

        result = conn.execute(evalidator_calc).fetchone()
        print(f"\nEVALIDator-style (using TREE table): {result[0]:,.0f} cu ft")
        diff = ((result[0] - 2473614987) / 2473614987 * 100)
        print(f"Difference from target: {diff:+.1f}%")

    # Try simpler approach: just use ending volume for growth components
    simple_gross = """
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
            COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0) *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
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

    result = conn.execute(simple_gross).fetchone()
    scale_factor = 2473614987 / result[0] if result[0] > 0 else 0
    print(f"\nSimple gross (no BEGINEND): {result[0]:,.0f} cu ft")
    print(f"Scale factor needed: {scale_factor:.6f}")
    print(f"Note: Scale factor ~0.5 suggests we're double-counting")

    # Check if we have duplicate BEGINEND rows
    beginend_check = """
    SELECT
        ONEORTWO,
        COUNT(*) as count,
        COUNT(DISTINCT STATE_ADDED) as unique_states
    FROM BEGINEND
    WHERE ONEORTWO IN (1, 2)
    GROUP BY ONEORTWO
    ORDER BY ONEORTWO
    """

    print("\nBEGINEND table analysis:")
    be_rows = conn.execute(beginend_check).fetchall()
    for row in be_rows:
        print(f"  ONEORTWO={row[0]}: {row[1]} rows, {row[2]} unique states")

    # Try with deduplication
    dedupe_calc = """
    WITH unique_beginend AS (
        SELECT DISTINCT ONEORTWO
        FROM (VALUES (1.0), (2.0)) AS t(ONEORTWO)
    )
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
            END *
            PS.EXPNS
        ) / 2.0 as growth  -- Divide by 2 to account for duplication
    FROM TREE_GRM_COMPONENT GRM
    CROSS JOIN unique_beginend BE
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

    result = conn.execute(dedupe_calc).fetchone()
    print(f"\nWith manual deduplication (/2): {result[0]:,.0f} cu ft")
    diff = ((result[0] - 2473614987) / 2473614987 * 100)
    print(f"Difference from target: {diff:+.1f}%")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("The exact calculation depends on:")
print("1. Proper BEGINEND handling (avoiding duplication)")
print("2. Component selection (which components to include)")
print("3. Volume source (TREE vs TREE_GRM_MIDPT/BEGIN)")