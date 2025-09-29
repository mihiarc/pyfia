#!/usr/bin/env python
"""
Investigate why we might be quadruple-counting.
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("INVESTIGATING QUADRUPLE-COUNTING ISSUE")
    print("=" * 80)

    # Check BEGINEND table
    print("\n1. BEGINEND table contents:")
    beginend = conn.execute("SELECT * FROM BEGINEND ORDER BY ONEORTWO, STATE_ADDED").fetchall()
    for row in beginend:
        print(f"   ONEORTWO={row[0]}, STATE_ADDED={row[3]}")
    print(f"   Total BEGINEND rows: {len(beginend)}")

    # Check if we're getting duplicate rows in the cross-join
    check_duplication = """
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT TRE_CN) as unique_trees,
        COUNT(*) / COUNT(DISTINCT TRE_CN) as duplication_factor
    FROM (
        SELECT GRM.TRE_CN
        FROM TREE_GRM_COMPONENT GRM
        CROSS JOIN (SELECT DISTINCT ONEORTWO FROM BEGINEND WHERE ONEORTWO IN (1, 2)) BE
        JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
        WHERE PPSA.EVALID = 132303
    )
    """

    result = conn.execute(check_duplication).fetchone()
    print(f"\n2. Cross-join duplication check:")
    print(f"   Total rows after cross-join: {result[0]:,}")
    print(f"   Unique trees: {result[1]:,}")
    print(f"   Duplication factor: {result[2]:.1f}x")

    # Check how many unique ONEORTWO values we get
    unique_oneortwo = """
    SELECT COUNT(DISTINCT ONEORTWO) as unique_vals,
           GROUP_CONCAT(DISTINCT ONEORTWO) as values
    FROM BEGINEND
    WHERE ONEORTWO IN (1, 2)
    """

    result = conn.execute(unique_oneortwo).fetchone()
    print(f"\n3. Unique ONEORTWO values:")
    print(f"   Count: {result[0]}")
    print(f"   Values: {result[1]}")

    # Check if there's duplication in the TREE_GRM_COMPONENT table itself
    grm_check = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT TRE_CN) as unique_trees,
        COUNT(*) / COUNT(DISTINCT TRE_CN) as records_per_tree
    FROM TREE_GRM_COMPONENT GRM
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND GRM.SUBP_COMPONENT_GS_TIMBER IS NOT NULL
    """

    result = conn.execute(grm_check).fetchone()
    print(f"\n4. TREE_GRM_COMPONENT duplication:")
    print(f"   Total GRM records: {result[0]:,}")
    print(f"   Unique trees: {result[1]:,}")
    print(f"   Records per tree: {result[2]:.2f}")

    # Test calculation with only 2 distinct ONEORTWO values
    test_calc = """
    WITH be_values AS (
        SELECT DISTINCT ONEORTWO
        FROM (VALUES (1.0), (2.0)) AS t(ONEORTWO)
    )
    SELECT
        COUNT(*) as row_count,
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
                WHEN BE.ONEORTWO = 2 AND GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN BE.ONEORTWO = 2 AND GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN BE.ONEORTWO = 1 AND GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                THEN -COALESCE(BEGIN.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth
    FROM TREE_GRM_COMPONENT GRM
    CROSS JOIN be_values BE
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

    result = conn.execute(test_calc).fetchone()
    print(f"\n5. Test with manual ONEORTWO (1.0, 2.0):")
    print(f"   Row count: {result[0]:,}")
    print(f"   Growth total: {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"   Difference from target: {diff:+.1f}%")

    # Maybe we should NOT use cross-join at all?
    no_cross_join = """
    SELECT
        COUNT(*) as row_count,
        SUM(
            GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER *
            CASE GRM.SUBP_SUBPTYP_GRM_GS_TIMBER
                WHEN 0 THEN 0
                WHEN 1 THEN PS.ADJ_FACTOR_SUBP
                WHEN 2 THEN PS.ADJ_FACTOR_MICR
                WHEN 3 THEN PS.ADJ_FACTOR_MACR
                ELSE 0
            END *
            -- Just use ending volume for all growth components
            CASE
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) * 0.5 as growth  -- Scale by 0.5 to account for something
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
    """

    result = conn.execute(no_cross_join).fetchone()
    print(f"\n6. Without BEGINEND cross-join (ending volume * 0.5):")
    print(f"   Row count: {result[0]:,}")
    print(f"   Growth total: {result[1]:,.0f} cu ft")
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"   Difference from target: {diff:+.1f}%")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("The 4x factor suggests we have 4 BEGINEND rows (2 per state)")
print("causing quadruple-counting when we cross-join.")