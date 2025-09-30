#!/usr/bin/env python
"""
Investigate the relationship between TREE, TREE_GRM_MIDPT, and TREE_GRM_BEGIN tables.

Understanding which volume to use for growth calculation.
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("INVESTIGATING GRM TABLE RELATIONSHIPS")
    print("=" * 80)

    # Check what TREE_GRM_MIDPT represents
    print("\n1. Understanding TREE_GRM_MIDPT:")

    # Check if TREE and TREE_GRM_MIDPT volumes are the same
    volume_comparison = """
    SELECT
        COUNT(*) as total_trees,
        SUM(CASE WHEN T.VOLCFNET = MIDPT.VOLCFNET THEN 1 ELSE 0 END) as same_volume,
        SUM(CASE WHEN T.VOLCFNET > MIDPT.VOLCFNET THEN 1 ELSE 0 END) as tree_greater,
        SUM(CASE WHEN T.VOLCFNET < MIDPT.VOLCFNET THEN 1 ELSE 0 END) as tree_less,
        AVG(T.VOLCFNET) as avg_tree_vol,
        AVG(MIDPT.VOLCFNET) as avg_midpt_vol,
        AVG(BEGIN.VOLCFNET) as avg_begin_vol
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    LEFT JOIN TREE_GRM_BEGIN BEGIN ON GRM.TRE_CN = BEGIN.TRE_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND GRM.SUBP_COMPONENT_GS_TIMBER IN ('SURVIVOR', 'INGROWTH')
    """

    result = conn.execute(volume_comparison).fetchone()
    print(f"  Total trees: {result[0]:,}")
    print(f"  Same volume (TREE = MIDPT): {result[1]:,} ({result[1]/result[0]*100:.1f}%)")
    print(f"  TREE > MIDPT: {result[2]:,} ({result[2]/result[0]*100:.1f}%)")
    print(f"  TREE < MIDPT: {result[3]:,} ({result[3]/result[0]*100:.1f}%)")
    print(f"  Average TREE volume: {result[4]:.1f} cu ft")
    print(f"  Average MIDPT volume: {result[5]:.1f} cu ft")
    print(f"  Average BEGIN volume: {result[6]:.1f} cu ft")

    # Check diameter relationships
    print("\n2. Diameter relationships:")
    dia_comparison = """
    SELECT
        AVG(GRM.DIA_BEGIN) as avg_dia_begin,
        AVG(GRM.DIA_MIDPT) as avg_dia_midpt,
        AVG(GRM.DIA_END) as avg_dia_end,
        AVG(T.DIA) as avg_tree_dia,
        AVG(MIDPT.DIA) as avg_midpt_dia,
        COUNT(*) as count
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
    """

    result = conn.execute(dia_comparison).fetchone()
    print(f"  DIA_BEGIN (GRM_COMPONENT): {result[0]:.2f} inches")
    print(f"  DIA_MIDPT (GRM_COMPONENT): {result[1]:.2f} inches")
    print(f"  DIA_END (GRM_COMPONENT): {result[2]:.2f} inches")
    print(f"  DIA (TREE table): {result[3]:.2f} inches")
    print(f"  DIA (TREE_GRM_MIDPT): {result[4]:.2f} inches")

    # Check if TREE represents the ending inventory
    print("\n3. Is TREE the ending inventory?")
    tree_vs_end = """
    SELECT
        SUM(CASE WHEN ABS(GRM.DIA_END - T.DIA) < 0.1 THEN 1 ELSE 0 END) as dia_match,
        SUM(CASE WHEN ABS(GRM.DIA_MIDPT - MIDPT.DIA) < 0.1 THEN 1 ELSE 0 END) as midpt_match,
        COUNT(*) as total
    FROM TREE_GRM_COMPONENT GRM
    JOIN TREE T ON GRM.TRE_CN = T.CN
    LEFT JOIN TREE_GRM_MIDPT MIDPT ON GRM.TRE_CN = MIDPT.TRE_CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
    """

    result = conn.execute(tree_vs_end).fetchone()
    print(f"  DIA_END matches TREE.DIA: {result[0]:,} / {result[2]:,} ({result[0]/result[2]*100:.1f}%)")
    print(f"  DIA_MIDPT matches MIDPT.DIA: {result[1]:,} / {result[2]:,} ({result[1]/result[2]*100:.1f}%)")

    # Check REMPER values
    print("\n4. Remeasurement periods:")
    remper_check = """
    SELECT
        PLOT.REMPER,
        COUNT(*) as count,
        AVG(GRM.DIA_END - GRM.DIA_BEGIN) as avg_dia_growth
    FROM TREE_GRM_COMPONENT GRM
    JOIN PLOT ON GRM.PLT_CN = PLOT.CN
    JOIN POP_PLOT_STRATUM_ASSGN PPSA ON GRM.PLT_CN = PPSA.PLT_CN
    WHERE PPSA.EVALID = 132303
      AND GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
    GROUP BY PLOT.REMPER
    ORDER BY count DESC
    LIMIT 10
    """

    print(f"  {'REMPER':8s} {'Count':>8s} {'Avg Growth':>12s}")
    results = conn.execute(remper_check).fetchall()
    for row in results:
        print(f"  {row[0]:8.1f} {row[1]:8,} {row[2]:12.3f} in")

    # What's the standard calculation?
    print("\n5. Standard NET growth calculation:")
    print("  For SURVIVOR: (Ending - Beginning) / REMPER")
    print("  For INGROWTH: Ending / REMPER")
    print("  For REVERSION: Ending / REMPER")

    # Test standard calculation
    standard_calc = """
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
                THEN (COALESCE(T.VOLCFNET, 0) - COALESCE(BEGIN.VOLCFNET, 0)) / COALESCE(PLOT.REMPER, 5.0)
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(T.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
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
    """

    result = conn.execute(standard_calc).fetchone()
    print(f"\n  Using TREE as ending: {result[0]:,.0f} cu ft")
    print(f"  Target:               2,473,614,987 cu ft")
    print(f"  Difference:           {((result[0] - 2473614987)/2473614987*100):+.1f}%")

    # Try with MIDPT
    midpt_calc = """
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
        ) as growth
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

    result = conn.execute(midpt_calc).fetchone()
    print(f"\n  Using MIDPT as ending: {result[0]:,.0f} cu ft")
    print(f"  Target:                2,473,614,987 cu ft")
    print(f"  Difference:            {((result[0] - 2473614987)/2473614987*100):+.1f}%")

print("\n" + "=" * 80)
print("CONCLUSION")
print("=" * 80)
print("TREE table appears to represent the ending (current) inventory")
print("TREE_GRM_MIDPT represents an interpolated midpoint value")
print("The correct approach should use TREE for ending volumes")