#!/usr/bin/env python
"""
Test different growth calculation hypotheses to match EVALIDator.

Target: 2,473,614,987 cu ft
Current NET (ONEORTWO sum): 1,497,121,950 cu ft (-39.5%)
Current GROSS (ONEORTWO=2): 9,868,948,138 cu ft (+299%)
"""

import duckdb

db_path = "./data/test_southern.duckdb"

with duckdb.connect(db_path, read_only=True) as conn:
    print("=" * 80)
    print("TESTING GROWTH CALCULATION HYPOTHESES")
    print("=" * 80)
    print("Target: 2,473,614,987 cu ft")
    print("=" * 80)

    # Hypothesis 1: Maybe we shouldn't multiply by EXPNS in both ONEORTWO rows?
    # (avoiding double-counting the expansion factor)
    hypothesis1 = """
    WITH calc AS (
        SELECT
            BE.ONEORTWO,
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
                PS.EXPNS / 2.0  -- Divide by 2 to account for duplication
            ) as growth
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
          AND GRM.SUBP_TPAGROW_UNADJ_GS_TIMBER > 0
        GROUP BY BE.ONEORTWO
    )
    SELECT 'Hypothesis 1: EXPNS/2' as method, SUM(growth) as total FROM calc
    """

    result = conn.execute(hypothesis1).fetchone()
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"\n{result[0]}: {result[1]:,.0f} ({diff:+.1f}%)")

    # Hypothesis 2: Only use positive growth (ONEORTWO=2) but scale differently
    hypothesis2 = """
    SELECT
        'Hypothesis 2: Only ONEORTWO=2, scaled' as method,
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
            PS.EXPNS * 0.25  -- Scale factor to match target
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

    result = conn.execute(hypothesis2).fetchone()
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"{result[0]}: {result[1]:,.0f} ({diff:+.1f}%)")

    # Hypothesis 3: Check if MIDPT volume should be used differently
    hypothesis3 = """
    SELECT
        'Hypothesis 3: Average of begin/end for SURVIVOR' as method,
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
                THEN ((COALESCE(MIDPT.VOLCFNET, 0) + COALESCE(BEGIN.VOLCFNET, 0)) / 2.0) / COALESCE(PLOT.REMPER, 5.0)
                WHEN GRM.SUBP_COMPONENT_GS_TIMBER IN ('INGROWTH')
                  OR GRM.SUBP_COMPONENT_GS_TIMBER LIKE 'REVERSION%'
                THEN COALESCE(MIDPT.VOLCFNET, 0) / COALESCE(PLOT.REMPER, 5.0)
                ELSE 0
            END *
            PS.EXPNS
        ) as growth
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

    result = conn.execute(hypothesis3).fetchone()
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"{result[0]}: {result[1]:,.0f} ({diff:+.1f}%)")

    # Hypothesis 4: What if we're missing certain conditions on the tree selection?
    hypothesis4 = """
    SELECT
        'Hypothesis 4: Filter by MIDPT volume > 0' as method,
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
      AND MIDPT.VOLCFNET > 0  -- Only trees with positive volume
    """

    result = conn.execute(hypothesis4).fetchone()
    diff = ((result[1] - 2473614987) / 2473614987 * 100)
    print(f"{result[0]}: {result[1]:,.0f} ({diff:+.1f}%)")

    print("\n" + "=" * 80)
    print("ANALYSIS")
    print("=" * 80)

    # Check what scale factor would make ONEORTWO=2 match the target
    gross_value = 9868948138
    target = 2473614987
    scale_factor = target / gross_value
    print(f"Scale factor needed for ONEORTWO=2: {scale_factor:.4f}")
    print(f"This is approximately 1/4, suggesting we might be quadruple-counting somehow")