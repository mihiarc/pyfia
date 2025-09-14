#!/usr/bin/env python
"""
Debug TPA calculations to understand why values are so low.
"""

import duckdb
import polars as pl

def main():
    print("Debugging TPA Calculations")
    print("="*60)

    # Connect directly to database
    conn = duckdb.connect("data/georgia.duckdb", read_only=True)

    # Check raw TPA_UNADJ values
    print("\n1. Raw TPA_UNADJ distribution from TREE table:")
    query = """
    SELECT
        COUNT(*) as n_trees,
        AVG(TPA_UNADJ) as avg_tpa_unadj,
        MIN(TPA_UNADJ) as min_tpa,
        MAX(TPA_UNADJ) as max_tpa,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TPA_UNADJ) as median_tpa
    FROM TREE
    WHERE STATUSCD = 1  -- Live trees
        AND PLT_CN IN (
            SELECT PLT_CN
            FROM POP_PLOT_STRATUM_ASSGN
            WHERE EVALID = 132301
        )
    """
    result = conn.execute(query).fetchone()
    print(f"   N trees: {result[0]:,}")
    print(f"   Avg TPA_UNADJ: {result[1]:.2f}")
    print(f"   Min: {result[2]:.2f}, Max: {result[3]:.2f}, Median: {result[4]:.2f}")

    # Check adjustment factors
    print("\n2. Adjustment factors from POP_STRATUM:")
    query = """
    SELECT
        AVG(ADJ_FACTOR_SUBP) as avg_subp,
        AVG(ADJ_FACTOR_MICR) as avg_micr,
        AVG(ADJ_FACTOR_MACR) as avg_macr,
        MIN(ADJ_FACTOR_SUBP) as min_subp,
        MAX(ADJ_FACTOR_SUBP) as max_subp
    FROM POP_STRATUM
    WHERE CN IN (
        SELECT STRATUM_CN
        FROM POP_PLOT_STRATUM_ASSGN
        WHERE EVALID = 132301
    )
    """
    result = conn.execute(query).fetchone()
    print(f"   ADJ_FACTOR_SUBP: avg={result[0]:.4f}, min={result[3]:.4f}, max={result[4]:.4f}")
    print(f"   ADJ_FACTOR_MICR: avg={result[1]:.4f}")
    print(f"   ADJ_FACTOR_MACR: avg={result[2]:.4f}")

    # Check expansion factors
    print("\n3. Expansion factors (EXPNS):")
    query = """
    SELECT
        AVG(EXPNS) as avg_expns,
        MIN(EXPNS) as min_expns,
        MAX(EXPNS) as max_expns,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXPNS) as median_expns
    FROM POP_STRATUM
    WHERE CN IN (
        SELECT STRATUM_CN
        FROM POP_PLOT_STRATUM_ASSGN
        WHERE EVALID = 132301
    )
    """
    result = conn.execute(query).fetchone()
    print(f"   Avg EXPNS: {result[0]:.2f}")
    print(f"   Min: {result[1]:.2f}, Max: {result[2]:.2f}, Median: {result[3]:.2f}")

    # Calculate expected TPA manually
    print("\n4. Manual TPA calculation:")
    query = """
    WITH tree_data AS (
        SELECT
            t.TPA_UNADJ,
            t.DIA,
            ps.EXPNS,
            ps.ADJ_FACTOR_SUBP,
            ps.ADJ_FACTOR_MICR,
            ps.ADJ_FACTOR_MACR,
            c.CONDPROP_UNADJ,
            CASE
                WHEN t.DIA < 5.0 THEN ps.ADJ_FACTOR_MICR
                ELSE ps.ADJ_FACTOR_SUBP
            END as adj_factor
        FROM TREE t
        JOIN COND c ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON t.PLT_CN = ppsa.PLT_CN
        JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
        WHERE t.STATUSCD = 1
            AND c.COND_STATUS_CD = 1
            AND ppsa.EVALID = 132301
    )
    SELECT
        COUNT(*) as n_trees,
        -- Trees per acre calculation
        SUM(TPA_UNADJ * adj_factor * EXPNS) / SUM(CONDPROP_UNADJ * EXPNS) as tpa_manual,
        -- Basal area calculation
        SUM(3.14159 * POWER(DIA/24.0, 2) * TPA_UNADJ * adj_factor * EXPNS) /
            SUM(CONDPROP_UNADJ * EXPNS) as baa_manual,
        -- Components
        SUM(TPA_UNADJ * adj_factor * EXPNS) as tpa_numerator,
        SUM(CONDPROP_UNADJ * EXPNS) as area_denominator
    FROM tree_data
    """
    result = conn.execute(query).fetchone()
    print(f"   N trees in calculation: {result[0]:,}")
    print(f"   TPA (manual): {result[1]:.1f} trees/acre")
    print(f"   BAA (manual): {result[2]:.1f} sq ft/acre")
    print(f"   TPA numerator: {result[3]:,.0f}")
    print(f"   Area denominator: {result[4]:,.0f}")

    # Check if issue is with forestland filter
    print("\n5. Check forestland vs all conditions:")
    query = """
    SELECT
        COND_STATUS_CD,
        COUNT(DISTINCT PLT_CN) as n_plots,
        COUNT(*) as n_conditions,
        SUM(CONDPROP_UNADJ) as total_cond_prop
    FROM COND
    WHERE PLT_CN IN (
        SELECT PLT_CN
        FROM POP_PLOT_STRATUM_ASSGN
        WHERE EVALID = 132301
    )
    GROUP BY COND_STATUS_CD
    ORDER BY COND_STATUS_CD
    """
    results = conn.execute(query).fetchall()
    for row in results:
        status = {1: "Forest", 2: "Non-forest", 3: "Noncensus water", 4: "Census water", 5: "Nonsampled"}.get(row[0], f"Code {row[0]}")
        print(f"   {status}: {row[1]:,} plots, {row[2]:,} conditions, prop_sum={row[3]:.2f}")

    # Check a sample of individual trees
    print("\n6. Sample of individual tree calculations:")
    query = """
    SELECT
        t.TPA_UNADJ,
        t.DIA,
        ps.ADJ_FACTOR_SUBP,
        ps.EXPNS,
        c.CONDPROP_UNADJ,
        t.TPA_UNADJ * ps.ADJ_FACTOR_SUBP * ps.EXPNS as expanded_tpa
    FROM TREE t
    JOIN COND c ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
    JOIN POP_PLOT_STRATUM_ASSGN ppsa ON t.PLT_CN = ppsa.PLT_CN
    JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
    WHERE t.STATUSCD = 1
        AND c.COND_STATUS_CD = 1
        AND ppsa.EVALID = 132301
        AND t.DIA >= 5.0  -- Subplot trees
    LIMIT 5
    """
    results = conn.execute(query).fetchall()
    print("   TPA_UNADJ | DIA | ADJ_FACTOR | EXPNS | CONDPROP | EXPANDED")
    for row in results:
        print(f"   {row[0]:8.2f} | {row[1]:4.1f} | {row[2]:9.4f} | {row[3]:6.2f} | {row[4]:8.4f} | {row[5]:10.2f}")

    conn.close()

if __name__ == "__main__":
    main()