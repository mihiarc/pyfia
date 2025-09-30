#!/usr/bin/env python
"""
Debug pyFIA growth calculation to understand discrepancy.
"""

import sys
sys.path.insert(0, '/home/mihiarc/pyfia/src')

import polars as pl
from pyfia import FIA
import duckdb

# Test with the database
db_path = "./data/test_southern.duckdb"

# Connect to database and filter to Georgia
db = FIA(db_path)
db.clip_by_evalid([132303])

print("=" * 80)
print("DEBUGGING PYFIA GROWTH CALCULATION")
print("=" * 80)

# Get the growth estimator
from pyfia.estimation.estimators.growth import GrowthEstimator

config = {
    "grp_by": None,
    "by_species": False,
    "by_size_class": False,
    "land_type": "timber",
    "tree_type": "gs",
    "measure": "volume",
    "tree_domain": None,
    "area_domain": None,
    "totals": True,
    "variance": False,
    "most_recent": False,
    "include_cv": False
}

estimator = GrowthEstimator(db, config)

# Load data
print("\n1. Loading data...")
try:
    data = estimator.load_data()
    if data:
        # Collect to check
        df = data.limit(10).collect()
        print(f"   Data loaded: {len(df)} sample rows")
        print(f"   Columns: {df.columns[:10]}...")  # First 10 columns

        # Check for key columns
        key_cols = ["ONEORTWO", "COMPONENT", "TPAGROW_UNADJ", "TREE_VOLCFNET",
                    "VOLCFNET", "BEGIN_VOLCFNET", "PTREE_VOLCFNET"]
        for col in key_cols:
            if col in df.columns:
                print(f"   ✓ {col} present")
            else:
                print(f"   ✗ {col} MISSING")
except Exception as e:
    print(f"   ERROR loading data: {e}")
    import traceback
    traceback.print_exc()

# Apply filters
print("\n2. Applying filters...")
try:
    filtered_data = estimator.apply_filters(data)
    # Count records after filtering
    count_query = filtered_data.select([
        pl.count().alias("total_records"),
        pl.col("COMPONENT").n_unique().alias("unique_components"),
        pl.col("ONEORTWO").n_unique().alias("unique_oneortwo")
    ]).collect()

    print(f"   Records after filtering: {count_query['total_records'][0]:,}")
    print(f"   Unique components: {count_query['unique_components'][0]}")
    print(f"   Unique ONEORTWO values: {count_query['unique_oneortwo'][0]}")
except Exception as e:
    print(f"   ERROR applying filters: {e}")

# Calculate values
print("\n3. Calculating values...")
try:
    calculated_data = estimator.calculate_values(filtered_data)

    # Check calculation by ONEORTWO and component
    summary = calculated_data.group_by(["ONEORTWO", "COMPONENT"]).agg([
        pl.count().alias("count"),
        pl.col("GROWTH_VALUE").sum().alias("growth_sum")
    ]).sort(["ONEORTWO", "COMPONENT"]).collect()

    print("   Growth by ONEORTWO and component:")
    for row in summary.iter_rows():
        if row[1]:  # If component is not null
            print(f"   ONEORTWO={row[0]:.0f}, {row[1]:20s}: {row[3]:15,.0f}")
except Exception as e:
    print(f"   ERROR calculating values: {e}")

# Direct SQL comparison
print("\n4. Direct SQL comparison:")
with duckdb.connect(db_path, read_only=True) as conn:
    sql_result = conn.execute("""
    SELECT SUM(growth) / 2.0 as avg_growth
    FROM (
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
                            THEN T.VOLCFNET / PLOT.REMPER
                            ELSE 0
                        END
                    WHEN BE.ONEORTWO = 1 THEN
                        CASE
                            WHEN GRM.SUBP_COMPONENT_GS_TIMBER = 'SURVIVOR'
                            THEN -COALESCE(BEGIN.VOLCFNET, 0) / PLOT.REMPER
                            ELSE 0
                        END
                    ELSE 0
                END *
                PS.EXPNS
            ) as growth
        FROM BEGINEND BE
        CROSS JOIN TREE_GRM_COMPONENT GRM
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
        GROUP BY BE.ONEORTWO
    )
    """).fetchone()

    print(f"   Direct SQL (simplified): {sql_result[0]:,.0f} cu ft")
    print(f"   Target:                   2,473,614,987 cu ft")

print("\n" + "=" * 80)