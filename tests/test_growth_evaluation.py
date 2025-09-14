#!/usr/bin/env python
"""
Test script to evaluate the growth() function against EVALIDator query.

This script runs the EVALIDator SQL query for average annual net growth
and compares it with pyFIA's growth() function results.
"""

import duckdb
import polars as pl
from pyfia import FIA
from pyfia.estimation.estimators.growth import growth


def run_evalidator_query(db_path: str, evalid: int = 132303) -> pl.DataFrame:
    """
    Run the EVALIDator query for growth on forest land grouped by stocking class.

    Query calculates: Average annual net growth of merchantable bole wood volume
    of growing-stock trees (at least 5 inches d.b.h.), in cubic feet, on forest land.
    """

    # The EVALIDator SQL query with Georgia EVALID
    query = """
    SELECT
        GRP1,
        GRP2,
        sum(ESTIMATED_VALUE * EXPNS) ESTIMATE
    FROM (
        SELECT
            pop_stratum.estn_unit_cn,
            pop_stratum.cn STRATACN,
            plot.cn plot_cn,
            plot.prev_plt_cn,
            cond.cn cond_cn,
            plot.lat,
            plot.lon,
            pop_stratum.expns EXPNS,
            case coalesce(cond.alstkcd,-1)
                when 1 then '`0001 Overstocked'
                when 2 then '`0002 Fully stocked'
                when 3 then '`0003 Medium stocked'
                when 4 then '`0004 Poorly stocked'
                when 5 then '`0005 Nonstocked'
                when -1 then '`0006 Unavailable'
                else '`0007 Other'
            end GRP1,
            case coalesce(cond.alstkcd,-1)
                when 1 then '`0001 Overstocked'
                when 2 then '`0002 Fully stocked'
                when 3 then '`0003 Medium stocked'
                when 4 then '`0004 Poorly stocked'
                when 5 then '`0005 Nonstocked'
                when -1 then '`0006 Unavailable'
                else '`0007 Other'
            end GRP2,
            SUM(
                GRM.TPAGROW_UNADJ *
                (CASE
                    WHEN COALESCE(GRM.SUBPTYP_GRM, 0) = 0 THEN (0)
                    WHEN GRM.SUBPTYP_GRM = 1 THEN POP_STRATUM.ADJ_FACTOR_SUBP
                    WHEN GRM.SUBPTYP_GRM = 2 THEN POP_STRATUM.ADJ_FACTOR_MICR
                    WHEN GRM.SUBPTYP_GRM = 3 THEN POP_STRATUM.ADJ_FACTOR_MACR
                    ELSE (0)
                END) *
                (CASE
                    WHEN BE.ONEORTWO = 2 THEN
                        (CASE
                            WHEN (GRM.COMPONENT = 'SURVIVOR' OR GRM.COMPONENT = 'INGROWTH' OR GRM.COMPONENT LIKE 'REVERSION%')
                            THEN (TREE.VOLCFNET / PLOT.REMPER)
                            WHEN (GRM.COMPONENT LIKE 'CUT%' OR GRM.COMPONENT LIKE 'DIVERSION%')
                            THEN (TRE_MIDPT.VOLCFNET / PLOT.REMPER)
                            ELSE (0)
                        END)
                    ELSE
                        (CASE
                            WHEN (GRM.COMPONENT = 'SURVIVOR' OR GRM.COMPONENT = 'CUT1' OR
                                  GRM.COMPONENT = 'DIVERSION1' OR GRM.COMPONENT = 'MORTALITY1')
                            THEN
                                CASE
                                    WHEN TRE_BEGIN.TRE_CN IS NOT NULL
                                    THEN - (TRE_BEGIN.VOLCFNET / PLOT.REMPER)
                                    ELSE - (PTREE.VOLCFNET / PLOT.REMPER)
                                END
                            ELSE (0)
                        END)
                END)
            ) AS ESTIMATED_VALUE
        FROM
            BEGINEND BE,
            POP_STRATUM POP_STRATUM
            JOIN POP_PLOT_STRATUM_ASSGN POP_PLOT_STRATUM_ASSGN ON (POP_STRATUM.CN = POP_PLOT_STRATUM_ASSGN.STRATUM_CN)
            JOIN PLOT PLOT ON (POP_PLOT_STRATUM_ASSGN.PLT_CN = PLOT.CN)
            JOIN PLOTGEOM PLOTGEOM ON (PLOT.CN = PLOTGEOM.CN)
            JOIN PLOT PPLOT ON (PLOT.PREV_PLT_CN = PPLOT.CN)
            JOIN COND PCOND ON (PLOT.PREV_PLT_CN = PCOND.PLT_CN)
            JOIN COND COND ON (PLOT.CN = COND.PLT_CN)
            JOIN TREE TREE ON (TREE.CONDID = COND.CONDID AND TREE.PLT_CN = PLOT.CN AND TREE.PREVCOND = PCOND.CONDID)
            LEFT OUTER JOIN TREE PTREE ON (TREE.PREV_TRE_CN = PTREE.CN)
            LEFT OUTER JOIN TREE_GRM_BEGIN TRE_BEGIN ON (TREE.CN = TRE_BEGIN.TRE_CN)
            LEFT OUTER JOIN TREE_GRM_MIDPT TRE_MIDPT ON (TREE.CN = TRE_MIDPT.TRE_CN)
            LEFT OUTER JOIN (
                SELECT
                    TRE_CN,
                    DIA_BEGIN,
                    DIA_MIDPT,
                    DIA_END,
                    SUBP_COMPONENT_GS_FOREST AS COMPONENT,
                    SUBP_SUBPTYP_GRM_GS_FOREST AS SUBPTYP_GRM,
                    SUBP_TPAGROW_UNADJ_GS_FOREST AS TPAGROW_UNADJ
                FROM TREE_GRM_COMPONENT
            ) GRM ON (TREE.CN = GRM.TRE_CN)
        WHERE
            1=1
            AND ((pop_stratum.rscd=33 and pop_stratum.evalid={evalid}))
        GROUP BY
            pop_stratum.estn_unit_cn,
            pop_stratum.cn,
            plot.cn,
            plot.prev_plt_cn,
            cond.cn,
            plot.lat,
            plot.lon,
            pop_stratum.expns,
            case coalesce(cond.alstkcd,-1)
                when 1 then '`0001 Overstocked'
                when 2 then '`0002 Fully stocked'
                when 3 then '`0003 Medium stocked'
                when 4 then '`0004 Poorly stocked'
                when 5 then '`0005 Nonstocked'
                when -1 then '`0006 Unavailable'
                else '`0007 Other'
            end,
            case coalesce(cond.alstkcd,-1)
                when 1 then '`0001 Overstocked'
                when 2 then '`0002 Fully stocked'
                when 3 then '`0003 Medium stocked'
                when 4 then '`0004 Poorly stocked'
                when 5 then '`0005 Nonstocked'
                when -1 then '`0006 Unavailable'
                else '`0007 Other'
            end
    )
    GROUP BY GRP1, GRP2
    ORDER BY GRP1, GRP2
    """.format(evalid=evalid)

    with duckdb.connect(db_path, read_only=True) as conn:
        result = conn.execute(query).fetchall()

    # Convert to Polars DataFrame
    df = pl.DataFrame(
        result,
        schema=["GRP1", "GRP2", "ESTIMATE"],
        orient="row"
    )

    return df


def run_pyfia_growth(db_path: str, evalid: int = 132303) -> pl.DataFrame:
    """
    Run new pyFIA growth() function with parameters matching EVALIDator query.
    """

    # Initialize FIA database
    with FIA(db_path) as db:
        # Filter to specific EVALID
        db.clip_by_evalid([evalid])

        # Run growth estimation using new GRM-based implementation
        # EVALIDator query groups by ALSTKCD (stocking class)
        # and calculates net growth of growing stock on forest land
        results = growth(
            db,
            grp_by="ALSTKCD",    # Group by stocking class
            land_type="forest",  # Forest land only
            tree_type="gs",      # Growing stock trees
            measure="volume",    # Volume in cubic feet
            tree_domain="DIA_MIDPT >= 5.0",  # Growing stock trees >= 5" DBH
            totals=True          # Include totals for comparison
        )

    return results




def main():
    """Main test execution."""

    db_path = "data/georgia.duckdb"
    evalid = 132303

    print("=" * 80)
    print("GROWTH FUNCTION EVALUATION")
    print("=" * 80)
    print(f"Database: {db_path}")
    print(f"EVALID: {evalid}")
    print(f"Metric: Average annual net growth of merchantable bole wood volume")
    print(f"Trees: Growing-stock trees (at least 5 inches d.b.h.)")
    print(f"Land: Forest land")
    print(f"Grouping: By stocking class (ALSTKCD)")
    print()

    # Run EVALIDator query
    print("Running EVALIDator query...")
    try:
        eval_results = run_evalidator_query(db_path, evalid)
        print(f"EVALIDator returned {len(eval_results)} rows")
        print("\nEVALIDator Results:")
        print(eval_results)
    except Exception as e:
        print(f"EVALIDator query failed: {e}")
        eval_results = None

    print("\n" + "-" * 80)

    # Run new pyFIA growth function
    print("\nRunning new pyFIA growth() function...")
    try:
        pyfia_results = run_pyfia_growth(db_path, evalid)
        print(f"pyFIA returned {len(pyfia_results)} rows")
        print("\npyFIA Results:")
        print(pyfia_results)
    except Exception as e:
        print(f"pyFIA growth() failed: {e}")
        import traceback
        traceback.print_exc()
        pyfia_results = None

    print("\n" + "=" * 80)
    print("COMPARISON")
    print("=" * 80)

    if eval_results is not None and pyfia_results is not None:
        # Compare EVALIDator with new pyFIA results
        print("\nComparing EVALIDator vs new pyFIA growth():")

        # Show both results side by side
        print("\nEVALIDator totals by stocking class:")
        eval_sorted = eval_results.sort("GRP1")
        for row in eval_sorted.iter_rows(named=True):
            stocking = row["GRP1"].replace("`000", "").replace(" ", "")
            print(f"  {stocking:15}: {row['ESTIMATE']:>15,.0f} cubic feet")

        if len(pyfia_results) > 0:
            print("\npyFIA totals by stocking class:")
            # Check if we have ALSTKCD grouping in results
            if "ALSTKCD" in pyfia_results.columns:
                pyfia_sorted = pyfia_results.sort("ALSTKCD")
                for row in pyfia_sorted.iter_rows(named=True):
                    alstkcd = row["ALSTKCD"]
                    # Map ALSTKCD to readable labels
                    stocking_map = {
                        1: "1Overstocked",
                        2: "2Fullystocked",
                        3: "3Mediumstocked",
                        4: "4Poorlystocked",
                        5: "5Nonstocked",
                        None: "6Unavailable"
                    }
                    stocking = stocking_map.get(alstkcd, f"{alstkcd}Unknown")
                    growth_val = row.get("GROWTH_TOTAL", 0)
                    print(f"  {stocking:15}: {growth_val:>15,.0f} cubic feet")
            else:
                # Single total result
                growth_val = pyfia_results["GROWTH_TOTAL"][0] if "GROWTH_TOTAL" in pyfia_results.columns else 0
                print(f"  Total Growth   : {growth_val:>15,.0f} cubic feet")

        # Calculate total comparison
        eval_total = eval_results["ESTIMATE"].sum()
        if "GROWTH_TOTAL" in pyfia_results.columns:
            pyfia_total = pyfia_results["GROWTH_TOTAL"].sum()
            print(f"\nTotal Comparison:")
            print(f"  EVALIDator: {eval_total:>15,.0f} cubic feet")
            print(f"  pyFIA:      {pyfia_total:>15,.0f} cubic feet")
            if eval_total > 0:
                diff_pct = ((pyfia_total - eval_total) / eval_total) * 100
                print(f"  Difference: {diff_pct:>14.1f}%")

    elif eval_results is not None:
        print("\nOnly EVALIDator results available for comparison")
        print("pyFIA function failed to run")

    elif pyfia_results is not None:
        print("\nOnly pyFIA results available")
        print("EVALIDator query failed to run")

    print("\n" + "=" * 80)
    print("ANALYSIS")
    print("=" * 80)
    print("\nKey differences identified:")
    print("1. pyFIA uses TREE_GRM table (doesn't exist in georgia.duckdb)")
    print("2. EVALIDator uses TREE_GRM_COMPONENT with component-based logic")
    print("3. EVALIDator uses SUBPTYP_GRM for adjustment factors")
    print("4. EVALIDator calculates volume changes based on BEGINEND.ONEORTWO")
    print("5. pyFIA needs to be updated to use the GRM component methodology")


if __name__ == "__main__":
    main()