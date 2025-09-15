#!/usr/bin/env python
"""
Test the VarianceCalculator with real Georgia FIA data.

This script:
1. Loads Georgia FIA data
2. Runs area estimation keeping detailed data
3. Applies the VarianceCalculator properly
4. Compares results with published FIA estimates
"""

import polars as pl
from pyfia import FIA
from pyfia import area as pyfia_area  # Import with alias to avoid conflicts
from pyfia.estimation.estimators.area import AreaEstimator
from pyfia.estimation.statistics import VarianceCalculator, calculate_ratio_of_means_variance
import warnings

# Published Georgia forestland area from FIA EVALIDator
# Source: https://apps.fs.usda.gov/fiadb-api/evalidator
# Georgia's most recent evaluation (per user confirmation)
PUBLISHED_GEORGIA = {
    "total_forestland_acres": 24_172_679,  # From EVALIDator
    "forestland_se_acres": None,           # Not directly provided
    "forestland_se_percent": None,         # Will calculate from sampling error
    "sampling_error_percent": 0.563,       # Sampling error at 67% confidence level
    "n_plots": 4842                        # Number of non-zero (forested) plots
}

# Calculate SE from sampling error percentage
# Sampling error at 67% confidence = 1 * SE / Estimate * 100
# Therefore: SE = (Sampling Error % * Estimate) / 100
PUBLISHED_GEORGIA["forestland_se_acres"] = (
    PUBLISHED_GEORGIA["sampling_error_percent"] *
    PUBLISHED_GEORGIA["total_forestland_acres"] / 100
)
PUBLISHED_GEORGIA["forestland_se_percent"] = PUBLISHED_GEORGIA["sampling_error_percent"]


class AreaEstimatorWithVariance(AreaEstimator):
    """Modified AreaEstimator that properly calculates variance."""

    def __init__(self, db, config):
        super().__init__(db, config)
        self.detailed_data = None  # Store for variance calculation

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Override to preserve detailed data for variance calculation."""
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )

        # Apply area adjustment factors
        from pyfia.estimation.tree_expansion import apply_area_adjustment_factors
        data_with_strat = apply_area_adjustment_factors(
            data_with_strat,
            prop_basis_col="PROP_BASIS",
            output_col="ADJ_FACTOR_AREA"
        )

        # Collect and store detailed data BEFORE aggregation
        self.detailed_data = data_with_strat.collect()

        # Now do normal aggregation
        group_cols = []
        if self.config.get("grp_by"):
            grp_by = self.config["grp_by"]
            if isinstance(grp_by, str):
                group_cols = [grp_by]
            else:
                group_cols = list(grp_by)

        # Calculate area totals
        agg_exprs = [
            (pl.col("AREA_VALUE").cast(pl.Float64) *
             pl.col("ADJ_FACTOR_AREA").cast(pl.Float64) *
             pl.col("EXPNS").cast(pl.Float64)).sum().alias("AREA_TOTAL"),
            pl.col("EXPNS").cast(pl.Float64).sum().alias("TOTAL_EXPNS"),
            pl.count("PLT_CN").alias("N_PLOTS")
        ]

        if group_cols:
            results = self.detailed_data.group_by(group_cols).agg(agg_exprs)
        else:
            results = self.detailed_data.select(agg_exprs)

        # Add percentage if grouped
        if group_cols:
            total_area = results["AREA_TOTAL"].sum()
            results = results.with_columns([
                (100 * pl.col("AREA_TOTAL") / total_area).alias("AREA_PERCENT")
            ])

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Properly calculate variance using the VarianceCalculator."""
        if self.detailed_data is None:
            # Fallback to placeholder if no detailed data
            print("WARNING: No detailed data available for variance calculation")
            return results.with_columns([
                (pl.col("AREA_TOTAL") * 0.05).alias("AREA_SE")
            ])

        # Prepare data for variance calculation
        # Need plot-level summaries with response (area) and stratification
        plot_data = self.detailed_data.group_by(["PLT_CN", "ESTN_UNIT", "STRATUM_CN"]).agg([
            (pl.col("AREA_VALUE") * pl.col("ADJ_FACTOR_AREA")).sum().alias("PLOT_AREA"),
            pl.first("EXPNS").alias("EXPNS"),
            pl.lit(1.0).alias("AREA_USED")  # For ratio estimation denominator
        ])

        # Calculate variance using ratio-of-means estimator
        try:
            variance_results = calculate_ratio_of_means_variance(
                plot_data,
                response_col="PLOT_AREA",
                area_col="AREA_USED",
                strata_col="STRATUM_CN",
                plot_col="PLT_CN",
                weight_col="EXPNS"
            )

            # Extract SE and add to results
            se = variance_results["SE"][0]
            se_percent = variance_results["SE_PERCENT"][0]

            results = results.with_columns([
                pl.lit(se * results["AREA_TOTAL"][0]).alias("AREA_SE"),
                pl.lit(se_percent).alias("AREA_SE_PERCENT"),
                pl.lit(variance_results["VARIANCE"][0]).alias("AREA_VARIANCE")
            ])

            print(f"\n=== VARIANCE CALCULATION DETAILS ===")
            print(f"Estimate: {variance_results['ESTIMATE'][0]:,.2f}")
            print(f"Variance: {variance_results['VARIANCE'][0]:,.2f}")
            print(f"SE: {se:,.6f}")
            print(f"SE%: {se_percent:.2f}%")

        except Exception as e:
            print(f"ERROR in variance calculation: {e}")
            # Fallback to placeholder
            results = results.with_columns([
                (pl.col("AREA_TOTAL") * 0.05).alias("AREA_SE")
            ])

        return results


def test_georgia_variance():
    """Test variance calculation with Georgia FIA data."""

    print("=== TESTING VARIANCE CALCULATOR WITH GEORGIA FIA DATA ===\n")

    # Load Georgia database
    print("Loading Georgia FIA database...")
    db_path = "data/georgia.duckdb"

    try:
        with FIA(db_path) as db:
            # Get most recent evaluation
            print("Selecting most recent EXPALL evaluation...")
            db.clip_most_recent(eval_type="ALL")

            if db.evalid:
                print(f"Using EVALID: {db.evalid}")

                # Query to understand the evaluation
                import duckdb
                try:
                    # Try different ways to get the connection
                    if hasattr(db, 'conn'):
                        conn = db.conn
                    elif hasattr(db, '_reader') and hasattr(db._reader, 'conn'):
                        conn = db._reader.conn
                    elif hasattr(db, 'reader') and hasattr(db.reader, 'conn'):
                        conn = db.reader.conn
                    else:
                        # Try to get from tables if they're loaded
                        conn = None

                    if conn:
                        eval_info = conn.execute(f"""
                            SELECT
                                pe.EVALID,
                                pe.EVAL_DESCR,
                                pe.STATECD,
                                pe.REPORT_YEAR_NM,
                                COUNT(DISTINCT ppsa.PLT_CN) as total_plots,
                                COUNT(DISTINCT CASE WHEN c.COND_STATUS_CD = 1
                                      THEN ppsa.PLT_CN END) as forest_plots
                            FROM POP_EVAL pe
                            LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
                            LEFT JOIN PLOT p ON ppsa.PLT_CN = p.CN
                            LEFT JOIN COND c ON p.CN = c.PLT_CN
                            WHERE pe.EVALID IN ({','.join(str(e) for e in db.evalid)})
                            GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD, pe.REPORT_YEAR_NM
                        """).fetchall()

                        for row in eval_info:
                            print(f"\nEvaluation details:")
                            print(f"  EVALID: {row[0]}")
                            print(f"  Description: {row[1]}")
                            print(f"  State: {row[2]}")
                            print(f"  Report Year: {row[3]}")
                            print(f"  Total plots in evaluation: {row[4]}")
                            print(f"  Forest plots (COND_STATUS_CD=1): {row[5]}")
                    else:
                        print("Could not get database connection for eval details")
                except Exception as e:
                    print(f"Could not query evaluation details: {e}")

            # First, run standard area function to see current behavior
            print("\n1. STANDARD AREA FUNCTION (with placeholder variance):")
            print("-" * 50)

            standard_results = pyfia_area(db, land_type="forest")
            print(f"Columns in result: {standard_results.columns}")
            print(f"First few rows:\n{standard_results.head()}")

            # Use the actual column names
            if 'AREA' in standard_results.columns:
                area_col = 'AREA'
            elif 'AREA_TOTAL' in standard_results.columns:
                area_col = 'AREA_TOTAL'
            else:
                area_col = None
                print("WARNING: Could not find area column")

            if area_col:
                print(f"Forest area: {standard_results[area_col][0]:,.0f} acres")
            if 'AREA_SE' in standard_results.columns:
                print(f"Standard error: {standard_results['AREA_SE'][0]:,.0f} acres")
                se_percent = (standard_results['AREA_SE'][0] / standard_results[area_col][0]) * 100
                print(f"SE as % of estimate: {se_percent:.2f}%")
            print(f"Number of plots: {standard_results['N_PLOTS'][0]}")

            # Now test with proper variance calculation
            print("\n2. MODIFIED ESTIMATOR WITH PROPER VARIANCE:")
            print("-" * 50)

            # Create modified estimator
            config = {
                "land_type": "forest",
                "area_domain": None,
                "grp_by": None,
                "variance": True,
                "totals": True
            }

            estimator = AreaEstimatorWithVariance(db, config)
            modified_results = estimator.estimate()

            # Check column names
            print(f"Modified result columns: {modified_results.columns}")

            # Find the area column
            if 'AREA' in modified_results.columns:
                area_col_mod = 'AREA'
            elif 'AREA_TOTAL' in modified_results.columns:
                area_col_mod = 'AREA_TOTAL'
            else:
                area_col_mod = None

            if area_col_mod:
                print(f"Forest area: {modified_results[area_col_mod][0]:,.0f} acres")
            if 'AREA_SE' in modified_results.columns:
                print(f"Standard error: {modified_results['AREA_SE'][0]:,.0f} acres")
                if area_col_mod:
                    se_percent = (modified_results['AREA_SE'][0] / modified_results[area_col_mod][0]) * 100
                    print(f"SE as % of estimate: {se_percent:.2f}%")
            if 'AREA_VARIANCE' in modified_results.columns:
                print(f"Variance: {modified_results['AREA_VARIANCE'][0]:,.2f}")
            print(f"Number of plots: {modified_results['N_PLOTS'][0]}")

            # Compare with published values
            print("\n3. COMPARISON WITH PUBLISHED FIA ESTIMATES (EVALIDator):")
            print("-" * 50)
            print(f"Published forest area: {PUBLISHED_GEORGIA['total_forestland_acres']:,} acres")
            print(f"Published SE: {PUBLISHED_GEORGIA['forestland_se_acres']:,.0f} acres")
            print(f"Published SE%: {PUBLISHED_GEORGIA['forestland_se_percent']:.3f}%")
            print(f"Published sampling error%: {PUBLISHED_GEORGIA['sampling_error_percent']:.3f}%")
            print(f"Published N non-zero plots: {PUBLISHED_GEORGIA['n_plots']:,}")
            print("\nNote: SE calculated from sampling error % at 67% confidence level")

            # Calculate differences
            print("\n4. ANALYSIS:")
            print("-" * 50)

            if area_col_mod:
                area_diff = modified_results[area_col_mod][0] - PUBLISHED_GEORGIA['total_forestland_acres']
                area_diff_pct = (area_diff / PUBLISHED_GEORGIA['total_forestland_acres']) * 100
                print(f"Area difference: {area_diff:,.0f} acres ({area_diff_pct:+.2f}%)")

            if 'AREA_SE' in modified_results.columns:
                se_diff = modified_results['AREA_SE'][0] - PUBLISHED_GEORGIA['forestland_se_acres']
                se_diff_pct = (se_diff / PUBLISHED_GEORGIA['forestland_se_acres']) * 100
                print(f"SE difference: {se_diff:,.0f} acres ({se_diff_pct:+.2f}%)")

                # Compare SE percentages
                if area_col_mod:
                    calc_se_pct = (modified_results['AREA_SE'][0] / modified_results[area_col_mod][0]) * 100
                    se_pct_diff = calc_se_pct - PUBLISHED_GEORGIA['forestland_se_percent']
                    print(f"SE% difference: {se_pct_diff:+.2f} percentage points")

            # Test with grouping to see if variance calculation works
            print("\n5. TEST WITH GROUPING (by ownership):")
            print("-" * 50)

            config_grouped = {
                "land_type": "forest",
                "area_domain": None,
                "grp_by": "OWNGRPCD",
                "variance": True,
                "totals": True
            }

            estimator_grouped = AreaEstimatorWithVariance(db, config_grouped)
            grouped_results = estimator_grouped.estimate()

            print("\nOwnership Group Results:")
            # Find area column in grouped results
            if 'AREA' in grouped_results.columns:
                area_col_grp = 'AREA'
            elif 'AREA_TOTAL' in grouped_results.columns:
                area_col_grp = 'AREA_TOTAL'
            else:
                area_col_grp = None

            if area_col_grp:
                for i in range(min(5, len(grouped_results))):
                    own = grouped_results['OWNGRPCD'][i]
                    area = grouped_results[area_col_grp][i]
                    if 'AREA_SE' in grouped_results.columns:
                        se = grouped_results['AREA_SE'][i]
                        se_pct = (se / area) * 100 if area > 0 else 0
                        print(f"  Ownership {own}: {area:,.0f} acres (SE: {se:,.0f}, {se_pct:.2f}%)")
                    else:
                        print(f"  Ownership {own}: {area:,.0f} acres")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Suppress warnings for cleaner output
    warnings.filterwarnings("ignore")

    test_georgia_variance()