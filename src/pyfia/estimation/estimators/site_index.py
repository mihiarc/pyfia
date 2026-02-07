"""
Site index estimation for FIA data.

Calculates area-weighted mean site index following Bechtold & Patterson (2005)
methodology. Site index values represent expected dominant tree height (feet)
at a specified base age, indicating site productivity.
"""

from __future__ import annotations

import polars as pl

from ...core import FIA
from ...filtering import get_land_domain_indicator
from ..base import AggregationResult, BaseEstimator
from ..tree_expansion import apply_area_adjustment_factors
from ..utils import (
    ensure_evalid_set,
    ensure_fia_instance,
    validate_estimator_inputs,
)


class SiteIndexEstimator(BaseEstimator):
    """
    Site index estimator for FIA data.

    Estimates area-weighted mean site index values by various categories.
    Site index is a condition-level attribute representing expected dominant
    tree height at a base age.
    """

    def __init__(self, db: str | FIA, config: dict) -> None:
        """Initialize the site index estimator."""
        super().__init__(db, config)

    def get_required_tables(self) -> list[str]:
        """Site index estimation requires COND, PLOT, and stratification tables."""
        return ["COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_cond_columns(self) -> list[str]:
        """Get required condition columns for site index estimation."""
        # Core columns always needed
        core_cols = [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "PROP_BASIS",
            # Site index columns
            "SICOND",
            "SIBASE",
            "SISP",
        ]

        # Additional columns for timber land filtering
        filter_cols: set[str] = set()
        land_type = self.config.get("land_type", "forest")
        if land_type == "timber":
            filter_cols.update(["SITECLCD", "RESERVCD"])

        # Add columns needed for area_domain filtering
        area_domain = self.config.get("area_domain")
        if area_domain:
            from ...filtering.parser import DomainExpressionParser

            domain_cols = DomainExpressionParser.extract_columns(area_domain)
            for col in domain_cols:
                if col not in core_cols:
                    filter_cols.add(col)

        # Add grouping columns if specified
        grouping_cols: set[str] = set()
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grouping_cols.add(grp_by)
            else:
                grouping_cols.update(grp_by)

        # Combine all columns, remove duplicates while preserving order
        all_cols = core_cols + list(filter_cols) + list(grouping_cols)
        seen: set[str] = set()
        result = []
        for col in all_cols:
            if col not in seen:
                seen.add(col)
                result.append(col)

        return result

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate site index values for aggregation.

        For area-weighted mean, we need:
        - Numerator: SICOND * CONDPROP_UNADJ (weighted by area proportion)
        - Denominator: CONDPROP_UNADJ (total area with site index)
        """
        # Create weighted site index value
        if "DOMAIN_IND" in data.collect_schema().names():
            data = data.with_columns(
                [
                    (
                        pl.col("SICOND").cast(pl.Float64)
                        * pl.col("CONDPROP_UNADJ").cast(pl.Float64)
                        * pl.col("DOMAIN_IND")
                    ).alias("SI_WEIGHTED"),
                    (
                        pl.col("CONDPROP_UNADJ").cast(pl.Float64) * pl.col("DOMAIN_IND")
                    ).alias("AREA_WEIGHTED"),
                ]
            )
        else:
            data = data.with_columns(
                [
                    (
                        pl.col("SICOND").cast(pl.Float64)
                        * pl.col("CONDPROP_UNADJ").cast(pl.Float64)
                    ).alias("SI_WEIGHTED"),
                    pl.col("CONDPROP_UNADJ").cast(pl.Float64).alias("AREA_WEIGHTED"),
                ]
            )

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply land type and domain filters.

        Uses domain indicator approach for proper variance calculation,
        but also filters out null SICOND values since they cannot contribute
        to the mean calculation.
        """
        # Create domain indicator based on land type
        land_type = self.config.get("land_type", "forest")
        land_filter_expr = get_land_domain_indicator(land_type)
        data = data.with_columns(
            [pl.when(land_filter_expr).then(1.0).otherwise(0.0).alias("DOMAIN_IND")]
        )

        # Apply area domain filter using centralized parser
        area_domain = self.config.get("area_domain")
        if area_domain:
            from ...filtering.parser import DomainExpressionParser

            area_domain_expr = DomainExpressionParser.parse(area_domain, "area")
            data = data.with_columns(
                [
                    pl.when(pl.col("DOMAIN_IND") == 1.0)
                    .then(pl.when(area_domain_expr).then(1.0).otherwise(0.0))
                    .otherwise(0.0)
                    .alias("DOMAIN_IND")
                ]
            )

        # Filter out null SICOND values - they cannot contribute to mean
        # This is different from area estimation where zero is a valid value
        data = data.filter(pl.col("SICOND").is_not_null())

        return data

    def _select_variance_columns(
        self, available_cols: list[str]
    ) -> tuple[list[str], list[str]]:
        """Select columns needed for variance calculation."""
        cols_to_select: list[str] = ["PLT_CN"]

        if "CONDID" in available_cols:
            cols_to_select.append("CONDID")

        # Stratification columns
        if "ESTN_UNIT" in available_cols:
            cols_to_select.append("ESTN_UNIT")
        elif "UNITCD" in available_cols:
            cols_to_select.append("UNITCD")

        if "STRATUM_CN" in available_cols:
            cols_to_select.append("STRATUM_CN")
        elif "STRATUM" in available_cols:
            cols_to_select.append("STRATUM")

        # Site index and area columns
        for col in [
            "SICOND",
            "SI_WEIGHTED",
            "AREA_WEIGHTED",
            "ADJ_FACTOR_AREA",
            "EXPNS",
            "DOMAIN_IND",
            "SIBASE",
        ]:
            if col in available_cols and col not in cols_to_select:
                cols_to_select.append(col)

        # Grouping columns - SIBASE always included
        group_cols: list[str] = ["SIBASE"]

        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                if grp_by != "SIBASE":
                    group_cols.append(grp_by)
            else:
                for col in grp_by:
                    if col != "SIBASE":
                        group_cols.append(col)

        # Add grouping columns to selection
        for col in group_cols:
            if col in available_cols and col not in cols_to_select:
                cols_to_select.append(col)

        return cols_to_select, group_cols

    def aggregate_results(self, data: pl.LazyFrame) -> AggregationResult:  # type: ignore[override]
        """Aggregate site index with area weighting and stratification.

        Returns area-weighted mean: sum(SICOND * area) / sum(area)
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply area adjustment factors based on PROP_BASIS
        data_with_strat = apply_area_adjustment_factors(  # type: ignore[assignment]
            data_with_strat, prop_basis_col="PROP_BASIS", output_col="ADJ_FACTOR_AREA"
        )

        # Get available columns
        available_cols = data_with_strat.collect_schema().names()

        # Select columns for variance calculation
        cols_to_select, group_cols = self._select_variance_columns(available_cols)

        # Store plot-condition data for variance calculation
        plot_condition_data = data_with_strat.select(cols_to_select).collect()

        # Aggregation expressions for ratio-of-means
        agg_exprs = [
            # Numerator: sum(SI_WEIGHTED * ADJ_FACTOR_AREA * EXPNS)
            (
                pl.col("SI_WEIGHTED").cast(pl.Float64)
                * pl.col("ADJ_FACTOR_AREA").cast(pl.Float64)
                * pl.col("EXPNS").cast(pl.Float64)
            )
            .sum()
            .alias("SI_NUM"),
            # Denominator: sum(AREA_WEIGHTED * ADJ_FACTOR_AREA * EXPNS)
            (
                pl.col("AREA_WEIGHTED").cast(pl.Float64)
                * pl.col("ADJ_FACTOR_AREA").cast(pl.Float64)
                * pl.col("EXPNS").cast(pl.Float64)
            )
            .sum()
            .alias("SI_DENOM"),
            # Counts
            pl.col("PLT_CN")
            .filter(pl.col("DOMAIN_IND") > 0)
            .n_unique()
            .alias("N_PLOTS"),
            pl.len().alias("N_CONDITIONS"),
        ]

        if group_cols:
            results_lazy = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results_lazy = data_with_strat.select(agg_exprs)

        results_df = results_lazy.collect()

        # Calculate area-weighted mean: SI_MEAN = SI_NUM / SI_DENOM
        results_df = results_df.with_columns(
            [
                pl.when(pl.col("SI_DENOM") > 0)
                .then(pl.col("SI_NUM") / pl.col("SI_DENOM"))
                .otherwise(None)
                .alias("SI_MEAN")
            ]
        )

        return AggregationResult(
            results=results_df,
            plot_tree_data=plot_condition_data,
            group_cols=group_cols,
        )

    def _aggregate_to_plot_level(
        self, plot_cond_data: pl.DataFrame, group_cols: list[str]
    ) -> pl.DataFrame:
        """Aggregate condition data to plot level for variance calculation."""
        base_cols = ["PLT_CN"]

        # Add stratification columns
        if "STRATUM_CN" in plot_cond_data.columns:
            base_cols.append("STRATUM_CN")
        elif "STRATUM" in plot_cond_data.columns:
            base_cols.append("STRATUM")

        if "EXPNS" in plot_cond_data.columns:
            base_cols.append("EXPNS")

        # Add grouping columns
        for col in group_cols:
            if col in plot_cond_data.columns and col not in base_cols:
                base_cols.append(col)

        return plot_cond_data.group_by(base_cols).agg(
            [
                # Y: weighted site index per plot (numerator)
                (pl.col("SI_WEIGHTED") * pl.col("ADJ_FACTOR_AREA")).sum().alias("y_i"),
                # X: area proportion per plot (denominator)
                (pl.col("AREA_WEIGHTED") * pl.col("ADJ_FACTOR_AREA"))
                .sum()
                .alias("x_i"),
            ]
        )

    def _calculate_ratio_variance(
        self,
        plot_data: pl.DataFrame,
        ratio: float | None,
        total_x: float | None,
    ) -> dict[str, float | None]:
        """Calculate variance for ratio estimator.

        Uses the standard FIA ratio variance formula:
        V(R) = (1/X^2) * [V(Y) - 2R*Cov(Y,X) + R^2*V(X)]

        Where R = Y/X is the ratio (mean site index),
        Y = sum(SI * area), X = sum(area)
        """
        # Determine stratification column
        if "STRATUM_CN" in plot_data.columns:
            strat_col = "STRATUM_CN"
        elif "STRATUM" in plot_data.columns:
            strat_col = "STRATUM"
        else:
            # No stratification, treat as single stratum
            plot_data = plot_data.with_columns(pl.lit(1).alias("_STRATUM"))
            strat_col = "_STRATUM"

        # Calculate stratum-level statistics
        strata_stats = plot_data.group_by(strat_col).agg(
            [
                pl.count("PLT_CN").alias("n_h"),
                pl.mean("y_i").alias("ybar_h"),
                pl.mean("x_i").alias("xbar_h"),
                pl.var("y_i", ddof=1).alias("s2_y"),
                pl.var("x_i", ddof=1).alias("s2_x"),
                pl.cov("y_i", "x_i", ddof=1).alias("cov_yx"),
                pl.first("EXPNS").cast(pl.Float64).alias("w_h"),
            ]
        )

        # Handle null variances (single observation in stratum)
        strata_stats = strata_stats.with_columns(
            [
                pl.col("s2_y").fill_null(0.0),
                pl.col("s2_x").fill_null(0.0),
                pl.col("cov_yx").fill_null(0.0),
            ]
        )

        # Calculate variance components
        # V(R) = (1/X^2) * sum_h [w_h^2 * n_h * (s2_y - 2R*cov_yx + R^2*s2_x)]
        r = ratio if ratio is not None else 0.0

        variance_components = strata_stats.with_columns(
            [
                pl.when(pl.col("n_h") > 1)
                .then(
                    pl.col("w_h") ** 2
                    * pl.col("n_h")
                    * (
                        pl.col("s2_y")
                        - 2 * r * pl.col("cov_yx")
                        + r**2 * pl.col("s2_x")
                    )
                )
                .otherwise(0.0)
                .alias("v_h")
            ]
        )

        total_variance = variance_components["v_h"].sum()
        if total_variance is None or total_variance < 0:
            total_variance = 0.0

        # Ratio variance: V(R) = V(total) / X^2
        if total_x is not None and total_x > 0:
            ratio_variance = total_variance / (total_x**2)
        else:
            ratio_variance = 0.0

        se = ratio_variance**0.5

        return {
            "variance": ratio_variance,
            "se": se,
        }

    def calculate_variance(self, agg_result: AggregationResult) -> pl.DataFrame:  # type: ignore[override]
        """Calculate variance using ratio-of-means formula.

        For site index (ratio estimator), variance is:
        V(R) = (1/X^2) * [V(Y) - 2R*Cov(Y,X) + R^2*V(X)]

        Where R = Y/X is the ratio (mean site index),
        Y = sum(SI * area), X = sum(area)
        """
        results = agg_result.results
        plot_cond_data = agg_result.plot_tree_data
        group_cols = agg_result.group_cols

        # Aggregate to plot level
        plot_data = self._aggregate_to_plot_level(plot_cond_data, group_cols)

        if group_cols:
            variance_results = []
            for row in results.iter_rows(named=True):
                # Build filter for this group
                group_filter = pl.lit(True)
                group_dict = {}

                for col in group_cols:
                    if col in plot_data.columns:
                        val = row.get(col)
                        group_dict[col] = val
                        if val is None:
                            group_filter = group_filter & pl.col(col).is_null()
                        else:
                            group_filter = group_filter & (pl.col(col) == val)

                group_plot_data = plot_data.filter(group_filter)

                if len(group_plot_data) > 0:
                    var_stats = self._calculate_ratio_variance(
                        group_plot_data, row.get("SI_MEAN"), row.get("SI_DENOM")
                    )
                    variance_results.append(
                        {
                            **group_dict,
                            "SI_SE": var_stats["se"],
                            "SI_VARIANCE": var_stats["variance"],
                        }
                    )

            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=group_cols, how="left")
            else:
                # No variance results, add null columns
                results = results.with_columns(
                    [
                        pl.lit(None).cast(pl.Float64).alias("SI_SE"),
                        pl.lit(None).cast(pl.Float64).alias("SI_VARIANCE"),
                    ]
                )
        else:
            # No grouping, calculate overall variance
            if len(plot_data) > 0:
                var_stats = self._calculate_ratio_variance(
                    plot_data, results["SI_MEAN"][0], results["SI_DENOM"][0]
                )
                results = results.with_columns(
                    [
                        pl.lit(var_stats["se"]).alias("SI_SE"),
                        pl.lit(var_stats["variance"]).alias("SI_VARIANCE"),
                    ]
                )
            else:
                results = results.with_columns(
                    [
                        pl.lit(None).cast(pl.Float64).alias("SI_SE"),
                        pl.lit(None).cast(pl.Float64).alias("SI_VARIANCE"),
                    ]
                )

        # Drop intermediate columns
        cols_to_drop = ["SI_NUM", "SI_DENOM"]
        results = results.drop([c for c in cols_to_drop if c in results.columns])

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format site index estimation output."""
        # Filter out rows where grouping column is null AND SI_MEAN is null
        grp_by = self.config.get("grp_by")
        if grp_by and "SI_MEAN" in results.columns:
            if isinstance(grp_by, str):
                grp_cols = [grp_by]
            else:
                grp_cols = list(grp_by)

            for col in grp_cols:
                if col in results.columns:
                    results = results.filter(
                        ~(pl.col(col).is_null() & pl.col("SI_MEAN").is_null())
                    )

        # Add year from evaluation
        year = self._extract_evaluation_year()
        results = results.with_columns([pl.lit(year).alias("YEAR")])

        # Reorder columns: YEAR first, then grouping, then metrics
        ordered_cols = ["YEAR"]
        if "SIBASE" in results.columns:
            ordered_cols.append("SIBASE")
        if grp_by:
            for col in grp_cols if grp_by else []:
                if col in results.columns and col not in ordered_cols:
                    ordered_cols.append(col)

        # Add metric columns
        metric_cols = ["SI_MEAN", "SI_SE", "SI_VARIANCE", "N_PLOTS", "N_CONDITIONS"]
        for col in metric_cols:
            if col in results.columns:
                ordered_cols.append(col)

        # Add any remaining columns
        for col in results.columns:
            if col not in ordered_cols:
                ordered_cols.append(col)

        results = results.select([c for c in ordered_cols if c in results.columns])

        return results


def site_index(
    db: str | FIA,
    grp_by: str | list[str] | None = None,
    land_type: str = "forest",
    area_domain: str | None = None,
    plot_domain: str | None = None,
    most_recent: bool = False,
    eval_type: str | None = None,
) -> pl.DataFrame:
    """
    Estimate area-weighted mean site index from FIA data.

    Calculates area-weighted site index estimates using FIA's design-based
    estimation methods. Site index represents expected dominant tree height
    (in feet) at a specified base age, indicating site productivity.

    Results are always grouped by SIBASE (base age) because site index
    values are not comparable across different base ages.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Common grouping columns:

        **Site Index Species:**
        - 'SISP': Species code used for site index determination

        **Forest Characteristics:**
        - 'FORTYPCD': Forest type code
        - 'STDSZCD': Stand size class
        - 'OWNGRPCD': Ownership group

        **Location:**
        - 'STATECD': State FIPS code
        - 'COUNTYCD': County code
        - 'UNITCD': FIA survey unit
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include:

        - 'forest': All forestland
        - 'timber': Timberland only (unreserved, productive)
        - 'all': All land types
    area_domain : str, optional
        SQL-like filter for condition-level attributes. Examples:

        - "OWNGRPCD == 40": Private land only
        - "STDAGE > 20": Stands over 20 years old
        - "FORTYPCD IN (161, 162)": Specific forest types
    plot_domain : str, optional
        SQL-like filter for plot-level attributes. Examples:

        - "COUNTYCD == 183": Single county
        - "LAT >= 35.0 AND LAT <= 36.0": Latitude range
    most_recent : bool, default False
        If True, automatically select most recent evaluation.
    eval_type : str, optional
        Evaluation type if most_recent=True. Default is 'ALL'.

    Returns
    -------
    pl.DataFrame
        Site index estimates with columns:

        - **YEAR** : int - Inventory year
        - **SIBASE** : int - Base age (always included)
        - **[grouping columns]** : varies - Columns from grp_by
        - **SI_MEAN** : float - Area-weighted mean site index (feet)
        - **SI_SE** : float - Standard error of mean
        - **SI_VARIANCE** : float - Variance of estimate
        - **N_PLOTS** : int - Number of plots in estimate
        - **N_CONDITIONS** : int - Number of conditions with site index

    See Also
    --------
    pyfia.area : Estimate forest area
    pyfia.volume : Estimate tree volume

    Notes
    -----
    Site index estimation uses the area-weighted mean formula:

    SI_mean = sum(SICOND * CONDPROP_UNADJ * ADJ_FACTOR * EXPNS) /
              sum(CONDPROP_UNADJ * ADJ_FACTOR * EXPNS)

    This ratio-of-means estimator requires proper variance calculation
    accounting for covariance between numerator and denominator.

    **Important Considerations:**

    1. **Base Age Comparability**: Results are always grouped by SIBASE
       because site index values are only meaningful within the same base
       age. Common base ages are 25 years (southern pines) and 50 years
       (northern species).

    2. **Species Specificity**: SISP indicates which species equation was
       used for site index determination. Different species may have
       different site index scales.

    3. **Missing Values**: Conditions without site index (non-productive
       land, recently disturbed, etc.) are excluded from calculations.

    Examples
    --------
    Basic site index estimation:

    >>> from pyfia import FIA, site_index
    >>> with FIA("path/to/fia.duckdb") as db:
    ...     db.clip_by_state(37)  # North Carolina
    ...     results = site_index(db)

    Site index by ownership group:

    >>> results = site_index(db, grp_by="OWNGRPCD")

    Site index by site index species:

    >>> results = site_index(db, grp_by="SISP")

    Site index for private timberland:

    >>> results = site_index(
    ...     db,
    ...     land_type="timber",
    ...     area_domain="OWNGRPCD == 40",
    ... )

    County-level site index:

    >>> results = site_index(db, grp_by="COUNTYCD")
    """
    # Validate inputs
    inputs = validate_estimator_inputs(
        land_type=land_type,
        grp_by=grp_by,
        area_domain=area_domain,
        plot_domain=plot_domain,
        variance=False,
        totals=False,
        most_recent=most_recent,
    )

    # Ensure db is a FIA instance
    db_instance, owns_db = ensure_fia_instance(db)

    # Ensure EVALID is set
    ensure_evalid_set(
        db_instance, eval_type=eval_type or "ALL", estimator_name="site_index"
    )

    # Create config
    config = {
        "grp_by": inputs.grp_by,
        "land_type": inputs.land_type,
        "area_domain": inputs.area_domain,
        "plot_domain": inputs.plot_domain,
        "most_recent": inputs.most_recent,
        "eval_type": eval_type,
    }

    try:
        estimator = SiteIndexEstimator(db_instance, config)
        return estimator.estimate()
    finally:
        if owns_db and hasattr(db_instance, "close"):
            db_instance.close()
