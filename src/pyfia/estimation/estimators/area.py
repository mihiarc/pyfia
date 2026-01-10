"""
Area estimation for FIA data.

Simple, straightforward implementation without unnecessary abstractions.
"""

from typing import Dict, List, Optional, Tuple, Union

import polars as pl

from ...core import FIA
from ...filtering import get_land_domain_indicator
from ..base import AggregationResult, BaseEstimator
from ..tree_expansion import apply_area_adjustment_factors
from ..utils import (
    ensure_evalid_set,
    ensure_fia_instance,
    format_output_columns,
    validate_estimator_inputs,
)


class AreaEstimator(BaseEstimator):
    """
    Area estimator for FIA data.

    Estimates forest area by various categories without complex
    abstractions or deep inheritance hierarchies.
    """

    def __init__(self, db: Union[str, FIA], config: dict) -> None:
        """Initialize the area estimator."""
        super().__init__(db, config)

    def get_required_tables(self) -> List[str]:
        """Area estimation requires COND, PLOT, and stratification tables."""
        return ["COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_cond_columns(self) -> List[str]:
        """Get required condition columns based on actual usage."""
        # Core columns always needed for area calculation
        core_cols = [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "PROP_BASIS",
        ]

        # Additional columns needed based on land_type filter
        filter_cols = set()
        land_type = self.config.get("land_type", "forest")
        if land_type == "timber":
            filter_cols.update(["SITECLCD", "RESERVCD"])

        # Add columns needed for area_domain filtering
        area_domain = self.config.get("area_domain")
        if area_domain:
            from ...filtering.parser import DomainExpressionParser

            # Extract column names using centralized parser
            domain_cols = DomainExpressionParser.extract_columns(area_domain)
            for col in domain_cols:
                if col not in core_cols:
                    filter_cols.add(col)

        # Add grouping columns if specified
        grouping_cols = set()
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grouping_cols.add(grp_by)
            else:
                grouping_cols.update(grp_by)

        # Combine all needed columns
        all_cols = core_cols + list(filter_cols) + list(grouping_cols)

        # Remove duplicates while preserving order
        seen = set()
        result = []
        for col in all_cols:
            if col not in seen:
                seen.add(col)
                result.append(col)

        return result

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate area values.

        For area estimation, the value is CONDPROP_UNADJ multiplied by
        the domain indicator to properly handle domain estimation.
        """
        # Area calculation uses domain indicator if present
        if "DOMAIN_IND" in data.collect_schema().names():
            # Domain indicator approach for proper variance
            data = data.with_columns(
                [(pl.col("CONDPROP_UNADJ") * pl.col("DOMAIN_IND")).alias("AREA_VALUE")]
            )
        else:
            # Fallback to simple approach
            data = data.with_columns([pl.col("CONDPROP_UNADJ").alias("AREA_VALUE")])

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply land type and domain filters using domain indicator approach.

        For proper variance calculation in domain estimation, we must keep ALL plots
        but create a domain indicator rather than filtering them out.

        This method operates entirely on LazyFrames to enable server-side
        execution on cloud backends like MotherDuck.
        """
        # Create domain indicator based on land type using centralized indicator function
        # This replaces magic numbers with named constants from status_codes.py
        # All operations work on LazyFrame - no .collect() needed
        land_type = self.config.get("land_type", "forest")

        # Get the boolean expression for this land type and convert to 0/1 indicator
        land_filter_expr = get_land_domain_indicator(land_type)
        data = data.with_columns(
            [pl.when(land_filter_expr).then(1.0).otherwise(0.0).alias("DOMAIN_IND")]
        )

        # Apply area domain filter using centralized parser
        area_domain = self.config.get("area_domain")
        if area_domain:
            from ...filtering.parser import DomainExpressionParser

            # For area estimation with domain indicators, we need to incorporate
            # the area_domain into the DOMAIN_IND rather than filtering rows.
            # This ensures proper variance calculation by keeping all plots.
            # The area_domain condition is ANDed with the existing land_type indicator.
            area_domain_expr = DomainExpressionParser.parse(area_domain, "area")
            data = data.with_columns(
                [
                    pl.when(pl.col("DOMAIN_IND") == 1.0)
                    .then(pl.when(area_domain_expr).then(1.0).otherwise(0.0))
                    .otherwise(0.0)
                    .alias("DOMAIN_IND")
                ]
            )

        return data

    def _select_variance_columns(
        self, available_cols: List[str]
    ) -> Tuple[List[Union[str, pl.Expr]], List[str]]:
        """
        Select columns needed for variance calculation.

        Parameters
        ----------
        available_cols : List[str]
            List of available column names in the data

        Returns
        -------
        tuple[List[Union[str, pl.Expr]], List[str]]
            Tuple of (columns_to_select, group_columns)
            - columns_to_select: List of column names or Polars expressions to select
            - group_columns: List of grouping column names
        """
        # Build column list based on what's available
        cols_to_select: List[Union[str, pl.Expr]] = ["PLT_CN"]

        # Add condition ID if available
        if "CONDID" in available_cols:
            cols_to_select.append("CONDID")

        # Add estimation unit (prefer ESTN_UNIT, fallback to UNITCD)
        if "ESTN_UNIT" in available_cols:
            cols_to_select.append("ESTN_UNIT")
        elif "UNITCD" in available_cols:
            cols_to_select.append("UNITCD")

        # Add stratum identifier (prefer STRATUM_CN, fallback to STRATUM)
        if "STRATUM_CN" in available_cols:
            cols_to_select.append("STRATUM_CN")
        elif "STRATUM" in available_cols:
            cols_to_select.append("STRATUM")

        # Add the essential columns for variance calculation
        cols_to_select.extend(
            [
                "AREA_VALUE",  # This is CONDPROP_UNADJ from calculate_values
                "ADJ_FACTOR_AREA",
                "EXPNS",
            ]
        )

        # Add grouping columns if they exist
        group_cols: List[str] = []
        if self.config.get("grp_by"):
            grp_by = self.config["grp_by"]
            if isinstance(grp_by, str):
                group_cols = [grp_by]
            else:
                group_cols = list(grp_by)

            for col in group_cols:
                if col in available_cols and col not in cols_to_select:
                    cols_to_select.append(col)

        return cols_to_select, group_cols

    def aggregate_results(self, data: pl.LazyFrame) -> AggregationResult:  # type: ignore[override]
        """Aggregate area with stratification, preserving data for variance calculation.

        Returns
        -------
        AggregationResult
            Bundle containing results, plot_condition_data (as plot_tree_data field),
            and group_cols for explicit variance calculation.
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply area adjustment factors based on PROP_BASIS
        data_with_strat = apply_area_adjustment_factors(  # type: ignore[assignment]
            data_with_strat, prop_basis_col="PROP_BASIS", output_col="ADJ_FACTOR_AREA"
        )

        # Need to check which columns are available for variance calculation
        available_cols = data_with_strat.collect_schema().names()

        # Use helper method to select columns for variance calculation
        cols_to_select, group_cols = self._select_variance_columns(available_cols)

        # Store the plot-condition data for variance calculation
        plot_condition_data = data_with_strat.select(cols_to_select).collect()

        # Calculate area totals with proper FIA expansion logic
        # Area = CONDPROP_UNADJ * ADJ_FACTOR_AREA * EXPNS
        agg_exprs = [
            (
                pl.col("AREA_VALUE").cast(pl.Float64)
                * pl.col("ADJ_FACTOR_AREA").cast(pl.Float64)
                * pl.col("EXPNS").cast(pl.Float64)
            )
            .sum()
            .alias("AREA_TOTAL"),
            pl.col("EXPNS").cast(pl.Float64).sum().alias("TOTAL_EXPNS"),
            # Count only plots with non-zero area values (non-zero plots in EVALIDator terms)
            pl.col("PLT_CN")
            .filter(pl.col("AREA_VALUE") > 0)
            .n_unique()
            .alias("N_PLOTS"),
        ]

        if group_cols:
            results_lazy = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results_lazy = data_with_strat.select(agg_exprs)

        results_df: pl.DataFrame = results_lazy.collect()

        # Add percentage
        # For grouped data: percentage of total area in groups
        # For ungrouped data: percentage of total land area (using TOTAL_EXPNS)
        if group_cols:
            total_area = results_df["AREA_TOTAL"].sum()
            results_df = results_df.with_columns(
                [(100 * pl.col("AREA_TOTAL") / total_area).alias("AREA_PERCENT")]
            )
        else:
            # For ungrouped data, calculate percentage of total land area
            # TOTAL_EXPNS represents the total land area when summed
            results_df = results_df.with_columns(
                [
                    (100 * pl.col("AREA_TOTAL") / pl.col("TOTAL_EXPNS")).alias(
                        "AREA_PERCENT"
                    )
                ]
            )

        # Return AggregationResult with explicit data passing
        # Note: We use plot_tree_data field for plot_condition_data
        return AggregationResult(
            results=results_df,
            plot_tree_data=plot_condition_data,
            group_cols=group_cols,
        )

    def calculate_variance(self, agg_result: AggregationResult) -> pl.DataFrame:  # type: ignore[override]
        """Calculate variance for area estimates using domain total estimation formula.

        Implements Bechtold & Patterson (2005) stratified variance calculation
        for domain totals.

        Parameters
        ----------
        agg_result : AggregationResult
            Bundle containing results, plot_condition_data (as plot_tree_data field),
            and group_cols from aggregate_results().

        Returns
        -------
        pl.DataFrame
            Results with variance columns added.
        """
        # Extract data from AggregationResult
        results = agg_result.results
        plot_condition_data = (
            agg_result.plot_tree_data
        )  # Note: uses plot_tree_data field
        group_cols = agg_result.group_cols

        # Step 1: Calculate condition-level areas
        cond_data = plot_condition_data.with_columns(
            [
                (
                    pl.col("AREA_VALUE").cast(pl.Float64)
                    * pl.col("ADJ_FACTOR_AREA").cast(pl.Float64)
                ).alias("h_ic")
            ]
        )

        # Determine stratification columns
        available_cols = cond_data.columns
        strat_cols = []
        if "ESTN_UNIT" in available_cols:
            strat_cols.append("ESTN_UNIT")
        if "STRATUM_CN" in available_cols:
            strat_cols.append("STRATUM_CN")
        elif "STRATUM" in available_cols:
            strat_cols.append("STRATUM")

        if not strat_cols:
            # No stratification columns found, treat as single stratum
            strat_expr = [pl.lit(1).alias("STRATUM")]
            cond_data = cond_data.with_columns(strat_expr)
            strat_cols = ["STRATUM"]

        # Step 2: Aggregate to plot level (sum conditions within plot)
        # Include grouping columns so they're preserved for variance calculation by group
        plot_group_cols = ["PLT_CN"] + strat_cols + ["EXPNS"]
        if group_cols:
            # Add any grouping columns that exist in the condition data
            for col in group_cols:
                if col in cond_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_data = cond_data.group_by(plot_group_cols).agg(
            [
                pl.sum("h_ic").alias("y_i")  # Total adjusted area per plot
            ]
        )

        # If we have grouping variables, calculate variance for each group
        if group_cols:
            # Calculate variance for each group separately
            variance_results = []

            for group_vals in results.iter_rows():
                # Filter plot data for this group
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(group_cols):
                    if col in plot_data.columns:
                        val = group_vals[results.columns.index(col)]
                        group_dict[col] = val
                        if val is None:
                            group_filter = group_filter & pl.col(col).is_null()
                        else:
                            group_filter = group_filter & (pl.col(col) == val)

                group_plot_data = plot_data.filter(group_filter)

                if len(group_plot_data) > 0:
                    # Calculate variance for this group
                    var_stats = self._calculate_variance_for_group(
                        group_plot_data, strat_cols
                    )
                    variance_results.append(
                        {
                            **group_dict,
                            "AREA_SE": var_stats["se_total"],
                            "AREA_SE_PERCENT": var_stats["se_percent"],
                            "AREA_VARIANCE": var_stats["variance"],
                        }
                    )

            # Join variance results back to main results
            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=group_cols, how="left")
        else:
            # No grouping, calculate overall variance
            var_stats = self._calculate_variance_for_group(plot_data, strat_cols)

            # Calculate SE% using actual area total
            area_total = results["AREA_TOTAL"][0]
            se_total = var_stats["se_total"]
            se_percent = (
                100 * se_total / area_total
                if area_total is not None and area_total > 0 and se_total is not None
                else 0
            )

            results = results.with_columns(
                [
                    pl.lit(var_stats["se_total"]).alias("AREA_SE"),
                    pl.lit(se_percent).alias("AREA_SE_PERCENT"),
                    pl.lit(var_stats["variance"]).alias("AREA_VARIANCE"),
                ]
            )

        return results

    def _calculate_variance_for_group(
        self, plot_data: pl.DataFrame, strat_cols: List[str]
    ) -> Dict[str, Optional[float]]:
        """Calculate variance for a single group using domain total estimation formula.

        For domain (subset) estimation in FIA, we're estimating a total over a domain.
        The correct variance formula for domain totals is:
        V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h]

        Where:
        - w_h = EXPNS (expansion factor in acres per plot)
        - s²_yDh = variance of domain indicator (0 for non-domain, proportion for domain)
        - n_h = number of sampled plots in stratum (including non-domain plots)

        This formula accounts for the fact that we're summing over plots, not averaging.
        """

        # Step 3: Calculate stratum statistics
        # The y_i values are proportions (0-1) of plot area that belong to the domain
        strata_stats = plot_data.group_by(strat_cols).agg(
            [
                pl.count("PLT_CN").alias("n_h"),
                pl.mean("y_i").alias("ybar_h"),  # Mean proportion
                pl.var("y_i", ddof=1).alias("s2_yh"),  # Variance of proportions
                pl.first("EXPNS").alias("w_h"),  # Expansion factor (acres per plot)
            ]
        )

        # Handle case where variance is null (single plot in stratum)
        strata_stats = strata_stats.with_columns(
            [
                pl.when(pl.col("s2_yh").is_null())
                .then(0.0)
                .otherwise(pl.col("s2_yh"))
                .alias("s2_yh")
            ]
        )

        # Step 4: Calculate variance components
        # For domain total estimation in stratified sampling:
        # V(Ŷ_D) = Σ_h [w_h² × s²_yDh × n_h]
        #
        # This is different from population mean estimation where we divide by n_h.
        # For domain totals, we multiply by n_h because we're summing across plots.
        # The adjustment factors (ADJ_FACTOR_AREA) are already included in the y_i values.

        variance_components = strata_stats.with_columns(
            [
                # Multiply by n_h, not divide by it!
                # Cast w_h to Float64 to handle decimal types from database
                (
                    pl.col("w_h").cast(pl.Float64) ** 2
                    * pl.col("s2_yh")
                    * pl.col("n_h")
                ).alias("v_h")
            ]
        )

        # Step 5: Sum variance components
        total_variance = variance_components["v_h"].sum()
        if total_variance is None or total_variance < 0:
            total_variance = 0.0

        se_total = total_variance**0.5

        # Get total estimate for this group (sum of expanded means)
        # Total = Σ_h (ybar_h × w_h × n_h)
        # But wait, that's not right either. The correct formula is:
        # Total = Σ_h (ybar_h × N_h × acres_per_plot)
        # Where N_h is total plots in stratum in population
        # But w_h (EXPNS) already includes this: w_h = N_h * acres_per_plot / n_h
        # So: Total = Σ_h (ybar_h × w_h × n_h)

        # Actually, let's use a different approach
        # The total is already calculated in the main results
        # We shouldn't recalculate it here
        # For now, return just the variance components

        return {
            "variance": total_variance,
            "se_total": se_total,
            "se_percent": None,  # Will be calculated using actual total
            "estimate": None,  # Don't recalculate
        }

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format area estimation output."""
        # Filter out rows where grouping column is null AND area is 0
        # These come from non-forest conditions in domain indicator approach
        grp_by = self.config.get("grp_by")
        if grp_by and "AREA_TOTAL" in results.columns:
            if isinstance(grp_by, str):
                grp_cols = [grp_by]
            else:
                grp_cols = list(grp_by)

            # Filter out rows where any grouping column is null AND area is 0
            for col in grp_cols:
                if col in results.columns:
                    results = results.filter(
                        ~(pl.col(col).is_null() & (pl.col("AREA_TOTAL") == 0))
                    )

        # Add year
        results = results.with_columns([pl.lit(2023).alias("YEAR")])

        # Format columns
        results = format_output_columns(
            results,
            estimation_type="area",
            include_se=True,
            include_cv=self.config.get("include_cv", False),
        )

        return results


def area(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    land_type: str = "forest",
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    most_recent: bool = False,
    eval_type: Optional[str] = None,
    variance: bool = False,
    totals: bool = True,
) -> pl.DataFrame:
    """
    Estimate forest area from FIA data.

    Calculates area estimates using FIA's design-based estimation methods
    with proper expansion factors and stratification. Automatically handles
    EVALID selection to prevent overcounting from multiple evaluations.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        PLOT and COND tables. Common grouping columns include:

        **Ownership and Management:**
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'OWNCD': Detailed ownership code (see REF_RESEARCH_STATION)
        - 'ADFORCD': Administrative forest code
        - 'RESERVCD': Reserved status (0=Not reserved, 1=Reserved)

        **Forest Characteristics:**
        - 'FORTYPCD': Forest type code (see REF_FOREST_TYPE)
        - 'STDSZCD': Stand size class (1=Large diameter, 2=Medium diameter,
          3=Small diameter, 4=Seedling/sapling, 5=Nonstocked)
        - 'STDORGCD': Stand origin (0=Natural, 1=Planted)
        - 'STDAGE': Stand age in years

        **Site Characteristics:**
        - 'SITECLCD': Site productivity class (1=225+ cu ft/ac/yr,
          2=165-224, 3=120-164, 4=85-119, 5=50-84, 6=20-49, 7=0-19)
        - 'PHYSCLCD': Physiographic class code

        **Location:**
        - 'STATECD': State FIPS code
        - 'UNITCD': FIA survey unit code
        - 'COUNTYCD': County code
        - 'INVYR': Inventory year

        **Disturbance and Treatment:**
        - 'DSTRBCD1', 'DSTRBCD2', 'DSTRBCD3': Disturbance codes
        - 'TRTCD1', 'TRTCD2', 'TRTCD3': Treatment codes

        For complete column descriptions, see USDA FIA Database User Guide.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland (COND_STATUS_CD = 1)
        - 'timber': Timberland only (unreserved, productive forestland)
        - 'all': All land types including non-forest
    area_domain : str, optional
        SQL-like filter expression for COND-level attributes. Examples:

        - "STDAGE > 50": Stands older than 50 years
        - "FORTYPCD IN (161, 162)": Specific forest types
        - "OWNGRPCD == 10": National Forest lands only
        - "PHYSCLCD == 31 AND STDSZCD == 1": Xeric sites with large trees
    plot_domain : str, optional
        SQL-like filter expression for PLOT-level attributes. This parameter
        enables filtering by plot location and attributes that are not available
        in the COND table. Examples:

        **Location filtering:**
        - "COUNTYCD == 183": Wake County, NC (single county)
        - "COUNTYCD IN (183, 185, 187)": Multiple counties
        - "UNITCD == 1": Survey unit 1

        **Geographic filtering:**
        - "LAT >= 35.0 AND LAT <= 36.0": Latitude range
        - "LON >= -80.0 AND LON <= -79.0": Longitude range
        - "ELEV > 2000": Elevation above 2000 feet

        **Temporal filtering:**
        - "INVYR == 2019": Inventory year
        - "MEASYEAR >= 2015": Measured since 2015

        Note: plot_domain filters apply to PLOT table columns only. For
        condition-level attributes (ownership, forest type, etc.), use
        area_domain instead.
    most_recent : bool, default False
        If True, automatically select the most recent evaluation for each
        state/region. Equivalent to calling db.clip_most_recent() first.
    eval_type : str, optional
        Evaluation type to select if most_recent=True. Options:
        'ALL', 'VOL', 'GROW', 'MORT', 'REMV', 'CHANGE', 'DWM', 'INV'.
        Default is 'ALL' for area estimation.
    variance : bool, default False
        If True, return variance instead of standard error.
    totals : bool, default True
        If True, include total area estimates expanded to population level.
        If False, only return per-acre values.

    Returns
    -------
    pl.DataFrame
        Area estimates with the following columns:

        - **YEAR** : int
            Inventory year
        - **[grouping columns]** : varies
            Any columns specified in grp_by parameter
        - **AREA_PCT** : float
            Percentage of total area
        - **AREA_SE** : float (if variance=False)
            Standard error of area percentage
        - **AREA_VAR** : float (if variance=True)
            Variance of area percentage
        - **N_PLOTS** : int
            Number of plots in estimate
        - **AREA** : float (if totals=True)
            Total area in acres
        - **AREA_TOTAL_SE** : float (if totals=True and variance=False)
            Standard error of total area

    See Also
    --------
    pyfia.volume : Estimate tree volume
    pyfia.biomass : Estimate tree biomass
    pyfia.tpa : Estimate trees per acre
    pyfia.constants.ForestTypes : Forest type code definitions
    pyfia.constants.StateCodes : State FIPS code definitions

    Notes
    -----
    The area estimation follows USDA FIA's design-based estimation procedures
    as described in Bechtold & Patterson (2005). The basic formula is:

    Area = Σ(CONDPROP_UNADJ × ADJ_FACTOR × EXPNS)

    Where:
    - CONDPROP_UNADJ: Proportion of plot in the condition
    - ADJ_FACTOR: Adjustment factor based on PROP_BASIS
    - EXPNS: Expansion factor from stratification

    **EVALID Handling:**
    If no EVALID is specified, the function automatically selects the most
    recent EXPALL evaluation to prevent overcounting from multiple evaluations.
    For explicit control, use db.clip_by_evalid() before calling area().

    **Valid Grouping Columns:**
    The function loads comprehensive sets of columns from COND and PLOT tables.
    Not all columns are suitable for grouping - continuous variables like
    LAT, LON, ELEV should not be used. The function will error if a requested
    grouping column is not available in the loaded data.

    **NULL Value Handling:**
    Some grouping columns may contain NULL values (e.g., PHYSCLCD ~18% NULL,
    DSTRBCD1 ~22% NULL). NULL values are handled safely by Polars and will
    appear as a separate group in results if present.

    Examples
    --------
    Basic forest area estimation:

    >>> from pyfia import FIA, area
    >>> with FIA("path/to/fia.duckdb") as db:
    ...     db.clip_by_state(37)  # North Carolina
    ...     results = area(db, land_type="forest")

    Area by ownership group:

    >>> results = area(db, grp_by="OWNGRPCD")
    >>> # Results will show area for each ownership category

    Timber area by forest type for stands over 50 years:

    >>> results = area(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     land_type="timber",
    ...     area_domain="STDAGE > 50"
    ... )

    Multiple grouping variables:

    >>> results = area(
    ...     db,
    ...     grp_by=["STATECD", "OWNGRPCD", "STDSZCD"],
    ...     land_type="forest"
    ... )

    Area by disturbance type:

    >>> results = area(
    ...     db,
    ...     grp_by="DSTRBCD1",
    ...     area_domain="DSTRBCD1 > 0"  # Only disturbed areas
    ... )

    Filter by county using plot_domain:

    >>> results = area(
    ...     db,
    ...     plot_domain="COUNTYCD == 183",  # Wake County, NC
    ...     land_type="forest"
    ... )

    Combine plot and area domain filters:

    >>> results = area(
    ...     db,
    ...     plot_domain="COUNTYCD IN (183, 185, 187)",  # Multiple counties
    ...     area_domain="OWNGRPCD == 40",  # Private land only
    ...     grp_by="FORTYPCD"
    ... )

    Geographic filtering with plot_domain:

    >>> results = area(
    ...     db,
    ...     plot_domain="LAT >= 35.0 AND LAT <= 36.0 AND ELEV > 1000",
    ...     land_type="forest"
    ... )
    """
    # Validate inputs using shared utility
    inputs = validate_estimator_inputs(
        land_type=land_type,
        grp_by=grp_by,
        area_domain=area_domain,
        plot_domain=plot_domain,
        variance=variance,
        totals=totals,
        most_recent=most_recent,
    )

    # Ensure db is a FIA instance using shared utility
    db, owns_db = ensure_fia_instance(db)

    # Ensure EVALID is set using shared utility
    # Use "ALL" for area estimation (EXPALL evaluations)
    ensure_evalid_set(db, eval_type="ALL", estimator_name="area")

    # Create simple config dict using validated inputs
    config = {
        "grp_by": inputs.grp_by,
        "land_type": inputs.land_type,
        "area_domain": inputs.area_domain,
        "plot_domain": inputs.plot_domain,
        "most_recent": inputs.most_recent,
        "eval_type": eval_type,
        "variance": inputs.variance,
        "totals": inputs.totals,
    }

    try:
        # Create estimator and run
        estimator = AreaEstimator(db, config)
        return estimator.estimate()
    finally:
        # Clean up if we created the db
        if owns_db and hasattr(db, "close"):
            db.close()
