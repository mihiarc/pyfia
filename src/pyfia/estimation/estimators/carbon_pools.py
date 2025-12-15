"""
Carbon pool estimation using FIA's pre-calculated carbon columns.

This estimator uses CARBON_AG and CARBON_BG columns directly from the TREE table,
which contain species-specific carbon values calculated by FIA. This approach
matches EVALIDator's snum=55000 methodology and produces more accurate results
than the biomass-derived 47% conversion factor approach.

Key differences from biomass-derived carbon:
- Uses FIA's pre-calculated CARBON_AG and CARBON_BG columns
- Species-specific carbon conversion factors (not a flat 47%)
- Includes both aboveground and belowground carbon pools
- Matches EVALIDator exactly for live tree carbon estimates
"""

from typing import List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..tree_expansion import apply_tree_adjustment_factors
from ..variance import calculate_ratio_variance

# Unit conversion constant
# CARBON_AG and CARBON_BG are stored in pounds in the FIA database
# EVALIDator and FIA standard reporting use short tons (2000 lbs per ton)
# Using short tons for consistency with EVALIDator validation
LBS_TO_SHORT_TONS = 1.0 / 2000.0


class CarbonPoolEstimator(BaseEstimator):
    """
    Carbon pool estimator using FIA's pre-calculated carbon columns.

    This estimator uses the CARBON_AG (aboveground) and CARBON_BG (belowground)
    columns from the TREE table, which contain species-specific carbon values
    calculated by FIA using their regional allometric equations.

    This approach matches EVALIDator's snum=55000 (total live tree carbon)
    methodology exactly, eliminating the 1.62% discrepancy from the
    biomass-derived 47% conversion factor approach.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database
    config : dict
        Configuration dictionary with:
        - pool : str ('ag', 'bg', 'total') - Carbon pool to estimate
        - grp_by : Optional grouping columns
        - by_species : bool - Group by species
        - land_type : str - Land type filter
        - tree_type : str - Tree type filter
        - tree_domain : Optional SQL filter for trees
        - area_domain : Optional SQL filter for conditions
        - totals : bool - Include population totals
        - variance : bool - Calculate variance estimates
    """

    def __init__(self, db: Union[str, FIA], config: dict):
        """Initialize with storage for variance calculation."""
        super().__init__(db, config)
        self.plot_tree_data: Optional[pl.DataFrame] = None
        self.group_cols: Optional[List[str]] = None

    def get_required_tables(self) -> List[str]:
        """Carbon estimation requires tree, condition, and stratification tables."""
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_tree_columns(self) -> List[str]:
        """Required tree columns for carbon estimation."""
        pool = self.config.get("pool", "total")

        # Base columns always needed
        cols = [
            "CN",
            "PLT_CN",
            "CONDID",
            "STATUSCD",
            "SPCD",
            "DIA",
            "TPA_UNADJ",
        ]

        # Add carbon columns based on pool
        if pool == "ag":
            cols.append("CARBON_AG")
        elif pool == "bg":
            cols.append("CARBON_BG")
        else:  # total
            cols.extend(["CARBON_AG", "CARBON_BG"])

        return cols

    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        return [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "OWNGRPCD",
            "FORTYPCD",
            "SITECLCD",
            "RESERVCD",
        ]

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate carbon per acre using FIA's pre-calculated carbon columns.

        Carbon per acre = (CARBON * TPA_UNADJ) / 2000

        Where:
        - CARBON is in pounds (from CARBON_AG, CARBON_BG, or sum)
        - TPA_UNADJ is trees per acre unadjusted
        - 2000 converts pounds to short tons (FIA/EVALIDator standard)
        """
        pool = self.config.get("pool", "total")

        # Select carbon component based on pool
        if pool == "ag":
            carbon_expr = pl.col("CARBON_AG").fill_null(0.0)
        elif pool == "bg":
            carbon_expr = pl.col("CARBON_BG").fill_null(0.0)
        else:  # total
            carbon_expr = pl.col("CARBON_AG").fill_null(0.0) + pl.col(
                "CARBON_BG"
            ).fill_null(0.0)

        # Calculate carbon per acre in short tons (FIA/EVALIDator standard)
        data = data.with_columns(
            [
                (
                    carbon_expr.cast(pl.Float64)
                    * pl.col("TPA_UNADJ").cast(pl.Float64)
                    * LBS_TO_SHORT_TONS
                ).alias("CARBON_ACRE")
            ]
        )

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:  # type: ignore[override]
        """
        Aggregate carbon with two-stage aggregation for correct per-acre estimates.

        Implements FIA's design-based estimation methodology:
        Stage 1: Aggregate trees to plot-condition level
        Stage 2: Apply expansion factors and calculate ratio-of-means
        """
        # Validate required columns exist
        data_schema = data.collect_schema()
        required_cols = ["PLT_CN", "CARBON_ACRE"]
        missing_cols = [col for col in required_cols if col not in data_schema.names()]
        if missing_cols:
            raise ValueError(f"Required columns missing from data: {missing_cols}")

        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply adjustment factors based on tree DIA
        data_with_strat = apply_tree_adjustment_factors(  # type: ignore[assignment]
            data_with_strat, size_col="DIA", macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
        )

        # Apply adjustment to carbon
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("CARBON_ACRE") * pl.col("ADJ_FACTOR")).alias("CARBON_ADJ")]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        self.group_cols = group_cols

        # CRITICAL: Store plot-tree level data for variance calculation
        self.plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=["CARBON_ADJ"],
            group_cols=group_cols,
        )

        # Use shared two-stage aggregation method
        metric_mappings = {"CARBON_ADJ": "CONDITION_CARBON"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=False,
        )

        # Handle totals based on config
        if not self.config.get("totals", True):
            cols_to_drop = ["CARBON_TOTAL"]
            cols_to_drop = [col for col in cols_to_drop if col in results.columns]
            if cols_to_drop:
                results = results.drop(cols_to_drop)

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate variance for carbon estimates using proper ratio estimation formula.

        Carbon estimation uses ratio-of-means: R = Y/X where Y is carbon and X is area.
        The variance formula accounts for covariance between numerator and denominator.

        Following Bechtold & Patterson (2005) methodology for stratified sampling.

        Raises
        ------
        ValueError
            If plot_tree_data is not available for variance calculation.
        """
        if self.plot_tree_data is None:
            raise ValueError(
                "Plot-tree data is required for carbon variance calculation. "
                "Cannot compute statistically valid standard errors without tree-level "
                "data. Ensure data preservation is working correctly in the estimation "
                "pipeline."
            )

        # Step 1: Aggregate to plot-condition level
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in self.plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")

        # Add grouping columns
        if self.group_cols:
            for col in self.group_cols:
                if col in self.plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_agg = [
            pl.sum("CARBON_ADJ").alias("y_carb_ic"),  # Carbon per condition
        ]

        plot_cond_data = self.plot_tree_data.group_by(plot_group_cols).agg(
            plot_cond_agg
        )

        # Step 2: Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if self.group_cols:
            plot_level_cols.extend(
                [c for c in self.group_cols if c in plot_cond_data.columns]
            )

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(
            [
                pl.sum("y_carb_ic").alias("y_carb_i"),  # Total carbon per plot
                pl.lit(1.0).alias("x_i"),  # Area proportion per plot (full plot = 1)
            ]
        )

        # Step 3: Calculate variance for each group or overall
        if self.group_cols:
            # Get ALL plots in the evaluation for proper variance calculation
            strat_data = self._get_stratification_data()
            all_plots = (
                strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
            )

            # Calculate variance for each group separately
            variance_results = []

            for group_vals in results.iter_rows():
                # Build filter for this group
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(self.group_cols):
                    if col in plot_data.columns:
                        group_dict[col] = group_vals[results.columns.index(col)]
                        group_filter = group_filter & (
                            pl.col(col) == group_vals[results.columns.index(col)]
                        )

                # Filter plot data for this specific group
                group_plot_data = plot_data.filter(group_filter)

                # Join with ALL plots, filling missing with zeros
                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_carb_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [
                        pl.col("y_carb_i").fill_null(0.0),
                        pl.col("x_i").fill_null(0.0),
                    ]
                )

                if len(all_plots_group) > 0:
                    carb_stats = calculate_ratio_variance(all_plots_group, "y_carb_i")
                    variance_results.append(
                        {
                            **group_dict,
                            "CARBON_ACRE_SE": carb_stats["se_acre"],
                            "CARBON_TOTAL_SE": carb_stats["se_total"],
                        }
                    )
                else:
                    variance_results.append(
                        {
                            **group_dict,
                            "CARBON_ACRE_SE": 0.0,
                            "CARBON_TOTAL_SE": 0.0,
                        }
                    )

            # Join variance results back to main results
            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=self.group_cols, how="left")
        else:
            # No grouping, calculate overall variance
            carb_stats = calculate_ratio_variance(plot_data, "y_carb_i")

            results = results.with_columns(
                [
                    pl.lit(carb_stats["se_acre"]).alias("CARBON_ACRE_SE"),
                    pl.lit(carb_stats["se_total"]).alias("CARBON_TOTAL_SE"),
                ]
            )

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format carbon estimation output."""
        # Extract year from evaluation data using shared helper
        year = self._extract_evaluation_year()

        # Add pool identifier
        pool = self.config.get("pool", "total").upper()
        results = results.with_columns(
            [pl.lit(year).alias("YEAR"), pl.lit(pool).alias("POOL")]
        )

        # Standard column order
        col_order = [
            "YEAR",
            "POOL",
            "CARBON_ACRE",
            "CARBON_TOTAL",
            "CARBON_ACRE_SE",
            "CARBON_TOTAL_SE",
            "N_PLOTS",
            "N_TREES",
        ]

        # Add any grouping columns after POOL
        for col in results.columns:
            if col not in col_order:
                col_order.insert(2, col)

        # Select only existing columns in order
        final_cols = [col for col in col_order if col in results.columns]
        results = results.select(final_cols)

        return results


def carbon_pool(
    db: Union[str, FIA],
    pool: str = "total",
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate carbon stocks using FIA's pre-calculated carbon columns.

    This function uses CARBON_AG and CARBON_BG columns directly from the TREE table,
    which contain species-specific carbon values. This approach matches EVALIDator's
    snum=55000 methodology exactly.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database.
    pool : {'ag', 'bg', 'total'}, default 'total'
        Carbon pool to estimate:
        - 'ag': Aboveground carbon only (CARBON_AG)
        - 'bg': Belowground carbon only (CARBON_BG)
        - 'total': Total carbon (CARBON_AG + CARBON_BG) - matches EVALIDator snum=55000
    grp_by : str or list of str, optional
        Column name(s) to group results by.
    by_species : bool, default False
        If True, group results by species code (SPCD).
    by_size_class : bool, default False
        If True, group results by diameter size classes.
    land_type : {'forest', 'timber'}, default 'forest'
        Land type to include in estimation.
    tree_type : {'live', 'dead', 'gs', 'all'}, default 'live'
        Tree type to include.
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate variance and standard error estimates.
    most_recent : bool, default False
        If True, filter to most recent evaluation.

    Returns
    -------
    pl.DataFrame
        Carbon estimates with columns:
        - YEAR : int - Evaluation reference year
        - POOL : str - Carbon pool identifier
        - CARBON_ACRE : float - Carbon per acre (short tons/acre)
        - CARBON_TOTAL : float - Total carbon (short tons)
        - CARBON_ACRE_SE : float (if variance=True) - Standard error of per-acre
        - CARBON_TOTAL_SE : float (if variance=True) - Standard error of total
        - N_PLOTS : int - Number of plots
        - N_TREES : int - Number of trees

    Notes
    -----
    This estimator uses FIA's pre-calculated carbon columns (CARBON_AG, CARBON_BG)
    which incorporate species-specific carbon conversion factors. This is more
    accurate than applying a flat 47% carbon fraction to biomass.

    The output is in short tons (2000 lbs/ton) to match FIA and EVALIDator
    standard reporting. CARBON columns in the FIA database are in pounds;
    this function converts to short tons by dividing by 2000.

    Examples
    --------
    Total live tree carbon (matches EVALIDator snum=55000):

    >>> result = carbon_pool(db, pool="total")
    >>> print(f"Total carbon: {result['CARBON_TOTAL'][0]:,.0f} metric tons")

    Aboveground carbon by species:

    >>> result = carbon_pool(db, pool="ag", by_species=True)
    """
    # Import validation functions
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_tree_type,
    )

    # Validate pool
    pool = pool.lower()
    valid_pools = ["ag", "bg", "total"]
    if pool not in valid_pools:
        raise ValueError(
            f"Invalid pool '{pool}'. Must be one of: {', '.join(valid_pools)}"
        )

    # Validate other inputs
    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

    # Create config
    config = {
        "pool": pool,
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
    }

    # Create and run estimator
    estimator = CarbonPoolEstimator(db, config)
    return estimator.estimate()
