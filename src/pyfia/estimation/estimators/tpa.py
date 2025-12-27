"""
Trees per acre (TPA) and basal area (BAA) estimation for FIA data.

Simple implementation for calculating tree density and basal area
without unnecessary abstractions.
"""

import math
from typing import TYPE_CHECKING, List, Optional, Union

import polars as pl

from ..base import BaseEstimator
from ..tree_expansion import apply_tree_adjustment_factors
from ..variance import calculate_domain_total_variance

if TYPE_CHECKING:
    from ...core import FIA


class TPAEstimator(BaseEstimator):
    """
    Trees per acre and basal area estimator for FIA data.

    Estimates tree density (TPA) and basal area per acre (BAA).
    """

    def __init__(self, db, config):
        """Initialize with storage for variance calculation."""
        super().__init__(db, config)
        self.plot_tree_data = None  # Store for variance calculation
        self.group_cols = None  # Store grouping columns

    def get_required_tables(self) -> List[str]:
        """TPA requires tree, condition, and stratification tables."""
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]

    def get_tree_columns(self) -> List[str]:
        """Required tree columns for TPA estimation."""
        cols = [
            "CN",
            "PLT_CN",
            "CONDID",
            "STATUSCD",
            "SPCD",
            "DIA",
            "TPA_UNADJ",
            "TREECLCD",
        ]

        # Add grouping columns if needed
        if self.config.get("grp_by"):
            grp_cols = self.config["grp_by"]
            if isinstance(grp_cols, str):
                grp_cols = [grp_cols]
            for col in grp_cols:
                # Common TREE columns for grouping
                if col not in cols and col in [
                    "HT", "ACTUALHT", "CR", "CCLCD", "SPGRPCD", "SPCD",
                    "TREECLCD", "DECAYCD"
                ]:
                    cols.append(col)

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
        Calculate TPA and BAA values.

        Trees per acre (TPA) and Basal Area per Acre (BAA) calculation:
        - TPA = TPA_UNADJ (direct from FIA field)
        - BAA = π × (DIA/24)² × TPA_UNADJ

        The BAA formula derivation:
        1. Basal area of one tree = π × radius²
        2. DIA is diameter at breast height in inches
        3. Convert to feet: DIA/12, then to radius: (DIA/12)/2 = DIA/24
        4. Basal area = π × (DIA/24)² square feet
        5. Multiply by TPA_UNADJ to get basal area per acre
        """
        # Ensure proper data types for calculation precision
        data = data.with_columns(
            [
                # TPA is directly from TPA_UNADJ
                pl.col("TPA_UNADJ").cast(pl.Float64).alias("TPA"),
                # BAA calculation with explicit formula documentation
                # π × (DIA in inches / 24)² × TPA_UNADJ = basal area in sq ft per acre
                (
                    math.pi
                    * (pl.col("DIA").cast(pl.Float64) / 24.0) ** 2
                    * pl.col("TPA_UNADJ").cast(pl.Float64)
                ).alias("BAA"),
            ]
        )

        # Add size class if requested
        if self.config.get("by_size_class", False):
            # Create 2-inch diameter classes (0, 2, 4, 6, 8, ...)
            data = data.with_columns(
                [((pl.col("DIA") / 2.0).floor() * 2).cast(pl.Int32).alias("SIZE_CLASS")]
            )

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:  # type: ignore[override]
        """
        Aggregate TPA and BAA with proper FIA two-stage stratification.

        Implements FIA's two-stage aggregation methodology:
        Stage 1: Sum trees to plot-condition level
        Stage 2: Apply expansion factors and calculate ratio-of-means

        This follows the FIA EVALIDator pattern to ensure correct statistical estimates.
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply adjustment factors based on tree size
        # FIA uses different plot sizes for different tree sizes
        data_with_strat = apply_tree_adjustment_factors(  # type: ignore[assignment]
            data_with_strat, size_col="DIA", macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
        )

        # Apply adjustment to get adjusted values
        data_with_strat = data_with_strat.with_columns(
            [
                (
                    pl.col("TPA").cast(pl.Float64)
                    * pl.col("ADJ_FACTOR").cast(pl.Float64)
                ).alias("TPA_ADJ"),
                (
                    pl.col("BAA").cast(pl.Float64)
                    * pl.col("ADJ_FACTOR").cast(pl.Float64)
                ).alias("BAA_ADJ"),
            ]
        )

        # Setup grouping columns - need to include condition-level grouping
        group_cols = self._setup_grouping()

        # Add species to grouping if requested
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")

        # Add size class to grouping if requested
        if self.config.get("by_size_class", False) and "SIZE_CLASS" not in group_cols:
            group_cols.append("SIZE_CLASS")

        self.group_cols = group_cols  # Store for variance calculation

        # CRITICAL: Store plot-tree level data for variance calculation
        self.plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=["TPA_ADJ", "BAA_ADJ"],
            group_cols=group_cols,
        )

        # Use shared two-stage aggregation method
        metric_mappings = {"TPA_ADJ": "CONDITION_TPA", "BAA_ADJ": "CONDITION_BAA"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=False,
        )

        # Rename columns to match expected output
        results = results.rename({"TPA_ACRE": "TPA", "BAA_ACRE": "BAA"})

        # Handle totals based on config
        if not self.config.get("totals", False):
            # Remove total columns if not requested
            cols_to_drop = ["TPA_TOTAL", "BAA_TOTAL"]
            cols_to_drop = [col for col in cols_to_drop if col in results.columns]
            if cols_to_drop:
                results = results.drop(cols_to_drop)

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate variance for TPA and BAA estimates using domain total variance formula.

        Uses the stratified domain total variance formula from Bechtold & Patterson (2005):
        V(Ŷ) = Σ_h w_h² × s²_yh × n_h

        This matches EVALIDator's variance calculation for tree-based estimates.

        Raises
        ------
        ValueError
            If plot_tree_data is not available for variance calculation.
        """
        if self.plot_tree_data is None:
            raise ValueError(
                "Plot-tree data is required for TPA/BAA variance calculation. "
                "Cannot compute statistically valid standard errors without tree-level "
                "data. Ensure data preservation is working correctly in the estimation "
                "pipeline."
            )

        # Step 1: Aggregate to plot-condition level
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in self.plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")
        if "CONDPROP_UNADJ" in self.plot_tree_data.columns:
            plot_group_cols.append("CONDPROP_UNADJ")

        # Add grouping columns
        if self.group_cols:
            for col in self.group_cols:
                if col in self.plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_agg = [
            pl.sum("TPA_ADJ").alias("y_tpa_ic"),
            pl.sum("BAA_ADJ").alias("y_baa_ic"),
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
                pl.sum("y_tpa_ic").alias("y_tpa_i"),
                pl.sum("y_baa_ic").alias("y_baa_i"),
                pl.sum("CONDPROP_UNADJ").cast(pl.Float64).alias("x_i"),
            ]
        )

        # Step 3: Get ALL plots in the evaluation for proper variance calculation
        strat_data = self._get_stratification_data()
        all_plots = (
            strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
        )

        # Step 4: Calculate variance for each group or overall
        if self.group_cols:
            variance_results = []

            for group_vals in results.iter_rows():
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(self.group_cols):
                    if col in plot_data.columns:
                        group_dict[col] = group_vals[results.columns.index(col)]
                        group_filter = group_filter & (
                            pl.col(col) == group_vals[results.columns.index(col)]
                        )

                group_plot_data = plot_data.filter(group_filter)

                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_tpa_i", "y_baa_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [
                        pl.col("y_tpa_i").fill_null(0.0),
                        pl.col("y_baa_i").fill_null(0.0),
                        pl.col("x_i").fill_null(0.0),
                    ]
                )

                if len(all_plots_group) > 0:
                    # Calculate variance using domain total formula (matches EVALIDator)
                    tpa_stats = calculate_domain_total_variance(
                        all_plots_group, "y_tpa_i"
                    )
                    baa_stats = calculate_domain_total_variance(
                        all_plots_group, "y_baa_i"
                    )

                    # Calculate per-acre SE by dividing total SE by total area
                    total_area = (
                        all_plots_group["EXPNS"] * all_plots_group["x_i"]
                    ).sum()
                    tpa_se_acre = (
                        tpa_stats["se_total"] / total_area if total_area > 0 else 0.0
                    )
                    baa_se_acre = (
                        baa_stats["se_total"] / total_area if total_area > 0 else 0.0
                    )

                    variance_results.append(
                        {
                            **group_dict,
                            "TPA_SE": tpa_se_acre,
                            "TPA_TOTAL_SE": tpa_stats["se_total"],
                            "BAA_SE": baa_se_acre,
                            "BAA_TOTAL_SE": baa_stats["se_total"],
                        }
                    )
                else:
                    variance_results.append(
                        {
                            **group_dict,
                            "TPA_SE": 0.0,
                            "TPA_TOTAL_SE": 0.0,
                            "BAA_SE": 0.0,
                            "BAA_TOTAL_SE": 0.0,
                        }
                    )

            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=self.group_cols, how="left")
        else:
            # No grouping, calculate overall variance with ALL plots
            all_plots_with_values = all_plots.join(
                plot_data.select(["PLT_CN", "y_tpa_i", "y_baa_i", "x_i"]),
                on="PLT_CN",
                how="left",
            ).with_columns(
                [
                    pl.col("y_tpa_i").fill_null(0.0),
                    pl.col("y_baa_i").fill_null(0.0),
                    pl.col("x_i").fill_null(0.0),
                ]
            )

            tpa_stats = calculate_domain_total_variance(
                all_plots_with_values, "y_tpa_i"
            )
            baa_stats = calculate_domain_total_variance(
                all_plots_with_values, "y_baa_i"
            )

            # Calculate per-acre SE by dividing total SE by total area
            total_area = (
                all_plots_with_values["EXPNS"] * all_plots_with_values["x_i"]
            ).sum()
            tpa_se_acre = tpa_stats["se_total"] / total_area if total_area > 0 else 0.0
            baa_se_acre = baa_stats["se_total"] / total_area if total_area > 0 else 0.0

            results = results.with_columns(
                [
                    pl.lit(tpa_se_acre).alias("TPA_SE"),
                    pl.lit(baa_se_acre).alias("BAA_SE"),
                ]
            )

            if "TPA_TOTAL" in results.columns:
                results = results.with_columns(
                    [
                        pl.lit(tpa_stats["se_total"]).alias("TPA_TOTAL_SE"),
                        pl.lit(baa_stats["se_total"]).alias("BAA_TOTAL_SE"),
                    ]
                )

        # Convert to variance if requested
        if self.config.get("variance", False):
            results = results.with_columns(
                [
                    (pl.col("TPA_SE") ** 2).alias("TPA_VAR"),
                    (pl.col("BAA_SE") ** 2).alias("BAA_VAR"),
                ]
            )
            results = results.drop(["TPA_SE", "BAA_SE"])

            if "TPA_TOTAL_SE" in results.columns:
                results = results.with_columns(
                    [
                        (pl.col("TPA_TOTAL_SE") ** 2).alias("TPA_TOTAL_VAR"),
                        (pl.col("BAA_TOTAL_SE") ** 2).alias("BAA_TOTAL_VAR"),
                    ]
                )
                results = results.drop(["TPA_TOTAL_SE", "BAA_TOTAL_SE"])

        # Add warning for small sample sizes
        if "N_PLOTS" in results.columns:
            min_plots_val = results["N_PLOTS"].min()
            if min_plots_val is not None:
                min_plots = int(min_plots_val)  # type: ignore[arg-type]
                if min_plots < 10:
                    import warnings

                    warnings.warn(
                        f"Small sample size detected (min {min_plots} plots). "
                        "Variance estimates may be unreliable. Consider aggregating to larger areas.",
                        UserWarning,
                    )

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """
        Format TPA estimation output with consistent column ordering.

        Follows FIA standard output format:
        1. YEAR (inventory year)
        2. Grouping columns (if any)
        3. Estimate columns (TPA, BAA)
        4. Uncertainty columns (SE or VAR)
        5. Total columns (if requested)
        6. Sample size columns (N_PLOTS, N_TREES)
        """
        # Extract actual inventory year from EVALID or INVYR
        year = self._extract_evaluation_year()
        results = results.with_columns([pl.lit(year).alias("YEAR")])

        # Build column order based on what's present
        col_order = ["YEAR"]

        # Add grouping columns (maintain their order)
        grouping_cols = []
        if self.config.get("grp_by"):
            grp_by = self.config["grp_by"]
            if isinstance(grp_by, str):
                grouping_cols = [grp_by]
            else:
                grouping_cols = list(grp_by)

        # Add special grouping columns
        if self.config.get("by_species") and "SPCD" not in grouping_cols:
            grouping_cols.append("SPCD")
        if self.config.get("by_size_class") and "SIZE_CLASS" not in grouping_cols:
            grouping_cols.append("SIZE_CLASS")

        # Add grouping columns that exist in results
        for col in grouping_cols:
            if col in results.columns:
                col_order.append(col)

        # Add per-acre estimate columns
        col_order.extend(["TPA", "BAA"])

        # Add uncertainty columns (SE or VAR based on config)
        if self.config.get("variance", False):
            if "TPA_VAR" in results.columns:
                col_order.extend(["TPA_VAR", "BAA_VAR"])
        else:
            if "TPA_SE" in results.columns:
                col_order.extend(["TPA_SE", "BAA_SE"])

        # Add total columns if present
        if self.config.get("totals", False):
            if "TPA_TOTAL" in results.columns:
                col_order.extend(["TPA_TOTAL", "BAA_TOTAL"])
                if self.config.get("variance", False):
                    if "TPA_TOTAL_VAR" in results.columns:
                        col_order.extend(["TPA_TOTAL_VAR", "BAA_TOTAL_VAR"])
                else:
                    if "TPA_TOTAL_SE" in results.columns:
                        col_order.extend(["TPA_TOTAL_SE", "BAA_TOTAL_SE"])

        # Add CV columns if present
        if self.config.get("include_cv", False):
            if "TPA_CV" in results.columns:
                col_order.extend(["TPA_CV", "BAA_CV"])

        # Add sample size columns
        col_order.extend(["AREA_TOTAL", "N_PLOTS", "N_TREES"])

        # Select only existing columns in the specified order
        final_cols = [col for col in col_order if col in results.columns]
        results = results.select(final_cols)

        # Sort by grouping columns if present (for cleaner output)
        if grouping_cols:
            existing_group_cols = [
                col for col in grouping_cols if col in results.columns
            ]
            if existing_group_cols:
                results = results.sort(existing_group_cols)

        return results


def tpa(
    db: "FIA",
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    plot_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = False,
) -> pl.DataFrame:
    """
    Estimate trees per acre (TPA) and basal area per acre (BAA) from FIA data.

    Calculates tree density and basal area estimates using FIA's design-based
    estimation methods with proper expansion factors and stratification. Automatically
    handles EVALID selection to prevent overcounting from multiple evaluations.

    Parameters
    ----------
    db : FIA
        FIA database connection object. Must have EVALID set to prevent
        overcounting from multiple evaluations.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        TREE, PLOT, and COND tables. Common grouping columns include:

        **Tree Attributes:**
        - 'SPCD': Species code (see REF_SPECIES)
        - 'SPGRPCD': Species group code
        - 'DIA': Diameter at breast height (inches)
        - 'HT': Total tree height (feet)
        - 'CR': Compacted crown ratio (percent)
        - 'CCLCD': Crown class code (1=Open grown, 2=Dominant, 3=Codominant,
          4=Intermediate, 5=Overtopped)
        - 'TREECLCD': Tree class code (2=Growing stock, 3=Rough cull, 4=Rotten cull)
        - 'STATUSCD': Tree status (1=Live, 2=Dead, 3=Removed)

        **Forest Characteristics:**
        - 'FORTYPCD': Forest type code (see REF_FOREST_TYPE)
        - 'STDSZCD': Stand size class (1=Large diameter, 2=Medium diameter,
          3=Small diameter, 4=Seedling/sapling, 5=Nonstocked)
        - 'STDAGE': Stand age in years
        - 'SITECLCD': Site productivity class (1=225+ cu ft/ac/yr,
          2=165-224, 3=120-164, 4=85-119, 5=50-84, 6=20-49, 7=0-19)

        **Ownership and Location:**
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'STATECD': State FIPS code
        - 'UNITCD': FIA survey unit code
        - 'COUNTYCD': County code
        - 'INVYR': Inventory year

        **Disturbance and Treatment:**
        - 'DSTRBCD1', 'DSTRBCD2', 'DSTRBCD3': Disturbance codes
        - 'TRTCD1', 'TRTCD2', 'TRTCD3': Treatment codes

        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). This is a convenience
        parameter equivalent to adding 'SPCD' to grp_by.
    by_size_class : bool, default False
        If True, group results by diameter size classes. Size classes are
        defined as 2-inch DBH classes: 0-1.9", 2-3.9", 4-5.9", etc.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland (COND_STATUS_CD = 1)
        - 'timber': Timberland only (unreserved, productive forestland with
          SITECLCD in [1,2,3,4,5,6] and RESERVCD = 0)
        - 'all': All land types including non-forest
    tree_type : {'live', 'dead', 'gs', 'all'}, default 'live'
        Tree type to include in estimation:

        - 'live': All live trees (STATUSCD = 1)
        - 'dead': Standing dead trees (STATUSCD = 2)
        - 'gs': Growing stock trees (live trees meeting merchantability standards,
          typically TREECLCD = 2)
        - 'all': All trees regardless of status
    tree_domain : str, optional
        SQL-like filter expression for tree-level attributes. Applied to
        the TREE table. Examples:

        - "DIA >= 10.0": Trees 10 inches DBH and larger
        - "SPCD IN (131, 110)": Specific species (loblolly and Virginia pine)
        - "HT > 50 AND CR > 30": Tall trees with good crowns
        - "TREECLCD == 2": Growing stock trees only
    area_domain : str, optional
        SQL-like filter expression for area/condition-level attributes.
        Applied to the COND table. Examples:

        - "STDAGE > 50": Stands older than 50 years
        - "FORTYPCD IN (161, 162)": Specific forest types
        - "OWNGRPCD == 40": Private lands only
        - "PHYSCLCD == 31 AND STDSZCD == 1": Xeric sites with large trees
    totals : bool, default False
        If True, include population-level total estimates (TPA_TOTAL, BAA_TOTAL)
        in addition to per-acre values. Total estimates are expanded using
        stratification factors.
    variance : bool, default False
        If True, return variance instead of standard error. Standard error
        is calculated as the square root of variance.

    Returns
    -------
    pl.DataFrame
        Trees per acre and basal area estimates with the following columns:

        - **YEAR** : int
            Representative inventory year
        - **[grouping columns]** : varies
            Any columns specified in grp_by parameter
        - **TPA** : float
            Trees per acre
        - **BAA** : float
            Basal area per acre (square feet)
        - **TPA_SE** : float (if variance=False)
            Standard error of TPA estimate
        - **BAA_SE** : float (if variance=False)
            Standard error of BAA estimate
        - **TPA_VAR** : float (if variance=True)
            Variance of TPA estimate
        - **BAA_VAR** : float (if variance=True)
            Variance of BAA estimate
        - **TPA_TOTAL** : float (if totals=True)
            Total trees expanded to population level
        - **BAA_TOTAL** : float (if totals=True)
            Total basal area expanded to population level
        - **TPA_TOTAL_SE** : float (if totals=True and variance=False)
            Standard error of total TPA
        - **BAA_TOTAL_SE** : float (if totals=True and variance=False)
            Standard error of total BAA
        - **N_PLOTS** : int
            Number of FIA plots in estimate
        - **N_TREES** : int
            Number of individual tree records

    See Also
    --------
    pyfia.volume : Estimate tree volume per acre
    pyfia.biomass : Estimate tree biomass per acre
    pyfia.area : Estimate forest area

    External References
    -------------------
    FIA EVALIDator : USDA Forest Service online tool for validation
        https://apps.fs.usda.gov/Evalidator/evalidator.jsp
    rFIA : R package for FIA analysis (independent validation)
        https://cran.r-project.org/package=rFIA
    Bechtold & Patterson (2005) : The enhanced FIA national program
        https://doi.org/10.2737/SRS-GTR-80
    pyfia.mortality : Estimate annual tree mortality
    pyfia.growth : Estimate annual tree growth
    pyfia.constants.SpeciesCodes : Species code definitions
    pyfia.constants.ForestTypes : Forest type code definitions
    pyfia.utils.reference_tables : Functions for adding species/forest type names

    Notes
    -----
    Trees per acre (TPA) and basal area per acre (BAA) are fundamental forest
    inventory metrics. TPA represents tree density, while BAA represents the
    cross-sectional area of trees at breast height (4.5 feet).

    **Calculation Formulas (Two-Stage Aggregation):**

    Stage 1 - Plot-Condition Aggregation:
        CONDITION_TPA = Σ(TPA_UNADJ × ADJ_FACTOR) for each tree in condition
        CONDITION_BAA = Σ(π × (DIA/24)² × TPA_UNADJ × ADJ_FACTOR) for each tree

    Stage 2 - Population Expansion:
        TPA = Σ(CONDITION_TPA × EXPNS) / Σ(CONDPROP_UNADJ × EXPNS)
        BAA = Σ(CONDITION_BAA × EXPNS) / Σ(CONDPROP_UNADJ × EXPNS)

    Where:
    - TPA_UNADJ: Unadjusted trees per acre from plot design
    - DIA: Diameter at breast height in inches
    - ADJ_FACTOR: Plot size adjustment factor (SUBP, MICR, or MACR)
    - EXPNS: Stratification expansion factor
    - CONDPROP_UNADJ: Proportion of plot in the condition

    The DIA/24 term converts diameter in inches to radius in feet:
    - DIA/12 converts inches to feet
    - Divide by 2 to get radius
    - Simplified: (DIA/24)²

    **CRITICAL - FUNDAMENTAL REQUIREMENT**: The two-stage aggregation is not
    optional - it is mathematically required for statistically valid FIA
    estimates. Any deviation from this order (applying expansion factors before
    condition-level aggregation) will produce **fundamentally incorrect results**
    that can be orders of magnitude wrong. This is a core requirement of FIA's
    design-based estimation methodology, not an implementation choice.

    **EVALID Requirements:**
    The FIA database must have EVALID set before calling this function.
    Use db.clip_by_evalid() or db.clip_most_recent() to select appropriate
    evaluations and prevent overcounting from multiple evaluations.

    **Plot Size Adjustments:**
    FIA uses different plot sizes for different tree sizes:
    - Microplot (6.8 ft radius): Trees 1.0-4.9" DBH
    - Subplot (24.0 ft radius): Trees 5.0"+ DBH (or to breakpoint)
    - Macroplot (58.9 ft radius): Trees above breakpoint diameter

    The adjustment factors account for these different sampling intensities.

    **Valid Grouping Columns:**
    The function joins TREE, COND, and PLOT tables, so any column from these
    tables can be used for grouping. Continuous variables (LAT, LON, ELEV)
    should not be used for grouping. Some columns may contain NULL values.

    **Size Class Definition:**
    When by_size_class=True, trees are grouped into 2-inch diameter classes
    based on DBH. The size class value represents the lower bound of each
    2-inch class (0, 2, 4, 6, 8, etc.).

    Warnings
    --------
    **BREAKING CHANGE (v1.0.0+)**: This version fixes a critical aggregation bug
    in previous releases. The two-stage aggregation now correctly sums trees to
    condition level before applying expansion factors. Previous versions may have
    produced estimates that were **orders of magnitude incorrect** (up to 26x
    higher than correct values). Users upgrading should validate their results
    against FIA EVALIDator or rFIA. Historical analyses using pyfia <1.0.0 should
    be rerun with corrected aggregation.

    The variance calculation follows Bechtold & Patterson (2005) methodology
    for ratio-of-means estimation with stratified sampling. The calculation
    accounts for covariance between the numerator (TPA/BAA) and denominator
    (area). Small sample sizes (<10 plots) will trigger additional warnings.
    For applications requiring the most precise variance estimates, consider
    also validating against the FIA EVALIDator tool or rFIA R package.

    Examples
    --------
    Basic trees per acre on forestland:

    >>> from pyfia import FIA, tpa
    >>> db = FIA("path/to/fia.duckdb")
    >>> db.clip_by_state(37)  # North Carolina
    >>> db.clip_most_recent(eval_type="VOL")  # Required: select EVALID
    >>> results = tpa(db, land_type="forest")
    >>> print(f"TPA: {results['TPA'][0]:.1f} trees/acre")
    >>> print(f"BAA: {results['BAA'][0]:.1f} sq ft/acre")

    TPA and BAA by species:

    >>> results = tpa(db, by_species=True)
    >>> # Top 5 species by trees per acre
    >>> top_species = results.sort(by='TPA', descending=True).head(5)

    Large trees only (≥10 inches DBH):

    >>> results = tpa(
    ...     db,
    ...     tree_domain="DIA >= 10.0",
    ...     land_type="forest"
    ... )

    By size class on timberland:

    >>> results = tpa(
    ...     db,
    ...     by_size_class=True,
    ...     land_type="timber",
    ...     tree_type="live"
    ... )
    >>> # Shows distribution across diameter classes

    Multiple grouping variables:

    >>> results = tpa(
    ...     db,
    ...     grp_by=["OWNGRPCD", "FORTYPCD"],
    ...     land_type="forest",
    ...     totals=True
    ... )

    Growing stock trees by forest type:

    >>> results = tpa(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     tree_type="gs",
    ...     tree_domain="TREECLCD == 2"
    ... )

    Standing dead trees by species:

    >>> results = tpa(
    ...     db,
    ...     by_species=True,
    ...     tree_type="dead",
    ...     tree_domain="DIA >= 5.0"
    ... )

    Validation against FIA EVALIDator:

    >>> # Using Texas data (STATECD=48, EVALID=482300)
    >>> # Corrected two-stage aggregation produces:
    >>> # TPA: 23.8 trees/acre (matches EVALIDator)
    >>> # Previous incorrect aggregation would have produced:
    >>> # TPA: 619.3 trees/acre (26x higher - INCORRECT)
    >>> #
    >>> # This demonstrates the critical importance of proper
    >>> # condition-level aggregation before expansion
    """
    # Import validation functions
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_tree_type,
    )

    # Validate inputs
    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    plot_domain = validate_domain_expression(plot_domain, "plot_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")

    # Validate EVALID is set
    if db.evalid is None:
        raise ValueError(
            "EVALID must be set before calling tpa(). "
            "Use db.clip_by_evalid() or db.clip_most_recent() to select evaluations."
        )

    # Create config - simpler without deprecated parameters
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "plot_domain": plot_domain,
        "totals": totals,
        "variance": variance,
    }

    # Create and run estimator - simple and clean
    estimator = TPAEstimator(db, config)
    return estimator.estimate()
