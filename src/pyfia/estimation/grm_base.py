"""
Base class for Growth-Removal-Mortality (GRM) estimators.

This module provides the shared functionality for growth, mortality,
and removals estimation using FIA's GRM tables.
"""

from __future__ import annotations

import logging
from typing import Literal

import polars as pl

from ..core.exceptions import NoEVALIDError
from ..filtering.utils import create_size_class_expr
from .base import AggregationResult, BaseEstimator
from .columns import get_cond_columns as _get_cond_columns

logger = logging.getLogger(__name__)


class GRMBaseEstimator(BaseEstimator):
    """
    Base class for Growth-Removal-Mortality (GRM) estimators.

    Provides shared functionality for growth, mortality, and removals estimation
    using FIA's GRM tables (TREE_GRM_COMPONENT, TREE_GRM_MIDPT, TREE_GRM_BEGIN).

    Subclasses must implement:
    - component_type: Property returning 'growth', 'mortality', or 'removals'
    - calculate_values: Component-specific value calculation
    - get_component_filter: Optional filter for specific components
    """

    def __init__(self, db, config):
        """Initialize the GRM base estimator."""
        super().__init__(db, config)
        self._grm_columns = None
        # Ensure EVALID filter is set for GRM estimation
        self._ensure_grm_evalid_filter()

    def _ensure_grm_evalid_filter(self) -> None:
        """
        Ensure an EVALID filter is set for GRM estimation.

        GRM estimates (growth, mortality, removals) require filtering to a specific
        EVALID to avoid counting trees multiple times across different evaluations.

        Without this filter, totals (e.g., MORT_TOTAL) can be ~60x too high because
        the same trees appear in multiple EVALIDs across different annual evaluations.
        Per-acre values remain correct because the inflation cancels in the ratio.

        Raises
        ------
        NoEVALIDError
            If no EVALID filter has been set. Users must explicitly call
            db.clip_by_evalid() or db.clip_most_recent(eval_type='GRM').
        """
        if self.db.evalid is None:
            raise NoEVALIDError(
                operation=f"{self.component_type} estimation",
                suggestion=(
                    "GRM estimates require an EVALID filter to avoid counting trees "
                    "multiple times across evaluations. Call "
                    "db.clip_by_evalid(evalid) or db.clip_most_recent(eval_type='GRM') "
                    "before calling estimation functions."
                ),
            )

    @property
    def component_type(self) -> Literal["growth", "mortality", "removals"]:
        """Return the GRM component type: 'growth', 'mortality', or 'removals'."""
        raise NotImplementedError("Subclasses must implement component_type property")

    @property
    def metric_prefix(self) -> str:
        """Return the metric column prefix (e.g., 'GROWTH', 'MORT', 'REMV')."""
        prefixes = {"growth": "GROWTH", "mortality": "MORT", "removals": "REMV"}
        return prefixes.get(self.component_type, "VALUE")

    @property
    def value_column(self) -> str:
        """Return the value column name for this component."""
        return f"{self.metric_prefix}_VALUE"

    @property
    def adjusted_column(self) -> str:
        """Return the adjusted value column name."""
        return f"{self.metric_prefix}_ADJ"

    def get_required_tables(self) -> list[str]:
        """GRM estimators require GRM tables."""
        from .grm import get_grm_required_tables

        return get_grm_required_tables(self.component_type)

    def get_cond_columns(self) -> list[str]:
        """Standard condition columns for GRM estimation.

        Uses centralized column resolution from columns.py to reduce duplication.
        GRM estimation needs additional columns for filtering and grouping.
        """
        base_cols = _get_cond_columns(
            land_type=self.config.get("land_type", "forest"),
            grp_by=self.config.get("grp_by"),
            include_prop_basis=False,
        )

        # GRM estimation needs these columns for aggregate_cond_to_plot()
        # and filtering by forest type, ownership, etc.
        grm_cols = ["OWNGRPCD", "FORTYPCD", "SITECLCD", "RESERVCD", "ALSTKCD"]
        for col in grm_cols:
            if col not in base_cols:
                base_cols.append(col)

        return base_cols

    def _resolve_grm_columns(self):
        """Resolve GRM column names based on config."""
        from .grm import resolve_grm_columns

        if self._grm_columns is None:
            tree_type = self.config.get("tree_type", "gs")
            land_type = self.config.get("land_type", "forest")
            self._grm_columns = resolve_grm_columns(
                component_type=self.component_type,
                tree_type=tree_type,
                land_type=land_type,
            )
        return self._grm_columns

    def _load_simple_grm_data(self) -> pl.LazyFrame | None:
        """
        Load GRM data using the simple pattern (for mortality/removals).

        This pattern:
        1. Loads GRM component table
        2. Joins with GRM midpt table
        3. Applies EVALID filtering
        4. Joins with aggregated COND data
        """
        from .grm import (
            aggregate_cond_to_plot,
            filter_by_evalid,
            load_grm_component,
            load_grm_midpt,
        )

        measure = self.config.get("measure", "volume")

        # Resolve GRM column names
        grm_cols = self._resolve_grm_columns()

        # Load GRM component table
        grm_component = load_grm_component(
            self.db,
            grm_cols,
            include_dia_end=(self.component_type != "removals"),
        )

        # Load GRM midpt table
        grm_midpt = load_grm_midpt(self.db, measure=measure)

        # Join component with midpt
        data = grm_component.join(grm_midpt, on="TRE_CN", how="inner")

        # Check if AGENTCD is requested for grouping - need to join with TREE table
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            if "AGENTCD" in grp_by:
                # Load TREE table with AGENTCD
                if "TREE" not in self.db.tables:
                    self.db.load_table("TREE", columns=["CN", "AGENTCD"])
                else:
                    # Check if AGENTCD is in the cached TREE table
                    tree = self.db.tables["TREE"]
                    tree_cols = (
                        tree.collect_schema().names()
                        if isinstance(tree, pl.LazyFrame)
                        else tree.columns
                    )
                    if "AGENTCD" not in tree_cols:
                        # Reload with AGENTCD
                        del self.db.tables["TREE"]
                        self.db.load_table("TREE", columns=["CN", "AGENTCD"])

                tree = self.db.tables["TREE"]
                if not isinstance(tree, pl.LazyFrame):
                    tree = tree.lazy()

                # Join on TRE_CN = CN to get AGENTCD
                data = data.join(
                    tree.select(["CN", "AGENTCD"]),
                    left_on="TRE_CN",
                    right_on="CN",
                    how="left",
                )

        # Apply EVALID filtering
        data = filter_by_evalid(data, self.db)

        # Load and aggregate COND to plot level
        # Required columns for aggregate_cond_to_plot()
        cond_cols = self.get_cond_columns()

        # Check if cached COND has all required columns
        if "COND" in self.db.tables:
            cached = self.db.tables["COND"]
            cached_cols = set(
                cached.collect_schema().names()
                if isinstance(cached, pl.LazyFrame)
                else cached.columns
            )
            required_cols = set(cond_cols)
            if not required_cols.issubset(cached_cols):
                # Reload with all required columns
                del self.db.tables["COND"]

        if "COND" not in self.db.tables:
            self.db.load_table("COND", columns=cond_cols)

        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        cond_agg = aggregate_cond_to_plot(cond)
        data = data.join(cond_agg, on="PLT_CN", how="left")

        return data

    def _apply_grm_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply common GRM filters.

        This method applies all GRM-specific filters directly on the LazyFrame
        without materializing the data, enabling efficient memory usage.

        Applies:
        1. Area domain filter
        2. Tree domain filter
        3. Positive TPA_UNADJ filter
        4. Component-specific filters (via get_component_filter)
        5. Tree type filter (growing stock >= 5 inches)
        """
        # Get column names once for conditional filtering
        columns = data.collect_schema().names()

        # Apply area domain filter (works with LazyFrames)
        if self.config.get("area_domain"):
            from ..filtering.filters import apply_area_filters

            data = apply_area_filters(data, area_domain=self.config["area_domain"])

        # Apply tree domain filter (works with LazyFrames)
        if self.config.get("tree_domain"):
            from ..filtering.parser import DomainExpressionParser

            data = DomainExpressionParser.apply_to_dataframe(
                data, self.config["tree_domain"], "tree"
            )

        # Filter to records with positive TPA
        data = data.filter(
            pl.col("TPA_UNADJ").is_not_null() & (pl.col("TPA_UNADJ") > 0)
        )

        # Apply component-specific filter
        component_filter = self.get_component_filter()
        if component_filter is not None:
            data = data.filter(component_filter)

        # Apply tree type filter (growing stock >= 5 inches)
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs" and "DIA_MIDPT" in columns:
            data = data.filter(pl.col("DIA_MIDPT") >= 5.0)

        return data

    def get_component_filter(self) -> pl.Expr | None:
        """
        Return component-specific filter expression.

        Override in subclasses to filter to specific GRM components.
        Returns None for no additional filtering.
        """
        return None

    def _aggregate_grm_results(
        self,
        data: pl.LazyFrame,
        value_col: str,
        adjusted_col: str,
    ) -> AggregationResult:
        """
        Aggregate GRM results with two-stage aggregation.

        Common aggregation pattern for all GRM estimators:
        1. Get stratification data
        2. Apply GRM adjustment factors
        3. Setup grouping
        4. Preserve plot-tree data for variance
        5. Apply two-stage aggregation

        Returns
        -------
        AggregationResult
            Bundle containing results, plot_tree_data, and group_cols for
            explicit variance calculation.
        """
        from .grm import apply_grm_adjustment

        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply GRM-specific adjustment factors
        data_with_strat = apply_grm_adjustment(data_with_strat)

        # Apply adjustment to values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col(value_col) * pl.col("ADJ_FACTOR")).alias(adjusted_col)]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")

        # Add size class grouping if requested
        if self.config.get("by_size_class", False):
            schema = data_with_strat.collect_schema().names()
            if "DIA_MIDPT" in schema:
                size_class_type = self.config.get("size_class_type", "standard")
                size_class_expr = create_size_class_expr(
                    dia_col="DIA_MIDPT",
                    size_class_type=size_class_type,
                )
                data_with_strat = data_with_strat.with_columns(size_class_expr)
                if "SIZE_CLASS" not in group_cols:
                    group_cols.append("SIZE_CLASS")

        # Preserve plot-tree level data for variance calculation
        plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=[adjusted_col],
            group_cols=group_cols,
        )

        # Build metric mappings for two-stage aggregation
        condition_col = f"CONDITION_{self.metric_prefix}"
        metric_mappings = {adjusted_col: condition_col}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,
        )

        # Return AggregationResult with explicit data passing
        return AggregationResult(
            results=results,
            plot_tree_data=plot_tree_data,
            group_cols=group_cols,
        )

    def _calculate_grm_variance(
        self,
        results: pl.DataFrame,
        adjusted_col: str,
        acre_se_col: str,
        total_se_col: str,
        plot_tree_data: pl.DataFrame | None = None,
        group_cols: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Calculate variance for GRM estimates using domain total variance formula.

        Uses the stratified domain total variance formula from Bechtold & Patterson (2005):
        V(Ŷ) = Σ_h w_h² × s²_yh × n_h

        This matches EVALIDator's variance calculation for GRM estimates.

        Parameters
        ----------
        results : pl.DataFrame
            Results dataframe to add variance columns to.
        adjusted_col : str
            Name of the adjusted metric column in plot_tree_data.
        acre_se_col : str
            Name for the per-acre standard error column.
        total_se_col : str
            Name for the total standard error column.
        plot_tree_data : pl.DataFrame, optional
            Plot-tree level data for variance calculation.
        group_cols : list[str], optional
            Grouping columns used in aggregation.

        Returns
        -------
        pl.DataFrame
            Results with variance columns added.

        Raises
        ------
        ValueError
            If plot_tree_data is not available for variance calculation.
        """
        from .variance import calculate_ratio_of_means_variance

        if plot_tree_data is None:
            raise ValueError(
                f"Plot-tree data is required for {self.__class__.__name__} variance "
                "calculation. Cannot compute statistically valid standard errors "
                "without tree-level data. Ensure data preservation is working "
                "correctly in the estimation pipeline."
            )

        if group_cols is None:
            group_cols = []

        # Aggregate to plot-condition level
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")
        if "CONDPROP_UNADJ" in plot_tree_data.columns:
            plot_group_cols.append("CONDPROP_UNADJ")

        if group_cols:
            for col in group_cols:
                if col in plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_data = plot_tree_data.group_by(plot_group_cols).agg(
            [pl.sum(adjusted_col).alias("y_ic")]
        )

        # Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if group_cols:
            plot_level_cols.extend(
                [c for c in group_cols if c in plot_cond_data.columns]
            )

        # Include CONDPROP_UNADJ for area calculation
        condprop_col = (
            "CONDPROP_UNADJ" if "CONDPROP_UNADJ" in plot_cond_data.columns else None
        )
        agg_cols = [pl.sum("y_ic").alias("y_i")]
        if condprop_col:
            agg_cols.append(pl.sum(condprop_col).cast(pl.Float64).alias("x_i"))
        else:
            agg_cols.append(pl.lit(1.0).alias("x_i"))

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(agg_cols)

        # Get ALL plots in the evaluation for proper variance calculation
        strat_data = self._get_stratification_data()
        strat_schema = strat_data.collect_schema().names()
        # Select B&P variance columns when available
        bp_cols = ["ESTN_UNIT_CN", "STRATUM_WGT", "AREA_USED", "P2POINTCNT"]
        select_cols = ["PLT_CN", "STRATUM_CN", "EXPNS"] + [
            c for c in bp_cols if c in strat_schema
        ]
        all_plots = strat_data.select(select_cols).unique().collect()

        # Calculate variance using ratio-of-means for per-acre SE
        if group_cols:
            variance_results = []

            for group_vals in results.iter_rows():
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

                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [
                        pl.col("y_i").fill_null(0.0),
                        pl.col("x_i").fill_null(0.0),
                    ]
                )

                if len(all_plots_group) > 0:
                    ratio_stats = calculate_ratio_of_means_variance(
                        all_plots_group, "y_i", "x_i"
                    )

                    variance_results.append(
                        {
                            **group_dict,
                            acre_se_col: ratio_stats["se_ratio"],
                            total_se_col: ratio_stats["se_total"],
                        }
                    )
                else:
                    variance_results.append(
                        {
                            **group_dict,
                            acre_se_col: 0.0,
                            total_se_col: 0.0,
                        }
                    )

            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=group_cols, how="left")
        else:
            # No grouping, calculate overall variance with ALL plots
            all_plots_with_values = all_plots.join(
                plot_data.select(["PLT_CN", "y_i", "x_i"]),
                on="PLT_CN",
                how="left",
            ).with_columns(
                [
                    pl.col("y_i").fill_null(0.0),
                    pl.col("x_i").fill_null(0.0),
                ]
            )

            ratio_stats = calculate_ratio_of_means_variance(
                all_plots_with_values, "y_i", "x_i"
            )

            results = results.with_columns(
                [
                    pl.lit(ratio_stats["se_ratio"]).alias(acre_se_col),
                    pl.lit(ratio_stats["se_total"]).alias(total_se_col),
                ]
            )

        return results

    def _format_grm_output(
        self,
        results: pl.DataFrame,
        estimation_type: str,
        include_cv: bool = False,
    ) -> pl.DataFrame:
        """
        Format GRM estimation output with standard columns.
        """
        from .utils import format_output_columns

        measure = self.config.get("measure", "volume")
        land_type = self.config.get("land_type", "forest")
        tree_type = self.config.get("tree_type", "gs")

        # Extract year using shared helper
        year = self._extract_evaluation_year()

        results = results.with_columns(
            [
                pl.lit(year).alias("YEAR"),
                pl.lit(measure.upper()).alias("MEASURE"),
                pl.lit(land_type.upper()).alias("LAND_TYPE"),
                pl.lit(tree_type.upper()).alias("TREE_TYPE"),
            ]
        )

        results = format_output_columns(
            results,
            estimation_type=estimation_type,
            include_se=True,
            include_cv=include_cv,
        )

        return results
