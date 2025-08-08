"""
Tree count estimation aligned with the BaseEstimator architecture.

This module provides a TreeCountEstimator that follows the same
template-method workflow used by `volume`, `biomass`, `tpa`, etc.
It computes population-level tree counts using FIA adjustment factors
and stratification expansion.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ..core import FIA
from .base import BaseEstimator, EstimatorConfig
from ..filters.classification import assign_tree_basis


class TreeCountEstimator(BaseEstimator):
    """
    Estimator that computes population-level tree counts.

    Calculation outline
    - Filter trees/conditions using common filters
    - Join plot-level design info and stratum adjustment/expansion factors
    - Compute tree-level counts per acre: TPA_UNADJ adjusted by basis factor
    - Aggregate to plot, then expand by EXPNS to population totals
    - Sum to population and add variance/SE via base hook
    """

    def get_required_tables(self) -> List[str]:
        tables = [
            "PLOT",
            "TREE",
            "COND",
            "POP_STRATUM",
            "POP_PLOT_STRATUM_ASSGN",
        ]
        # Species names are optional; grouping uses SPCD only
        return tables

    def get_response_columns(self) -> Dict[str, str]:
        # Internal calculation column -> output column
        return {"TREE_COUNT_VALUE": "TREE_COUNT"}

    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        # Ensure plot macro breakpoint is present and keep plots for basis assignment
        plots = self.db.get_plots(columns=["CN", "MACRO_BREAKPOINT_DIA"])  # collects
        if "MACRO_BREAKPOINT_DIA" not in data.columns:
            data = data.join(
                plots.select(["CN", "MACRO_BREAKPOINT_DIA"]).rename({"CN": "PLT_CN"}),
                on="PLT_CN",
                how="left",
            )

        # Attach stratum adjustment factors to each plot via PPSA -> POP_STRATUM
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"].collect()
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        strat = ppsa.join(
            pop_stratum.select(["CN", "EXPNS", "ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]).rename(
                {"CN": "STRATUM_CN"}
            ),
            on="STRATUM_CN",
            how="inner",
        )
        data = data.join(
            strat.select(["PLT_CN", "EXPNS", "ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]).unique(),
            on="PLT_CN",
            how="left",
        )

        # Assign tree basis (MICR/SUBP/MACR) for adjustment using plot macro breakpoint
        data = assign_tree_basis(data, plot_df=plots.rename({"CN": "PLT_CN"}), include_macro=True)

        # Basis-specific adjustment factor
        adj_expr = (
            pl.when(pl.col("TREE_BASIS") == "MICR").then(pl.col("ADJ_FACTOR_MICR"))
            .when(pl.col("TREE_BASIS") == "MACR").then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(pl.col("ADJ_FACTOR_SUBP"))
            .cast(pl.Float64)
            .alias("_ADJ_FACTOR")
        )
        data = data.with_columns(adj_expr)

        # Tree-level count per acre adjusted
        return data.with_columns(
            (
                pl.col("TPA_UNADJ").cast(pl.Float64)
                * pl.col("_ADJ_FACTOR").cast(pl.Float64)
            ).alias("TREE_COUNT_VALUE")
        )

    # Override stratification to expand plot-level sums by EXPNS
    def _apply_stratification(self, plot_data: pl.DataFrame) -> pl.DataFrame:
        # PPSA filtered by EVALID (if any)
        ppsa = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(self.db.evalid) if self.db.evalid else pl.lit(True))
            .collect()
        )
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        strat = ppsa.join(
            pop_stratum.select(["CN", "EXPNS"]).rename({"CN": "STRATUM_CN"}),
            on="STRATUM_CN",
            how="inner",
        )

        # Join EXPNS to plot rows
        plot_with_expns = plot_data.join(
            strat.select(["PLT_CN", "EXPNS"]).unique(),
            on="PLT_CN",
            how="inner",
        )

        # Expand plot-level values by EXPNS to create totals
        response_cols = self.get_response_columns()
        total_exprs: List[pl.Expr] = []
        for _, output_name in response_cols.items():
            plot_col = f"PLOT_{output_name}"
            if plot_col in plot_with_expns.columns:
                total_exprs.append(
                    (pl.col(plot_col) * pl.col("EXPNS").cast(pl.Float64)).alias(f"TOTAL_{output_name}")
                )
        if total_exprs:
            plot_with_expns = plot_with_expns.with_columns(total_exprs)

        return plot_with_expns

    # Override population step to sum totals (no per-acre ratio)
    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        response_cols = self.get_response_columns()
        agg_exprs: List[pl.Expr] = []
        for _, output_name in response_cols.items():
            total_col = f"TOTAL_{output_name}"
            if total_col in expanded_data.columns:
                # Final population tree count under name TREE_COUNT
                agg_exprs.append(pl.sum(total_col).alias(output_name))

        agg_exprs.append(pl.len().alias("nPlots"))

        if self._group_cols:
            pop_estimates = expanded_data.group_by(self._group_cols).agg(agg_exprs)
        else:
            pop_estimates = expanded_data.select(agg_exprs)

        # Variance/SE for TREE_COUNT
        for _, output_name in response_cols.items():
            if output_name in pop_estimates.columns:
                pop_estimates = self.calculate_variance(pop_estimates, output_name)

        # Add metadata columns
        pop_estimates = pop_estimates.with_columns(
            [
                pl.lit(self._get_year()).alias("YEAR"),
                pl.col("nPlots").alias("N"),
                pl.col("nPlots").alias("nPlots_TREE"),
                pl.col("nPlots").alias("nPlots_AREA"),
            ]
        )

        return pop_estimates

    def get_output_columns(self) -> List[str]:
        cols = ["YEAR", "nPlots_TREE", "nPlots_AREA", "N", "TREE_COUNT"]
        # Add SE/VAR depending on config
        if self.config.variance:
            cols.append("TREE_COUNT_VAR")
        else:
            cols.append("TREE_COUNT_SE")
        return cols


def tree_count(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    method: str = "TI",
    lambda_: float = 0.5,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    by_plot: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate population tree counts following FIA methodology.

    Parameters mirror other estimators to maintain a consistent API.
    """
    config = EstimatorConfig(
        grp_by=grp_by,
        by_species=by_species,
        by_size_class=by_size_class,
        land_type=land_type,
        tree_type=tree_type,
        tree_domain=tree_domain,
        area_domain=area_domain,
        method=method,
        lambda_=lambda_,
        totals=totals,
        variance=variance,
        by_plot=by_plot,
        most_recent=most_recent,
    )

    with TreeCountEstimator(db, config) as estimator:
        return estimator.estimate()


def tree_count_simple(
    db: Union[str, FIA],
    species_code: Optional[int] = None,
    state_code: Optional[int] = None,
    tree_status: int = 1,
) -> pl.DataFrame:
    """
    Simplified helper for quick tree counts by species/state.
    """
    tree_type = "live" if tree_status == 1 else ("dead" if tree_status == 2 else "all")
    tree_domain = f"SPCD == {species_code}" if species_code is not None else None
    area_domain = f"STATECD == {state_code}" if state_code is not None else None

    return tree_count(
        db=db,
        grp_by=None,
        by_species=bool(species_code),
        by_size_class=False,
        land_type="forest",
        tree_type=tree_type,
        tree_domain=tree_domain,
        area_domain=area_domain,
        totals=True,
        variance=False,
        most_recent=False,
    )
