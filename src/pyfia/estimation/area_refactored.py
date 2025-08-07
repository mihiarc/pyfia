"""
Refactored area estimation using composition-based architecture.

This module implements forest area estimation following FIA procedures,
using a composition-based architecture for better maintainability
and testability while preserving backward compatibility.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Union
from dataclasses import dataclass

import polars as pl

from ..constants.constants import (
    LandStatus,
    PlotBasis,
    ReserveStatus,
    SiteClass,
)
from ..core import FIA
from .base import BaseEstimator, EstimatorConfig
from .utils import ratio_var

# Import the new components
from .statistics import VarianceCalculator, PercentageCalculator
from .statistics.expressions import PolarsExpressionBuilder
from .domain import DomainIndicatorCalculator, LandTypeClassifier
from .aggregation import PopulationEstimationWorkflow, AreaAggregationBuilder, AggregationConfig
from .stratification import AreaStratificationHandler


@dataclass
class AreaEstimatorComponents:
    """Dependency injection container for area estimation components."""
    domain_calculator: DomainIndicatorCalculator
    variance_calculator: VarianceCalculator
    percentage_calculator: PercentageCalculator
    expression_builder: PolarsExpressionBuilder
    land_type_classifier: LandTypeClassifier
    population_workflow: PopulationEstimationWorkflow
    stratification_handler: AreaStratificationHandler


class AreaEstimator(BaseEstimator):
    """
    Area estimator implementing FIA forest area calculation procedures.

    This class uses composition-based architecture with specialized components
    for domain calculation, variance estimation, and percentage calculation.
    The refactored design maintains API compatibility while improving
    maintainability and testability.
    """

    def __init__(
        self, 
        db: Union[str, FIA], 
        config: EstimatorConfig,
        components: Optional[AreaEstimatorComponents] = None
    ):
        """
        Initialize the area estimator.

        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        config : EstimatorConfig
            Configuration with estimation parameters
        components : Optional[AreaEstimatorComponents], default None
            Pre-configured components for dependency injection
        """
        super().__init__(db, config)

        # Extract area-specific parameters
        self.by_land_type = config.extra_params.get("by_land_type", False)
        self.land_type = config.land_type

        # Store whether we need tree filtering
        self._needs_tree_filtering = config.tree_domain is not None

        # Initialize components through composition
        self.components = components or self._create_default_components()

    def _create_default_components(self) -> AreaEstimatorComponents:
        """
        Factory method for creating default components.
        
        Returns
        -------
        AreaEstimatorComponents
            Default components configured for this estimator instance
        """
        # Create domain calculator with appropriate configuration
        domain_calculator = DomainIndicatorCalculator(
            land_type=self.land_type,
            by_land_type=self.by_land_type,
            tree_domain=self.config.tree_domain,
            area_domain=self.config.area_domain,
            data_cache=self._data_cache
        )
        
        # Create aggregation configuration
        agg_config = AggregationConfig(
            group_cols=self._group_cols,
            by_land_type=self.by_land_type,
            include_totals=self.config.totals,
            include_variance=self.config.variance
        )
        
        # Create and return component container
        return AreaEstimatorComponents(
            domain_calculator=domain_calculator,
            variance_calculator=VarianceCalculator(),
            percentage_calculator=PercentageCalculator(),
            expression_builder=PolarsExpressionBuilder(),
            land_type_classifier=LandTypeClassifier(),
            population_workflow=PopulationEstimationWorkflow(agg_config),
            stratification_handler=AreaStratificationHandler(self.db)
        )

    def get_required_tables(self) -> List[str]:
        """Return required database tables for area estimation."""
        tables = ["PLOT", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
        if self._needs_tree_filtering:
            tables.append("TREE")
        return tables

    def get_response_columns(self) -> Dict[str, str]:
        """Define area response columns."""
        return {
            "fa": "AREA_NUMERATOR",
            "fad": "AREA_DENOMINATOR",
        }

    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate area values and domain indicators using components."""
        # Add land type categories if requested
        if self.by_land_type:
            data = self.components.land_type_classifier.classify_land_types(
                data, self.land_type, self.by_land_type
            )
            if "LAND_TYPE" not in self._group_cols:
                self._group_cols.append("LAND_TYPE")

        # Calculate domain indicators using component
        data = self.components.domain_calculator.calculate_all_indicators(data)

        # Calculate area values (proportion * indicator)
        data = data.with_columns([
            (pl.col("CONDPROP_UNADJ") * pl.col("aDI")).alias("fa"),
            (pl.col("CONDPROP_UNADJ") * pl.col("pDI")).alias("fad"),
        ])

        return data

    def get_output_columns(self) -> List[str]:
        """Define the output column structure for area estimates."""
        output_cols = ["AREA_PERC"]
        
        if self.config.totals:
            output_cols.append("AREA")

        if self.config.variance:
            output_cols.append("AREA_PERC_VAR")
            if self.config.totals:
                output_cols.append("AREA_VAR")
        else:
            output_cols.append("AREA_PERC_SE")
            if self.config.totals:
                output_cols.append("AREA_SE")

        output_cols.append("N_PLOTS")
        return output_cols

    def _get_filtered_data(self) -> tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """Override base class to use area_estimation_mode filtering."""
        from ..filters.common import apply_area_filters_common, apply_tree_filters_common
        
        # Always get condition data
        cond_df = self.db.get_conditions()

        # Apply area filters with area_estimation_mode=True
        cond_df = apply_area_filters_common(
            cond_df,
            self.config.land_type,
            self.config.area_domain,
            area_estimation_mode=True
        )

        # Get tree data if needed
        tree_df = None
        if "TREE" in self.get_required_tables():
            tree_df = self.db.get_trees()
            tree_df = apply_tree_filters_common(
                tree_df,
                tree_type="all",
                tree_domain=self.config.tree_domain
            )

        return tree_df, cond_df

    def _prepare_estimation_data(self, tree_df: Optional[pl.DataFrame],
                                cond_df: pl.DataFrame) -> pl.DataFrame:
        """Override base class to handle area-specific data preparation."""
        # Store tree data for domain filtering
        if tree_df is not None:
            self._data_cache["TREE"] = tree_df

        # Set up grouping columns
        self._group_cols = []
        if self.config.grp_by:
            if isinstance(self.config.grp_by, str):
                self._group_cols = [self.config.grp_by]
            else:
                self._group_cols = list(self.config.grp_by)

        return cond_df

    def _calculate_plot_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """Calculate plot-level area estimates."""
        # Determine grouping columns
        plot_groups = ["PLT_CN"]
        if self._group_cols:
            plot_groups.extend(self._group_cols)

        # Aggregate area values to plot level
        plot_estimates = data.group_by(plot_groups).agg([
            pl.sum("fa").alias("PLOT_AREA_NUMERATOR"),
            pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS"),
        ])

        # Calculate denominator separately (not grouped by land type)
        plot_denom = data.group_by("PLT_CN").agg([
            pl.sum("fad").alias("PLOT_AREA_DENOMINATOR"),
        ])

        # Join numerator and denominator
        plot_estimates = plot_estimates.join(
            plot_denom,
            on="PLT_CN",
            how="left"
        )

        # Fill nulls with zeros
        plot_estimates = plot_estimates.with_columns([
            pl.col("PLOT_AREA_NUMERATOR").fill_null(0),
            pl.col("PLOT_AREA_DENOMINATOR").fill_null(0),
        ])

        return plot_estimates

    def _apply_stratification(self, plot_data: pl.DataFrame) -> pl.DataFrame:
        """Apply stratification using component."""
        return self.components.stratification_handler.apply_stratification(plot_data)

    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """Calculate population estimates using component."""
        return self.components.population_workflow.calculate_population_estimates(expanded_data)

    def format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """Format output to match rFIA area() function structure."""
        output_cols = []
        
        # Add grouping columns first
        if self._group_cols:
            output_cols.extend(self._group_cols)
        
        # Add primary estimates
        output_cols.append("AREA_PERC")
        
        if self.config.totals:
            output_cols.append("AREA")
        
        # Add uncertainty measures
        if self.config.variance:
            output_cols.append("AREA_PERC_VAR")
            if self.config.totals:
                output_cols.append("AREA_VAR")
        else:
            output_cols.append("AREA_PERC_SE")
            if self.config.totals:
                output_cols.append("AREA_SE")
        
        # Add metadata
        output_cols.append("N_PLOTS")
        
        # Select only columns that exist
        available_cols = [col for col in output_cols if col in estimates.columns]
        
        return estimates.select(available_cols)

    # === Backward Compatibility Helper Methods ===

    def _add_land_type_categories(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Add land type categories for grouping (delegates to component)."""
        return self.components.land_type_classifier.classify_land_types(
            cond_df, self.land_type, self.by_land_type
        )

    def _apply_tree_domain_to_conditions(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Apply tree domain filtering (delegates to component)."""
        return self.components.domain_calculator._apply_tree_domain_filtering(cond_df)

    def _calculate_domain_indicators(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Calculate domain indicators (delegates to component)."""
        return self.components.domain_calculator.calculate_all_indicators(cond_df)

    def _calculate_land_type_percentages(self, pop_est: pl.DataFrame) -> pl.DataFrame:
        """Calculate land type percentages (delegates to component)."""
        return self.components.percentage_calculator.calculate_land_type_percentages(pop_est)

    def _calculate_standard_percentages(self, pop_est: pl.DataFrame) -> pl.DataFrame:
        """Calculate standard percentages (delegates to component)."""
        return self.components.percentage_calculator.calculate_standard_percentages(pop_est)

    def _safe_std(self, col_name: str) -> pl.Expr:
        """Calculate safe standard deviation (delegates to component)."""
        return self.components.expression_builder.safe_std(col_name)

    def _safe_correlation(self, col1: str, col2: str) -> pl.Expr:
        """Calculate safe correlation (delegates to component)."""
        return self.components.expression_builder.safe_correlation(col1, col2)

    def _variance_component(self, var_name: str) -> pl.Expr:
        """Calculate variance component (delegates to component)."""
        return self.components.variance_calculator.variance_component(var_name)

    def _covariance_component(self) -> pl.Expr:
        """Calculate covariance component (delegates to component)."""
        return self.components.variance_calculator.covariance_component()

    def _safe_sqrt(self, col_name: str) -> pl.Expr:
        """Calculate safe square root (delegates to component)."""
        return self.components.expression_builder.safe_sqrt(col_name)


def area(
    db,
    grp_by: Optional[List[str]] = None,
    by_land_type: bool = False,
    land_type: str = "forest",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    method: str = "TI",
    lambda_: float = 0.5,
    totals: bool = False,
    variance: bool = False,
    most_recent: bool = False,
) -> pl.DataFrame:
    """
    Estimate forest area and land proportions from FIA data.

    This function uses a refactored composition-based architecture while
    maintaining full backward compatibility.

    Parameters
    ----------
    db : FIA
        FIA database object with data loaded
    grp_by : list of str, optional
        Columns to group estimates by
    by_land_type : bool, default False
        Group by land type (timber, non-timber forest, non-forest, water)
    land_type : str, default "forest"
        Land type filter: "forest", "timber", or "all"
    tree_domain : str, optional
        SQL-like condition to filter trees
    area_domain : str, optional
        SQL-like condition to filter area
    method : str, default "TI"
        Estimation method (currently only "TI" supported)
    lambda_ : float, default 0.5
        Temporal weighting parameter (not used for TI)
    totals : bool, default False
        Include total area in addition to percentages
    variance : bool, default False
        Return variance instead of standard error
    most_recent : bool, default False
        Use only most recent evaluation

    Returns
    -------
    pl.DataFrame
        DataFrame with area estimates including:
        - AREA_PERC: Percentage of total area meeting criteria
        - AREA: Total acres (if totals=True)
        - Standard errors or variances
        - N_PLOTS: Number of plots
    """
    # Create configuration
    config = EstimatorConfig(
        grp_by=grp_by,
        land_type=land_type,
        tree_domain=tree_domain,
        area_domain=area_domain,
        method=method,
        lambda_=lambda_,
        totals=totals,
        variance=variance,
        most_recent=most_recent,
        extra_params={"by_land_type": by_land_type}
    )
    
    # Create estimator and run estimation
    estimator = AreaEstimator(db, config)
    return estimator.estimate()