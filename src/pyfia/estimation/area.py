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
from .statistics.rfia_variance import RFIAVarianceCalculator
from .domain import DomainIndicatorCalculator, LandTypeClassifier
from .aggregation import PopulationEstimationWorkflow, AreaAggregationBuilder, AggregationConfig
from .stratification import AreaStratificationHandler


@dataclass
class AreaEstimatorComponents:
    """Dependency injection container for area estimation components."""
    domain_calculator: DomainIndicatorCalculator
    variance_calculator: VarianceCalculator
    rfia_variance_calculator: RFIAVarianceCalculator
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

        # Initialize group columns early
        self._group_cols = []
        if self.config.grp_by:
            if isinstance(self.config.grp_by, str):
                self._group_cols = [self.config.grp_by]
            else:
                self._group_cols = list(self.config.grp_by)
        
        # Add LAND_TYPE to group cols if using by_land_type
        if self.by_land_type and "LAND_TYPE" not in self._group_cols:
            self._group_cols.append("LAND_TYPE")

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
            rfia_variance_calculator=RFIAVarianceCalculator(self.db),
            percentage_calculator=PercentageCalculator(),
            expression_builder=PolarsExpressionBuilder(),
            land_type_classifier=LandTypeClassifier(),
            population_workflow=PopulationEstimationWorkflow(agg_config),
            stratification_handler=AreaStratificationHandler(self.db)
        )

    def get_required_tables(self) -> List[str]:
        """Return required database tables for area estimation."""
        tables = ["PLOT", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN", "POP_ESTN_UNIT"]
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
            # For area estimation, don't apply tree domain filtering here
            # The domain calculator will handle tree domain filtering
            tree_df = apply_tree_filters_common(
                tree_df,
                tree_type="all",
                tree_domain=None  # Don't filter by tree domain here
            )

        return tree_df, cond_df

    def _prepare_estimation_data(self, tree_df: Optional[pl.DataFrame],
                                cond_df: pl.DataFrame) -> pl.DataFrame:
        """Override base class to handle area-specific data preparation."""
        # Store tree data for domain filtering
        if tree_df is not None:
            self._data_cache["TREE"] = tree_df

        # Grouping columns are already set up in __init__
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
        """Apply stratification using component with legacy fallback.

        If POP_ESTN_UNIT is unavailable (as in some unit tests/mocks), fall back to
        a minimal stratification that uses only POP_STRATUM and PPSA and performs
        direct expansion without full design factors. This is sufficient for
        non-variance tests and maintains backwards compatibility.
        """
        try:
            # Use full stratification if POP_ESTN_UNIT is available
            if "POP_ESTN_UNIT" in self.db.tables:
                return self.components.stratification_handler.apply_stratification(plot_data)
        except Exception:
            # If any issue, fall through to legacy
            pass

        # Legacy minimal stratification path
        # Load assignments filtered to active evaluation
        if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables or "POP_STRATUM" not in self.db.tables:
            raise ValueError("Missing required population tables for stratification fallback")

        ppsa_df = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(self.db.evalid) if self.db.evalid else pl.lit(True))
            .collect()
        )
        pop_stratum_df = self.db.tables["POP_STRATUM"].collect()

        strat_df = _prepare_area_stratification(ppsa_df, pop_stratum_df)

        # Join and compute adjusted and expanded values
        plot_with_strat = plot_data.join(
            strat_df.select([
                "PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR",
            ]),
            on="PLT_CN",
            how="inner",
        ).with_columns([
            pl.when(pl.col("PROP_BASIS") == PlotBasis.MACROPLOT)
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(pl.col("ADJ_FACTOR_SUBP")).alias("ADJ_FACTOR")
        ])

        expanded = plot_with_strat.with_columns([
            # Adjusted values for potential variance paths
            (pl.col("PLOT_AREA_NUMERATOR") * pl.col("ADJ_FACTOR")).alias("fa_adjusted"),
            (pl.col("PLOT_AREA_DENOMINATOR") * pl.col("ADJ_FACTOR")).alias("fad_adjusted"),
            # Direct expansion totals
            (pl.col("PLOT_AREA_NUMERATOR") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("TOTAL_AREA_NUMERATOR"),
            (pl.col("PLOT_AREA_DENOMINATOR") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("TOTAL_AREA_DENOMINATOR"),
        ])

        return expanded

    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate population estimates.

        If full design factors are available, use rFIA-compatible variance. Otherwise,
        use the legacy direct expansion method sufficient for most tests.
        """
        if "POP_ESTN_UNIT" in self.db.tables:
            return _calculate_population_area_estimates_rfia(
                expanded_data,
                self.db,
                group_cols=self._group_cols,
                by_land_type=self.by_land_type,
            )
        else:
            return _calculate_population_area_estimates(
                expanded_data,
                group_cols=self._group_cols,
                by_land_type=self.by_land_type,
            )

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


# === Compatibility Functions for Tests ===
# These maintain compatibility with existing tests

def _add_land_type_categories(cond_df: pl.DataFrame) -> pl.DataFrame:
    """Add land type categories for grouping (compatibility function)."""
    return cond_df.with_columns(
        pl.when(
            (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
            & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
        )
        .then(pl.lit("Timber"))
        .when(pl.col("COND_STATUS_CD") == LandStatus.FOREST)
        .then(pl.lit("Non-Timber Forest"))
        .when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST)
        .then(pl.lit("Non-Forest"))
        .when(pl.col("COND_STATUS_CD").is_in([LandStatus.WATER, LandStatus.CENSUS_WATER]))
        .then(pl.lit("Water"))
        .otherwise(pl.lit("Other"))
        .alias("LAND_TYPE")
    )


def _apply_tree_domain_to_conditions(
    cond_df: pl.DataFrame, tree_df: pl.DataFrame, tree_domain: str
) -> pl.DataFrame:
    """Apply tree domain filtering at the condition level (compatibility function)."""
    if tree_df is None:
        return cond_df
    
    # Filter trees by domain
    qualifying_trees = tree_df.filter(pl.sql_expr(tree_domain))
    
    # Get unique PLT_CN/CONDID combinations with qualifying trees
    qualifying_conds = (
        qualifying_trees.select(["PLT_CN", "CONDID"])
        .unique()
        .with_columns(pl.lit(1).alias("HAS_QUALIFYING_TREE"))
    )
    
    # Join back to conditions
    cond_df = cond_df.join(
        qualifying_conds, 
        on=["PLT_CN", "CONDID"], 
        how="left"
    ).with_columns(
        pl.col("HAS_QUALIFYING_TREE").fill_null(0)
    )
    
    return cond_df


def _calculate_domain_indicators(
    cond_df: pl.DataFrame, land_type: str = "forest", by_land_type: bool = False
) -> pl.DataFrame:
    """Calculate domain indicators for area estimation (compatibility function)."""
    # Land type domain indicator
    if by_land_type and "LAND_TYPE" in cond_df.columns:
        # For by_land_type, landD is 1 for each specific land type
        cond_df = cond_df.with_columns(pl.lit(1).alias("landD"))
    elif land_type == "forest":
        cond_df = cond_df.with_columns(
            (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            .cast(pl.Int32).alias("landD")
        )
    elif land_type == "timber":
        cond_df = cond_df.with_columns(
            (
                (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
            .cast(pl.Int32)
            .alias("landD")
        )
    else:  # "all"
        cond_df = cond_df.with_columns(pl.lit(1).alias("landD"))
    
    # Area domain indicator (already filtered)
    cond_df = cond_df.with_columns(pl.lit(1).alias("aD"))
    
    # Tree domain indicator
    if "HAS_QUALIFYING_TREE" in cond_df.columns:
        cond_df = cond_df.with_columns(
            pl.col("HAS_QUALIFYING_TREE").alias("tD")
        )
    else:
        cond_df = cond_df.with_columns(pl.lit(1).alias("tD"))
    
    # Comprehensive domain indicator (numerator)
    if by_land_type:
        cond_df = cond_df.with_columns(pl.col("aD").alias("aDI"))
    else:
        cond_df = cond_df.with_columns(
            (pl.col("landD") * pl.col("aD") * pl.col("tD")).alias("aDI")
        )
    
    # Partial domain indicator (denominator)
    if by_land_type:
        # For by_land_type: use only land conditions (excludes water)
        cond_df = cond_df.with_columns(
            pl.when(
                pl.col("COND_STATUS_CD").is_in([LandStatus.FOREST, LandStatus.NONFOREST])
            )
            .then(pl.col("aD"))
            .otherwise(0)
            .alias("pDI")
        )
    else:
        # Regular: denominator matches numerator domain
        cond_df = cond_df.with_columns(
            (pl.col("landD") * pl.col("aD")).alias("pDI")
        )
    
    return cond_df


def _prepare_area_stratification(ppsa_df: pl.DataFrame,
                               pop_stratum_df: pl.DataFrame) -> pl.DataFrame:
    """
    Prepare stratification data with both SUBP and MACR adjustment factors (compatibility function).
    """
    # Join to get both adjustment factors
    strat_df = ppsa_df.join(
        pop_stratum_df.select([
            "CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "P2POINTCNT"
        ]),
        left_on="STRATUM_CN",
        right_on="CN",
        how="inner"
    )
    
    return strat_df


def _calculate_plot_area_estimates(
    cond_df: pl.DataFrame, 
    group_cols: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Calculate plot-level area estimates (compatibility function).
    """
    # Determine grouping columns
    plot_groups = ["PLT_CN"]
    if group_cols:
        plot_groups.extend(group_cols)

    # Aggregate area values to plot level
    plot_estimates = cond_df.group_by(plot_groups).agg([
        pl.sum("fa").alias("PLOT_AREA_NUMERATOR"),
        # Get dominant PROP_BASIS for adjustment factor selection
        pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS"),
    ])

    # Calculate denominator separately (not grouped by land type)
    plot_denom = cond_df.group_by("PLT_CN").agg([
        pl.sum("fad").alias("PLOT_AREA_DENOMINATOR"),
    ])

    # Join numerator and denominator
    plot_estimates = plot_estimates.join(
        plot_denom,
        on="PLT_CN",
        how="left"
    )

    return plot_estimates


def _calculate_stratum_area_estimates(
    plot_estimates: pl.DataFrame, 
    strat_df: pl.DataFrame,
    group_cols: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Calculate stratum-level area estimates (compatibility function).
    """
    # Join with stratification data
    plot_with_strat = plot_estimates.join(
        strat_df.select([
            "PLT_CN", "STRATUM_CN", "EXPNS", 
            "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"
        ]),
        on="PLT_CN",
        how="inner"
    )
    
    # Select appropriate adjustment factor based on PROP_BASIS
    plot_with_strat = plot_with_strat.with_columns(
        pl.when(pl.col("PROP_BASIS") == PlotBasis.MACROPLOT)
        .then(pl.col("ADJ_FACTOR_MACR"))
        .otherwise(pl.col("ADJ_FACTOR_SUBP"))
        .alias("ADJ_FACTOR")
    )
    
    # Apply expansion using direct expansion method
    expanded_data = plot_with_strat.with_columns([
        # Direct expansion totals
        (pl.col("PLOT_AREA_NUMERATOR") * pl.col("ADJ_FACTOR") * pl.col("EXPNS"))
        .alias("TOTAL_AREA_NUMERATOR"),
        
        (pl.col("PLOT_AREA_DENOMINATOR") * pl.col("ADJ_FACTOR") * pl.col("EXPNS"))
        .alias("TOTAL_AREA_DENOMINATOR"),
        
        # Keep adjusted values for variance calculation
        (pl.col("PLOT_AREA_NUMERATOR") * pl.col("ADJ_FACTOR"))
        .alias("fa_adjusted"),
        
        (pl.col("PLOT_AREA_DENOMINATOR") * pl.col("ADJ_FACTOR"))
        .alias("fad_adjusted"),
    ])
    
    return expanded_data


def _calculate_population_area_estimates_rfia(
    expanded_data: pl.DataFrame,
    db: FIA,
    group_cols: Optional[List[str]] = None,
    by_land_type: bool = False
) -> pl.DataFrame:
    """
    Calculate population-level area estimates using rFIA-compatible variance methodology.
    
    This function replaces the legacy variance calculations with rFIA's exact 
    stratified sampling methodology for proper sampling error calculation.
    """
    # Initialize rFIA variance calculator
    rfia_calc = RFIAVarianceCalculator(db)
    
    # Ensure required design factors are present
    required_cols = ["ESTN_UNIT_CN", "STRATUM_CN", "P2POINTCNT", "fa_adjusted", "fad_adjusted", "EXPNS"]
    missing_cols = [col for col in required_cols if col not in expanded_data.columns]
    
    if missing_cols:
        raise ValueError(f"Missing required FIA design factor columns: {missing_cols}")
    
    # Use rFIA variance calculator for complete calculation
    variance_results = rfia_calc.calculate_area_variance(
        expanded_data, 
        grouping_cols=group_cols
    )
    
    # Calculate percentage with proper handling for by_land_type
    if by_land_type and "LAND_TYPE" in variance_results.columns:
        final_results = _calculate_land_type_percentages_rfia(variance_results)
    else:
        final_results = _calculate_standard_percentages_rfia(variance_results)
    
    return final_results


def _calculate_population_area_estimates(
    expanded_data: pl.DataFrame, 
    group_cols: Optional[List[str]] = None,
    by_land_type: bool = False
) -> pl.DataFrame:
    """
    Calculate population-level area estimates (compatibility function).
    
    LEGACY FUNCTION - This uses the old variance methodology that produces
    sampling errors 100-300x too low. Use _calculate_population_area_estimates_rfia
    for correct rFIA-compatible variance calculations.
    """
    # Helper functions for safe calculations
    def _safe_std(col_name: str) -> pl.Expr:
        return (
            pl.when(pl.count(col_name) > 1)
            .then(pl.std(col_name, ddof=1))
            .otherwise(0.0)
        )

    def _safe_correlation(col1: str, col2: str) -> pl.Expr:
        return (
            pl.when(pl.count(col1) > 1)
            .then(
                pl.when((pl.std(col1) == 0) & (pl.std(col2) == 0))
                .then(1.0)
                .otherwise(pl.corr(col1, col2).fill_null(0))
            )
            .otherwise(0.0)
        )

    def _variance_component(var_name: str) -> pl.Expr:
        return (
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("w_h") ** 2) * 
                (pl.col(f"s_{var_name}_h") ** 2) / 
                pl.col("n_h")
            )
            .otherwise(0.0)
            .sum()
        )

    def _covariance_component() -> pl.Expr:
        return (
            pl.when(pl.col("n_h") > 1)
            .then(
                (pl.col("w_h") ** 2) * 
                pl.col("s_fa_fad_h") / 
                pl.col("n_h")
            )
            .otherwise(0.0)
            .sum()
        )

    # Calculate stratum-level statistics for variance
    if group_cols:
        strat_groups = ["STRATUM_CN"] + group_cols
    else:
        strat_groups = ["STRATUM_CN"]
    
    stratum_est = expanded_data.group_by(strat_groups).agg([
        # Sample size
        pl.len().alias("n_h"),
        
        # Direct expansion totals
        pl.sum("TOTAL_AREA_NUMERATOR").alias("fa_expanded_total"),
        pl.sum("TOTAL_AREA_DENOMINATOR").alias("fad_expanded_total"),
        
        # Statistics for variance (using adjusted values)
        pl.mean("fa_adjusted").alias("fa_bar_h"),
        _safe_std("fa_adjusted").alias("s_fa_h"),
        
        pl.mean("fad_adjusted").alias("fad_bar_h"),
        _safe_std("fad_adjusted").alias("s_fad_h"),
        
        # Correlation for ratio variance
        _safe_correlation("fa_adjusted", "fad_adjusted").alias("corr_fa_fad"),
        
        # Stratum weight
        pl.first("EXPNS").alias("w_h"),
    ])
    
    # Calculate covariance from correlation
    stratum_est = stratum_est.with_columns(
        pl.when((pl.col("s_fa_h") == 0) | (pl.col("s_fad_h") == 0))
        .then(0.0)
        .otherwise(pl.col("corr_fa_fad") * pl.col("s_fa_h") * pl.col("s_fad_h"))
        .alias("s_fa_fad_h")
    )
    
    # Aggregate to population level
    agg_exprs = [
        # Direct expansion totals
        pl.col("fa_expanded_total").sum().alias("FA_TOTAL"),
        pl.col("fad_expanded_total").sum().alias("FAD_TOTAL"),
        
        # Variance components
        _variance_component("fa").alias("FA_VAR"),
        _variance_component("fad").alias("FAD_VAR"),
        _covariance_component().alias("COV_FA_FAD"),
        
        # Sample size
        pl.col("n_h").sum().alias("N_PLOTS"),
    ]
    
    if group_cols:
        pop_est = stratum_est.group_by(group_cols).agg(agg_exprs)
    else:
        pop_est = stratum_est.select(agg_exprs)
    
    # Calculate percentage with proper handling for by_land_type
    if by_land_type and "LAND_TYPE" in pop_est.columns:
        pop_est = _calculate_land_type_percentages(pop_est)
    else:
        pop_est = _calculate_standard_percentages(pop_est)
    
    return pop_est


def _calculate_land_type_percentages_rfia(variance_results: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate land type percentages using rFIA-compatible variance results.
    
    This function processes the rFIA variance calculation output and converts
    it to the expected format for land type analysis.
    """
    # The rFIA variance calculator already provides AREA_PERC, AREA_TOTAL, and sampling errors
    # We just need to ensure the column names match expected output format
    return variance_results.rename({
        "AREA_TOTAL": "FA_TOTAL",
        "AREA_TOTAL_DEN": "FAD_TOTAL", 
        "AREA_TOTAL_VAR": "FA_VAR",
        "AREA_PERC_VAR": "AREA_PERC_VAR",
        "AREA_TOTAL_SE_ACRES": "FA_SE",
        "AREA_PERC_SE": "AREA_PERC_SE",
        "total_plots": "N_PLOTS"
    }).select([
        # Keep any grouping columns
        *[col for col in variance_results.columns if col not in ["AREA_TOTAL", "AREA_TOTAL_DEN", "AREA_TOTAL_VAR", "AREA_PERC_VAR", "AREA_TOTAL_SE_ACRES", "AREA_TOTAL_SE_PCT", "AREA_PERC_SE", "total_plots", "AREA_PERC"]],
        # Standard output columns  
        "FA_TOTAL", "FAD_TOTAL", "AREA_PERC", "FA_VAR", "AREA_PERC_VAR", 
        "FA_SE", "AREA_PERC_SE", "N_PLOTS"
    ])


def _calculate_standard_percentages_rfia(variance_results: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate standard percentages using rFIA-compatible variance results.
    
    This function processes the rFIA variance calculation output and converts
    it to the expected format for standard area analysis.
    """
    # The rFIA variance calculator already provides AREA_PERC, AREA_TOTAL, and sampling errors
    # We just need to ensure the column names match expected output format
    
    return variance_results.rename({
        "AREA_TOTAL": "AREA",
        "AREA_TOTAL_DEN": "AREA_DEN",
        "AREA_TOTAL_VAR": "AREA_VAR", 
        "AREA_PERC_VAR": "AREA_PERC_VAR",
        "AREA_TOTAL_SE_ACRES": "AREA_SE",
        "AREA_PERC_SE": "AREA_PERC_SE",
        "total_plots": "N_PLOTS"
    }).select([
        # Keep any grouping columns
        *[col for col in variance_results.columns if col not in ["AREA_TOTAL", "AREA_TOTAL_DEN", "AREA_TOTAL_VAR", "AREA_PERC_VAR", "AREA_TOTAL_SE_ACRES", "AREA_TOTAL_SE_PCT", "AREA_PERC_SE", "total_plots", "AREA_PERC"]],
        # Standard output columns
        "AREA", "AREA_DEN", "AREA_PERC", "AREA_VAR", "AREA_PERC_VAR",
        "AREA_SE", "AREA_PERC_SE", "N_PLOTS"
    ])


def _calculate_land_type_percentages(pop_est: pl.DataFrame) -> pl.DataFrame:
    """Calculate percentages for by_land_type analysis with common denominator (compatibility function)."""
    # Get total land area (excluding water)
    land_area_total = (
        pop_est.filter(~pl.col("LAND_TYPE").str.contains("Water"))
        .select(pl.sum("FAD_TOTAL").alias("TOTAL_LAND_AREA"))
    )[0, 0]
    
    # Use Decimal for precise calculation with safe handling
    def safe_land_type_percentage(x):
        """Calculate land type percentage safely."""
        try:
            if land_area_total is None or land_area_total == 0:
                return 0.0
            if x is None:
                x = 0
            
            x_decimal = Decimal(str(x))
            land_area_decimal = Decimal(str(land_area_total))
            result = (x_decimal / land_area_decimal * Decimal('100')).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            return float(result)
        except (ValueError, TypeError, ZeroDivisionError):
            return 0.0
    
    # Calculate percentage with proper precision
    pop_est = pop_est.with_columns(
        pl.col("FA_TOTAL").map_elements(
            safe_land_type_percentage,
            return_dtype=pl.Float64
        ).alias("AREA_PERC")
    )
    
    # Calculate variance for common denominator
    land_area_var = (
        pop_est.filter(~pl.col("LAND_TYPE").str.contains("Water"))
        .select(pl.sum("FAD_VAR").alias("TOTAL_LAND_VAR"))
    )[0, 0]
    
    # Ratio variance with common denominator
    pop_est = pop_est.with_columns(
        pl.when(land_area_total == 0)
        .then(0.0)
        .otherwise(
            (1 / (land_area_total ** 2)) * (
                pl.col("FA_VAR") +
                ((pl.col("FA_TOTAL") / land_area_total) ** 2) * land_area_var -
                2 * (pl.col("FA_TOTAL") / land_area_total) * pl.col("FA_VAR")
            )
        )
        .alias("PERC_VAR_RATIO")
    )
    
    # Convert to percentage variance
    pop_est = pop_est.with_columns(
        pl.when(pl.col("PERC_VAR_RATIO") < 0)
        .then(0.0)
        .otherwise(pl.col("PERC_VAR_RATIO") * 10000)  # (100)^2
        .alias("AREA_PERC_VAR")
    )
    
    return pop_est


def _calculate_standard_percentages(pop_est: pl.DataFrame) -> pl.DataFrame:
    """Calculate standard area percentages with ratio-of-means (compatibility function)."""
    # Use Decimal for precise percentage calculation
    def safe_percentage_calc(row):
        """Safely calculate percentage with Decimal precision."""
        try:
            fa_total = row["FA_TOTAL"] 
            fad_total = row["FAD_TOTAL"]
            
            # Handle zero or null denominators
            if fad_total is None or fad_total == 0:
                return 0.0
                
            # Handle zero or null numerators
            if fa_total is None:
                fa_total = 0
            
            # Convert to Decimal and calculate
            fa_decimal = Decimal(str(fa_total))
            fad_decimal = Decimal(str(fad_total))
            result = (fa_decimal / fad_decimal * Decimal('100')).quantize(
                Decimal('0.01'), rounding=ROUND_HALF_UP
            )
            return float(result)
        except (ValueError, TypeError, ZeroDivisionError):
            return 0.0
    
    pop_est = pop_est.with_columns(
        pl.struct(["FA_TOTAL", "FAD_TOTAL"]).map_elements(
            safe_percentage_calc,
            return_dtype=pl.Float64
        ).alias("AREA_PERC")
    )
    
    # Calculate ratio variance
    pop_est = pop_est.with_columns(
        ratio_var(
            pl.col("FA_TOTAL"),
            pl.col("FAD_TOTAL"),
            pl.col("FA_VAR"),
            pl.col("FAD_VAR"),
            pl.col("COV_FA_FAD"),
        ).alias("PERC_VAR_RATIO")
    )
    
    # Convert to percentage variance
    pop_est = pop_est.with_columns(
        pl.when(pl.col("PERC_VAR_RATIO") < 0)
        .then(0.0)
        .otherwise(pl.col("PERC_VAR_RATIO") * 10000)  # (100)^2
        .alias("AREA_PERC_VAR")
    )
    
    # Add standard error in percentage units (sqrt of percentage variance)
    pop_est = pop_est.with_columns(
        pl.col("AREA_PERC_VAR").sqrt().alias("AREA_PERC_SE")
    )
    
    # Provide AREA columns for totals output compatibility
    if "FA_TOTAL" in pop_est.columns:
        pop_est = pop_est.with_columns(
            pl.col("FA_TOTAL").alias("AREA"),
            pl.col("FAD_TOTAL").alias("AREA_DEN")
        )
    
    return pop_est