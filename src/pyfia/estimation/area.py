"""
Area estimation functions for pyFIA using the BaseEstimator architecture.

This module implements forest area estimation following FIA procedures,
matching the functionality of rFIA::area() while using the new
BaseEstimator architecture for cleaner, more maintainable code.
"""

from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Union

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


class AreaEstimator(BaseEstimator):
    """
    Area estimator implementing FIA forest area calculation procedures.

    This class calculates forest area estimates (acres and percentages)
    for forest inventory data, supporting land type classification and
    various grouping options. The estimator handles area-based metrics
    including total forest area, land type proportions, and custom
    domain-specific area estimates.

    The estimator follows the standard FIA estimation workflow:
    1. Filter conditions based on land type and area domain
    2. Apply tree domain filtering if specified
    3. Calculate domain indicators for area estimation
    4. Aggregate to plot level with proper proportion handling
    5. Apply stratification and expansion using appropriate adjustment factors
    6. Calculate population estimates with ratio-of-means variance

    Attributes
    ----------
    by_land_type : bool
        Whether to group estimates by land type categories
    land_type : str
        Land type filter for area calculations
    """

    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """
        Initialize the area estimator.

        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        config : EstimatorConfig
            Configuration with estimation parameters
        """
        super().__init__(db, config)

        # Extract area-specific parameters
        self.by_land_type = config.extra_params.get("by_land_type", False)
        self.land_type = config.land_type

        # Store whether we need tree filtering
        self._needs_tree_filtering = config.tree_domain is not None

    def get_required_tables(self) -> List[str]:
        """
        Return required database tables for area estimation.

        Returns
        -------
        List[str]
            Required tables, including TREE if tree_domain is specified
        """
        tables = ["PLOT", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
        
        # Only load TREE table if tree_domain filtering is needed
        if self._needs_tree_filtering:
            tables.append("TREE")
        
        return tables

    def get_response_columns(self) -> Dict[str, str]:
        """
        Define area response columns.

        For area estimation, we track both the area meeting criteria (fa)
        and the total area in the domain (fad) for ratio estimation.

        Returns
        -------
        Dict[str, str]
            Mapping of internal calculation names to output names
        """
        return {
            "fa": "AREA_NUMERATOR",  # Area meeting all criteria
            "fad": "AREA_DENOMINATOR",  # Total area in domain
        }

    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate area values and domain indicators.

        This method:
        1. Adds land type categories if by_land_type is True
        2. Applies tree domain filtering if specified
        3. Calculates domain indicators (landD, aD, tD)
        4. Creates comprehensive (aDI) and partial (pDI) domain indicators
        5. Calculates weighted area proportions

        Parameters
        ----------
        data : pl.DataFrame
            Condition data with CONDPROP_UNADJ

        Returns
        -------
        pl.DataFrame
            Data with calculated area values and indicators
        """
        # Add land type categories if requested
        if self.by_land_type:
            data = self._add_land_type_categories(data)
            # Ensure LAND_TYPE is in group columns
            if "LAND_TYPE" not in self._group_cols:
                self._group_cols.append("LAND_TYPE")

        # Apply tree domain filtering if specified
        if self._needs_tree_filtering and "TREE" in self.db.tables:
            data = self._apply_tree_domain_to_conditions(data)

        # Calculate domain indicators
        data = self._calculate_domain_indicators(data)

        # Calculate area values (proportion * indicator)
        data = data.with_columns([
            (pl.col("CONDPROP_UNADJ") * pl.col("aDI")).alias("fa"),
            (pl.col("CONDPROP_UNADJ") * pl.col("pDI")).alias("fad"),
        ])

        return data

    def get_output_columns(self) -> List[str]:
        """
        Define the output column structure for area estimates.

        Returns
        -------
        List[str]
            Standard output columns for area estimation
        """
        output_cols = []

        # Primary estimate columns
        output_cols.append("AREA_PERC")  # Percentage of area
        
        # Add total area if requested
        if self.config.totals:
            output_cols.append("AREA")  # Total acres

        # Add standard error or variance
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

        return output_cols

    def _get_filtered_data(self) -> tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """
        Override base class to use area_estimation_mode filtering.
        
        For area estimation, we need all land types included so we can
        calculate proper land type percentages. The land type filtering
        is handled through domain indicators instead.
        
        Returns
        -------
        tuple[Optional[pl.DataFrame], pl.DataFrame]
            Filtered tree dataframe (None if not needed) and condition dataframe
        """
        from ..filters.common import apply_area_filters_common, apply_tree_filters_common
        
        # Always get condition data
        cond_df = self.db.get_conditions()

        # Apply area filters with area_estimation_mode=True
        # This preserves all land types for proper percentage calculation
        cond_df = apply_area_filters_common(
            cond_df,
            self.config.land_type,
            self.config.area_domain,
            area_estimation_mode=True  # Key difference from base class
        )

        # Get tree data if needed (only for tree domain filtering)
        tree_df = None
        if "TREE" in self.get_required_tables():
            tree_df = self.db.get_trees()

            # Apply tree filters (no special handling needed)
            tree_df = apply_tree_filters_common(
                tree_df,
                tree_type="all",  # Area estimation doesn't filter trees by type
                tree_domain=self.config.tree_domain
            )

        return tree_df, cond_df

    def _prepare_estimation_data(self, tree_df: Optional[pl.DataFrame],
                                cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Override base class to handle area-specific data preparation.

        For area estimation, we primarily work with condition data,
        only using tree data for domain filtering if specified.

        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree data (only used for tree_domain filtering)
        cond_df : pl.DataFrame
            Condition data

        Returns
        -------
        pl.DataFrame
            Prepared condition data
        """
        # Store tree data for later use in tree domain filtering
        if tree_df is not None:
            self._data_cache["TREE"] = tree_df

        # Set up grouping columns
        self._group_cols = []
        if self.config.grp_by:
            if isinstance(self.config.grp_by, str):
                self._group_cols = [self.config.grp_by]
            else:
                self._group_cols = list(self.config.grp_by)

        # For area estimation, we work with condition data
        return cond_df

    def _calculate_plot_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate plot-level area estimates.

        Override base class to handle area-specific aggregation including
        proper handling of PROP_BASIS for adjustment factor selection.

        Parameters
        ----------
        data : pl.DataFrame
            Condition data with area indicators

        Returns
        -------
        pl.DataFrame
            Plot-level area estimates
        """
        # Determine grouping columns
        plot_groups = ["PLT_CN"]
        if self._group_cols:
            plot_groups.extend(self._group_cols)

        # Aggregate area values to plot level
        plot_estimates = data.group_by(plot_groups).agg([
            pl.sum("fa").alias("PLOT_AREA_NUMERATOR"),
            # Get dominant PROP_BASIS for adjustment factor selection
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

    def prepare_stratification_data(self, ppsa_df: pl.DataFrame,
                                   pop_stratum_df: pl.DataFrame) -> pl.DataFrame:
        """
        Prepare stratification data with both SUBP and MACR adjustment factors.

        Override base class to include both adjustment factors needed for
        proper area expansion based on PROP_BASIS.

        Parameters
        ----------
        ppsa_df : pl.DataFrame
            Plot-stratum assignments
        pop_stratum_df : pl.DataFrame
            Population stratum data

        Returns
        -------
        pl.DataFrame
            Stratification data with both adjustment factors
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

    def _apply_stratification(self, plot_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply stratification with proper adjustment factor selection.

        Override base class to select adjustment factor based on PROP_BASIS
        and apply proper expansion for area estimation.

        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level estimates with PROP_BASIS

        Returns
        -------
        pl.DataFrame
            Data with expansion factors applied
        """
        # Get stratification data
        ppsa = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(self.db.evalid)
                   if self.db.evalid else pl.lit(True))
            .collect()
        )
        
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        
        # Prepare stratification with both adjustment factors
        strat_df = self.prepare_stratification_data(ppsa, pop_stratum)
        
        # Join with plot data
        plot_with_strat = plot_data.join(
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
        
        # Apply expansion using direct expansion method (not post-stratified means)
        plot_with_strat = plot_with_strat.with_columns([
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
        
        return plot_with_strat

    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate population-level area estimates using ratio estimation.

        Override base class to implement proper area percentage calculation
        with ratio-of-means variance estimation.

        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied

        Returns
        -------
        pl.DataFrame
            Population-level area estimates with percentages and variance
        """
        # First calculate stratum-level statistics for variance
        if self._group_cols:
            strat_groups = ["STRATUM_CN"] + self._group_cols
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
            self._safe_std("fa_adjusted").alias("s_fa_h"),
            
            pl.mean("fad_adjusted").alias("fad_bar_h"),
            self._safe_std("fad_adjusted").alias("s_fad_h"),
            
            # Correlation for ratio variance
            self._safe_correlation("fa_adjusted", "fad_adjusted").alias("corr_fa_fad"),
            
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
            self._variance_component("fa").alias("FA_VAR"),
            self._variance_component("fad").alias("FAD_VAR"),
            self._covariance_component().alias("COV_FA_FAD"),
            
            # Sample size
            pl.col("n_h").sum().alias("N_PLOTS"),
        ]
        
        if self._group_cols:
            pop_est = stratum_est.group_by(self._group_cols).agg(agg_exprs)
        else:
            pop_est = stratum_est.select(agg_exprs)
        
        # Calculate percentage with proper handling for by_land_type
        if self.by_land_type and "LAND_TYPE" in pop_est.columns:
            pop_est = self._calculate_land_type_percentages(pop_est)
        else:
            pop_est = self._calculate_standard_percentages(pop_est)
        
        # Add total area if requested
        if self.config.totals:
            pop_est = pop_est.with_columns(
                pl.col("FA_TOTAL").alias("AREA")
            )
        
        # Add standard errors
        if not self.config.variance:
            pop_est = pop_est.with_columns([
                self._safe_sqrt("AREA_PERC_VAR").alias("AREA_PERC_SE"),
            ])
            if self.config.totals:
                pop_est = pop_est.with_columns(
                    self._safe_sqrt("FA_VAR").alias("AREA_SE")
                )
        else:
            # Rename variance columns to match expected output
            if self.config.totals:
                pop_est = pop_est.with_columns(
                    pl.col("FA_VAR").alias("AREA_VAR")
                )
        
        return pop_est

    def format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """
        Format output to match rFIA area() function structure.

        Parameters
        ----------
        estimates : pl.DataFrame
            Raw estimation results

        Returns
        -------
        pl.DataFrame
            Formatted output matching rFIA structure
        """
        # Select columns in the expected order
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

    # === Helper Methods ===

    def _add_land_type_categories(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Add land type categories for grouping."""
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

    def _apply_tree_domain_to_conditions(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Apply tree domain filtering at the condition level."""
        if "TREE" not in self._data_cache:
            return cond_df
        
        tree_df = self._data_cache["TREE"]
        
        # Filter trees by domain
        qualifying_trees = tree_df.filter(pl.sql_expr(self.config.tree_domain))
        
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

    def _calculate_domain_indicators(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """Calculate domain indicators for area estimation."""
        # Land type domain indicator
        if self.by_land_type and "LAND_TYPE" in cond_df.columns:
            # For by_land_type, landD is 1 for each specific land type
            cond_df = cond_df.with_columns(pl.lit(1).alias("landD"))
        elif self.land_type == "forest":
            cond_df = cond_df.with_columns(
                (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                .cast(pl.Int32).alias("landD")
            )
        elif self.land_type == "timber":
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
        if self.by_land_type:
            cond_df = cond_df.with_columns(pl.col("aD").alias("aDI"))
        else:
            cond_df = cond_df.with_columns(
                (pl.col("landD") * pl.col("aD") * pl.col("tD")).alias("aDI")
            )
        
        # Partial domain indicator (denominator)
        if self.by_land_type:
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

    def _calculate_land_type_percentages(self, pop_est: pl.DataFrame) -> pl.DataFrame:
        """Calculate percentages for by_land_type analysis with common denominator."""
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

    def _calculate_standard_percentages(self, pop_est: pl.DataFrame) -> pl.DataFrame:
        """Calculate standard area percentages with ratio-of-means."""
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
        
        return pop_est

    def _safe_std(self, col_name: str) -> pl.Expr:
        """Calculate standard deviation with protection for small samples."""
        return (
            pl.when(pl.count(col_name) > 1)
            .then(pl.std(col_name, ddof=1))
            .otherwise(0.0)
        )

    def _safe_correlation(self, col1: str, col2: str) -> pl.Expr:
        """Calculate correlation with protection for edge cases."""
        return (
            pl.when(pl.count(col1) > 1)
            .then(
                pl.when((pl.std(col1) == 0) & (pl.std(col2) == 0))
                .then(1.0)  # Perfect correlation when both are constant
                .otherwise(pl.corr(col1, col2).fill_null(0))
            )
            .otherwise(0.0)
        )

    def _variance_component(self, var_name: str) -> pl.Expr:
        """Calculate variance component for stratified sampling."""
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

    def _covariance_component(self) -> pl.Expr:
        """Calculate covariance component for ratio variance."""
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

    def _safe_sqrt(self, col_name: str) -> pl.Expr:
        """Calculate square root with protection for negative values."""
        return (
            pl.when(pl.col(col_name) >= 0)
            .then(pl.col(col_name).sqrt())
            .otherwise(0.0)
        )


# === Compatibility Functions for Tests ===

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
    """Apply tree domain filtering at condition level (compatibility function)."""
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
    cond_df: pl.DataFrame, land_type: str, by_land_type: bool = False
) -> pl.DataFrame:
    """Calculate domain indicators (compatibility function)."""
    # Land type domain indicator
    if by_land_type and "LAND_TYPE" in cond_df.columns:
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


def _prepare_area_stratification(
    stratum_df: pl.DataFrame, assgn_df: pl.DataFrame
) -> pl.DataFrame:
    """Prepare stratification data (compatibility function)."""
    # Filter assignments to current evaluation if EVALID is present
    if "EVALID" in assgn_df.columns and len(assgn_df) > 0:
        current_evalid = assgn_df["EVALID"][0]
        assgn_df = assgn_df.filter(pl.col("EVALID") == current_evalid)

    # Join assignment with stratum info
    strat_df = assgn_df.join(
        stratum_df.select(
            ["CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "P2POINTCNT"]
        ),
        left_on="STRATUM_CN",
        right_on="CN",
        how="inner",
    )

    return strat_df


def _calculate_plot_area_estimates(
    plot_df: pl.DataFrame,
    cond_df: pl.DataFrame,
    strat_df: pl.DataFrame,
    grp_by: Optional[List[str]],
) -> pl.DataFrame:
    """Calculate plot-level area estimates (compatibility function)."""
    # This is a simplified version for compatibility
    # The new AreaEstimator handles this more comprehensively
    
    # Ensure plot_df has PLT_CN column
    if "PLT_CN" not in plot_df.columns:
        plot_df = plot_df.rename({"CN": "PLT_CN"})

    # Calculate area proportions for each plot
    if grp_by:
        cond_groups = ["PLT_CN"] + grp_by
    else:
        cond_groups = ["PLT_CN"]

    # Area meeting criteria (numerator)
    area_num = cond_df.group_by(cond_groups).agg([
        (pl.col("CONDPROP_UNADJ") * pl.col("aDI")).sum().alias("fa"),
        pl.col("PROP_BASIS").mode().first().alias("PROP_BASIS"),
    ])

    # Total area in domain (denominator)
    area_den = cond_df.group_by("PLT_CN").agg([
        (pl.col("CONDPROP_UNADJ") * pl.col("pDI")).sum().alias("fad"),
    ])

    # Join numerator and denominator
    plot_est = area_num.join(area_den, on="PLT_CN", how="left")

    # Join with stratification
    plot_est = plot_est.join(
        strat_df.select(
            ["PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]
        ),
        on="PLT_CN",
        how="left",
    )

    # Select appropriate adjustment factor
    plot_est = plot_est.with_columns(
        pl.when(pl.col("PROP_BASIS") == PlotBasis.MACROPLOT)
        .then(pl.col("ADJ_FACTOR_MACR"))
        .otherwise(pl.col("ADJ_FACTOR_SUBP"))
        .alias("ADJ_FACTOR")
    )

    # Apply adjustment factor
    plot_est = plot_est.with_columns([
        (pl.col("fa") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("fa_expanded"),
        (pl.col("fad") * pl.col("ADJ_FACTOR") * pl.col("EXPNS")).alias("fad_expanded"),
        (pl.col("fa") * pl.col("ADJ_FACTOR")).alias("fa"),
        (pl.col("fad") * pl.col("ADJ_FACTOR")).alias("fad"),
    ])

    # Fill missing values
    plot_est = plot_est.with_columns([
        pl.col("fa").fill_null(0),
        pl.col("fad").fill_null(0),
        pl.col("fa_expanded").fill_null(0),
        pl.col("fad_expanded").fill_null(0),
    ])

    return plot_est


def _calculate_stratum_area_estimates(
    plot_est: pl.DataFrame, grp_by: Optional[List[str]]
) -> pl.DataFrame:
    """Calculate stratum-level area estimates (compatibility function)."""
    if grp_by:
        strat_groups = ["STRATUM_CN"] + grp_by
    else:
        strat_groups = ["STRATUM_CN"]

    # Calculate stratum totals and variance components
    stratum_est = plot_est.group_by(strat_groups).agg([
        # Sample size
        pl.len().alias("n_h"),
        # Direct expansion totals
        pl.sum("fa_expanded").alias("fa_expanded_total"),
        pl.sum("fad_expanded").alias("fad_expanded_total"),
        # Area estimates for variance
        pl.mean("fa").alias("fa_bar_h"),
        pl.when(pl.count("fa") > 1)
        .then(pl.std("fa", ddof=1))
        .otherwise(0.0)
        .alias("s_fa_h"),
        # Total area for variance
        pl.mean("fad").alias("fad_bar_h"),
        pl.when(pl.count("fad") > 1)
        .then(pl.std("fad", ddof=1))
        .otherwise(0.0)
        .alias("s_fad_h"),
        # Correlation for ratio variance
        pl.when(pl.count("fa") > 1)
        .then(
            pl.when((pl.std("fa") == 0) & (pl.std("fad") == 0))
            .then(1.0)
            .otherwise(pl.corr("fa", "fad").fill_null(0))
        )
        .otherwise(0.0)
        .alias("corr_fa_fad"),
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

    return stratum_est


def _calculate_population_area_estimates(
    stratum_est: pl.DataFrame, grp_by: Optional[List[str]], totals: bool, variance: bool
) -> pl.DataFrame:
    """Calculate population-level area estimates (compatibility function)."""
    if grp_by:
        pop_groups = grp_by
    else:
        pop_groups = []

    # Calculate totals using direct expansion
    agg_exprs = [
        # Direct expansion totals
        pl.col("fa_expanded_total").sum().alias("FA_TOTAL"),
        pl.col("fad_expanded_total").sum().alias("FAD_TOTAL"),
        # Variance components
        pl.when(pl.col("n_h") > 1)
        .then((pl.col("w_h") ** 2) * (pl.col("s_fa_h") ** 2) / pl.col("n_h"))
        .otherwise(0.0)
        .sum()
        .alias("FA_VAR"),
        pl.when(pl.col("n_h") > 1)
        .then((pl.col("w_h") ** 2) * (pl.col("s_fad_h") ** 2) / pl.col("n_h"))
        .otherwise(0.0)
        .sum()
        .alias("FAD_VAR"),
        # Covariance term
        pl.when(pl.col("n_h") > 1)
        .then((pl.col("w_h") ** 2) * pl.col("s_fa_fad_h") / pl.col("n_h"))
        .otherwise(0.0)
        .sum()
        .alias("COV_FA_FAD"),
        # Sample size
        pl.col("n_h").sum().alias("N_PLOTS"),
    ]

    if pop_groups:
        pop_est = stratum_est.group_by(pop_groups).agg(agg_exprs)
    else:
        pop_est = stratum_est.select(agg_exprs)

    # Calculate percentage
    pop_est = pop_est.with_columns(
        pl.when(pl.col("FAD_TOTAL") == 0)
        .then(0.0)
        .otherwise((pl.col("FA_TOTAL") / pl.col("FAD_TOTAL")) * 100)
        .alias("AREA_PERC")
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
        .otherwise(pl.col("PERC_VAR_RATIO") * 10000)
        .alias("AREA_PERC_VAR")
    )

    # Calculate standard errors
    pop_est = pop_est.with_columns([
        pl.when(pl.col("AREA_PERC_VAR") >= 0)
        .then(pl.col("AREA_PERC_VAR").sqrt())
        .otherwise(0.0)
        .alias("AREA_PERC_SE"),
    ])

    # Select final columns
    cols = pop_groups + ["AREA_PERC", "N_PLOTS"]

    if totals:
        pop_est = pop_est.with_columns(pl.col("FA_TOTAL").alias("AREA"))
        cols.append("AREA")

    if variance:
        cols.append("AREA_PERC_VAR")
        if totals:
            cols.append("FA_VAR")
    else:
        cols.append("AREA_PERC_SE")

    return pop_est.select(cols)


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

    This function maintains backward compatibility with the original area()
    function while using the new AreaEstimator class internally.

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

    Examples
    --------
    >>> # Estimate forest area by forest type
    >>> results = area(db, by_land_type=True)
    
    >>> # Estimate timber area with custom grouping
    >>> results = area(db, land_type="timber", grp_by=["FORTYPCD"])
    
    >>> # Estimate area with tree domain filtering
    >>> results = area(db, tree_domain="DIA >= 10.0", totals=True)
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
    with AreaEstimator(db, config) as estimator:
        return estimator.estimate()