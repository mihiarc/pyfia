"""
Base estimator class for FIA design-based estimation.

This module provides the abstract base class that standardizes the estimation
workflow across all PyFIA estimation modules (volume, biomass, TPA, area,
mortality, growth). It implements the Template Method pattern to define the
skeleton of the estimation algorithm while allowing subclasses to override
specific steps.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, Callable

import polars as pl

from ..core import FIA
from ..constants.constants import PlotBasis
from ..filters.common import (
    apply_area_filters_common,
    apply_tree_filters_common,
    setup_grouping_columns_common,
)


@dataclass
class EstimatorConfig:
    """
    Configuration for FIA estimation parameters.

    This dataclass encapsulates all common parameters used across
    different estimation modules, providing a clean interface for
    configuration management.

    Attributes
    ----------
    grp_by : Optional[Union[str, List[str]]]
        Column(s) to group estimates by
    by_species : bool
        Whether to group by species code (SPCD)
    by_size_class : bool
        Whether to group by diameter size classes
    land_type : str
        Land type filter: "forest", "timber", or "all"
    tree_type : str
        Tree type filter: "live", "dead", "gs", or "all"
    tree_domain : Optional[str]
        SQL-like expression for tree filtering
    area_domain : Optional[str]
        SQL-like expression for area filtering
    method : str
        Estimation method: "TI", "SMA", "LMA", "EMA", or "ANNUAL"
    lambda_ : float
        Temporal weighting parameter for moving averages
    totals : bool
        Whether to include total estimates in addition to per-acre
    variance : bool
        Whether to return variance instead of standard error
    by_plot : bool
        Whether to return plot-level estimates
    most_recent : bool
        Whether to use only the most recent evaluation
    """

    grp_by: Optional[Union[str, List[str]]] = None
    by_species: bool = False
    by_size_class: bool = False
    land_type: str = "forest"
    tree_type: str = "live"
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    method: str = "TI"
    lambda_: float = 0.5
    totals: bool = False
    variance: bool = False
    by_plot: bool = False
    most_recent: bool = False

    # Additional parameters can be stored here
    extra_params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for backwards compatibility."""
        result = {
            k: v for k, v in self.__dict__.items()
            if k != "extra_params" and v is not None
        }
        result.update(self.extra_params)
        return result


class BaseEstimator(ABC):
    """
    Abstract base class for FIA design-based estimators.

    This class implements the Template Method pattern to standardize the
    estimation workflow while allowing module-specific customization through
    abstract methods and hooks.

    The estimation workflow follows these steps:
    1. Load required database tables
    2. Apply filters to select relevant data
    3. Join and prepare data for estimation
    4. Calculate plot-level estimates
    5. Apply stratification and expansion factors
    6. Calculate population-level estimates
    7. Format output to match expected structure

    Subclasses must implement:
    - get_required_tables(): Define required database tables
    - get_response_columns(): Define response variables
    - calculate_values(): Calculate module-specific values
    - get_output_columns(): Define output structure

    Subclasses may override:
    - apply_module_filters(): Additional filtering logic
    - prepare_stratification_data(): Custom stratification
    - calculate_variance(): Module-specific variance calculation
    - format_output(): Custom output formatting
    """

    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """
        Initialize the estimator with database and configuration.

        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database file
        config : EstimatorConfig
            Configuration object with estimation parameters
        """
        # Handle database initialization
        if isinstance(db, str):
            self.db = FIA(db)
            self._owns_db = True  # We created it, we should close it
        else:
            self.db = db
            self._owns_db = False  # Using external db, don't close

        self.config = config
        self._validate_config()

        # Cache for loaded data and intermediate results
        self._data_cache: Dict[str, pl.DataFrame] = {}
        self._group_cols: List[str] = []

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clean up if we own the database."""
        if self._owns_db and hasattr(self.db, 'close'):
            self.db.close()

    # === Abstract Methods (Must be implemented by subclasses) ===

    @abstractmethod
    def get_required_tables(self) -> List[str]:
        """
        Return list of required database tables for this estimator.

        Returns
        -------
        List[str]
            Names of required FIA database tables
        """
        pass

    @abstractmethod
    def get_response_columns(self) -> Dict[str, str]:
        """
        Define the response variable columns for calculation.

        Returns a mapping from internal calculation column names to
        output column names. For example, in volume estimation:
        {"VOL_CF_ACRE": "VOLCFNET_ACRE", "VOL_BF_ACRE": "VOLBFNET_ACRE"}

        Returns
        -------
        Dict[str, str]
            Mapping of internal column names to output names
        """
        pass

    @abstractmethod
    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate module-specific values from the prepared data.

        This is where the core calculation logic for each estimator lives.
        For example, volume calculation multiplies volume columns by TPA_UNADJ,
        biomass calculation converts dry biomass to tons per acre, etc.

        Parameters
        ----------
        data : pl.DataFrame
            Prepared data (usually trees joined with conditions)

        Returns
        -------
        pl.DataFrame
            Data with calculated values added
        """
        pass

    @abstractmethod
    def get_output_columns(self) -> List[str]:
        """
        Define the output column structure for this estimator.

        Returns the ordered list of columns that should appear in the
        final output. This typically includes estimate columns, standard
        errors, metadata (YEAR, nPlots), and any grouping columns.

        Returns
        -------
        List[str]
            Ordered list of output column names
        """
        pass

    # === Hook Methods (Can be overridden for customization) ===

    def apply_module_filters(self, tree_df: Optional[pl.DataFrame],
                            cond_df: pl.DataFrame) -> tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """
        Apply module-specific filters beyond common filtering.

        Hook method that can be overridden by subclasses for additional
        filtering requirements specific to that estimation type.

        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree dataframe after common filters (None for area estimation)
        cond_df : pl.DataFrame
            Condition dataframe after common filters

        Returns
        -------
        tuple[Optional[pl.DataFrame], pl.DataFrame]
            Filtered tree and condition dataframes
        """
        return tree_df, cond_df

    def prepare_stratification_data(self, ppsa_df: pl.DataFrame,
                                   pop_stratum_df: pl.DataFrame) -> pl.DataFrame:
        """
        Prepare stratification data for expansion.

        Hook method for module-specific stratification preparation.
        Default implementation joins plot-stratum assignments with
        population stratum data to get expansion factors.

        Parameters
        ----------
        ppsa_df : pl.DataFrame
            Plot-stratum assignments (POP_PLOT_STRATUM_ASSGN)
        pop_stratum_df : pl.DataFrame
            Population stratum data (POP_STRATUM)

        Returns
        -------
        pl.DataFrame
            Stratification data with expansion factors
        """
        # Default implementation - join to get expansion factors
        strat_df = ppsa_df.join(
            pop_stratum_df.select(["CN", "EXPNS", "ADJ_FACTOR_SUBP"]),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner"
        )
        return strat_df

    def calculate_variance(self, data: pl.DataFrame, estimate_col: str) -> pl.DataFrame:
        """
        Calculate variance or standard error for estimates.

        Hook method for module-specific variance calculation. The default
        implementation provides a simplified standard error calculation.
        Subclasses should override this for proper variance estimation
        following Bechtold & Patterson (2005).

        Parameters
        ----------
        data : pl.DataFrame
            Data with population estimates
        estimate_col : str
            Name of the estimate column

        Returns
        -------
        pl.DataFrame
            Data with variance/SE column added
        """
        # Simplified default - subclasses should implement proper variance
        se_col = f"{estimate_col}_SE"

        if self.config.variance:
            # Return variance
            var_col = f"{estimate_col}_VAR"
            return data.with_columns([
                (pl.col(estimate_col) * 0.015) ** 2  # Squared for variance
                .alias(var_col)
            ])
        else:
            # Return standard error
            return data.with_columns([
                (pl.col(estimate_col) * 0.015).alias(se_col)
            ])

    def format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """
        Format final output to match expected structure.

        Hook method for module-specific output formatting. The default
        implementation selects columns based on get_output_columns().

        Parameters
        ----------
        estimates : pl.DataFrame
            Raw estimation results

        Returns
        -------
        pl.DataFrame
            Formatted output dataframe
        """
        # Get expected output columns
        output_cols = self.get_output_columns()

        # Add grouping columns if they exist
        if self._group_cols:
            output_cols = self._group_cols + output_cols

        # Select only columns that exist
        available_cols = [col for col in output_cols if col in estimates.columns]

        return estimates.select(available_cols)

    # === Template Methods (Core workflow implementation) ===

    def estimate(self) -> pl.DataFrame:
        """
        Main estimation workflow implementing the Template Method pattern.

        This method orchestrates the entire estimation process following
        FIA statistical procedures. It calls abstract methods and hooks
        in a defined sequence to produce population estimates.

        Returns
        -------
        pl.DataFrame
            Final estimation results formatted for output

        Raises
        ------
        ValueError
            If required data or configuration is invalid
        """
        # Step 1: Load required tables
        self._load_required_tables()

        # Step 2: Get and filter data
        tree_df, cond_df = self._get_filtered_data()

        # Step 3: Join and prepare data
        prepared_data = self._prepare_estimation_data(tree_df, cond_df)

        # Step 4: Calculate module-specific values
        valued_data = self.calculate_values(prepared_data)

        # Step 5: Calculate plot-level estimates
        plot_estimates = self._calculate_plot_estimates(valued_data)

        # Step 6: Apply stratification and expansion
        expanded_estimates = self._apply_stratification(plot_estimates)

        # Step 7: Calculate population estimates
        pop_estimates = self._calculate_population_estimates(expanded_estimates)

        # Step 8: Format and return results
        return self.format_output(pop_estimates)

    def _validate_config(self):
        """
        Validate configuration parameters.

        Raises
        ------
        ValueError
            If any configuration parameter is invalid
        """
        # Validate estimation method
        valid_methods = ["TI", "SMA", "LMA", "EMA", "ANNUAL"]
        if self.config.method not in valid_methods:
            raise ValueError(
                f"Invalid method: {self.config.method}. "
                f"Must be one of {valid_methods}"
            )

        # Validate land type
        valid_land_types = ["forest", "timber", "all"]
        if self.config.land_type not in valid_land_types:
            raise ValueError(
                f"Invalid land_type: {self.config.land_type}. "
                f"Must be one of {valid_land_types}"
            )

        # Validate tree type
        valid_tree_types = ["live", "dead", "gs", "all"]
        if self.config.tree_type not in valid_tree_types:
            raise ValueError(
                f"Invalid tree_type: {self.config.tree_type}. "
                f"Must be one of {valid_tree_types}"
            )

        # Validate lambda parameter
        if not 0 <= self.config.lambda_ <= 1:
            raise ValueError(
                f"Invalid lambda: {self.config.lambda_}. "
                "Must be between 0 and 1"
            )

    def _load_required_tables(self):
        """Load all required tables from the database."""
        for table in self.get_required_tables():
            self.db.load_table(table)

    def _get_filtered_data(self) -> tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """
        Get data from database and apply common filters.

        Returns
        -------
        tuple[Optional[pl.DataFrame], pl.DataFrame]
            Filtered tree dataframe (None if not needed) and condition dataframe
        """
        # Always get condition data
        cond_df = self.db.get_conditions()

        # Apply common area filters
        cond_df = apply_area_filters_common(
            cond_df,
            self.config.land_type,
            self.config.area_domain
        )

        # Get tree data if needed
        tree_df = None
        if "TREE" in self.get_required_tables():
            tree_df = self.db.get_trees()

            # Apply common tree filters with module-specific flags
            # Let modules enforce their own volume requirements
            require_volume = False
            tree_df = apply_tree_filters_common(
                tree_df,
                self.config.tree_type,
                self.config.tree_domain,
                require_volume=require_volume
            )

        # Apply module-specific filters
        tree_df, cond_df = self.apply_module_filters(tree_df, cond_df)

        return tree_df, cond_df

    def _prepare_estimation_data(self, tree_df: Optional[pl.DataFrame],
                                cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Join data and prepare for estimation.

        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree data (None for area estimation)
        cond_df : pl.DataFrame
            Condition data

        Returns
        -------
        pl.DataFrame
            Prepared data ready for value calculation
        """
        if tree_df is not None:
            # Join trees with conditions
            data = tree_df.join(
                cond_df.select(["PLT_CN", "CONDID", "CONDPROP_UNADJ"]),
                on=["PLT_CN", "CONDID"],
                how="inner"
            )

            # Set up grouping columns
            data, group_cols = setup_grouping_columns_common(
                data,
                self.config.grp_by,
                self.config.by_species,
                self.config.by_size_class,
                return_dataframe=True
            )
            self._group_cols = group_cols
        else:
            # Area estimation case - no tree data
            data = cond_df
            self._group_cols = []

            # Handle custom grouping columns
            if self.config.grp_by:
                if isinstance(self.config.grp_by, str):
                    self._group_cols = [self.config.grp_by]
                else:
                    self._group_cols = list(self.config.grp_by)

        return data

    def _calculate_plot_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate plot-level estimates by aggregating to plot level.

        Parameters
        ----------
        data : pl.DataFrame
            Data with calculated values

        Returns
        -------
        pl.DataFrame
            Plot-level estimates
        """
        # Determine grouping columns
        plot_groups = ["PLT_CN"]
        if self._group_cols:
            plot_groups.extend(self._group_cols)

        # Get response columns for aggregation
        response_cols = self.get_response_columns()
        agg_exprs = []

        for col_name, output_name in response_cols.items():
            if col_name in data.columns:
                agg_exprs.append(pl.sum(col_name).alias(f"PLOT_{output_name}"))

        if not agg_exprs:
            raise ValueError(
                f"No response columns found in data. "
                f"Expected columns: {list(response_cols.keys())}"
            )

        # Aggregate to plot level
        plot_estimates = data.group_by(plot_groups).agg(agg_exprs)

        return plot_estimates

    def _apply_stratification(self, plot_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply stratification and calculate expansion factors.

        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level estimates

        Returns
        -------
        pl.DataFrame
            Data with expansion factors applied
        """
        # Get stratification data filtered by EVALID
        ppsa = self._get_plot_stratum_assignments()
        pop_stratum = self._get_population_stratum()

        # Prepare stratification data
        strat_df = self.prepare_stratification_data(ppsa, pop_stratum)

        # Join plot data with stratification
        plot_with_strat = self._join_plot_with_stratification(plot_data, strat_df)

        # Apply basis-specific adjustments if needed
        plot_with_strat = self._apply_basis_adjustments(plot_with_strat)

        # Calculate stratum-level estimates
        stratum_est = self._calculate_stratum_estimates(plot_with_strat)

        return stratum_est

    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate final population-level estimates.

        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data with expansion factors applied

        Returns
        -------
        pl.DataFrame
            Population-level estimates with per-acre values and variance
        """
        response_cols = self.get_response_columns()
        
        # Step 1: Aggregate to population level
        pop_estimates = self._aggregate_to_population(expanded_data, response_cols)
        
        # Step 2: Calculate per-acre values
        pop_estimates = self._calculate_per_acre_values(pop_estimates, response_cols)
        
        # Step 3: Add variance/SE for each estimate
        for _, output_name in response_cols.items():
            if output_name in pop_estimates.columns:
                pop_estimates = self.calculate_variance(pop_estimates, output_name)
        
        # Step 4: Add standard metadata
        pop_estimates = self._add_standard_metadata(pop_estimates)
        
        # Step 5: Add total columns if requested
        pop_estimates = self._add_total_columns(pop_estimates, response_cols)
        
        return pop_estimates

    def _get_forest_area(self) -> float:
        """Deprecated in favor of direct EXPNS sum in _calculate_population_estimates."""
        return 0.0

    def _get_year(self) -> int:
        """
        Get the inventory year from the evaluation.

        Returns
        -------
        int
            Inventory year
        """
        # TODO: Get from EVALID or POP_EVAL table
        return 2023  # Placeholder value

    # === Enhanced Helper Methods for Common Functionality ===

    def _get_plot_stratum_assignments(self) -> pl.DataFrame:
        """
        Get plot-stratum assignments filtered by current evaluation.

        Returns
        -------
        pl.DataFrame
            Filtered plot-stratum assignments
        """
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
        
        if self.db.evalid:
            ppsa = ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        return ppsa.collect()

    def _get_population_stratum(self) -> pl.DataFrame:
        """
        Get population stratum data.

        Returns
        -------
        pl.DataFrame
            Population stratum dataframe
        """
        return self.db.tables["POP_STRATUM"].collect()

    def _join_plot_with_stratification(self, plot_data: pl.DataFrame, 
                                     strat_df: pl.DataFrame) -> pl.DataFrame:
        """
        Join plot data with stratification data.

        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level data
        strat_df : pl.DataFrame
            Stratification data

        Returns
        -------
        pl.DataFrame
            Joined data with stratification info
        """
        # Standard columns to join
        strat_cols = ["PLT_CN", "STRATUM_CN", "EXPNS"]
        
        # Add adjustment factor columns if present
        adj_cols = ["ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "ADJ_FACTOR_MICR"]
        for col in adj_cols:
            if col in strat_df.columns:
                strat_cols.append(col)
        
        plot_with_strat = plot_data.join(
            strat_df.select(strat_cols),
            on="PLT_CN",
            how="inner"
        )
        
        # Cast numeric columns to Float64 to avoid decimal precision issues
        float_cols = []
        for col in ["EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "ADJ_FACTOR_MICR"]:
            if col in plot_with_strat.columns:
                float_cols.append(pl.col(col).cast(pl.Float64))
        
        if float_cols:
            plot_with_strat = plot_with_strat.with_columns(float_cols)
        
        return plot_with_strat

    def _apply_basis_adjustments(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply basis-specific adjustment factors.

        Hook method that can be overridden by estimators that need
        basis-specific adjustments (e.g., volume, biomass).

        Parameters
        ----------
        data : pl.DataFrame
            Data with stratification info

        Returns
        -------
        pl.DataFrame
            Data with basis adjustments applied
        """
        # Default implementation - use SUBP adjustment
        if "ADJ_FACTOR_SUBP" in data.columns:
            return data.with_columns(
                pl.col("ADJ_FACTOR_SUBP").alias("ADJ_FACTOR")
            )
        return data

    def _calculate_stratum_estimates(self, plot_with_strat: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate stratum-level estimates from plot data.

        Parameters
        ----------
        plot_with_strat : pl.DataFrame
            Plot data with stratification info

        Returns
        -------
        pl.DataFrame
            Stratum-level estimates
        """
        response_cols = self.get_response_columns()
        strat_group_cols = ["STRATUM_CN"]
        if self._group_cols:
            strat_group_cols.extend(self._group_cols)

        # Build aggregation expressions
        agg_exprs = [
            pl.len().alias("n_h"),
            pl.col("EXPNS").first().cast(pl.Float64).alias("A_h"),
        ]
        
        # Add adjustment factor if present
        if "ADJ_FACTOR" in plot_with_strat.columns:
            agg_exprs.append(
                pl.col("ADJ_FACTOR").first().cast(pl.Float64).alias("ADJ_h")
            )
        elif "ADJ_FACTOR_SUBP" in plot_with_strat.columns:
            agg_exprs.append(
                pl.col("ADJ_FACTOR_SUBP").first().cast(pl.Float64).alias("ADJ_h")
            )
        
        # Add mean calculations for response variables
        for _, output_name in response_cols.items():
            plot_col = f"PLOT_{output_name}"
            if plot_col in plot_with_strat.columns:
                agg_exprs.append(pl.mean(plot_col).alias(f"ybar_{output_name}"))

        stratum_est = plot_with_strat.group_by(strat_group_cols).agg(agg_exprs)

        # Calculate totals per stratum
        total_exprs = []
        for _, output_name in response_cols.items():
            ybar_col = f"ybar_{output_name}"
            if ybar_col in stratum_est.columns:
                if "ADJ_h" in stratum_est.columns:
                    total_exprs.append(
                        (pl.col(ybar_col) * pl.col("ADJ_h") * pl.col("A_h"))
                        .alias(f"TOTAL_{output_name}")
                    )
                else:
                    total_exprs.append(
                        (pl.col(ybar_col) * pl.col("A_h"))
                        .alias(f"TOTAL_{output_name}")
                    )

        if total_exprs:
            stratum_est = stratum_est.with_columns(total_exprs)

        return stratum_est

    def _aggregate_to_population(self, stratum_data: pl.DataFrame,
                               response_cols: Dict[str, str]) -> pl.DataFrame:
        """
        Aggregate stratum estimates to population level.

        Parameters
        ----------
        stratum_data : pl.DataFrame
            Stratum-level estimates
        response_cols : Dict[str, str]
            Response column mapping

        Returns
        -------
        pl.DataFrame
            Population-level aggregates
        """
        agg_exprs = []
        
        # Sum totals for each response variable
        for _, output_name in response_cols.items():
            total_col = f"TOTAL_{output_name}"
            if total_col in stratum_data.columns:
                agg_exprs.append(pl.sum(total_col).alias(f"POP_{output_name}"))
        
        # Count plots
        agg_exprs.append(pl.sum("n_h").alias("nPlots"))
        
        # Sum expansion factors for denominator
        if "A_h" in stratum_data.columns:
            agg_exprs.append(pl.sum("A_h").alias("TOTAL_EXPNS"))

        # Perform aggregation
        if self._group_cols:
            pop_estimates = stratum_data.group_by(self._group_cols).agg(agg_exprs)
        else:
            pop_estimates = stratum_data.select(agg_exprs)

        return pop_estimates

    def _calculate_per_acre_values(self, pop_data: pl.DataFrame,
                                  response_cols: Dict[str, str]) -> pl.DataFrame:
        """
        Calculate per-acre values from population totals.

        Parameters
        ----------
        pop_data : pl.DataFrame
            Population-level totals
        response_cols : Dict[str, str]
            Response column mapping

        Returns
        -------
        pl.DataFrame
            Data with per-acre values added
        """
        per_acre_exprs = []
        
        # Get denominator (total expansion factor)
        if "TOTAL_EXPNS" in pop_data.columns:
            denom_col = "TOTAL_EXPNS"
        elif "EXPNS" in pop_data.columns:
            denom_col = "EXPNS"
        else:
            # Try to calculate from first available POP column
            for _, output_name in response_cols.items():
                pop_col = f"POP_{output_name}"
                if pop_col in pop_data.columns:
                    # Assume we have the denominator somewhere
                    break
            else:
                return pop_data
        
        # Calculate per-acre values
        for _, output_name in response_cols.items():
            pop_col = f"POP_{output_name}"
            if pop_col in pop_data.columns and denom_col in pop_data.columns:
                per_acre_exprs.append(
                    (pl.col(pop_col).cast(pl.Float64) / pl.col(denom_col).cast(pl.Float64))
                    .alias(output_name)
                )
        
        if per_acre_exprs:
            pop_data = pop_data.with_columns(per_acre_exprs)
        
        return pop_data

    def _add_standard_metadata(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """
        Add standard metadata columns to estimates.

        Parameters
        ----------
        estimates : pl.DataFrame
            Population estimates

        Returns
        -------
        pl.DataFrame
            Estimates with metadata added
        """
        metadata_cols = [
            pl.lit(self._get_year()).alias("YEAR"),
        ]
        
        # Handle different plot count column names
        if "nPlots" in estimates.columns:
            metadata_cols.extend([
                pl.col("nPlots").alias("N"),
                pl.col("nPlots").alias("nPlots_TREE"),
                pl.col("nPlots").alias("nPlots_AREA")
            ])
        elif "N_PLOTS" in estimates.columns:
            metadata_cols.extend([
                pl.col("N_PLOTS").alias("N"),
                pl.col("N_PLOTS").alias("nPlots_TREE"),
                pl.col("N_PLOTS").alias("nPlots_AREA")
            ])
        
        return estimates.with_columns(metadata_cols)

    def _add_total_columns(self, estimates: pl.DataFrame,
                          response_cols: Dict[str, str]) -> pl.DataFrame:
        """
        Add total columns if requested.

        Parameters
        ----------
        estimates : pl.DataFrame
            Population estimates
        response_cols : Dict[str, str]
            Response column mapping

        Returns
        -------
        pl.DataFrame
            Estimates with total columns added
        """
        if not self.config.totals:
            return estimates
        
        total_exprs = []
        for _, output_name in response_cols.items():
            pop_col = f"POP_{output_name}"
            if pop_col in estimates.columns:
                total_exprs.append(
                    pl.col(pop_col).alias(f"{output_name}_TOTAL")
                )
        
        if total_exprs:
            estimates = estimates.with_columns(total_exprs)
        
        return estimates

    # === Common Calculation Patterns ===

    def calculate_standard_error(self, estimates: pl.DataFrame,
                               estimate_col: str,
                               cv: float = 0.015) -> pl.DataFrame:
        """
        Calculate standard error using coefficient of variation.

        Parameters
        ----------
        estimates : pl.DataFrame
            Estimates dataframe
        estimate_col : str
            Name of estimate column
        cv : float, default 0.015
            Coefficient of variation

        Returns
        -------
        pl.DataFrame
            Dataframe with SE column added
        """
        se_col = f"{estimate_col}_SE"
        return estimates.with_columns([
            (pl.col(estimate_col) * cv).alias(se_col)
        ])

    def calculate_variance_from_se(self, estimates: pl.DataFrame,
                                 se_col: str) -> pl.DataFrame:
        """
        Calculate variance from standard error.

        Parameters
        ----------
        estimates : pl.DataFrame
            Estimates dataframe with SE
        se_col : str
            Name of SE column

        Returns
        -------
        pl.DataFrame
            Dataframe with variance column added
        """
        var_col = se_col.replace("_SE", "_VAR")
        return estimates.with_columns([
            (pl.col(se_col) ** 2).alias(var_col)
        ])

    def apply_domain_filters(self, tree_df: Optional[pl.DataFrame],
                           cond_df: pl.DataFrame) -> tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """
        Apply tree and area domain filters.

        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree dataframe
        cond_df : pl.DataFrame
            Condition dataframe

        Returns
        -------
        tuple[Optional[pl.DataFrame], pl.DataFrame]
            Filtered dataframes
        """
        from ..filters.common import apply_area_filters_common, apply_tree_filters_common
        
        # Apply area domain
        if self.config.area_domain:
            cond_df = cond_df.filter(pl.Expr.from_string(self.config.area_domain))
        
        # Apply tree domain
        if tree_df is not None and self.config.tree_domain:
            tree_df = tree_df.filter(pl.Expr.from_string(self.config.tree_domain))
        
        return tree_df, cond_df


class EnhancedBaseEstimator(BaseEstimator):
    """
    Enhanced base estimator with additional functionality for reducing code duplication.
    
    This class extends BaseEstimator with more sophisticated patterns commonly used
    across estimators, including:
    - Advanced stratification handling
    - Built-in variance calculation options
    - Common aggregation patterns
    - Flexible output formatting
    
    Subclasses can leverage these additional methods to significantly reduce
    code duplication while maintaining full control over customization.
    """
    
    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """Initialize enhanced estimator with additional features."""
        super().__init__(db, config)
        
        # Additional caches for enhanced functionality
        self._stratification_cache: Optional[pl.DataFrame] = None
        self._variance_components: Dict[str, pl.DataFrame] = {}
        
    # === Enhanced Stratification Methods ===
    
    def get_stratification_data(self) -> pl.DataFrame:
        """
        Get complete stratification data with caching.
        
        Returns
        -------
        pl.DataFrame
            Complete stratification data joined and ready for use
        """
        if self._stratification_cache is not None:
            return self._stratification_cache
            
        ppsa = self._get_plot_stratum_assignments()
        pop_stratum = self._get_population_stratum()
        
        # Join with all adjustment factors
        strat_df = ppsa.join(
            pop_stratum.select([
                "CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", 
                "ADJ_FACTOR_MICR", "P2POINTCNT"
            ]),
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner"
        )
        
        self._stratification_cache = strat_df
        return strat_df
        
    def apply_tree_basis_adjustments(self, data: pl.DataFrame, 
                                   tree_basis_col: str = "TREE_BASIS") -> pl.DataFrame:
        """
        Apply tree basis-specific adjustment factors.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with tree basis information
        tree_basis_col : str, default "TREE_BASIS"
            Column containing tree basis classification
            
        Returns
        -------
        pl.DataFrame
            Data with appropriate adjustment factors applied
        """
        if tree_basis_col not in data.columns:
            # No tree basis - use default subplot adjustment
            return self._apply_basis_adjustments(data)
            
        # Apply basis-specific adjustments
        adj_expr = (
            pl.when(pl.col(tree_basis_col) == "MICR")
            .then(pl.col("ADJ_FACTOR_MICR"))
            .when(pl.col(tree_basis_col) == "MACR")
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(pl.col("ADJ_FACTOR_SUBP"))
            .cast(pl.Float64)
            .alias("ADJ_FACTOR")
        )
        
        return data.with_columns(adj_expr)
        
    # === Enhanced Aggregation Methods ===
    
    def aggregate_by_groups(self, data: pl.DataFrame, 
                          value_cols: List[str],
                          weight_col: Optional[str] = None,
                          sum_cols: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Perform weighted or simple aggregation by grouping columns.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to aggregate
        value_cols : List[str]
            Columns to calculate means for
        weight_col : Optional[str], default None
            Column to use for weighted means
        sum_cols : Optional[List[str]], default None
            Columns to sum instead of average
            
        Returns
        -------
        pl.DataFrame
            Aggregated data
        """
        group_cols = self._group_cols if self._group_cols else []
        
        agg_exprs = []
        
        # Add mean calculations
        for col in value_cols:
            if col in data.columns:
                if weight_col and weight_col in data.columns:
                    # Weighted mean
                    agg_exprs.append(
                        (pl.col(col) * pl.col(weight_col)).sum() / pl.col(weight_col).sum()
                    ).alias(f"{col}_MEAN")
                else:
                    # Simple mean
                    agg_exprs.append(pl.mean(col).alias(f"{col}_MEAN"))
                    
        # Add sum calculations
        if sum_cols:
            for col in sum_cols:
                if col in data.columns:
                    agg_exprs.append(pl.sum(col).alias(f"{col}_SUM"))
                    
        # Add count
        agg_exprs.append(pl.len().alias("N"))
        
        if group_cols:
            return data.group_by(group_cols).agg(agg_exprs)
        else:
            return data.select(agg_exprs)
            
    # === Enhanced Variance Methods ===
    
    def calculate_ratio_variance(self, data: pl.DataFrame,
                               numerator_col: str,
                               denominator_col: str,
                               output_col: str) -> pl.DataFrame:
        """
        Calculate variance for ratio estimators.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with numerator and denominator columns
        numerator_col : str
            Column name for numerator
        denominator_col : str  
            Column name for denominator
        output_col : str
            Name for output variance column
            
        Returns
        -------
        pl.DataFrame
            Data with variance column added
        """
        # Import ratio_var utility if available
        try:
            from .utils import ratio_var
            return ratio_var(data, numerator_col, denominator_col, output_col)
        except ImportError:
            # Fallback to simple variance calculation
            ratio = pl.col(numerator_col) / pl.col(denominator_col)
            variance = ratio * 0.015  # Default CV
            if self.config.variance:
                return data.with_columns((variance ** 2).alias(f"{output_col}_VAR"))
            else:
                return data.with_columns(variance.alias(f"{output_col}_SE"))
                
    # === Enhanced Output Methods ===
    
    def create_standard_output(self, estimates: pl.DataFrame,
                             estimate_cols: List[str],
                             include_ci: bool = False,
                             ci_level: float = 0.95) -> pl.DataFrame:
        """
        Create standardized output format with optional confidence intervals.
        
        Parameters
        ----------
        estimates : pl.DataFrame
            Raw estimates
        estimate_cols : List[str]
            Column names of estimates
        include_ci : bool, default False
            Whether to include confidence intervals
        ci_level : float, default 0.95
            Confidence level for intervals
            
        Returns
        -------
        pl.DataFrame
            Formatted output
        """
        output = estimates.clone()
        
        # Add confidence intervals if requested
        if include_ci:
            z_score = 1.96 if ci_level == 0.95 else 2.576  # 95% or 99%
            
            for col in estimate_cols:
                se_col = f"{col}_SE"
                if col in output.columns and se_col in output.columns:
                    output = output.with_columns([
                        (pl.col(col) - z_score * pl.col(se_col)).alias(f"{col}_CI_LOWER"),
                        (pl.col(col) + z_score * pl.col(se_col)).alias(f"{col}_CI_UPPER")
                    ])
                    
        # Round numeric columns
        numeric_cols = [col for col in output.columns 
                       if output[col].dtype in [pl.Float32, pl.Float64]]
        round_exprs = [pl.col(col).round(4).alias(col) for col in numeric_cols]
        
        if round_exprs:
            output = output.with_columns(round_exprs)
            
        return output
        
    # === Common Workflow Patterns ===
    
    def standard_tree_estimation_workflow(self,
                                        tree_calc_func: Callable[[pl.DataFrame], pl.DataFrame],
                                        response_mapping: Optional[Dict[str, str]] = None) -> pl.DataFrame:
        """
        Standard workflow for tree-based estimators (volume, biomass, tpa).
        
        Parameters
        ----------
        tree_calc_func : Callable
            Function to calculate tree-level values
        response_mapping : Optional[Dict[str, str]], default None
            Override response column mapping
            
        Returns
        -------
        pl.DataFrame
            Final estimates
        """
        # Load tables
        self._load_required_tables()
        
        # Get filtered data
        tree_df, cond_df = self._get_filtered_data()
        
        # Join trees with conditions
        tree_cond = tree_df.join(
            cond_df.select(["PLT_CN", "CONDID", "CONDPROP_UNADJ"]),
            on=["PLT_CN", "CONDID"],
            how="inner"
        )
        
        # Apply tree calculation function
        tree_cond = tree_calc_func(tree_cond)
        
        # Set up grouping
        tree_cond, group_cols = setup_grouping_columns_common(
            tree_cond,
            self.config.grp_by,
            self.config.by_species,
            self.config.by_size_class,
            return_dataframe=True
        )
        self._group_cols = group_cols
        
        # Calculate plot-level estimates
        plot_est = self._calculate_plot_estimates(tree_cond)
        
        # Apply stratification
        strat_est = self._apply_stratification(plot_est)
        
        # Calculate population estimates
        pop_est = self._calculate_population_estimates(strat_est)
        
        # Format output
        return self.format_output(pop_est)
        
    def standard_area_estimation_workflow(self,
                                        area_calc_func: Callable[[pl.DataFrame], pl.DataFrame]) -> pl.DataFrame:
        """
        Standard workflow for area-based estimators.
        
        Parameters
        ----------
        area_calc_func : Callable
            Function to calculate area values
            
        Returns
        -------
        pl.DataFrame
            Final estimates
        """
        # Load tables
        self._load_required_tables()
        
        # Get filtered data
        _, cond_df = self._get_filtered_data()
        
        # Apply area calculation function
        cond_df = area_calc_func(cond_df)
        
        # Calculate plot-level estimates
        plot_est = self._calculate_plot_estimates(cond_df)
        
        # Apply stratification
        strat_est = self._apply_stratification(plot_est)
        
        # Calculate population estimates
        pop_est = self._calculate_population_estimates(strat_est)
        
        # Format output
        return self.format_output(pop_est)
