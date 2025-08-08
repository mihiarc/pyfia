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
from typing import Any, Dict, List, Optional, Union

import polars as pl

from ..core import FIA
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
        ppsa = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(self.db.evalid)
                   if self.db.evalid else pl.lit(True))
            .collect()
        )

        pop_stratum = self.db.tables["POP_STRATUM"].collect()

        # Prepare stratification data
        strat_df = self.prepare_stratification_data(ppsa, pop_stratum)

        # Join plot data with stratification, keep STRATUM_CN to aggregate by stratum
        plot_with_strat = plot_data.join(
            strat_df.select(["PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP"]),
            on="PLT_CN",
            how="inner"
        )

        # Cast factors to float to avoid decimal precision issues
        plot_with_strat = plot_with_strat.with_columns([
            pl.col("ADJ_FACTOR_SUBP").cast(pl.Float64),
            pl.col("EXPNS").cast(pl.Float64),
        ])

        # Compute ratio-of-means within stratum: y_bar_h for each plot metric
        response_cols = self.get_response_columns()
        strat_group_cols = ["STRATUM_CN"]
        if self._group_cols:
            strat_group_cols.extend(self._group_cols)

        agg_exprs = [
            pl.len().alias("n_h"),
            pl.col("EXPNS").first().cast(pl.Float64).alias("A_h"),
            pl.col("ADJ_FACTOR_SUBP").first().cast(pl.Float64).alias("ADJ_h"),
        ]
        for _, output_name in response_cols.items():
            plot_col = f"PLOT_{output_name}"
            if plot_col in plot_with_strat.columns:
                agg_exprs.append(pl.mean(plot_col).alias(f"ybar_{output_name}"))

        stratum_est = plot_with_strat.group_by(strat_group_cols).agg(agg_exprs)

        # Convert to TOTAL per stratum: TOTAL = ybar_h * ADJ_h * A_h
        total_exprs = []
        for _, output_name in response_cols.items():
            ybar_col = f"ybar_{output_name}"
            if ybar_col in stratum_est.columns:
                total_exprs.append(
                    (pl.col(ybar_col) * pl.col("ADJ_h") * pl.col("A_h")).alias(f"TOTAL_{output_name}")
                )

        stratum_est = stratum_est.with_columns(total_exprs)

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
        # Aggregate by grouping columns (sum totals already expanded by EXPNS upstream for volume)
        response_cols = self.get_response_columns()
        agg_exprs = []

        for _, output_name in response_cols.items():
            total_col = f"TOTAL_{output_name}"
            if total_col in expanded_data.columns:
                agg_exprs.append(pl.sum(total_col).alias(f"POP_{output_name}"))

        agg_exprs.append(pl.len().alias("nPlots"))

        # Perform aggregation
        if self._group_cols:
            pop_estimates = expanded_data.group_by(self._group_cols).agg(agg_exprs)
        else:
            pop_estimates = expanded_data.select(agg_exprs)

        # Calculate per-acre values using ratio-of-means: sum(TOTAL)/sum(A_h)
        denom = (
            expanded_data.select(pl.col("EXPNS").cast(pl.Float64).sum()).item()
            if "EXPNS" in expanded_data.columns
            else None
        )
        per_acre_exprs = []
        for _, output_name in response_cols.items():
            pop_col = f"POP_{output_name}"
            if pop_col in pop_estimates.columns and denom:
                per_acre_exprs.append(
                    (pl.col(pop_col).cast(pl.Float64) / float(denom)).alias(output_name)
                )

        if per_acre_exprs:
            pop_estimates = pop_estimates.with_columns(per_acre_exprs)

        # Add variance/SE for each estimate
        for _, output_name in response_cols.items():
            if output_name in pop_estimates.columns:
                pop_estimates = self.calculate_variance(pop_estimates, output_name)

        # Add metadata columns
        pop_estimates = pop_estimates.with_columns([
            pl.lit(self._get_year()).alias("YEAR"),
            pl.col("nPlots").alias("N"),
            pl.col("nPlots").alias("nPlots_TREE"),
            pl.col("nPlots").alias("nPlots_AREA")
        ])

        # Add totals if requested
        if self.config.totals:
            total_exprs = []
            for _, output_name in response_cols.items():
                pop_col = f"POP_{output_name}"
                if pop_col in pop_estimates.columns:
                    total_exprs.append(
                        pl.col(pop_col).alias(f"{output_name}_TOTAL")
                    )
            if total_exprs:
                pop_estimates = pop_estimates.with_columns(total_exprs)

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
