"""
Stratification and variance calculation steps for FIA estimation pipeline.

This module implements pipeline steps for applying FIA post-stratification,
calculating expansion factors, and computing variance estimates following
Bechtold & Patterson (2005) design-based estimation methods.

Steps include:
- ApplyStratificationStep: Apply FIA stratification from POP_PLOT_STRATUM_ASSGN
- CalculateExpansionFactorsStep: Calculate plot expansion factors
- CalculateVarianceStep: Calculate variance using design-based methods
- CalculateStandardErrorStep: Calculate standard errors from variance
- CalculatePopulationTotalsStep: Calculate population-level totals

All steps follow FIA statistical methodology for proper post-stratified
estimation with ratio-of-means estimators.
"""

from typing import Dict, List, Optional, Set, Type, Union
import warnings

import polars as pl
from pydantic import Field

from ....core import FIA
from ....filters.common import apply_area_filters_common
from ...config import EstimatorConfig
from ...lazy_evaluation import LazyFrameWrapper
from ...statistics.variance_calculator import VarianceCalculator
from ...statistics.expressions import safe_divide

from ..base_steps import BaseEstimationStep
from ..core import (
    ExecutionContext,
    PipelineException,
    StepValidationError
)
from ..contracts import (
    PlotEstimatesContract,
    StratifiedEstimatesContract,
    PopulationEstimatesContract
)


class ApplyStratificationStep(BaseEstimationStep[PlotEstimatesContract, StratifiedEstimatesContract]):
    """
    Apply FIA stratification and expansion factors to plot estimates.
    
    This step assigns plots to strata using the POP_PLOT_STRATUM_ASSGN table
    and calculates appropriate expansion factors for post-stratified estimation.
    It handles both standard post-stratification and special cases like
    growth/removal/mortality evaluations.
    """
    
    def __init__(
        self,
        db: FIA,
        evalid: Optional[Union[int, List[int]]] = None,
        stratification_method: str = "post",
        use_adjustment_factors: bool = True,
        **kwargs
    ):
        """
        Initialize stratification step.
        
        Parameters
        ----------
        db : FIA
            Database connection for loading stratification data
        evalid : Optional[Union[int, List[int]]]
            EVALID filter for stratification
        stratification_method : str
            Stratification method (post, pre)
        use_adjustment_factors : bool
            Whether to apply adjustment factors
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.db = db
        self.evalid = evalid
        self.stratification_method = stratification_method
        self.use_adjustment_factors = use_adjustment_factors
        self.variance_calculator = VarianceCalculator()
    
    def get_input_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def get_output_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def execute_step(
        self,
        input_data: PlotEstimatesContract,
        context: ExecutionContext
    ) -> StratifiedEstimatesContract:
        """
        Apply stratification to plot estimates.
        
        Parameters
        ----------
        input_data : PlotEstimatesContract
            Plot-level estimates to stratify
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        StratifiedEstimatesContract
            Stratified estimates with expansion factors
        """
        try:
            # Load stratification assignments
            stratum_data = self._load_stratum_assignments()
            
            # Join plot estimates with stratum assignments
            stratified_data = self._join_with_stratum(input_data.data, stratum_data)
            
            # Calculate expansion factors
            expanded_data = self._calculate_expansion_factors(stratified_data)
            
            # Apply adjustment factors if requested
            if self.use_adjustment_factors:
                expanded_data = self._apply_adjustment_factors(expanded_data)
            
            # Create output contract
            output = StratifiedEstimatesContract(
                data=expanded_data,
                expansion_columns=["EXPNS", "ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"],
                stratum_columns=["ESTN_UNIT", "STRATUM"],
                estimate_columns=input_data.estimate_columns,
                stratification_method=self.stratification_method,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("evalid", self.evalid)
            output.add_processing_metadata("stratification_method", self.stratification_method)
            
            # Count strata
            if isinstance(expanded_data.frame, pl.LazyFrame):
                strata_count = (
                    expanded_data.frame
                    .select([pl.col("ESTN_UNIT"), pl.col("STRATUM")])
                    .unique()
                    .select(pl.count())
                    .collect()
                    .item()
                )
            else:
                strata_count = expanded_data.frame[["ESTN_UNIT", "STRATUM"]].unique().height
            
            output.strata_count = strata_count
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply stratification: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _load_stratum_assignments(self) -> LazyFrameWrapper:
        """Load POP_PLOT_STRATUM_ASSGN table with appropriate filters."""
        reader = self.db.data_reader
        
        # Build WHERE clause for EVALID if needed
        where_clause = None
        if self.evalid:
            if isinstance(self.evalid, list):
                evalid_str = ",".join(map(str, self.evalid))
                where_clause = f"EVALID IN ({evalid_str})"
            else:
                where_clause = f"EVALID = {self.evalid}"
        
        # Load stratum assignments
        stratum_data = reader.load_table("POP_PLOT_STRATUM_ASSGN", where_clause=where_clause)
        
        # Select relevant columns
        stratum_frame = stratum_data.select([
            pl.col("PLT_CN"),
            pl.col("EVALID"),
            pl.col("ESTN_UNIT"),
            pl.col("STRATUM"),
            pl.col("P1POINTCNT"),
            pl.col("P1PNTCNT_EU"),
            pl.col("P2POINTCNT"),
            pl.col("P2PNTCNT_EU"),
            pl.col("EXPNS"),
            pl.col("ADJ_FACTOR_MICR"),
            pl.col("ADJ_FACTOR_SUBP"),
            pl.col("ADJ_FACTOR_MACR")
        ])
        
        return LazyFrameWrapper(stratum_frame)
    
    def _join_with_stratum(
        self,
        plot_data: LazyFrameWrapper,
        stratum_data: LazyFrameWrapper
    ) -> LazyFrameWrapper:
        """Join plot estimates with stratum assignments."""
        joined_frame = plot_data.frame.join(
            stratum_data.frame,
            on="PLT_CN",
            how="inner"
        )
        return LazyFrameWrapper(joined_frame)
    
    def _calculate_expansion_factors(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Calculate appropriate expansion factors for each plot."""
        # Base expansion is EXPNS from POP_PLOT_STRATUM_ASSGN
        # Additional adjustments may be needed based on estimation type
        expanded_frame = data.frame.with_columns([
            # Ensure expansion factor is present and valid
            pl.when(pl.col("EXPNS").is_null() | (pl.col("EXPNS") <= 0))
            .then(pl.lit(1.0))
            .otherwise(pl.col("EXPNS"))
            .alias("EXPNS")
        ])
        
        return LazyFrameWrapper(expanded_frame)
    
    def _apply_adjustment_factors(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Apply microplot, subplot, and macroplot adjustment factors."""
        adjusted_frame = data.frame.with_columns([
            # Apply adjustment factors with proper null handling
            (pl.col("EXPNS") * 
             pl.coalesce(pl.col("ADJ_FACTOR_MICR"), 1.0) *
             pl.coalesce(pl.col("ADJ_FACTOR_SUBP"), 1.0) *
             pl.coalesce(pl.col("ADJ_FACTOR_MACR"), 1.0))
            .alias("EXPNS_ADJUSTED")
        ])
        
        return LazyFrameWrapper(adjusted_frame)


class CalculateVarianceStep(BaseEstimationStep[StratifiedEstimatesContract, StratifiedEstimatesContract]):
    """
    Calculate variance using FIA design-based methods.
    
    This step calculates variance estimates for stratified sampling following
    Bechtold & Patterson (2005). It handles both simple variance for totals
    and ratio variance for per-acre estimates using the delta method.
    """
    
    def __init__(
        self,
        estimation_type: str = "total",
        confidence_level: float = 0.68,
        use_finite_population_correction: bool = True,
        **kwargs
    ):
        """
        Initialize variance calculation step.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation (total, ratio)
        confidence_level : float
            Confidence level for intervals (default 0.68 for 1 SE)
        use_finite_population_correction : bool
            Whether to apply finite population correction
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.estimation_type = estimation_type
        self.confidence_level = confidence_level
        self.use_finite_population_correction = use_finite_population_correction
        self.variance_calculator = VarianceCalculator()
    
    def get_input_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def get_output_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def execute_step(
        self,
        input_data: StratifiedEstimatesContract,
        context: ExecutionContext
    ) -> StratifiedEstimatesContract:
        """
        Calculate variance for stratified estimates.
        
        Parameters
        ----------
        input_data : StratifiedEstimatesContract
            Stratified estimates
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        StratifiedEstimatesContract
            Estimates with variance columns added
        """
        try:
            # Calculate stratum-level statistics
            stratum_stats = self._calculate_stratum_statistics(
                input_data.data,
                input_data.estimate_columns
            )
            
            # Calculate variance components
            if self.estimation_type == "ratio":
                variance_data = self._calculate_ratio_variance(
                    stratum_stats,
                    input_data.estimate_columns
                )
            else:
                variance_data = self._calculate_total_variance(
                    stratum_stats,
                    input_data.estimate_columns
                )
            
            # Apply finite population correction if requested
            if self.use_finite_population_correction:
                variance_data = self._apply_fpc(variance_data)
            
            # Add variance columns to output
            output = StratifiedEstimatesContract(
                data=variance_data,
                expansion_columns=input_data.expansion_columns,
                stratum_columns=input_data.stratum_columns,
                estimate_columns=input_data.estimate_columns,
                stratification_method=input_data.stratification_method,
                strata_count=input_data.strata_count,
                step_id=self.step_id
            )
            
            # Add variance column names
            variance_columns = [f"{col}_VAR" for col in input_data.estimate_columns]
            output.add_processing_metadata("variance_columns", variance_columns)
            output.add_processing_metadata("confidence_level", self.confidence_level)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate variance: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _calculate_stratum_statistics(
        self,
        data: LazyFrameWrapper,
        estimate_columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate within-stratum statistics needed for variance."""
        # Group by stratum and calculate statistics
        agg_exprs = []
        for col in estimate_columns:
            agg_exprs.extend([
                pl.col(col).count().alias(f"n_{col}"),
                pl.col(col).mean().alias(f"mean_{col}"),
                pl.col(col).std().alias(f"std_{col}"),
                pl.col(col).sum().alias(f"sum_{col}")
            ])
        
        # Add stratum size and weight calculations
        agg_exprs.extend([
            pl.col("P1PNTCNT_EU").first().alias("N_h"),  # Total points in stratum
            pl.col("P1POINTCNT").count().alias("n_h"),   # Sampled points in stratum
            (pl.col("P1PNTCNT_EU").first() / pl.col("P1PNTCNT_EU").sum())
            .alias("w_h")  # Stratum weight
        ])
        
        stats_frame = data.frame.group_by(["ESTN_UNIT", "STRATUM"]).agg(agg_exprs)
        
        return LazyFrameWrapper(stats_frame)
    
    def _calculate_total_variance(
        self,
        stratum_stats: LazyFrameWrapper,
        estimate_columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate variance for population totals."""
        variance_exprs = []
        
        for col in estimate_columns:
            # Variance of total = sum over strata of w_h^2 * s_h^2 / n_h
            var_expr = (
                pl.when(pl.col("n_h") > 1)
                .then(
                    pl.col("w_h").pow(2) * 
                    pl.col(f"std_{col}").pow(2) / 
                    pl.col("n_h")
                )
                .otherwise(0.0)
            ).alias(f"{col}_VAR")
            
            variance_exprs.append(var_expr)
        
        variance_frame = stratum_stats.frame.with_columns(variance_exprs)
        return LazyFrameWrapper(variance_frame)
    
    def _calculate_ratio_variance(
        self,
        stratum_stats: LazyFrameWrapper,
        estimate_columns: List[str]
    ) -> LazyFrameWrapper:
        """Calculate variance for ratio estimates using delta method."""
        # For ratio estimates, we need covariance terms
        # This is a simplified implementation - full version would calculate
        # covariances between numerator and denominator within strata
        
        variance_exprs = []
        
        for col in estimate_columns:
            # Simplified ratio variance calculation
            # In practice, would need numerator/denominator decomposition
            var_expr = self.variance_calculator.variance_component(col).alias(f"{col}_VAR")
            variance_exprs.append(var_expr)
        
        variance_frame = stratum_stats.frame.with_columns(variance_exprs)
        return LazyFrameWrapper(variance_frame)
    
    def _apply_fpc(self, data: LazyFrameWrapper) -> LazyFrameWrapper:
        """Apply finite population correction to variance estimates."""
        fpc_frame = data.frame.with_columns([
            # FPC = (N_h - n_h) / N_h
            ((pl.col("N_h") - pl.col("n_h")) / pl.col("N_h"))
            .alias("fpc")
        ])
        
        # Apply FPC to variance columns
        variance_cols = [col for col in fpc_frame.collect_schema().names() if col.endswith("_VAR")]
        for var_col in variance_cols:
            fpc_frame = fpc_frame.with_columns([
                (pl.col(var_col) * pl.col("fpc")).alias(var_col)
            ])
        
        return LazyFrameWrapper(fpc_frame)


class CalculateStandardErrorStep(BaseEstimationStep[StratifiedEstimatesContract, StratifiedEstimatesContract]):
    """
    Calculate standard errors from variance estimates.
    
    This step converts variance estimates to standard errors and calculates
    related statistics like coefficient of variation and sampling error percentage.
    """
    
    def __init__(
        self,
        calculate_cv: bool = True,
        calculate_sampling_error: bool = True,
        **kwargs
    ):
        """
        Initialize standard error calculation step.
        
        Parameters
        ----------
        calculate_cv : bool
            Whether to calculate coefficient of variation
        calculate_sampling_error : bool
            Whether to calculate sampling error percentage
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.calculate_cv = calculate_cv
        self.calculate_sampling_error = calculate_sampling_error
    
    def get_input_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def get_output_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def execute_step(
        self,
        input_data: StratifiedEstimatesContract,
        context: ExecutionContext
    ) -> StratifiedEstimatesContract:
        """
        Calculate standard errors from variance.
        
        Parameters
        ----------
        input_data : StratifiedEstimatesContract
            Estimates with variance
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        StratifiedEstimatesContract
            Estimates with standard error columns added
        """
        try:
            # Get variance columns from metadata
            variance_columns = input_data.get_processing_metadata("variance_columns", [])
            
            # Calculate standard errors
            se_exprs = []
            for var_col in variance_columns:
                # Extract base column name
                base_col = var_col.replace("_VAR", "")
                
                # Standard error = sqrt(variance)
                se_exprs.append(
                    pl.col(var_col).sqrt().alias(f"{base_col}_SE")
                )
                
                # Coefficient of variation if requested
                if self.calculate_cv:
                    se_exprs.append(
                        safe_divide(
                            pl.col(f"{base_col}_SE"),
                            pl.col(base_col)
                        ).alias(f"{base_col}_CV")
                    )
                
                # Sampling error percentage if requested
                if self.calculate_sampling_error:
                    se_exprs.append(
                        (safe_divide(
                            pl.col(f"{base_col}_SE"),
                            pl.col(base_col)
                        ) * 100).alias(f"{base_col}_SAMPLING_ERROR_PCT")
                    )
            
            # Apply calculations
            se_data = LazyFrameWrapper(
                input_data.data.frame.with_columns(se_exprs)
            )
            
            # Create output contract
            output = StratifiedEstimatesContract(
                data=se_data,
                expansion_columns=input_data.expansion_columns,
                stratum_columns=input_data.stratum_columns,
                estimate_columns=input_data.estimate_columns,
                stratification_method=input_data.stratification_method,
                strata_count=input_data.strata_count,
                step_id=self.step_id
            )
            
            # Add SE column names to metadata
            se_columns = [f"{col}_SE" for col in input_data.estimate_columns]
            output.add_processing_metadata("se_columns", se_columns)
            output.add_processing_metadata("variance_columns", variance_columns)
            
            if self.calculate_cv:
                cv_columns = [f"{col}_CV" for col in input_data.estimate_columns]
                output.add_processing_metadata("cv_columns", cv_columns)
            
            if self.calculate_sampling_error:
                sampling_error_columns = [f"{col}_SAMPLING_ERROR_PCT" for col in input_data.estimate_columns]
                output.add_processing_metadata("sampling_error_columns", sampling_error_columns)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate standard errors: {e}",
                step_id=self.step_id,
                cause=e
            )


class CalculatePopulationTotalsStep(BaseEstimationStep[StratifiedEstimatesContract, PopulationEstimatesContract]):
    """
    Calculate population-level total estimates from stratified data.
    
    This step aggregates stratified plot-level estimates to population totals,
    applying expansion factors and calculating associated variance estimates.
    It handles both total and ratio estimation approaches.
    """
    
    def __init__(
        self,
        estimation_method: str = "post_stratified",
        include_totals: bool = True,
        group_columns: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize population totals calculation step.
        
        Parameters
        ----------
        estimation_method : str
            Statistical estimation method
        include_totals : bool
            Whether to include total estimates
        group_columns : Optional[List[str]]
            Columns to group by for estimates
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.estimation_method = estimation_method
        self.include_totals = include_totals
        self.group_columns = group_columns or []
    
    def get_input_contract(self) -> Type[StratifiedEstimatesContract]:
        return StratifiedEstimatesContract
    
    def get_output_contract(self) -> Type[PopulationEstimatesContract]:
        return PopulationEstimatesContract
    
    def execute_step(
        self,
        input_data: StratifiedEstimatesContract,
        context: ExecutionContext
    ) -> PopulationEstimatesContract:
        """
        Calculate population totals from stratified estimates.
        
        Parameters
        ----------
        input_data : StratifiedEstimatesContract
            Stratified plot estimates
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        PopulationEstimatesContract
            Population-level estimates
        """
        try:
            # Build aggregation expressions for population totals
            agg_exprs = self._build_aggregation_expressions(input_data)
            
            # Group and aggregate to population level
            if self.group_columns:
                pop_data = self._aggregate_with_groups(input_data.data, agg_exprs)
            else:
                pop_data = self._aggregate_overall(input_data.data, agg_exprs)
            
            # Add metadata columns
            pop_data = self._add_metadata_columns(pop_data, input_data)
            
            # Create output contract
            output = PopulationEstimatesContract(
                data=pop_data,
                estimate_columns=input_data.estimate_columns,
                variance_columns=[f"{col}_VAR" for col in input_data.estimate_columns],
                total_columns=[f"{col}_TOTAL" for col in input_data.estimate_columns] if self.include_totals else [],
                estimation_method=self.estimation_method,
                step_id=self.step_id
            )
            
            # Copy over confidence level from variance calculation
            confidence_level = input_data.get_processing_metadata("confidence_level", 0.68)
            output.confidence_level = confidence_level
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to calculate population totals: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _build_aggregation_expressions(
        self,
        input_data: StratifiedEstimatesContract
    ) -> List[pl.Expr]:
        """Build expressions for population-level aggregation."""
        agg_exprs = []
        
        # Get column lists from metadata
        se_columns = input_data.get_processing_metadata("se_columns", [])
        variance_columns = input_data.get_processing_metadata("variance_columns", [])
        
        for col in input_data.estimate_columns:
            # Sum expanded estimates for population total
            agg_exprs.append(
                (pl.col(col) * pl.col("EXPNS")).sum().alias(f"{col}_TOTAL")
            )
            
            # Mean for per-unit estimates
            agg_exprs.append(
                pl.col(col).mean().alias(col)
            )
            
            # Aggregate variance (sum of variance components)
            if f"{col}_VAR" in variance_columns:
                agg_exprs.append(
                    pl.col(f"{col}_VAR").sum().alias(f"{col}_VAR")
                )
            
            # Aggregate standard error
            if f"{col}_SE" in se_columns:
                # SE of sum = sqrt(sum of variances)
                agg_exprs.append(
                    pl.col(f"{col}_VAR").sum().sqrt().alias(f"{col}_SE")
                )
        
        # Add sample size information
        agg_exprs.extend([
            pl.col("PLT_CN").n_unique().alias("NPLOTS_SAMPLED"),
            pl.col("P1PNTCNT_EU").sum().alias("NPLOTS_TOTAL")
        ])
        
        return agg_exprs
    
    def _aggregate_with_groups(
        self,
        data: LazyFrameWrapper,
        agg_exprs: List[pl.Expr]
    ) -> LazyFrameWrapper:
        """Aggregate with grouping columns."""
        grouped_frame = data.frame.group_by(self.group_columns).agg(agg_exprs)
        return LazyFrameWrapper(grouped_frame)
    
    def _aggregate_overall(
        self,
        data: LazyFrameWrapper,
        agg_exprs: List[pl.Expr]
    ) -> LazyFrameWrapper:
        """Aggregate overall without groups."""
        # Use select instead of group_by for overall aggregation
        aggregated_frame = data.frame.select(agg_exprs)
        return LazyFrameWrapper(aggregated_frame)
    
    def _add_metadata_columns(
        self,
        data: LazyFrameWrapper,
        input_data: StratifiedEstimatesContract
    ) -> LazyFrameWrapper:
        """Add metadata columns to population estimates."""
        metadata_frame = data.frame.with_columns([
            pl.lit(self.estimation_method).alias("ESTIMATION_METHOD"),
            pl.lit(input_data.stratification_method).alias("STRATIFICATION_METHOD"),
            pl.lit(input_data.strata_count).alias("NSTRATA")
        ])
        
        return LazyFrameWrapper(metadata_frame)


class ApplyExpansionFactorsStep(BaseEstimationStep[PlotEstimatesContract, PlotEstimatesContract]):
    """
    Apply plot expansion factors for population estimates.
    
    This step applies the appropriate expansion factors to plot-level estimates
    to scale them to population totals. It handles different plot types and
    adjustment factors for microplots, subplots, and macroplots.
    """
    
    def __init__(
        self,
        expansion_factor_column: str = "EXPNS",
        apply_adjustments: bool = True,
        **kwargs
    ):
        """
        Initialize expansion factor application step.
        
        Parameters
        ----------
        expansion_factor_column : str
            Name of the expansion factor column
        apply_adjustments : bool
            Whether to apply subplot/microplot adjustments
        **kwargs
            Additional arguments passed to parent
        """
        super().__init__(**kwargs)
        self.expansion_factor_column = expansion_factor_column
        self.apply_adjustments = apply_adjustments
    
    def get_input_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def get_output_contract(self) -> Type[PlotEstimatesContract]:
        return PlotEstimatesContract
    
    def execute_step(
        self,
        input_data: PlotEstimatesContract,
        context: ExecutionContext
    ) -> PlotEstimatesContract:
        """
        Apply expansion factors to plot estimates.
        
        Parameters
        ----------
        input_data : PlotEstimatesContract
            Plot-level estimates
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        PlotEstimatesContract
            Estimates with expansion factors applied
        """
        try:
            # Check for expansion factor column
            if isinstance(input_data.data.frame, pl.LazyFrame):
                available_cols = set(input_data.data.frame.collect_schema().names())
            else:
                available_cols = set(input_data.data.frame.columns)
            
            if self.expansion_factor_column not in available_cols:
                raise StepValidationError(
                    f"Expansion factor column '{self.expansion_factor_column}' not found",
                    step_id=self.step_id
                )
            
            # Apply expansion factors to estimate columns
            expansion_exprs = []
            for col in input_data.estimate_columns:
                expansion_exprs.append(
                    (pl.col(col) * pl.col(self.expansion_factor_column))
                    .alias(f"{col}_EXPANDED")
                )
            
            # Apply adjustments if present and requested
            if self.apply_adjustments:
                expansion_exprs = self._add_adjustment_expressions(
                    expansion_exprs,
                    available_cols
                )
            
            # Apply expansions
            expanded_data = LazyFrameWrapper(
                input_data.data.frame.with_columns(expansion_exprs)
            )
            
            # Create output contract with expanded columns
            output = PlotEstimatesContract(
                data=expanded_data,
                estimate_columns=[f"{col}_EXPANDED" for col in input_data.estimate_columns],
                estimate_type=input_data.estimate_type,
                group_columns=input_data.group_columns,
                aggregation_method=input_data.aggregation_method,
                plots_processed=input_data.plots_processed,
                step_id=self.step_id
            )
            
            # Add metadata about expansion
            output.add_processing_metadata("expansion_applied", True)
            output.add_processing_metadata("expansion_factor_column", self.expansion_factor_column)
            output.add_processing_metadata("adjustments_applied", self.apply_adjustments)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply expansion factors: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _add_adjustment_expressions(
        self,
        expressions: List[pl.Expr],
        available_cols: Set[str]
    ) -> List[pl.Expr]:
        """Add adjustment factor expressions if columns are available."""
        adjustment_cols = [
            "ADJ_FACTOR_MICR",
            "ADJ_FACTOR_SUBP", 
            "ADJ_FACTOR_MACR"
        ]
        
        # Check which adjustment columns are available
        available_adjustments = [col for col in adjustment_cols if col in available_cols]
        
        if available_adjustments:
            # Create combined adjustment factor
            adj_expr = pl.lit(1.0)
            for adj_col in available_adjustments:
                adj_expr = adj_expr * pl.coalesce(pl.col(adj_col), 1.0)
            
            # Apply to expanded columns
            adjusted_expressions = []
            for expr in expressions:
                # Extract the alias from the expression
                if hasattr(expr, 'meta') and hasattr(expr.meta, 'output_name'):
                    col_name = expr.meta.output_name()
                else:
                    # Fallback to string parsing
                    col_name = str(expr).split(".")[-1].replace("_EXPANDED", "_ADJUSTED")
                
                adjusted_expressions.append(
                    (expr * adj_expr).alias(col_name)
                )
            
            return adjusted_expressions
        
        return expressions


# Export all stratification step classes
__all__ = [
    "ApplyStratificationStep",
    "CalculateVarianceStep",
    "CalculateStandardErrorStep",
    "CalculatePopulationTotalsStep",
    "ApplyExpansionFactorsStep",
]