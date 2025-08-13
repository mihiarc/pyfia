"""
Mortality calculator for pyFIA.

This module implements the core mortality calculation logic following
FIA statistical methodology for tree mortality estimation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Union
import polars as pl

from ...core import FIA
from ..base import BaseEstimator, EstimatorConfig
from ..utils import ratio_var
from .variance import MortalityVarianceCalculator
from .group_handler import MortalityGroupHandler


@dataclass
class MortalityEstimatorConfig(EstimatorConfig):
    """
    Configuration specific to mortality estimation.
    
    Extends base EstimatorConfig with mortality-specific parameters.
    """
    # Mortality-specific grouping variables
    group_by_species_group: bool = False
    group_by_ownership: bool = False
    group_by_agent: bool = False
    group_by_disturbance: bool = False
    
    # Mortality type selection
    mortality_type: str = "all"  # "all", "growth", "removal"
    
    # Include component breakdowns
    include_components: bool = False
    
    def get_grouping_variables(self) -> List[str]:
        """Get list of grouping variables based on configuration."""
        groups = []
        
        # Standard grouping from base config
        if self.grp_by:
            if isinstance(self.grp_by, str):
                groups.append(self.grp_by)
            else:
                groups.extend(self.grp_by)
        
        # Species grouping
        if self.by_species:
            groups.append("SPCD")
        if self.group_by_species_group:
            groups.append("SPGRPCD")
            
        # Other groupings
        if self.group_by_ownership:
            groups.append("OWNGRPCD")
        if self.group_by_agent:
            groups.append("AGENTCD")
        if self.group_by_disturbance:
            groups.extend(["DSTRBCD1", "DSTRBCD2", "DSTRBCD3"])
            
        # Add UNITCD if specified in grp_by
        if self.grp_by and "UNITCD" in self.grp_by:
            if "UNITCD" not in groups:
                groups.append("UNITCD")
                
        return groups


class MortalityCalculator(BaseEstimator):
    """
    Calculator for FIA mortality estimation.
    
    This class implements design-based estimation of tree mortality
    following Bechtold & Patterson (2005) procedures. It supports
    various grouping variables and calculates mortality in terms of
    both trees per acre (TPA) and volume per acre.
    """
    
    def __init__(self, db: Union[str, FIA], config: MortalityEstimatorConfig):
        """
        Initialize the mortality calculator.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        config : MortalityEstimatorConfig
            Configuration for mortality estimation
        """
        super().__init__(db, config)
        self.mortality_config = config
        
        # Set up grouping columns based on configuration
        self._group_cols = config.get_grouping_variables()
        
        # Determine which mortality column to use based on tree_class and land_type
        self._mortality_col = self._get_mortality_column()
        
        # Initialize components
        self.variance_calculator = MortalityVarianceCalculator()
        self.group_handler = MortalityGroupHandler(db)
        
    def _get_mortality_column(self) -> str:
        """Determine which mortality column to use based on configuration."""
        tree_class = self.config.extra_params.get("tree_class", "all")
        land_type = self.config.land_type
        
        if tree_class == "growing_stock":
            if land_type == "forest":
                return "SUBP_TPAMORT_UNADJ_GS_FOREST"
            else:  # timber
                return "SUBP_TPAMORT_UNADJ_GS_TIMBER"
        else:  # all trees
            if land_type == "forest":
                return "SUBP_TPAMORT_UNADJ_AL_FOREST"
            else:  # timber
                return "SUBP_TPAMORT_UNADJ_AL_TIMBER"
    
    def get_required_tables(self) -> List[str]:
        """Return required database tables for mortality estimation."""
        return [
            "PLOT", 
            "COND", 
            "TREE", 
            "TREE_GRM_COMPONENT",
            "POP_STRATUM", 
            "POP_PLOT_STRATUM_ASSGN",
            "POP_ESTN_UNIT"
        ]
    
    def get_response_columns(self) -> Dict[str, str]:
        """Define mortality response columns."""
        return {
            "mortality_tpa": "MORTALITY_TPA",
            "mortality_ba": "MORTALITY_BA",
            "mortality_vol": "MORTALITY_VOL",
        }
    
    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate mortality values from GRM component data.
        
        Parameters
        ----------
        data : pl.DataFrame
            Joined data with tree, condition, and GRM component information
            
        Returns
        -------
        pl.DataFrame
            Data with calculated mortality values
        """
        # Calculate mortality TPA based on configured column
        data = data.with_columns([
            pl.col(self._mortality_col).alias("mortality_tpa"),
        ])
        
        # Calculate mortality basal area if BA column exists
        ba_col = self._mortality_col.replace("TPAMORT", "BAMORT")
        if ba_col in data.columns:
            data = data.with_columns([
                pl.col(ba_col).alias("mortality_ba")
            ])
        else:
            data = data.with_columns([
                pl.lit(0.0).alias("mortality_ba")
            ])
            
        # Calculate mortality volume if volume columns exist
        vol_col = self._mortality_col.replace("TPAMORT", "VOLMORT")
        if vol_col in data.columns:
            data = data.with_columns([
                pl.col(vol_col).alias("mortality_vol")
            ])
        else:
            data = data.with_columns([
                pl.lit(0.0).alias("mortality_vol")
            ])
        
        # Apply condition proportion adjustment
        data = data.with_columns([
            (pl.col("mortality_tpa") * pl.col("CONDPROP_UNADJ")).alias("mortality_tpa"),
            (pl.col("mortality_ba") * pl.col("CONDPROP_UNADJ")).alias("mortality_ba"),
            (pl.col("mortality_vol") * pl.col("CONDPROP_UNADJ")).alias("mortality_vol"),
        ])
        
        return data
    
    def get_output_columns(self) -> List[str]:
        """Define the output column structure for mortality estimates."""
        output_cols = ["MORTALITY_TPA"]
        
        # Add volume/BA if requested
        if self.mortality_config.include_components:
            output_cols.extend(["MORTALITY_BA", "MORTALITY_VOL"])
        
        # Add uncertainty measures
        if self.config.variance:
            output_cols.extend([
                "MORTALITY_TPA_VAR",
                "MORTALITY_BA_VAR" if self.mortality_config.include_components else None,
                "MORTALITY_VOL_VAR" if self.mortality_config.include_components else None,
            ])
        else:
            output_cols.extend([
                "MORTALITY_TPA_SE",
                "MORTALITY_BA_SE" if self.mortality_config.include_components else None,
                "MORTALITY_VOL_SE" if self.mortality_config.include_components else None,
            ])
        
        # Add totals if requested
        if self.config.totals:
            output_cols.extend([
                "MORTALITY_TPA_TOTAL",
                "MORTALITY_BA_TOTAL" if self.mortality_config.include_components else None,
                "MORTALITY_VOL_TOTAL" if self.mortality_config.include_components else None,
            ])
        
        # Add metadata
        output_cols.extend(["N_PLOTS", "YEAR"])
        
        # Filter out None values
        return [col for col in output_cols if col is not None]
    
    def _prepare_estimation_data(self, tree_df: Optional[pl.DataFrame],
                                cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Override to handle GRM component join for mortality data.
        
        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree data
        cond_df : pl.DataFrame  
            Condition data
            
        Returns
        -------
        pl.DataFrame
            Prepared data with GRM components joined
        """
        # Load GRM component data
        grm_df = self.db.tables["TREE_GRM_COMPONENT"].collect()
        
        # Filter GRM data by mortality column availability
        grm_df = grm_df.filter(
            (pl.col(self._mortality_col).is_not_null()) & 
            (pl.col(self._mortality_col) > 0)
        )
        
        # Join GRM with trees to get tree attributes
        if tree_df is not None:
            data = grm_df.join(
                tree_df.select([
                    "CN", "PLT_CN", "CONDID", "SPCD", "DIA", 
                    "SPGRPCD", "OWNGRPCD", "AGENTCD",
                    "DSTRBCD1", "DSTRBCD2", "DSTRBCD3"
                ]),
                left_on="TRE_CN",
                right_on="CN",
                how="inner"
            )
        else:
            data = grm_df
            
        # Join with conditions
        data = data.join(
            cond_df.select(["PLT_CN", "CONDID", "CONDPROP_UNADJ", "UNITCD"]),
            on=["PLT_CN", "CONDID"],
            how="inner"
        )
        
        # Apply group handler to add reference names if available
        if self._group_cols:
            data = self.group_handler.apply_grouping(
                data, 
                self._group_cols,
                include_names=True
            )
        
        return data
    
    def _calculate_plot_estimates(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate plot-level mortality estimates.
        
        Overrides base class to handle mortality-specific aggregation.
        """
        # Determine grouping columns
        plot_groups = ["PLT_CN"]
        if self._group_cols:
            # Only include grouping columns that exist in the data
            available_groups = [col for col in self._group_cols if col in data.columns]
            plot_groups.extend(available_groups)
        
        # Aggregate mortality values to plot level
        agg_exprs = [
            pl.sum("mortality_tpa").alias("PLOT_MORTALITY_TPA"),
            pl.sum("mortality_ba").alias("PLOT_MORTALITY_BA"),
            pl.sum("mortality_vol").alias("PLOT_MORTALITY_VOL"),
        ]
        
        plot_estimates = data.group_by(plot_groups).agg(agg_exprs)
        
        return plot_estimates
    
    def _calculate_population_estimates(self, expanded_data: pl.DataFrame) -> pl.DataFrame:
        """
        Override to use mortality-specific variance calculation.
        """
        # First get base population estimates
        pop_estimates = super()._calculate_population_estimates(expanded_data)
        
        # If variance calculation is requested, use proper stratified variance
        if self.config.variance or not self.config.variance:  # Always calculate proper SE
            # Prepare data for variance calculation
            plot_data_with_strat = expanded_data
            
            # Calculate variance for each response variable
            for response_col in ["MORTALITY_TPA", "MORTALITY_BA", "MORTALITY_VOL"]:
                plot_col = f"PLOT_{response_col}"
                if plot_col in plot_data_with_strat.columns:
                    # Calculate stratum variance
                    stratum_var = self.variance_calculator.calculate_stratum_variance(
                        plot_data_with_strat,
                        plot_col,
                        self._group_cols
                    )
                    
                    # Calculate population variance
                    pop_var = self.variance_calculator.calculate_population_variance(
                        stratum_var,
                        plot_col,
                        self._group_cols
                    )
                    
                    # Merge variance results back
                    join_cols = self._group_cols if self._group_cols else []
                    if join_cols:
                        pop_estimates = pop_estimates.join(
                            pop_var.select(join_cols + [
                                f"{response_col}_VAR",
                                f"{response_col}_SE", 
                                f"{response_col}_CV"
                            ]),
                            on=join_cols,
                            how="left"
                        )
                    else:
                        # No grouping columns, just add the variance columns
                        for col in [f"{response_col}_VAR", f"{response_col}_SE", f"{response_col}_CV"]:
                            if col in pop_var.columns:
                                pop_estimates = pop_estimates.with_columns(
                                    pl.lit(pop_var[col][0]).alias(col)
                                )
        
        return pop_estimates
    
    def calculate_variance(self, data: pl.DataFrame, estimate_col: str) -> pl.DataFrame:
        """
        Calculate variance or standard error for mortality estimates.
        
        This implements proper stratified variance calculation for
        mortality estimates.
        """
        # For simple per-acre estimates, use base class method for now
        # Full stratified variance will be implemented in _calculate_population_estimates
        return super().calculate_variance(data, estimate_col)
    
    def format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """Format output to match expected structure."""
        output_cols = []
        
        # Add grouping columns first
        if self._group_cols:
            available_groups = [col for col in self._group_cols if col in estimates.columns]
            output_cols.extend(available_groups)
        
        # Add primary estimates
        output_cols.append("MORTALITY_TPA")
        
        if self.mortality_config.include_components:
            output_cols.extend(["MORTALITY_BA", "MORTALITY_VOL"])
        
        # Add uncertainty measures
        if self.config.variance:
            output_cols.append("MORTALITY_TPA_VAR")
            if self.mortality_config.include_components:
                output_cols.extend(["MORTALITY_BA_VAR", "MORTALITY_VOL_VAR"])
        else:
            output_cols.append("MORTALITY_TPA_SE")
            if self.mortality_config.include_components:
                output_cols.extend(["MORTALITY_BA_SE", "MORTALITY_VOL_SE"])
        
        # Add totals if requested
        if self.config.totals:
            output_cols.append("MORTALITY_TPA_TOTAL")
            if self.mortality_config.include_components:
                output_cols.extend(["MORTALITY_BA_TOTAL", "MORTALITY_VOL_TOTAL"])
        
        # Add metadata
        output_cols.extend(["N_PLOTS", "YEAR"])
        
        # Select only columns that exist
        available_cols = [col for col in output_cols if col in estimates.columns]
        
        return estimates.select(available_cols)