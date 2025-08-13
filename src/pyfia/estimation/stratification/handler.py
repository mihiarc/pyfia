"""
Stratification handling for FIA area estimation.

This module provides the AreaStratificationHandler class which manages
the complex logic for applying FIA stratification including proper
adjustment factor selection based on PROP_BASIS and expansion factor
application.
"""

from typing import Optional, Dict, Any, Union
from enum import Enum

import polars as pl

from ...core import FIA


class PropBasis(str, Enum):
    """Enumeration for PROP_BASIS values."""
    SUBPLOT = "SUBP"
    MACROPLOT = "MACR"


class AreaStratificationHandler:
    """
    Handles stratification application for FIA area estimation.
    
    This class encapsulates the complex logic for applying FIA stratification
    to area estimation data, including:
    - Plot-stratum assignment joining
    - Proper adjustment factor selection based on PROP_BASIS
    - Expansion factor application with area calculations
    - Stratified sampling expansion
    
    The handler manages the relationship between plot-level estimates and
    population-level expansion through proper stratification procedures.
    """
    
    def __init__(self, db: Union[str, FIA]):
        """
        Initialize the stratification handler.
        
        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        """
        if isinstance(db, str):
            self.db = FIA(db)
        else:
            self.db = db
    
    def prepare_stratification_data(
        self, 
        ppsa_df: pl.DataFrame,
        pop_stratum_df: pl.DataFrame,
        pop_eu_df: Optional[pl.DataFrame] = None
    ) -> pl.DataFrame:
        """
        Prepare stratification data with FIA design factors for rFIA-compatible variance calculations.
        
        This method joins plot-stratum assignments with population stratum and estimation unit
        data to create a complete stratification dataset that includes all FIA design factors
        needed for proper variance calculations following rFIA methodology.
        
        Parameters
        ----------
        ppsa_df : pl.DataFrame
            Plot-stratum assignments (POP_PLOT_STRATUM_ASSGN)
        pop_stratum_df : pl.DataFrame
            Population stratum data (POP_STRATUM)
        pop_eu_df : Optional[pl.DataFrame], default None
            Population estimation unit data (POP_ESTN_UNIT). If None, will be loaded from database.
            
        Returns
        -------
        pl.DataFrame
            Stratification data with complete FIA design factors
        """
        # Load estimation unit data if not provided
        if pop_eu_df is None:
            pop_eu_df = self._load_population_estimation_units()
        
        # Join stratum data with estimation unit data to get design factors
        stratum_with_eu = pop_stratum_df.join(
            pop_eu_df.select([
                "CN", "AREA_USED", "P1PNTCNT_EU"
            ]).rename({"CN": "ESTN_UNIT_CN"}),
            on="ESTN_UNIT_CN",
            how="inner"
        ).with_columns([
            # Calculate stratum weight as in rFIA: P1POINTCNT / P1PNTCNT_EU
            # Note: Using P1POINTCNT since P1POINTCNT_INVYR is not available in this database
            (pl.col("P1POINTCNT") / pl.col("P1PNTCNT_EU")).alias("STRATUM_WGT")
        ])
        
        # Join plot-stratum assignments with complete design factors
        # Note: PPSA has ESTN_UNIT, we add ESTN_UNIT_CN from stratum data for rFIA variance calculations
        return ppsa_df.join(
            stratum_with_eu.select([
                "CN", "ESTN_UNIT_CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", 
                "P2POINTCNT", "AREA_USED", "P1PNTCNT_EU", "STRATUM_WGT"
            ]).rename({"CN": "STRATUM_CN"}),
            on="STRATUM_CN",
            how="inner"
        )
    
    def apply_stratification(self, plot_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply stratification with proper adjustment factor selection.
        
        This is the main method that applies FIA stratification to plot-level
        area estimates. It handles:
        - Loading stratification data from the database
        - Joining plot data with stratification information
        - Selecting appropriate adjustment factors based on PROP_BASIS
        - Calculating expansion factors and area estimates
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level estimates with PROP_BASIS information
            
        Returns
        -------
        pl.DataFrame
            Data with expansion factors applied and area calculations
        """
        # Load stratification data
        ppsa = self._load_plot_stratum_assignments()
        pop_stratum = self._load_population_strata()
        pop_eu = self._load_population_estimation_units()
        
        # Prepare stratification with complete FIA design factors
        strat_df = self.prepare_stratification_data(ppsa, pop_stratum, pop_eu)
        
        # Join plot data with stratification
        plot_with_strat = self._join_plot_stratification(plot_data, strat_df)
        
        # Select appropriate adjustment factors and calculate expansions
        expanded_data = self._apply_expansion_calculations(plot_with_strat)
        
        return expanded_data
    
    def _load_plot_stratum_assignments(self) -> pl.DataFrame:
        """
        Load plot-stratum assignments from database.
        
        Returns
        -------
        pl.DataFrame
            Plot-stratum assignments filtered by EVALID
        """
        ppsa_query = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(
                pl.col("EVALID").is_in(self.db.evalid) 
                if self.db.evalid else pl.lit(True)
            )
        )
        return ppsa_query.collect()
    
    def _load_population_strata(self) -> pl.DataFrame:
        """
        Load population stratum data from database.
        
        Returns
        -------
        pl.DataFrame
            Population stratum data
        """
        return self.db.tables["POP_STRATUM"].collect()
    
    def _load_population_estimation_units(self) -> pl.DataFrame:
        """
        Load population estimation unit data from database.
        
        Returns
        -------
        pl.DataFrame
            Population estimation unit data with FIA design factors
        """
        return self.db.tables["POP_ESTN_UNIT"].collect()
    
    def _join_plot_stratification(
        self, 
        plot_data: pl.DataFrame, 
        strat_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Join plot data with stratification information.
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level estimates
        strat_df : pl.DataFrame
            Stratification data
            
        Returns
        -------
        pl.DataFrame
            Plot data with stratification information
        """
        return plot_data.join(
            strat_df.select([
                "PLT_CN", "STRATUM_CN", "ESTN_UNIT_CN", "EXPNS",
                "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR", "P2POINTCNT",
                "AREA_USED", "P1PNTCNT_EU", "STRATUM_WGT"
            ]),
            on="PLT_CN",
            how="inner"
        )
    
    def _apply_expansion_calculations(self, plot_with_strat: pl.DataFrame) -> pl.DataFrame:
        """
        Apply expansion calculations with proper adjustment factor selection.
        
        This method handles the core logic of FIA area expansion including:
        - Selecting appropriate adjustment factors based on PROP_BASIS
        - Calculating adjusted area values for variance estimation
        - Applying expansion factors to get population estimates
        
        Parameters
        ----------
        plot_with_strat : pl.DataFrame
            Plot data with stratification information
            
        Returns
        -------
        pl.DataFrame
            Data with expansion calculations applied
        """
        # Select appropriate adjustment factor based on PROP_BASIS
        expanded_data = plot_with_strat.with_columns([
            self._create_adjustment_factor_expression(),
            self._create_area_basis_expression()
        ])
        
        # Calculate adjusted values for variance estimation
        expanded_data = expanded_data.with_columns([
            self._create_adjusted_numerator_expression(),
            self._create_adjusted_denominator_expression()
        ])
        
        # Apply expansion to get population estimates
        expanded_data = expanded_data.with_columns([
            self._create_expanded_numerator_expression(),
            self._create_expanded_denominator_expression()
        ])
        
        return expanded_data
    
    def _create_adjustment_factor_expression(self) -> pl.Expr:
        """
        Create expression for selecting appropriate adjustment factor.
        
        Returns
        -------
        pl.Expr
            Polars expression for adjustment factor selection
        """
        return (
            pl.when(pl.col("PROP_BASIS") == PropBasis.MACROPLOT)
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(pl.col("ADJ_FACTOR_SUBP"))
            .alias("ADJ_FACTOR")
        )
    
    def _create_area_basis_expression(self) -> pl.Expr:
        """
        Create expression for area basis calculation.
        
        Returns
        -------
        pl.Expr
            Polars expression for area basis
        """
        return (
            pl.when(pl.col("PROP_BASIS") == PropBasis.MACROPLOT)
            .then(pl.lit(1.0))  # MACR plots have area basis of 1
            .otherwise(pl.lit(4.0))  # SUBP plots have area basis of 4 (4 subplots)
            .alias("AREA_BASIS")
        )
    
    def _create_adjusted_numerator_expression(self) -> pl.Expr:
        """
        Create expression for adjusted numerator values (for variance).
        
        Returns
        -------
        pl.Expr
            Polars expression for adjusted numerator
        """
        return (
            (pl.col("PLOT_AREA_NUMERATOR").cast(pl.Float64) * pl.col("ADJ_FACTOR").cast(pl.Float64))
            .alias("fa_adjusted")
        )
    
    def _create_adjusted_denominator_expression(self) -> pl.Expr:
        """
        Create expression for adjusted denominator values (for variance).
        
        Returns
        -------
        pl.Expr
            Polars expression for adjusted denominator
        """
        return (
            (pl.col("PLOT_AREA_DENOMINATOR").cast(pl.Float64) * pl.col("ADJ_FACTOR").cast(pl.Float64))
            .alias("fad_adjusted")
        )
    
    def _create_expanded_numerator_expression(self) -> pl.Expr:
        """
        Create expression for expanded numerator totals.
        
        Returns
        -------
        pl.Expr
            Polars expression for expanded numerator
        """
        return (
            (pl.col("PLOT_AREA_NUMERATOR").cast(pl.Float64) * 
             pl.col("ADJ_FACTOR").cast(pl.Float64) * 
             pl.col("EXPNS").cast(pl.Float64))
            .alias("TOTAL_AREA_NUMERATOR")
        )
    
    def _create_expanded_denominator_expression(self) -> pl.Expr:
        """
        Create expression for expanded denominator totals.
        
        Returns
        -------
        pl.Expr
            Polars expression for expanded denominator
        """
        return (
            (pl.col("PLOT_AREA_DENOMINATOR").cast(pl.Float64) * 
             pl.col("ADJ_FACTOR").cast(pl.Float64) * 
             pl.col("EXPNS").cast(pl.Float64))
            .alias("TOTAL_AREA_DENOMINATOR")
        )
    
    def validate_stratification_data(
        self, 
        plot_data: pl.DataFrame, 
        strat_data: pl.DataFrame
    ) -> Dict[str, Any]:
        """
        Validate stratification data for consistency and completeness.
        
        Parameters
        ----------
        plot_data : pl.DataFrame
            Plot-level data to validate
        strat_data : pl.DataFrame
            Stratification data to validate
            
        Returns
        -------
        Dict[str, Any]
            Validation results with warnings and statistics
        """
        validation_results = {
            "is_valid": True,
            "warnings": [],
            "statistics": {}
        }
        
        # Check for required columns in plot data
        required_plot_cols = ["PLT_CN", "PLOT_AREA_NUMERATOR", "PLOT_AREA_DENOMINATOR", "PROP_BASIS"]
        missing_plot_cols = [col for col in required_plot_cols if col not in plot_data.columns]
        
        if missing_plot_cols:
            validation_results["is_valid"] = False
            validation_results["warnings"].append(f"Missing plot columns: {missing_plot_cols}")
        
        # Check for required columns in stratification data
        required_strat_cols = ["PLT_CN", "STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MACR"]
        missing_strat_cols = [col for col in required_strat_cols if col not in strat_data.columns]
        
        if missing_strat_cols:
            validation_results["is_valid"] = False
            validation_results["warnings"].append(f"Missing stratification columns: {missing_strat_cols}")
        
        if validation_results["is_valid"]:
            # Check join coverage
            plot_cnt = plot_data.height
            joined_cnt = plot_data.join(strat_data, on="PLT_CN", how="inner").height
            
            coverage = joined_cnt / plot_cnt if plot_cnt > 0 else 0
            validation_results["statistics"]["join_coverage"] = coverage
            
            if coverage < 0.95:
                validation_results["warnings"].append(
                    f"Low stratification coverage: {coverage:.2%} of plots have stratification data"
                )
            
            # Check for invalid PROP_BASIS values
            invalid_basis = plot_data.filter(
                ~pl.col("PROP_BASIS").is_in([PropBasis.SUBPLOT, PropBasis.MACROPLOT])
            ).height
            
            if invalid_basis > 0:
                validation_results["warnings"].append(
                    f"Found {invalid_basis} plots with invalid PROP_BASIS values"
                )
            
            # Check for zero or negative expansion factors
            zero_expns = strat_data.filter(pl.col("EXPNS") <= 0).height
            
            if zero_expns > 0:
                validation_results["warnings"].append(
                    f"Found {zero_expns} strata with zero or negative expansion factors"
                )
        
        return validation_results
    
    def get_stratification_summary(self, expanded_data: pl.DataFrame) -> Dict[str, Any]:
        """
        Get summary statistics for stratification results.
        
        Parameters
        ----------
        expanded_data : pl.DataFrame
            Data after stratification has been applied
            
        Returns
        -------
        Dict[str, Any]
            Summary statistics
        """
        summary = expanded_data.select([
            pl.col("STRATUM_CN").n_unique().alias("n_strata"),
            pl.col("PLT_CN").n_unique().alias("n_plots"),
            pl.col("EXPNS").mean().alias("mean_expansion_factor"),
            pl.col("EXPNS").min().alias("min_expansion_factor"),
            pl.col("EXPNS").max().alias("max_expansion_factor"),
            pl.col("PROP_BASIS").value_counts().alias("prop_basis_counts"),
            pl.col("TOTAL_AREA_NUMERATOR").sum().alias("total_expanded_numerator"),
            pl.col("TOTAL_AREA_DENOMINATOR").sum().alias("total_expanded_denominator"),
        ]).to_dicts()[0]
        
        return summary