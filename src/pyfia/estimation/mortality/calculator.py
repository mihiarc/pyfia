"""
Mortality estimation calculator.

This module implements the core mortality estimation logic following
FIA statistical procedures.
"""

import polars as pl
from typing import List, Optional, Dict, Any, Union

from pyfia.core import FIA
from ..config import MortalityConfig
from pyfia.filters.common import apply_area_filters_common, apply_tree_filters_common


class MortalityCalculator:
    """
    Calculator for mortality estimates.
    
    This class implements the mortality estimation methods from
    Bechtold & Patterson (2005).
    """
    
    def __init__(self, db: Union[str, FIA], config: MortalityConfig):
        """
        Initialize calculator.
        
        Args:
            db: FIA database object or path to database file
            config: Configuration object with estimation parameters
        """
        # Handle database initialization
        if isinstance(db, str):
            self.db = FIA(db)
            self._owns_db = True  # We created it, we should close it
        else:
            self.db = db
            self._owns_db = False  # Using external db, don't close

        self.config = config
        
    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clean up if we own the database."""
        if self._owns_db and hasattr(self.db, 'close'):
            self.db.close()

    def estimate(self) -> pl.DataFrame:
        """
        Main estimation workflow.

        Returns:
            DataFrame with mortality estimates
        """
        # Load required tables
        self.db.load_table("PLOT")
        self.db.load_table("TREE")
        self.db.load_table("COND")
        self.db.load_table("POP_STRATUM")
        self.db.load_table("POP_PLOT_STRATUM_ASSGN")

        # Get and filter data
        tree_df, cond_df = self._get_filtered_data()

        # Join and prepare data
        prepared_data = self._prepare_estimation_data(tree_df, cond_df)

        # Calculate plot-level mortality
        plot_mortality = self.calculate_plot_mortality(
            prepared_data,
            group_cols=self.config.get_grouping_columns()
        )

        # Calculate stratum-level mortality
        stratum_mortality = self.calculate_stratum_mortality(
            plot_mortality,
            group_cols=self.config.get_grouping_columns()
        )

        # Calculate population-level mortality
        pop_stratum = self.db.tables["POP_STRATUM"].collect()
        pop_mortality = self.calculate_population_mortality(
            stratum_mortality,
            pop_stratum,
            group_cols=self.config.get_grouping_columns(),
            include_variance=self.config.variance
        )

        return pop_mortality

    def _get_filtered_data(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Get data from database and apply filters.

        Returns:
            Tuple of (tree_df, cond_df) with filtered data
        """
        # Get condition data
        cond_df = self.db.get_conditions()

        # Apply area filters
        cond_df = apply_area_filters_common(
            cond_df,
            self.config.land_type,
            self.config.area_domain
        )

        # Get tree data
        tree_df = self.db.get_trees()

        # Apply tree filters
        tree_df = apply_tree_filters_common(
            tree_df,
            self.config.tree_type,
            self.config.tree_domain,
            require_volume=False
        )

        return tree_df, cond_df

    def _prepare_estimation_data(
        self,
        tree_df: pl.DataFrame,
        cond_df: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Join data and prepare for estimation.

        Args:
            tree_df: Tree data
            cond_df: Condition data

        Returns:
            Prepared data ready for calculation
        """
        # Join trees with conditions
        data = tree_df.join(
            cond_df.select(["PLT_CN", "CONDID", "CONDPROP_UNADJ"]),
            on=["PLT_CN", "CONDID"],
            how="inner"
        )

        # Get plot-stratum assignments
        ppsa = (
            self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("EVALID").is_in(self.db.evalid) if self.db.evalid else pl.lit(True))
            .collect()
        )

        # Get stratum data
        pop_stratum = self.db.tables["POP_STRATUM"].collect()

        # Join with stratum data
        strat = ppsa.join(
            pop_stratum.select(["CN", "EXPNS"]).rename({"CN": "STRATUM_CN"}),
            on="STRATUM_CN",
            how="inner",
        )

        # Join with plot data
        data = data.join(
            strat.select(["PLT_CN", "STRATUM_CN", "EXPNS"]).unique(),
            on="PLT_CN",
            how="inner",
        )

        return data
        
    def calculate_plot_mortality(
        self,
        data: pl.DataFrame,
        mortality_col: str = "SUBP_TPAMORT_UNADJ_AL_FOREST",
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate plot-level mortality.
        
        Args:
            data: DataFrame with tree and plot data
            mortality_col: Column containing mortality values
            group_cols: Optional grouping columns
            
        Returns:
            DataFrame with plot-level mortality
        """
        # Ensure numeric types
        data = data.with_columns([
            pl.col(mortality_col).cast(pl.Float64),
            pl.col("EXPNS").cast(pl.Float64)
        ])
        
        # Calculate plot mortality
        group_by = ["STRATUM_CN", "ESTN_UNIT_CN", "PLT_CN"]
        if group_cols:
            group_by.extend(group_cols)
            
        plot_mortality = data.group_by(group_by).agg([
            (pl.col(mortality_col) * pl.col("EXPNS")).sum().alias("MORTALITY_EXPANDED"),
            pl.col("PLT_CN").n_unique().alias("N_PLOTS"),
            pl.col("TRE_CN").count().alias("N_TREES")
        ])
        
        return plot_mortality
        
    def calculate_stratum_mortality(
        self,
        plot_data: pl.DataFrame,
        group_cols: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Calculate stratum-level mortality.
        
        Args:
            plot_data: DataFrame with plot-level mortality
            group_cols: Optional grouping columns
            
        Returns:
            DataFrame with stratum-level mortality
        """
        # Calculate stratum components
        group_by = ["STRATUM_CN", "ESTN_UNIT_CN"]
        if group_cols:
            group_by.extend(group_cols)
            
        stratum_mortality = plot_data.group_by(group_by).agg([
            pl.col("MORTALITY_EXPANDED").sum().alias("STRATUM_MORTALITY"),
            pl.col("PLT_CN").n_unique().alias("STRATUM_N_PLOTS"),
            pl.col("N_TREES").sum().alias("STRATUM_N_TREES"),
            # Variance components
            (pl.col("MORTALITY_EXPANDED") * pl.col("MORTALITY_EXPANDED"))
            .sum().alias("MORT_SQUARED_SUM"),
            pl.col("MORTALITY_EXPANDED").mean().alias("MORT_MEAN")
        ])
        
        return stratum_mortality
        
    def calculate_population_mortality(
        self,
        stratum_data: pl.DataFrame,
        pop_stratum: pl.DataFrame,
        group_cols: Optional[List[str]] = None,
        include_variance: bool = True
    ) -> pl.DataFrame:
        """
        Calculate population-level mortality.
        
        Args:
            stratum_data: DataFrame with stratum-level mortality
            pop_stratum: DataFrame with population stratum info
            group_cols: Optional grouping columns
            include_variance: Whether to calculate variance
            
        Returns:
            DataFrame with population-level mortality
        """
        # Join with population stratum data
        data = stratum_data.join(
            pop_stratum.select(["CN", "P2POINTCNT", "P1POINTCNT", "EXPNS"]),
            left_on="STRATUM_CN",
            right_on="CN",
            how="left"
        )
        
        # Calculate population totals
        group_by = ["ESTN_UNIT_CN"]
        if group_cols:
            group_by.extend(group_cols)
            
        pop_mortality = data.group_by(group_by).agg([
            (
                pl.col("STRATUM_MORTALITY") * 
                pl.col("P2POINTCNT").cast(pl.Float64) / 
                pl.col("P1POINTCNT").cast(pl.Float64)
            ).sum().alias("MORTALITY_TOTAL"),
            pl.col("EXPNS").sum().alias("TOTAL_AREA"),
            pl.col("STRATUM_N_PLOTS").sum().alias("N_PLOTS"),
            pl.col("STRATUM_N_TREES").sum().alias("N_TREES")
        ])
        
        # Calculate per-acre mortality
        pop_mortality = pop_mortality.with_columns([
            (
                pl.col("MORTALITY_TOTAL") / 
                pl.col("TOTAL_AREA")
            ).alias("MORTALITY_PER_ACRE")
        ])
        
        if include_variance:
            # Calculate variance components
            variance_data = data.group_by(group_by).agg([
                pl.col("MORT_SQUARED_SUM").sum().alias("MORT_SQUARED_SUM"),
                pl.col("STRATUM_N_PLOTS").sum().alias("TOTAL_PLOTS")
            ])
            
            # Join variance components
            pop_mortality = pop_mortality.join(
                variance_data,
                on=group_by,
                how="left"
            )
            
            # Calculate standard error
            pop_mortality = pop_mortality.with_columns([
                pl.col("MORT_SQUARED_SUM").sqrt().alias("SE_OF_ESTIMATE"),
                pl.when(pl.col("MORTALITY_TOTAL") != 0)
                .then(
                    (
                        pl.col("MORT_SQUARED_SUM").sqrt() / 
                        pl.col("MORTALITY_TOTAL").abs() * 100
                    )
                )
                .otherwise(0.0).alias("SE_OF_ESTIMATE_PCT")
            ])
        
        return pop_mortality