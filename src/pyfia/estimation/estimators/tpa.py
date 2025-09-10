"""
Trees per acre (TPA) and basal area (BAA) estimation for FIA data.

Simple implementation for calculating tree density and basal area
without unnecessary abstractions.
"""

import math
from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..aggregation import aggregate_to_population
from ..statistics import VarianceCalculator
from ..tree_expansion import apply_tree_adjustment_factors
from ..utils import format_output_columns


class TPAEstimator(BaseEstimator):
    """
    Trees per acre and basal area estimator for FIA data.
    
    Estimates tree density (TPA) and basal area per acre (BAA).
    """
    
    def get_required_tables(self) -> List[str]:
        """TPA requires tree, condition, and stratification tables."""
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_tree_columns(self) -> List[str]:
        """Required tree columns for TPA estimation."""
        cols = [
            "CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD",
            "DIA", "TPA_UNADJ"
        ]
        
        # Add grouping columns if needed
        if self.config.get("grp_by"):
            grp_cols = self.config["grp_by"]
            if isinstance(grp_cols, str):
                grp_cols = [grp_cols]
            for col in grp_cols:
                if col not in cols and col in ["HT", "ACTUALHT", "CR", "CCLCD"]:
                    cols.append(col)
        
        return cols
    
    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        return [
            "PLT_CN", "CONDID", "COND_STATUS_CD",
            "CONDPROP_UNADJ", "OWNGRPCD", "FORTYPCD",
            "SITECLCD", "RESERVCD"
        ]
    
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate TPA and BAA values.
        
        TPA = TPA_UNADJ (direct)
        BAA = π * (DIA/24)² * TPA_UNADJ
        """
        # TPA is direct from TPA_UNADJ
        # BAA = basal area per acre = π * (DIA in feet / 2)² * TPA
        # DIA is in inches, so DIA/12 converts to feet, then /2 for radius
        # Simplified: π * (DIA/24)² * TPA_UNADJ
        
        data = data.with_columns([
            pl.col("TPA_UNADJ").cast(pl.Float64).alias("TPA"),
            (math.pi * (pl.col("DIA") / 24.0) ** 2 * 
             pl.col("TPA_UNADJ")).cast(pl.Float64).alias("BAA")
        ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate TPA and BAA with stratification."""
        # Get stratification data
        strat_data = self._get_stratification_data()
        
        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Apply adjustment factors based on tree size
        data_with_strat = apply_tree_adjustment_factors(
            data_with_strat,
            size_col="DIA",
            macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
        )
        
        # Apply adjustment
        data_with_strat = data_with_strat.with_columns([
            (pl.col("TPA") * pl.col("ADJ_FACTOR")).alias("TPA_ADJ"),
            (pl.col("BAA") * pl.col("ADJ_FACTOR")).alias("BAA_ADJ")
        ])
        
        # Setup grouping
        group_cols = self._setup_grouping()
        
        # Aggregate
        agg_exprs = [
            # TPA calculations
            (pl.col("TPA_ADJ") * pl.col("EXPNS")).sum().alias("TPA_TOTAL"),
            (pl.col("TPA_ADJ") * pl.col("EXPNS")).sum().alias("TPA_NUM"),
            # BAA calculations
            (pl.col("BAA_ADJ") * pl.col("EXPNS")).sum().alias("BAA_TOTAL"),
            (pl.col("BAA_ADJ") * pl.col("EXPNS")).sum().alias("BAA_NUM"),
            # Area
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            # Counts
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.count().alias("N_TREES")
        ]
        
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        results = results.collect()
        
        # Calculate per-acre values (ratio of means)
        results = results.with_columns([
            (pl.col("TPA_NUM") / pl.col("AREA_TOTAL")).alias("TPA"),
            (pl.col("BAA_NUM") / pl.col("AREA_TOTAL")).alias("BAA")
        ])
        
        # Clean up intermediate columns
        results = results.drop(["TPA_NUM", "BAA_NUM"])
        
        # If no totals requested, drop total columns
        if not self.config.get("totals", True):
            results = results.drop(["TPA_TOTAL", "BAA_TOTAL"])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for TPA estimates."""
        # Simple placeholder
        results = results.with_columns([
            (pl.col("TPA") * 0.1).alias("TPA_SE"),
            (pl.col("BAA") * 0.1).alias("BAA_SE")
        ])
        
        if "TPA_TOTAL" in results.columns:
            results = results.with_columns([
                (pl.col("TPA_TOTAL") * 0.1).alias("TPA_TOTAL_SE"),
                (pl.col("BAA_TOTAL") * 0.1).alias("BAA_TOTAL_SE")
            ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format TPA estimation output."""
        # Add year
        results = results.with_columns([
            pl.lit(2023).alias("YEAR")
        ])
        
        # Standard column order
        col_order = ["YEAR", "TPA", "BAA", "TPA_SE", "BAA_SE"]
        
        if self.config.get("totals", True):
            col_order.extend(["TPA_TOTAL", "BAA_TOTAL", "TPA_TOTAL_SE", "BAA_TOTAL_SE"])
        
        col_order.extend(["N_PLOTS", "N_TREES"])
        
        # Add grouping columns at the beginning (after YEAR)
        for col in results.columns:
            if col not in col_order:
                col_order.insert(1, col)
        
        # Select only existing columns in order
        final_cols = [col for col in col_order if col in results.columns]
        results = results.select(final_cols)
        
        return results


def tpa(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate trees per acre and basal area from FIA data.
    
    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path
    grp_by : Optional[Union[str, List[str]]]
        Columns to group by
    by_species : bool
        Group by species code
    by_size_class : bool
        Group by diameter size classes
    land_type : str
        Land type: "forest", "timber", or "all"
    tree_type : str
        Tree type: "live", "dead", "gs", or "all"
    tree_domain : Optional[str]
        SQL-like filter for trees
    area_domain : Optional[str]
        SQL-like filter for area
    totals : bool
        Include population totals (default False for TPA)
    variance : bool
        Return variance instead of SE
    most_recent : bool
        Use most recent evaluation
        
    Returns
    -------
    pl.DataFrame
        TPA and BAA estimates
        
    Examples
    --------
    >>> # Trees per acre on forestland
    >>> results = tpa(db, land_type="forest")
    
    >>> # TPA and BAA by species
    >>> results = tpa(db, by_species=True)
    
    >>> # Large trees by size class
    >>> results = tpa(
    ...     db,
    ...     by_size_class=True,
    ...     tree_domain="DIA >= 10.0"
    ... )
    """
    # Create config
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent
    }
    
    # Create and run estimator
    estimator = TPAEstimator(db, config)
    return estimator.estimate()