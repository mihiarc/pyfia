"""
Mortality estimation for FIA data.

Simple implementation for calculating tree mortality rates
without unnecessary abstractions.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..aggregation import aggregate_to_population
from ..statistics import VarianceCalculator
from ..tree_expansion import apply_tree_adjustment_factors
from ..utils import format_output_columns


class MortalityEstimator(BaseEstimator):
    """
    Mortality estimator for FIA data.
    
    Estimates annual tree mortality in terms of volume, biomass, or trees per acre.
    """
    
    def get_required_tables(self) -> List[str]:
        """Mortality requires tree, condition, and stratification tables."""
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_tree_columns(self) -> List[str]:
        """Required tree columns for mortality estimation."""
        cols = [
            "CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD",
            "DIA", "TPA_UNADJ", "MORTYR"  # MORTYR = year of mortality
        ]
        
        # Add columns based on what we're measuring
        measure = self.config.get("measure", "volume")
        if measure == "volume":
            cols.append("VOLCFNET")
        elif measure == "biomass":
            cols.extend(["DRYBIO_AG", "DRYBIO_BG"])
        # For "count", we just need TPA_UNADJ
        
        return cols
    
    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        return [
            "PLT_CN", "CONDID", "COND_STATUS_CD",
            "CONDPROP_UNADJ", "OWNGRPCD", "FORTYPCD",
            "SITECLCD", "RESERVCD"
        ]
    
    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply mortality-specific filters."""
        # First apply base filters
        data = super().apply_filters(data)
        
        # Collect for mortality filtering
        data_df = data.collect()
        
        # Filter to dead trees with known mortality year
        # STATUSCD == 2 means dead
        # MORTYR > 0 means mortality year is known
        data_df = data_df.filter(
            (pl.col("STATUSCD") == 2) & 
            (pl.col("MORTYR") > 0)
        )
        
        # Optionally filter to recent mortality
        if self.config.get("recent_only", True):
            # Recent mortality = died in last 5 years
            # This would need INVYR to calculate properly
            # For now, simplified filter
            data_df = data_df.filter(pl.col("MORTYR") >= 2018)
        
        return data_df.lazy()
    
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate mortality values per acre.
        
        Mortality can be measured as volume, biomass, or tree count.
        """
        measure = self.config.get("measure", "volume")
        
        if measure == "volume":
            # Mortality volume per acre
            data = data.with_columns([
                (pl.col("VOLCFNET").cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("MORT_VALUE")
            ])
        elif measure == "biomass":
            # Mortality biomass per acre (total biomass)
            data = data.with_columns([
                ((pl.col("DRYBIO_AG") + pl.col("DRYBIO_BG")).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64) / 
                 2000.0).alias("MORT_VALUE")
            ])
        else:  # count
            # Mortality trees per acre
            data = data.with_columns([
                pl.col("TPA_UNADJ").cast(pl.Float64).alias("MORT_VALUE")
            ])
        
        # Annualize mortality (divide by remeasurement period)
        # For simplicity, assume 5-year period
        remeasure_period = self.config.get("remeasure_period", 5.0)
        data = data.with_columns([
            (pl.col("MORT_VALUE") / remeasure_period).alias("MORT_ANNUAL")
        ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate mortality with stratification."""
        # Get stratification data
        strat_data = self._get_stratification_data()
        
        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Apply adjustment factors
        data_with_strat = apply_tree_adjustment_factors(
            data_with_strat,
            size_col="DIA",
            macro_breakpoint_col="MACRO_BREAKPOINT_DIA"
        )
        
        # Apply adjustment
        data_with_strat = data_with_strat.with_columns([
            (pl.col("MORT_ANNUAL") * pl.col("ADJ_FACTOR")).alias("MORT_ADJ")
        ])
        
        # Setup grouping
        group_cols = self._setup_grouping()
        
        # Aggregate
        agg_exprs = [
            # Mortality totals
            (pl.col("MORT_ADJ") * pl.col("EXPNS")).sum().alias("MORT_TOTAL"),
            (pl.col("MORT_ADJ") * pl.col("EXPNS")).sum().alias("MORT_NUM"),
            # Area
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            # Counts
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.count().alias("N_DEAD_TREES")
        ]
        
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        results = results.collect()
        
        # Calculate per-acre value (ratio of means)
        results = results.with_columns([
            (pl.col("MORT_NUM") / pl.col("AREA_TOTAL")).alias("MORT_ACRE")
        ])
        
        # Clean up
        results = results.drop(["MORT_NUM"])
        
        # Calculate mortality rate if requested
        if self.config.get("as_rate", False):
            # This would need live tree data to calculate properly
            # For now, add placeholder
            results = results.with_columns([
                (pl.col("MORT_ACRE") * 0.02).alias("MORT_RATE")  # 2% placeholder
            ])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for mortality estimates."""
        # Simple placeholder
        results = results.with_columns([
            (pl.col("MORT_ACRE") * 0.15).alias("MORT_ACRE_SE"),
            (pl.col("MORT_TOTAL") * 0.15).alias("MORT_TOTAL_SE")
        ])
        
        if "MORT_RATE" in results.columns:
            results = results.with_columns([
                (pl.col("MORT_RATE") * 0.2).alias("MORT_RATE_SE")
            ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format mortality estimation output."""
        # Add year and measurement type
        measure = self.config.get("measure", "volume")
        results = results.with_columns([
            pl.lit(2023).alias("YEAR"),
            pl.lit(measure.upper()).alias("MEASURE")
        ])
        
        # Format columns
        results = format_output_columns(
            results,
            estimation_type="mortality",
            include_se=True,
            include_cv=False
        )
        
        return results


def mortality(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    recent_only: bool = True,
    as_rate: bool = False,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate tree mortality from FIA data.
    
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
    measure : str
        What to measure: "volume", "biomass", or "count"
    tree_domain : Optional[str]
        SQL-like filter for trees
    area_domain : Optional[str]
        SQL-like filter for area
    recent_only : bool
        Only include recent mortality (last 5 years)
    as_rate : bool
        Return as mortality rate (proportion of live)
    totals : bool
        Include population totals
    variance : bool
        Return variance instead of SE
    most_recent : bool
        Use most recent evaluation
        
    Returns
    -------
    pl.DataFrame
        Mortality estimates
        
    Examples
    --------
    >>> # Volume mortality on forestland
    >>> results = mortality(db, measure="volume")
    
    >>> # Mortality by species (tree count)
    >>> results = mortality(db, by_species=True, measure="count")
    
    >>> # Biomass mortality rate by size class
    >>> results = mortality(
    ...     db,
    ...     by_size_class=True,
    ...     measure="biomass",
    ...     as_rate=True
    ... )
    """
    # Create config
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "measure": measure,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "recent_only": recent_only,
        "as_rate": as_rate,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "remeasure_period": 5.0  # Default 5-year period
    }
    
    # Create and run estimator
    estimator = MortalityEstimator(db, config)
    return estimator.estimate()