"""
Biomass estimation for FIA data.

Simple implementation for calculating tree biomass and carbon
without unnecessary abstractions.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..aggregation import aggregate_to_population, apply_adjustment_factors
from ..statistics import VarianceCalculator
from ..utils import format_output_columns


class BiomassEstimator(BaseEstimator):
    """
    Biomass estimator for FIA data.
    
    Estimates tree biomass (dry weight in tons) and carbon content.
    """
    
    def get_required_tables(self) -> List[str]:
        """Biomass requires tree, condition, and stratification tables."""
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_tree_columns(self) -> List[str]:
        """Required tree columns for biomass estimation."""
        cols = [
            "CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD",
            "DIA", "TPA_UNADJ", "DRYBIO_AG", "DRYBIO_BG"
        ]
        
        # Add component-specific columns
        component = self.config.get("component", "AG")
        if component not in ["AG", "BG", "TOTAL"]:
            # Specific components like STEM, BRANCH, etc.
            biomass_col = f"DRYBIO_{component}"
            if biomass_col not in cols:
                cols.append(biomass_col)
        
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
        Calculate biomass per acre.
        
        Biomass per acre = (DRYBIO * TPA_UNADJ) / 2000
        Carbon = Biomass * 0.47
        """
        component = self.config.get("component", "AG")
        
        # Select biomass component
        if component == "TOTAL":
            # Total = aboveground + belowground
            data = data.with_columns([
                (pl.col("DRYBIO_AG") + pl.col("DRYBIO_BG")).alias("DRYBIO")
            ])
        elif component == "AG":
            data = data.with_columns([
                pl.col("DRYBIO_AG").alias("DRYBIO")
            ])
        elif component == "BG":
            data = data.with_columns([
                pl.col("DRYBIO_BG").alias("DRYBIO")
            ])
        else:
            # Specific component
            biomass_col = f"DRYBIO_{component}"
            data = data.with_columns([
                pl.col(biomass_col).alias("DRYBIO")
            ])
        
        # Calculate biomass per acre (convert pounds to tons)
        data = data.with_columns([
            (pl.col("DRYBIO").cast(pl.Float64) * 
             pl.col("TPA_UNADJ").cast(pl.Float64) / 
             2000.0).alias("BIOMASS_ACRE"),
            # Carbon is 47% of biomass
            (pl.col("DRYBIO").cast(pl.Float64) * 
             pl.col("TPA_UNADJ").cast(pl.Float64) / 
             2000.0 * 0.47).alias("CARBON_ACRE")
        ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate biomass with stratification."""
        # Get stratification data
        strat_data = self._get_stratification_data()
        
        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Apply adjustment factors
        data_with_strat = apply_adjustment_factors(
            data_with_strat,
            size_col="DIA"
        )
        
        # Apply adjustment
        data_with_strat = data_with_strat.with_columns([
            (pl.col("BIOMASS_ACRE") * pl.col("ADJ_FACTOR")).alias("BIOMASS_ADJ"),
            (pl.col("CARBON_ACRE") * pl.col("ADJ_FACTOR")).alias("CARBON_ADJ")
        ])
        
        # Setup grouping
        group_cols = self._setup_grouping()
        
        # Aggregate
        agg_exprs = [
            # Biomass totals
            (pl.col("BIOMASS_ADJ") * pl.col("EXPNS")).sum().alias("BIOMASS_TOTAL"),
            (pl.col("BIOMASS_ADJ") * pl.col("EXPNS")).sum().alias("BIOMASS_NUM"),
            # Carbon totals
            (pl.col("CARBON_ADJ") * pl.col("EXPNS")).sum().alias("CARBON_TOTAL"),
            (pl.col("CARBON_ADJ") * pl.col("EXPNS")).sum().alias("CARBON_NUM"),
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
            (pl.col("BIOMASS_NUM") / pl.col("AREA_TOTAL")).alias("BIO_ACRE"),
            (pl.col("CARBON_NUM") / pl.col("AREA_TOTAL")).alias("CARB_ACRE")
        ])
        
        # Rename totals
        results = results.rename({
            "BIOMASS_TOTAL": "BIO_TOTAL",
            "CARBON_TOTAL": "CARB_TOTAL"
        })
        
        # Clean up
        results = results.drop(["BIOMASS_NUM", "CARBON_NUM"])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for biomass estimates."""
        # Simple placeholder
        results = results.with_columns([
            (pl.col("BIO_ACRE") * 0.1).alias("BIO_ACRE_SE"),
            (pl.col("CARB_ACRE") * 0.1).alias("CARB_ACRE_SE"),
            (pl.col("BIO_TOTAL") * 0.1).alias("BIO_TOTAL_SE"),
            (pl.col("CARB_TOTAL") * 0.1).alias("CARB_TOTAL_SE")
        ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format biomass estimation output."""
        # Add year
        results = results.with_columns([
            pl.lit(2023).alias("YEAR")
        ])
        
        # Standard column order
        col_order = [
            "YEAR", "BIO_ACRE", "BIO_TOTAL", "CARB_ACRE", "CARB_TOTAL",
            "BIO_ACRE_SE", "BIO_TOTAL_SE", "CARB_ACRE_SE", "CARB_TOTAL_SE",
            "N_PLOTS", "N_TREES"
        ]
        
        # Add any grouping columns at the beginning
        for col in results.columns:
            if col not in col_order:
                col_order.insert(1, col)
        
        # Select only existing columns in order
        final_cols = [col for col in col_order if col in results.columns]
        results = results.select(final_cols)
        
        return results


def biomass(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    component: str = "AG",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate tree biomass from FIA data.
    
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
    component : str
        Biomass component: "AG", "BG", "TOTAL", "STEM", etc.
    tree_domain : Optional[str]
        SQL-like filter for trees
    area_domain : Optional[str]
        SQL-like filter for area
    totals : bool
        Include population totals
    variance : bool
        Return variance instead of SE
    most_recent : bool
        Use most recent evaluation
        
    Returns
    -------
    pl.DataFrame
        Biomass and carbon estimates
        
    Examples
    --------
    >>> # Aboveground biomass on forestland
    >>> results = biomass(db, component="AG")
    
    >>> # Total biomass by species
    >>> results = biomass(db, by_species=True, component="TOTAL")
    
    >>> # Biomass by ownership for large trees
    >>> results = biomass(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     tree_domain="DIA >= 20.0",
    ...     component="AG"
    ... )
    """
    # Create config
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "component": component,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent
    }
    
    # Create and run estimator
    estimator = BiomassEstimator(db, config)
    return estimator.estimate()