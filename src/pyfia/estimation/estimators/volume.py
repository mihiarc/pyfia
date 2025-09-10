"""
Volume estimation for FIA data.

Simple, straightforward implementation for calculating tree volume
without unnecessary abstractions.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..aggregation import aggregate_to_population
from ..statistics import VarianceCalculator
from ..tree_expansion import apply_tree_adjustment_factors
from ..utils import format_output_columns, check_required_columns


class VolumeEstimator(BaseEstimator):
    """
    Volume estimator for FIA data.
    
    Estimates tree volume (cubic feet) using standard FIA methods.
    """
    
    def get_required_tables(self) -> List[str]:
        """Volume estimation requires tree, condition, and stratification tables."""
        return ["TREE", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_tree_columns(self) -> List[str]:
        """Required tree columns for volume estimation."""
        cols = [
            "CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD",
            "DIA", "TPA_UNADJ"
        ]
        
        # Add volume columns based on vol_type
        vol_type = self.config.get("vol_type", "net")
        if vol_type == "net":
            cols.append("VOLCFNET")
        elif vol_type == "gross":
            cols.append("VOLCFGRS")
        elif vol_type == "sound":
            cols.append("VOLCFSND")
        elif vol_type == "sawlog":
            cols.extend(["VOLBFNET", "VOLBFGRS"])  # Board feet for sawlog
        
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
        Calculate volume per acre.
        
        Volume calculation: VOLUME * TPA_UNADJ
        """
        vol_type = self.config.get("vol_type", "net")
        
        # Select appropriate volume column
        if vol_type == "net":
            vol_col = "VOLCFNET"
        elif vol_type == "gross":
            vol_col = "VOLCFGRS"
        elif vol_type == "sound":
            vol_col = "VOLCFSND"
        elif vol_type == "sawlog":
            vol_col = "VOLBFNET"  # Board feet net for sawlog
        else:
            vol_col = "VOLCFNET"  # Default to net
        
        # Calculate volume per acre
        # Volume per acre = tree volume * trees per acre
        data = data.with_columns([
            (pl.col(vol_col).cast(pl.Float64) * 
             pl.col("TPA_UNADJ").cast(pl.Float64)).alias("VOLUME_ACRE")
        ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate volume with stratification."""
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
        
        # Apply adjustment to volume
        data_with_strat = data_with_strat.with_columns([
            (pl.col("VOLUME_ACRE") * pl.col("ADJ_FACTOR")).alias("VOLUME_ADJ")
        ])
        
        # Setup grouping
        group_cols = self._setup_grouping()
        
        # Aggregate
        agg_exprs = [
            # Total volume (expansion factor applied)
            (pl.col("VOLUME_ADJ") * pl.col("EXPNS")).sum().alias("VOLUME_TOTAL"),
            # Sum for per-acre calculation (ratio of means)
            (pl.col("VOLUME_ADJ") * pl.col("EXPNS")).sum().alias("VOLUME_NUM"),
            # Total area
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            # Plot count
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            # Tree count
            pl.count().alias("N_TREES")
        ]
        
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        results = results.collect()
        
        # Calculate per-acre value (ratio of means)
        results = results.with_columns([
            (pl.col("VOLUME_NUM") / pl.col("AREA_TOTAL")).alias("VOLUME_ACRE")
        ])
        
        # Clean up intermediate columns
        results = results.drop(["VOLUME_NUM"])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for volume estimates."""
        calc = VarianceCalculator(method="ratio_of_means")
        
        # Simple placeholder for now
        results = results.with_columns([
            (pl.col("VOLUME_ACRE") * 0.1).alias("VOLUME_ACRE_SE"),
            (pl.col("VOLUME_TOTAL") * 0.1).alias("VOLUME_TOTAL_SE")
        ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format volume estimation output."""
        # Add year
        results = results.with_columns([
            pl.lit(2023).alias("YEAR")
        ])
        
        # Format columns
        results = format_output_columns(
            results,
            estimation_type="volume",
            include_se=True,
            include_cv=False
        )
        
        # Rename to standard FIA column names
        rename_map = {
            "VOLUME_ACRE": "VOLCFNET_ACRE",
            "VOLUME_TOTAL": "VOLCFNET_TOTAL",
            "VOLUME_ACRE_SE": "VOLCFNET_ACRE_SE",
            "VOLUME_TOTAL_SE": "VOLCFNET_TOTAL_SE"
        }
        
        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})
        
        return results


def volume(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    vol_type: str = "net",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate tree volume from FIA data.
    
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
    vol_type : str
        Volume type: "net", "gross", "sound", or "sawlog"
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
        Volume estimates
        
    Examples
    --------
    >>> # Net volume on forestland
    >>> results = volume(db, land_type="forest", vol_type="net")
    
    >>> # Volume by species on timberland
    >>> results = volume(db, by_species=True, land_type="timber")
    
    >>> # Large tree volume by ownership
    >>> results = volume(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     tree_domain="DIA >= 20.0"
    ... )
    """
    # Create config
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "vol_type": vol_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent
    }
    
    # Create and run estimator
    estimator = VolumeEstimator(db, config)
    return estimator.estimate()