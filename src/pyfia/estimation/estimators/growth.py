"""
Growth, removal, and mortality (GRM) estimation for FIA data.

Simple implementation for calculating forest growth dynamics
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


class GrowthEstimator(BaseEstimator):
    """
    Growth, removal, and mortality estimator for FIA data.
    
    Estimates annual gross growth, removals (harvest), and mortality.
    Net change = Growth - Removals - Mortality
    """
    
    def get_required_tables(self) -> List[str]:
        """Growth requires tree growth and condition tables."""
        return ["TREE_GRM", "COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_tree_columns(self) -> List[str]:
        """Required tree columns for growth estimation."""
        # TREE_GRM table has growth-specific columns
        cols = [
            "CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD",
            "DIA", "PREVDIA",  # Current and previous diameter
            "TPA_UNADJ",
            "GROWCFAL",  # Annual gross growth (cu ft)
            "REMVCFAL",  # Annual removals (cu ft)
            "MORTCFAL"   # Annual mortality (cu ft)
        ]
        
        # Add measure-specific columns
        measure = self.config.get("measure", "volume")
        if measure == "biomass":
            cols.extend([
                "GROWBAAL",  # Annual biomass growth
                "REMVBAAL",  # Annual biomass removals
                "MORTBAAL"   # Annual biomass mortality
            ])
        elif measure == "basal_area":
            cols.extend([
                "GROWBAAL",  # Basal area growth
                "REMVBAAL",  # Basal area removals
                "MORTBAAL"   # Basal area mortality
            ])
        
        return cols
    
    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        return [
            "PLT_CN", "CONDID", "COND_STATUS_CD",
            "CONDPROP_UNADJ", "OWNGRPCD", "FORTYPCD",
            "SITECLCD", "RESERVCD"
        ]
    
    def load_data(self) -> pl.LazyFrame:
        """Load TREE_GRM table instead of regular TREE."""
        # Load TREE_GRM table
        if "TREE_GRM" not in self.db.tables:
            self.db.load_table("TREE_GRM")
        tree_grm = self.db.tables["TREE_GRM"]
        
        # Load COND table
        if "COND" not in self.db.tables:
            self.db.load_table("COND")
        cond_df = self.db.tables["COND"]
        
        # Ensure LazyFrames
        if not isinstance(tree_grm, pl.LazyFrame):
            tree_grm = tree_grm.lazy()
        if not isinstance(cond_df, pl.LazyFrame):
            cond_df = cond_df.lazy()
        
        # Apply EVALID filtering
        if self.db.evalid:
            tree_grm = tree_grm.filter(pl.col("EVALID").is_in(self.db.evalid))
            cond_df = cond_df.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        # Select columns
        tree_cols = self.get_tree_columns()
        cond_cols = self.get_cond_columns()
        
        tree_grm = tree_grm.select([col for col in tree_cols if col in tree_grm.columns])
        cond_df = cond_df.select([col for col in cond_cols if col in cond_df.columns])
        
        # Join
        data = tree_grm.join(
            cond_df,
            on=["PLT_CN", "CONDID"],
            how="inner"
        )
        
        return data
    
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate growth, removal, and mortality values per acre.
        
        These are already annualized in the TREE_GRM table.
        """
        measure = self.config.get("measure", "volume")
        component = self.config.get("component", "net")  # net, gross, removals, mortality
        
        if measure == "volume":
            growth_col = "GROWCFAL"
            removal_col = "REMVCFAL"
            mort_col = "MORTCFAL"
        elif measure == "biomass":
            growth_col = "GROWBAAL"
            removal_col = "REMVBAAL"
            mort_col = "MORTBAAL"
        else:  # basal_area
            growth_col = "GROWBAAL"
            removal_col = "REMVBAAL"
            mort_col = "MORTBAAL"
        
        # Calculate per-acre values based on component
        if component == "gross":
            # Gross growth only
            data = data.with_columns([
                (pl.col(growth_col).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("GROWTH_ACRE")
            ])
        elif component == "removals":
            # Removals only
            data = data.with_columns([
                (pl.col(removal_col).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("REMOVAL_ACRE")
            ])
        elif component == "mortality":
            # Mortality only
            data = data.with_columns([
                (pl.col(mort_col).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("MORTALITY_ACRE")
            ])
        else:  # net change
            # Net change = Growth - Removals - Mortality
            data = data.with_columns([
                (pl.col(growth_col).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("GROWTH_ACRE"),
                (pl.col(removal_col).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("REMOVAL_ACRE"),
                (pl.col(mort_col).cast(pl.Float64) * 
                 pl.col("TPA_UNADJ").cast(pl.Float64)).alias("MORTALITY_ACRE")
            ])
            
            # Calculate net change
            data = data.with_columns([
                (pl.col("GROWTH_ACRE") - 
                 pl.col("REMOVAL_ACRE") - 
                 pl.col("MORTALITY_ACRE")).alias("NET_CHANGE_ACRE")
            ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate growth components with stratification."""
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
        
        # Setup grouping
        group_cols = self._setup_grouping()
        component = self.config.get("component", "net")
        
        # Build aggregation expressions based on component
        agg_exprs = []
        
        if component in ["gross", "net"]:
            agg_exprs.extend([
                ((pl.col("GROWTH_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("GROWTH_TOTAL"),
                ((pl.col("GROWTH_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("GROWTH_NUM")
            ])
        
        if component in ["removals", "net"]:
            agg_exprs.extend([
                ((pl.col("REMOVAL_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("REMOVAL_TOTAL"),
                ((pl.col("REMOVAL_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("REMOVAL_NUM")
            ])
        
        if component in ["mortality", "net"]:
            agg_exprs.extend([
                ((pl.col("MORTALITY_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("MORTALITY_TOTAL"),
                ((pl.col("MORTALITY_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("MORTALITY_NUM")
            ])
        
        if component == "net":
            agg_exprs.extend([
                ((pl.col("NET_CHANGE_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("NET_CHANGE_TOTAL"),
                ((pl.col("NET_CHANGE_ACRE") * pl.col("ADJ_FACTOR") * 
                  pl.col("EXPNS"))).sum().alias("NET_CHANGE_NUM")
            ])
        
        # Add area and counts
        agg_exprs.extend([
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.count().alias("N_TREES")
        ])
        
        # Aggregate
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        results = results.collect()
        
        # Calculate per-acre values (ratio of means)
        if "GROWTH_NUM" in results.columns:
            results = results.with_columns([
                (pl.col("GROWTH_NUM") / pl.col("AREA_TOTAL")).alias("GROW_ACRE")
            ])
        if "REMOVAL_NUM" in results.columns:
            results = results.with_columns([
                (pl.col("REMOVAL_NUM") / pl.col("AREA_TOTAL")).alias("REMV_ACRE")
            ])
        if "MORTALITY_NUM" in results.columns:
            results = results.with_columns([
                (pl.col("MORTALITY_NUM") / pl.col("AREA_TOTAL")).alias("MORT_ACRE")
            ])
        if "NET_CHANGE_NUM" in results.columns:
            results = results.with_columns([
                (pl.col("NET_CHANGE_NUM") / pl.col("AREA_TOTAL")).alias("NET_CHG_ACRE")
            ])
        
        # Clean up intermediate columns
        for col in ["GROWTH_NUM", "REMOVAL_NUM", "MORTALITY_NUM", "NET_CHANGE_NUM"]:
            if col in results.columns:
                results = results.drop(col)
        
        # Rename totals
        rename_map = {
            "GROWTH_TOTAL": "GROW_TOTAL",
            "REMOVAL_TOTAL": "REMV_TOTAL",
            "MORTALITY_TOTAL": "MORT_TOTAL",
            "NET_CHANGE_TOTAL": "NET_CHG_TOTAL"
        }
        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for growth estimates."""
        # Add SE columns for all estimate columns
        est_cols = [col for col in results.columns 
                   if col.endswith("_ACRE") or col.endswith("_TOTAL")]
        
        for col in est_cols:
            se_col = col.replace("_ACRE", "_ACRE_SE").replace("_TOTAL", "_TOTAL_SE")
            results = results.with_columns([
                (pl.col(col).abs() * 0.12).alias(se_col)  # 12% CV placeholder
            ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format growth estimation output."""
        # Add year
        results = results.with_columns([
            pl.lit(2023).alias("YEAR")
        ])
        
        # Format columns
        results = format_output_columns(
            results,
            estimation_type="growth",
            include_se=True,
            include_cv=False
        )
        
        return results


def growth(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    component: str = "net",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate forest growth, removals, and mortality from FIA data.
    
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
    component : str
        Component to estimate: "net", "gross", "removals", or "mortality"
    measure : str
        Measurement type: "volume", "biomass", or "basal_area"
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
        Growth component estimates
        
    Examples
    --------
    >>> # Net volume change on forestland
    >>> results = growth(db, component="net", measure="volume")
    
    >>> # Gross growth by species
    >>> results = growth(db, by_species=True, component="gross")
    
    >>> # Removals by ownership
    >>> results = growth(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     component="removals"
    ... )
    """
    # Create config
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "component": component,
        "measure": measure,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent
    }
    
    # Create and run estimator
    estimator = GrowthEstimator(db, config)
    return estimator.estimate()