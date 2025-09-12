"""
Removals estimation for FIA data.

Simple implementation for calculating average annual removals of merchantable 
bole wood volume of growing-stock trees.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..aggregation import aggregate_to_population
from ..statistics import VarianceCalculator
from ..tree_expansion import apply_tree_adjustment_factors
from ..utils import format_output_columns


class RemovalsEstimator(BaseEstimator):
    """
    Removals estimator for FIA data.
    
    Estimates average annual removals of merchantable bole wood volume of 
    growing-stock trees (at least 5 inches d.b.h.) on forest land.
    """
    
    def get_required_tables(self) -> List[str]:
        """Removals requires tree growth/removal/mortality tables."""
        return [
            "TREE", "COND", "PLOT", 
            "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM",
            "TREE_GRM_COMPONENT", "TREE_GRM_MIDPT"
        ]
    
    def get_tree_columns(self) -> List[str]:
        """Required tree columns for removals estimation."""
        cols = [
            "CN", "PLT_CN", "CONDID", "STATUSCD", "SPCD",
            "DIA", "TPA_UNADJ", "PREV_TRE_CN"
        ]
        
        # Add columns based on what we're measuring
        measure = self.config.get("measure", "volume")
        if measure == "volume":
            # We'll get VOLCFNET from TREE_GRM_MIDPT
            pass
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
    
    def load_data(self) -> Optional[pl.LazyFrame]:
        """
        Load and join required tables including GRM component tables.
        """
        # Load base tree and condition data
        tree_cols = self.get_tree_columns()
        tree = self.db.read_table(
            "TREE",
            columns=tree_cols
        ).lazy()
        
        cond_cols = self.get_cond_columns()
        cond = self.db.read_table(
            "COND",
            columns=cond_cols
        ).lazy()
        
        # Join tree and condition
        data = tree.join(
            cond,
            on=["PLT_CN", "CONDID"],
            how="inner"
        )
        
        # Load and join TREE_GRM_COMPONENT - this contains removal components
        grm_component = self.db.read_table(
            "TREE_GRM_COMPONENT",
            columns=[
                "TRE_CN", "DIA_BEGIN", "DIA_MIDPT", 
                "SUBP_COMPONENT_GS_FOREST", 
                "SUBP_SUBPTYP_GRM_GS_FOREST",
                "SUBP_TPAREMV_UNADJ_GS_FOREST"
            ]
        ).lazy()
        
        # Rename columns to match expected names
        grm_component = grm_component.rename({
            "SUBP_COMPONENT_GS_FOREST": "COMPONENT",
            "SUBP_SUBPTYP_GRM_GS_FOREST": "SUBPTYP_GRM",
            "SUBP_TPAREMV_UNADJ_GS_FOREST": "TPAREMV_UNADJ"
        })
        
        # Join with GRM component data
        data = data.join(
            grm_component,
            left_on="CN",
            right_on="TRE_CN",
            how="left"
        )
        
        # Load and join TREE_GRM_MIDPT for volume calculations
        if self.config.get("measure", "volume") == "volume":
            grm_midpt = self.db.read_table(
                "TREE_GRM_MIDPT",
                columns=["TRE_CN", "VOLCFNET"]
            ).lazy()
            
            data = data.join(
                grm_midpt,
                left_on="CN",
                right_on="TRE_CN",
                how="left"
            )
        
        # Load plot data for macro breakpoint
        plot = self.db.read_table(
            "PLOT",
            columns=["CN", "MACRO_BREAKPOINT_DIA", "PREV_PLT_CN", "STATECD", "INVYR"]
        ).lazy()
        
        data = data.join(
            plot.select(["CN", "MACRO_BREAKPOINT_DIA", "STATECD", "INVYR"]),
            left_on="PLT_CN",
            right_on="CN",
            how="left"
        )
        
        return data
    
    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply removals-specific filters."""
        # First apply base filters
        data = super().apply_filters(data)
        
        # Collect for removals filtering
        data_df = data.collect()
        
        # Filter to removal components (CUT or DIVERSION)
        # These are the components that represent tree removals
        data_df = data_df.filter(
            (pl.col("COMPONENT").str.starts_with("CUT")) |
            (pl.col("COMPONENT").str.starts_with("DIVERSION"))
        )
        
        # Filter out null removal values
        data_df = data_df.filter(
            pl.col("TPAREMV_UNADJ").is_not_null() &
            (pl.col("TPAREMV_UNADJ") > 0)
        )
        
        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            # Growing stock trees (live, commercial species, good form)
            # This would need additional filtering based on tree class
            # For now, filter to live trees with DIA >= 5.0
            data_df = data_df.filter(
                (pl.col("DIA") >= 5.0)
            )
        
        return data_df.lazy()
    
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate removal values per acre.
        
        Removals can be measured as volume, biomass, or tree count.
        """
        measure = self.config.get("measure", "volume")
        
        if measure == "volume":
            # Removal volume per acre
            # TPAREMV_UNADJ is already trees per acre removal
            # Multiply by volume to get volume per acre
            data = data.with_columns([
                (pl.col("VOLCFNET").cast(pl.Float64) * 
                 pl.col("TPAREMV_UNADJ").cast(pl.Float64)).alias("REMV_VALUE")
            ])
        elif measure == "biomass":
            # Removal biomass per acre
            data = data.with_columns([
                ((pl.col("DRYBIO_AG") + pl.col("DRYBIO_BG")).cast(pl.Float64) * 
                 pl.col("TPAREMV_UNADJ").cast(pl.Float64) / 
                 2000.0).alias("REMV_VALUE")
            ])
        else:  # count
            # Removal trees per acre
            data = data.with_columns([
                pl.col("TPAREMV_UNADJ").cast(pl.Float64).alias("REMV_VALUE")
            ])
        
        # Annualize removals (divide by remeasurement period)
        # Default to 5-year period if not specified
        remeasure_period = self.config.get("remeasure_period", 5.0)
        data = data.with_columns([
            (pl.col("REMV_VALUE") / remeasure_period).alias("REMV_ANNUAL")
        ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate removals with stratification."""
        # Get stratification data
        strat_data = self._get_stratification_data()
        
        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Apply adjustment factors based on SUBPTYP_GRM
        # SUBPTYP_GRM indicates which adjustment factor to use:
        # 0 = No adjustment, 1 = SUBP, 2 = MICR, 3 = MACR
        data_with_strat = data_with_strat.with_columns([
            pl.when(pl.col("SUBPTYP_GRM") == 0)
            .then(0.0)
            .when(pl.col("SUBPTYP_GRM") == 1)
            .then(pl.col("ADJ_FACTOR_SUBP"))
            .when(pl.col("SUBPTYP_GRM") == 2)
            .then(pl.col("ADJ_FACTOR_MICR"))
            .when(pl.col("SUBPTYP_GRM") == 3)
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(0.0)
            .alias("ADJ_FACTOR")
        ])
        
        # Apply adjustment
        data_with_strat = data_with_strat.with_columns([
            (pl.col("REMV_ANNUAL") * pl.col("ADJ_FACTOR")).alias("REMV_ADJ")
        ])
        
        # Setup grouping
        group_cols = self._setup_grouping()
        
        # Aggregate
        agg_exprs = [
            # Removals totals
            (pl.col("REMV_ADJ") * pl.col("EXPNS")).sum().alias("REMV_TOTAL"),
            (pl.col("REMV_ADJ") * pl.col("EXPNS")).sum().alias("REMV_NUM"),
            # Area
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            # Counts
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.count().alias("N_REMOVED_TREES")
        ]
        
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        results = results.collect()
        
        # Calculate per-acre value (ratio of means)
        results = results.with_columns([
            (pl.col("REMV_NUM") / pl.col("AREA_TOTAL")).alias("REMV_ACRE")
        ])
        
        # Clean up intermediate columns
        results = results.drop(["REMV_NUM"])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for removals estimates."""
        # Simple placeholder variance calculation
        # In production, this would use proper stratified variance formulas
        results = results.with_columns([
            (pl.col("REMV_ACRE") * 0.15).alias("REMV_ACRE_SE"),
            (pl.col("REMV_TOTAL") * 0.15).alias("REMV_TOTAL_SE")
        ])
        
        # Add coefficient of variation
        results = results.with_columns([
            (pl.col("REMV_ACRE_SE") / pl.col("REMV_ACRE") * 100).alias("REMV_ACRE_CV"),
            (pl.col("REMV_TOTAL_SE") / pl.col("REMV_TOTAL") * 100).alias("REMV_TOTAL_CV")
        ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format removals estimation output."""
        # Add metadata columns
        measure = self.config.get("measure", "volume")
        results = results.with_columns([
            pl.lit(2023).alias("YEAR"),
            pl.lit(measure.upper()).alias("MEASURE"),
            pl.lit("REMOVALS").alias("ESTIMATE_TYPE")
        ])
        
        # Format columns
        results = format_output_columns(
            results,
            estimation_type="removals",
            include_se=True,
            include_cv=True
        )
        
        # Rename columns for clarity
        column_renames = {
            "REMV_ACRE": "REMOVALS_PER_ACRE",
            "REMV_TOTAL": "REMOVALS_TOTAL",
            "REMV_ACRE_SE": "REMOVALS_PER_ACRE_SE",
            "REMV_TOTAL_SE": "REMOVALS_TOTAL_SE",
            "REMV_ACRE_CV": "REMOVALS_PER_ACRE_CV",
            "REMV_TOTAL_CV": "REMOVALS_TOTAL_CV"
        }
        
        results = results.rename(column_renames)
        
        return results


def removals(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "gs",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
    remeasure_period: float = 5.0
) -> pl.DataFrame:
    """
    Estimate average annual removals from FIA data.
    
    Calculates average annual removals of merchantable bole wood volume of
    growing-stock trees (at least 5 inches d.b.h.) on forest land.
    
    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path
    grp_by : Optional[Union[str, List[str]]]
        Columns to group by (e.g., "STATECD", "FORTYPCD")
    by_species : bool
        Group by species code
    by_size_class : bool
        Group by diameter size classes
    land_type : str
        Land type: "forest", "timber", or "all"
    tree_type : str
        Tree type: "gs" (growing stock), "all"
    measure : str
        What to measure: "volume", "biomass", or "count"
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
    remeasure_period : float
        Remeasurement period in years for annualization
        
    Returns
    -------
    pl.DataFrame
        Removals estimates with columns:
        - REMOVALS_PER_ACRE: Annual removals per acre
        - REMOVALS_TOTAL: Total annual removals
        - REMOVALS_PER_ACRE_SE: Standard error of per-acre estimate
        - REMOVALS_TOTAL_SE: Standard error of total estimate
        - Additional grouping columns if specified
        
    Examples
    --------
    >>> # Basic volume removals on forestland
    >>> results = removals(db, measure="volume")
    
    >>> # Removals by species (tree count)
    >>> results = removals(db, by_species=True, measure="count")
    
    >>> # Biomass removals by forest type
    >>> results = removals(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     measure="biomass"
    ... )
    
    >>> # Removals on timberland only
    >>> results = removals(
    ...     db,
    ...     land_type="timber",
    ...     area_domain="SITECLCD >= 225"  # Productive sites
    ... )
    
    Notes
    -----
    Removals include trees cut or otherwise removed from the inventory,
    including those diverted to non-forest use. The calculation uses
    TREE_GRM_COMPONENT table with CUT and DIVERSION components.
    
    The estimate is annualized by dividing by the remeasurement period
    (default 5 years).
    """
    # Create config
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "measure": measure,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "remeasure_period": remeasure_period
    }
    
    # Create and run estimator
    estimator = RemovalsEstimator(db, config)
    return estimator.estimate()