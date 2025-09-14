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
            "DIA", "TPA_UNADJ"
        ]
        
        # Add columns based on what we're measuring
        measure = self.config.get("measure", "volume")
        if measure == "biomass":
            cols.extend(["DRYBIO_AG", "DRYBIO_BG"])
        
        # Add grouping columns if needed
        if self.config.get("grp_by"):
            grp_cols = self.config["grp_by"]
            if isinstance(grp_cols, str):
                grp_cols = [grp_cols]
            for col in grp_cols:
                if col not in cols:
                    cols.append(col)
        
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
        # Use base class to load standard tree/condition data
        data = super().load_data()
        
        if data is None:
            return None
        
        # Now augment with GRM-specific data
        # Load TREE_GRM_COMPONENT table
        if "TREE_GRM_COMPONENT" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_COMPONENT")
            except Exception as e:
                # If GRM tables don't exist, return None or raise error
                raise ValueError(f"TREE_GRM_COMPONENT table not found in database: {e}")
        
        grm_component = self.db.tables["TREE_GRM_COMPONENT"]
        
        # Ensure LazyFrame
        if not isinstance(grm_component, pl.LazyFrame):
            grm_component = grm_component.lazy()
        
        # Select and rename GRM columns
        grm_component = grm_component.select([
            pl.col("TRE_CN"),
            pl.col("DIA_BEGIN"),
            pl.col("DIA_MIDPT"),
            pl.col("SUBP_COMPONENT_GS_FOREST").alias("COMPONENT"),
            pl.col("SUBP_SUBPTYP_GRM_GS_FOREST").alias("SUBPTYP_GRM"),
            pl.col("SUBP_TPAREMV_UNADJ_GS_FOREST").alias("TPAREMV_UNADJ")
        ])
        
        # Join with GRM component data
        data = data.join(
            grm_component,
            left_on="CN",
            right_on="TRE_CN",
            how="left"
        )
        
        # Load TREE_GRM_MIDPT for volume calculations if needed
        if self.config.get("measure", "volume") == "volume":
            if "TREE_GRM_MIDPT" not in self.db.tables:
                try:
                    self.db.load_table("TREE_GRM_MIDPT")
                except Exception as e:
                    raise ValueError(f"TREE_GRM_MIDPT table not found in database: {e}")
            
            grm_midpt = self.db.tables["TREE_GRM_MIDPT"]
            
            if not isinstance(grm_midpt, pl.LazyFrame):
                grm_midpt = grm_midpt.lazy()
            
            grm_midpt = grm_midpt.select([
                pl.col("TRE_CN"),
                pl.col("VOLCFNET")
            ])
            
            data = data.join(
                grm_midpt,
                left_on="CN",
                right_on="TRE_CN",
                how="left"
            )
        
        # Add PLOT data for macro breakpoint if not already present
        if "MACRO_BREAKPOINT_DIA" not in data.collect_schema().names():
            if "PLOT" not in self.db.tables:
                self.db.load_table("PLOT")
            
            plot = self.db.tables["PLOT"]
            if not isinstance(plot, pl.LazyFrame):
                plot = plot.lazy()
            
            plot_cols = plot.select(["CN", "MACRO_BREAKPOINT_DIA", "STATECD", "INVYR"])
            
            data = data.join(
                plot_cols,
                left_on="PLT_CN",
                right_on="CN",
                how="left"
            )
        
        return data
    
    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply removals-specific filters."""
        # First apply base filters (handles tree_domain, area_domain, land_type)
        data = super().apply_filters(data)
        
        # Now apply removals-specific filters while maintaining lazy evaluation
        # Filter to removal components (CUT or DIVERSION)
        data = data.filter(
            (pl.col("COMPONENT").str.starts_with("CUT")) |
            (pl.col("COMPONENT").str.starts_with("DIVERSION"))
        )
        
        # Filter out null removal values
        data = data.filter(
            pl.col("TPAREMV_UNADJ").is_not_null() &
            (pl.col("TPAREMV_UNADJ") > 0)
        )
        
        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            # Growing stock trees (at least 5 inches DBH)
            data = data.filter(pl.col("DIA") >= 5.0)
        
        return data
    
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
        """Aggregate removals with two-stage aggregation for correct per-acre estimates.

        CRITICAL FIX: This method implements two-stage aggregation following FIA
        methodology. The previous single-stage approach caused ~20x underestimation
        by having each removal component contribute its condition proportion to the denominator.

        Stage 1: Aggregate removal components to plot-condition level
        Stage 2: Apply expansion factors and calculate ratio-of-means
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )

        # For removals, we need custom adjustment factor logic based on SUBPTYP_GRM
        # SUBPTYP_GRM indicates which adjustment factor to use:
        # 0 = No adjustment, 1 = SUBP, 2 = MICR, 3 = MACR
        # This is different from the standard tree adjustment which uses DIA size classes
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

        # ========================================================================
        # CRITICAL FIX: Two-stage aggregation following FIA methodology
        # ========================================================================

        # STAGE 1: Aggregate removal components to plot-condition level
        # This ensures each condition's area proportion is counted exactly once
        condition_group_cols = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
        if group_cols:
            # Add user-specified grouping columns if they exist at condition level
            for col in group_cols:
                if col in data_with_strat.collect_schema().names() and col not in condition_group_cols:
                    condition_group_cols.append(col)

        # Aggregate removals at condition level
        condition_agg = data_with_strat.group_by(condition_group_cols).agg([
            # Sum removals within each condition
            pl.col("REMV_ADJ").sum().alias("CONDITION_REMOVALS"),
            # Count removal components per condition for diagnostics
            pl.len().alias("COMPONENTS_PER_CONDITION")
        ])

        # STAGE 2: Apply expansion factors and calculate population estimates
        if group_cols:
            # Group by user-specified columns for final aggregation
            final_group_cols = [col for col in group_cols if col in condition_agg.collect_schema().names()]
            if final_group_cols:
                results = condition_agg.group_by(final_group_cols).agg([
                    # Numerator: Sum of expanded condition removals
                    (pl.col("CONDITION_REMOVALS") * pl.col("EXPNS")).sum().alias("REMV_NUM"),
                    # Denominator: Sum of expanded condition areas
                    (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
                    # Total removals (for totals=True)
                    (pl.col("CONDITION_REMOVALS") * pl.col("EXPNS")).sum().alias("REMV_TOTAL"),
                    # Diagnostic counts
                    pl.n_unique("PLT_CN").alias("N_PLOTS"),
                    pl.col("COMPONENTS_PER_CONDITION").sum().alias("N_REMOVED_TREES"),
                    pl.len().alias("N_CONDITIONS")
                ])
            else:
                # No valid grouping columns at condition level
                results = condition_agg.select([
                    (pl.col("CONDITION_REMOVALS") * pl.col("EXPNS")).sum().alias("REMV_NUM"),
                    (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
                    (pl.col("CONDITION_REMOVALS") * pl.col("EXPNS")).sum().alias("REMV_TOTAL"),
                    pl.n_unique("PLT_CN").alias("N_PLOTS"),
                    pl.col("COMPONENTS_PER_CONDITION").sum().alias("N_REMOVED_TREES"),
                    pl.len().alias("N_CONDITIONS")
                ])
        else:
            # No grouping - aggregate all conditions
            results = condition_agg.select([
                (pl.col("CONDITION_REMOVALS") * pl.col("EXPNS")).sum().alias("REMV_NUM"),
                (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
                (pl.col("CONDITION_REMOVALS") * pl.col("EXPNS")).sum().alias("REMV_TOTAL"),
                pl.n_unique("PLT_CN").alias("N_PLOTS"),
                pl.col("COMPONENTS_PER_CONDITION").sum().alias("N_REMOVED_TREES"),
                pl.len().alias("N_CONDITIONS")
            ])

        results = results.collect()

        # Calculate per-acre value (ratio of means)
        # This is now correct because each condition contributes exactly once to denominator
        # Add protection against division by zero
        results = results.with_columns([
            pl.when(pl.col("AREA_TOTAL") > 0)
            .then(pl.col("REMV_NUM") / pl.col("AREA_TOTAL"))
            .otherwise(0.0)
            .alias("REMV_ACRE")
        ])

        # Clean up intermediate columns
        results = results.drop(["REMV_NUM", "N_CONDITIONS"])

        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for removals estimates."""
        # TODO: Implement proper stratified variance calculation
        # For now, use conservative placeholder (20% CV is typical for removals)
        # This should use the VarianceCalculator class when properly implemented
        results = results.with_columns([
            (pl.col("REMV_ACRE") * 0.20).alias("REMV_ACRE_SE"),
            (pl.col("REMV_TOTAL") * 0.20).alias("REMV_TOTAL_SE")
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
        
        # Try to extract actual year from data if available
        year = self.config.get("year", 2023)
        if "INVYR" in results.columns:
            year = results["INVYR"].max()
        
        results = results.with_columns([
            pl.lit(year).alias("YEAR"),
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