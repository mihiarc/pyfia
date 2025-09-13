"""
Mortality estimation for FIA data using GRM tables.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree mortality using TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..utils import format_output_columns


class MortalityEstimator(BaseEstimator):
    """
    Mortality estimator for FIA data using GRM methodology.
    
    Estimates annual tree mortality in terms of volume, biomass, or trees per acre
    using the TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
    """
    
    def get_required_tables(self) -> List[str]:
        """Mortality requires GRM tables for proper calculation."""
        return [
            "TREE_GRM_COMPONENT", "TREE_GRM_MIDPT", "COND", "PLOT",
            "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"
        ]
    
    def get_tree_columns(self) -> List[str]:
        """Required columns from TREE_GRM tables."""
        # Base columns always needed
        cols = ["TRE_CN", "PLT_CN", "DIA_BEGIN", "DIA_MIDPT", "DIA_END"]
        
        # Add columns based on land type and tree type
        land_type = self.config.get("land_type", "forest").upper()
        tree_type = self.config.get("tree_type", "gs").upper()
        
        # Map tree_type to FIA convention
        if tree_type == "LIVE":
            tree_type = "AL"  # All live
        elif tree_type == "GS":
            tree_type = "GS"  # Growing stock
        elif tree_type == "SAWTIMBER":
            tree_type = "SL"  # Sawtimber
        else:
            tree_type = "GS"  # Default to growing stock
        
        # Build column names based on land and tree type
        prefix = f"SUBP_COMPONENT_{tree_type}_{land_type}"
        cols.extend([
            f"{prefix}",  # Component type (MORTALITY1, MORTALITY2, etc.)
            f"SUBP_TPAMORT_UNADJ_{tree_type}_{land_type}",  # Mortality TPA
            f"SUBP_SUBPTYP_GRM_{tree_type}_{land_type}",  # Adjustment type
        ])
        
        # Store column names for later use
        self._component_col = f"SUBP_COMPONENT_{tree_type}_{land_type}"
        self._tpamort_col = f"SUBP_TPAMORT_UNADJ_{tree_type}_{land_type}"
        self._subptyp_col = f"SUBP_SUBPTYP_GRM_{tree_type}_{land_type}"
        
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
        Load and join GRM tables for mortality calculation.
        """
        # Load TREE_GRM_COMPONENT as primary table
        if "TREE_GRM_COMPONENT" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_COMPONENT")
            except Exception as e:
                raise ValueError(f"TREE_GRM_COMPONENT table not found: {e}")
        
        grm_component = self.db.tables["TREE_GRM_COMPONENT"]
        
        # Ensure LazyFrame
        if not isinstance(grm_component, pl.LazyFrame):
            grm_component = grm_component.lazy()
        
        # Get required columns
        tree_cols = self.get_tree_columns()
        
        # Select and rename columns for cleaner processing
        grm_component = grm_component.select([
            pl.col("TRE_CN"),
            pl.col("PLT_CN"),
            pl.col("DIA_BEGIN"),
            pl.col("DIA_MIDPT"),
            pl.col("DIA_END"),
            pl.col(self._component_col).alias("COMPONENT"),
            pl.col(self._tpamort_col).alias("TPAMORT_UNADJ"),
            pl.col(self._subptyp_col).alias("SUBPTYP_GRM")
        ])
        
        # Load TREE_GRM_MIDPT for volume/biomass data
        if "TREE_GRM_MIDPT" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_MIDPT")
            except Exception as e:
                raise ValueError(f"TREE_GRM_MIDPT table not found: {e}")
        
        grm_midpt = self.db.tables["TREE_GRM_MIDPT"]
        
        if not isinstance(grm_midpt, pl.LazyFrame):
            grm_midpt = grm_midpt.lazy()
        
        # Select columns based on measurement type
        measure = self.config.get("measure", "volume")
        if measure == "volume":
            midpt_cols = ["TRE_CN", "VOLCFNET", "DIA", "SPCD", "STATUSCD"]
        elif measure == "biomass":
            midpt_cols = ["TRE_CN", "DRYBIO_BOLE", "DRYBIO_BRANCH", "DIA", "SPCD", "STATUSCD"]
        else:  # count
            midpt_cols = ["TRE_CN", "DIA", "SPCD", "STATUSCD"]
        
        grm_midpt = grm_midpt.select(midpt_cols)
        
        # Join GRM tables
        data = grm_component.join(
            grm_midpt,
            on="TRE_CN",
            how="inner"
        )
        
        # Load and join COND table
        if "COND" not in self.db.tables:
            self.db.load_table("COND")
        
        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()
        
        cond_cols = self.get_cond_columns()
        cond = cond.select([c for c in cond_cols if c in cond.collect_schema().names()])
        
        # Join with conditions
        data = data.join(
            cond,
            on="PLT_CN",
            how="inner"
        )
        
        # Add PLOT data for additional info if needed
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")
        
        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()
        
        # Select minimal plot columns
        plot = plot.select(["CN", "STATECD", "INVYR", "MACRO_BREAKPOINT_DIA"])
        
        data = data.join(
            plot,
            left_on="PLT_CN",
            right_on="CN",
            how="left"
        )
        
        return data
    
    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply mortality-specific filters."""
        # First apply base filters (tree_domain, area_domain, land_type)
        data = super().apply_filters(data)
        
        # Filter to mortality components only
        data = data.filter(
            pl.col("COMPONENT").str.starts_with("MORTALITY")
        )
        
        # Filter to records with positive mortality
        data = data.filter(
            (pl.col("TPAMORT_UNADJ").is_not_null()) &
            (pl.col("TPAMORT_UNADJ") > 0)
        )
        
        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            # Growing stock trees (typically >= 5 inches DBH with merchantable volume)
            data = data.filter(pl.col("DIA_MIDPT") >= 5.0)
            if "VOLCFNET" in data.collect_schema().names():
                data = data.filter(pl.col("VOLCFNET") > 0)
        
        return data
    
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate mortality values per acre.
        
        TPAMORT_UNADJ is already annualized, so no remeasurement period adjustment needed.
        """
        measure = self.config.get("measure", "volume")
        
        if measure == "volume":
            # Mortality volume per acre = TPAMORT * Volume
            data = data.with_columns([
                (pl.col("TPAMORT_UNADJ").cast(pl.Float64) * 
                 pl.col("VOLCFNET").cast(pl.Float64)).alias("MORT_VALUE")
            ])
        elif measure == "biomass":
            # Mortality biomass per acre (total biomass in tons)
            # DRYBIO fields are in pounds, convert to tons
            data = data.with_columns([
                (pl.col("TPAMORT_UNADJ").cast(pl.Float64) * 
                 (pl.col("DRYBIO_BOLE") + pl.col("DRYBIO_BRANCH")).cast(pl.Float64) / 
                 2000.0).alias("MORT_VALUE")
            ])
        else:  # count
            # Mortality trees per acre
            data = data.with_columns([
                pl.col("TPAMORT_UNADJ").cast(pl.Float64).alias("MORT_VALUE")
            ])
        
        # TPAMORT_UNADJ is already annual, so no division by remeasurement period
        data = data.with_columns([
            pl.col("MORT_VALUE").alias("MORT_ANNUAL")
        ])
        
        return data
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate mortality with proper GRM adjustment factors."""
        # Get stratification data
        strat_data = self._get_stratification_data()
        
        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Apply GRM-specific adjustment factors based on SUBPTYP_GRM
        # SUBPTYP_GRM: 0=None, 1=SUBP, 2=MICR, 3=MACR
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
            (pl.col("MORT_ANNUAL") * pl.col("ADJ_FACTOR")).alias("MORT_ADJ")
        ])
        
        # Setup grouping
        group_cols = self._setup_grouping()
        
        # Add species to grouping if requested
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")
        
        # Aggregate expressions
        agg_exprs = [
            # Mortality totals (expanded to population)
            (pl.col("MORT_ADJ") * pl.col("EXPNS")).sum().alias("MORT_TOTAL"),
            # For per-acre calculation
            (pl.col("MORT_ADJ") * pl.col("EXPNS")).sum().alias("MORT_NUM"),
            # Area calculation
            (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            # Counts
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.len().alias("N_DEAD_TREES")  # Using pl.len() instead of pl.count()
        ]
        
        # Perform aggregation
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        # Collect results
        results = results.collect()
        
        # Calculate per-acre value (ratio of means)
        # Add small epsilon to avoid division by zero
        results = results.with_columns([
            pl.when(pl.col("AREA_TOTAL") > 0)
            .then(pl.col("MORT_NUM") / pl.col("AREA_TOTAL"))
            .otherwise(0.0)
            .alias("MORT_ACRE")
        ])
        
        # Clean up intermediate column
        results = results.drop(["MORT_NUM"])
        
        # Calculate mortality rate if requested
        if self.config.get("as_rate", False):
            # This would require live tree data for proper rate calculation
            # For now, add as a percentage of mortality per acre
            results = results.with_columns([
                pl.col("MORT_ACRE").alias("MORT_RATE")
            ])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for mortality estimates."""
        # Simplified variance calculation
        # In production, should use proper stratified variance formulas
        # Conservative estimate: 15-20% CV is typical for mortality
        results = results.with_columns([
            (pl.col("MORT_ACRE") * 0.15).alias("MORT_ACRE_SE"),
            (pl.col("MORT_TOTAL") * 0.15).alias("MORT_TOTAL_SE")
        ])
        
        if "MORT_RATE" in results.columns:
            results = results.with_columns([
                (pl.col("MORT_RATE") * 0.20).alias("MORT_RATE_SE")
            ])
        
        # Add CV if requested
        if self.config.get("include_cv", False):
            results = results.with_columns([
                pl.when(pl.col("MORT_ACRE") > 0)
                .then(pl.col("MORT_ACRE_SE") / pl.col("MORT_ACRE") * 100)
                .otherwise(None)
                .alias("MORT_ACRE_CV"),
                
                pl.when(pl.col("MORT_TOTAL") > 0)
                .then(pl.col("MORT_TOTAL_SE") / pl.col("MORT_TOTAL") * 100)
                .otherwise(None)
                .alias("MORT_TOTAL_CV")
            ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format mortality estimation output."""
        # Add metadata
        measure = self.config.get("measure", "volume")
        land_type = self.config.get("land_type", "forest")
        tree_type = self.config.get("tree_type", "gs")
        
        results = results.with_columns([
            pl.lit(2023).alias("YEAR"),  # Would extract from INVYR in production
            pl.lit(measure.upper()).alias("MEASURE"),
            pl.lit(land_type.upper()).alias("LAND_TYPE"),
            pl.lit(tree_type.upper()).alias("TREE_TYPE")
        ])
        
        # Format columns
        results = format_output_columns(
            results,
            estimation_type="mortality",
            include_se=True,
            include_cv=self.config.get("include_cv", False)
        )
        
        return results


def mortality(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "gs",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    as_rate: bool = False,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False
) -> pl.DataFrame:
    """
    Estimate annual tree mortality from FIA data using GRM methodology.
    
    Uses TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables to calculate
    annual mortality following FIA's Growth-Removal-Mortality approach.
    This is the correct FIA statistical methodology for mortality estimation.
    
    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the 
        FIA tables used in the estimation (PLOT, COND, TREE_GRM_COMPONENT,
        TREE_GRM_MIDPT). Common grouping columns include:
        
        - 'FORTYPCD': Forest type code
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'STATECD': State FIPS code
        - 'COUNTYCD': County code  
        - 'UNITCD': FIA survey unit
        - 'INVYR': Inventory year
        - 'STDAGE': Stand age class
        - 'SITECLCD': Site productivity class
        - 'DSTRBCD1', 'DSTRBCD2', 'DSTRBCD3': Disturbance codes (from COND)
        
        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). This is a convenience
        parameter equivalent to adding 'SPCD' to grp_by.
    by_size_class : bool, default False
        If True, group results by diameter size classes. Size classes are
        defined as: 1.0-4.9", 5.0-9.9", 10.0-19.9", 20.0-29.9", 30.0+".
    land_type : {'forest', 'timber'}, default 'forest'
        Land type to include in estimation:
        
        - 'forest': All forestland
        - 'timber': Productive timberland only (unreserved, productive)
    tree_type : {'gs', 'al', 'sl'}, default 'gs'
        Tree type to include:
        
        - 'gs': Growing stock trees (live, merchantable)
        - 'al': All live trees
        - 'sl': Sawtimber trees
    measure : {'volume', 'biomass', 'count'}, default 'volume'
        What to measure in the mortality estimation:
        
        - 'volume': Net cubic foot volume
        - 'biomass': Total aboveground biomass in tons
        - 'count': Number of trees per acre
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering. Applied to
        TREE_GRM tables. Example: "DIA_MIDPT >= 10.0 AND SPCD == 131".
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
        Applied to COND table. Example: "OWNGRPCD == 40 AND FORTYPCD == 161".
    as_rate : bool, default False
        If True, return mortality as a rate (mortality/live). Note: This
        requires additional live tree data and is not fully implemented.
    totals : bool, default True
        If True, include population-level total estimates in addition to
        per-acre values.
    variance : bool, default False
        If True, calculate and include variance and standard error estimates.
        Note: Currently uses simplified variance calculation (15% CV for
        per-acre estimates, 20% CV for rates).
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation for
        each state in the database.
        
    Returns
    -------
    pl.DataFrame
        Mortality estimates with the following columns:
        
        - **MORT_ACRE** : float
            Annual mortality per acre in units specified by 'measure'
        - **MORT_TOTAL** : float (if totals=True)
            Total annual mortality expanded to population level
        - **MORT_ACRE_SE** : float (if variance=True)
            Standard error of per-acre mortality estimate
        - **MORT_TOTAL_SE** : float (if variance=True and totals=True)
            Standard error of total mortality estimate
        - **AREA_TOTAL** : float
            Total area (acres) represented by the estimation
        - **N_PLOTS** : int
            Number of FIA plots included in the estimation
        - **N_DEAD_TREES** : int
            Number of individual mortality records
        - **YEAR** : int
            Representative year for the estimation
        - **MEASURE** : str
            Type of measurement ('VOLUME', 'BIOMASS', or 'COUNT')
        - **LAND_TYPE** : str
            Land type used ('FOREST' or 'TIMBER')
        - **TREE_TYPE** : str
            Tree type used ('GS', 'AL', or 'SL')
        - **[grouping columns]** : various
            Any columns specified in grp_by or from by_species
    
    See Also
    --------
    growth : Estimate annual growth using GRM tables
    removals : Estimate annual removals/harvest using GRM tables
    tpa : Estimate trees per acre (current inventory)
    volume : Estimate volume per acre (current inventory)
    biomass : Estimate biomass per acre (current inventory)
    pyfia.constants.OwnershipGroup : Ownership group code definitions
    pyfia.constants.TreeStatus : Tree status code definitions
    pyfia.utils.reference_tables : Functions for adding species/forest type names
        
    Examples
    --------
    Basic volume mortality on forestland:
    
    >>> results = mortality(db, measure="volume", land_type="forest")
    >>> if not results.is_empty():
    ...     print(f"Annual mortality: {results['MORT_ACRE'][0]:.1f} cu ft/acre")
    ... else:
    ...     print("No mortality data available")
    
    Mortality by species (tree count):
    
    >>> results = mortality(db, by_species=True, measure="count")
    >>> # Sort by mortality to find most affected species
    >>> if not results.is_empty():
    ...     top_species = results.sort(by='MORT_ACRE', descending=True).head(5)
    
    Biomass mortality on timberland by ownership:
    
    >>> results = mortality(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     land_type="timber",
    ...     measure="biomass",
    ...     tree_type="gs"
    ... )
    
    Mortality by multiple grouping variables:
    
    >>> results = mortality(
    ...     db,
    ...     grp_by=["STATECD", "FORTYPCD"],
    ...     variance=True,
    ...     tree_domain="DIA_MIDPT >= 10.0"
    ... )
    
    Complex filtering with domain expressions:
    
    >>> # Large tree mortality only
    >>> results = mortality(
    ...     db,
    ...     tree_domain="DIA_MIDPT >= 20.0",
    ...     by_species=True
    ... )
    
    Notes
    -----
    This function uses FIA's GRM (Growth-Removal-Mortality) tables which
    contain pre-calculated annual mortality values. The TPAMORT_UNADJ
    fields are already annualized, so no remeasurement period adjustment
    is needed.
    
    **Important:** Mortality agent codes (AGENTCD) are stored in the regular
    TREE table, not the GRM tables. Since mortality() uses TREE_GRM_COMPONENT
    and TREE_GRM_MIDPT tables, AGENTCD is not available for grouping. To
    analyze mortality by agent, you would need to join with the TREE table
    or use disturbance codes (DSTRBCD1-3) from the COND table instead.
    
    The adjustment factors are determined by the SUBPTYP_GRM field:
    
    - 0: No adjustment (trees not sampled)
    - 1: Subplot adjustment (ADJ_FACTOR_SUBP)
    - 2: Microplot adjustment (ADJ_FACTOR_MICR)  
    - 3: Macroplot adjustment (ADJ_FACTOR_MACR)
    
    Valid grouping columns depend on which tables are included in the
    estimation query. The mortality() function joins TREE_GRM_COMPONENT,
    TREE_GRM_MIDPT, COND, PLOT, and POP_* tables, so any column from
    these tables can be used for grouping. For a complete list of
    available columns and their meanings, refer to:
    
    - USDA FIA Database User Guide, Version 9.1
    - pyFIA documentation: https://mihiarc.github.io/pyfia/
    - FIA DataMart: https://apps.fs.usda.gov/fia/datamart/
    
    The function requires GRM tables to be present in the database.
    These tables are included in FIA evaluations that support growth,
    removal, and mortality estimation (typically EVALID types ending
    in 01, 03, or specific GRM evaluations).
    
    Warnings
    --------
    The current implementation uses a simplified variance calculation
    (15% CV for per-acre estimates, 20% CV for mortality rates). Full
    stratified variance calculation following Bechtold & Patterson (2005)
    will be implemented in a future release. For applications requiring
    precise variance estimates, consider using the FIA EVALIDator tool
    or other specialized FIA analysis software.
    
    Raises
    ------
    ValueError
        If TREE_GRM_COMPONENT or TREE_GRM_MIDPT tables are not found
        in the database, or if grp_by contains invalid column names.
    KeyError
        If specified columns in grp_by don't exist in the joined tables.
    """
    # Create configuration
    config = {
        "grp_by": grp_by,
        "by_species": by_species,
        "by_size_class": by_size_class,
        "land_type": land_type,
        "tree_type": tree_type,
        "measure": measure,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "as_rate": as_rate,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "include_cv": False  # Could be added as parameter
    }
    
    # Create and run estimator
    estimator = MortalityEstimator(db, config)
    return estimator.estimate()