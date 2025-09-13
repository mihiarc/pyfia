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
        # Simplified variance calculation
        # Conservative estimate: 10-15% CV is typical for volume estimates
        # Higher CV for smaller areas or rare conditions
        results = results.with_columns([
            (pl.col("VOLUME_ACRE") * 0.12).alias("VOLUME_ACRE_SE"),
            (pl.col("VOLUME_TOTAL") * 0.12).alias("VOLUME_TOTAL_SE")
        ])

        # Add CV if requested
        if self.config.get("include_cv", False):
            results = results.with_columns([
                pl.when(pl.col("VOLUME_ACRE") > 0)
                .then(pl.col("VOLUME_ACRE_SE") / pl.col("VOLUME_ACRE") * 100)
                .otherwise(None)
                .alias("VOLUME_ACRE_CV"),

                pl.when(pl.col("VOLUME_TOTAL") > 0)
                .then(pl.col("VOLUME_TOTAL_SE") / pl.col("VOLUME_TOTAL") * 100)
                .otherwise(None)
                .alias("VOLUME_TOTAL_CV")
            ])

        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format volume estimation output."""
        # Add metadata
        vol_type = self.config.get("vol_type", "net")
        land_type = self.config.get("land_type", "forest")
        tree_type = self.config.get("tree_type", "live")

        results = results.with_columns([
            pl.lit(2023).alias("YEAR"),  # Would extract from INVYR in production
            pl.lit(vol_type.upper()).alias("VOL_TYPE"),
            pl.lit(land_type.upper()).alias("LAND_TYPE"),
            pl.lit(tree_type.upper()).alias("TREE_TYPE")
        ])

        # Format columns
        results = format_output_columns(
            results,
            estimation_type="volume",
            include_se=True,
            include_cv=self.config.get("include_cv", False)
        )

        # Rename to standard FIA column names based on vol_type
        if vol_type == "net":
            prefix = "VOLCFNET"
        elif vol_type == "gross":
            prefix = "VOLCFGRS"
        elif vol_type == "sound":
            prefix = "VOLCFSND"
        elif vol_type == "sawlog":
            prefix = "VOLBFNET"
        else:
            prefix = "VOLCFNET"

        rename_map = {
            "VOLUME_ACRE": f"{prefix}_ACRE",
            "VOLUME_TOTAL": f"{prefix}_TOTAL",
            "VOLUME_ACRE_SE": f"{prefix}_ACRE_SE",
            "VOLUME_TOTAL_SE": f"{prefix}_TOTAL_SE",
            "VOLUME_ACRE_CV": f"{prefix}_ACRE_CV",
            "VOLUME_TOTAL_CV": f"{prefix}_TOTAL_CV"
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
    most_recent: bool = False,
    eval_type: Optional[str] = None
) -> pl.DataFrame:
    """
    Estimate tree volume from FIA data.

    Calculates volume estimates using FIA's design-based estimation methods
    with proper expansion factors and stratification. Automatically handles
    EVALID selection to prevent overcounting from multiple evaluations.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        TREE, COND, and PLOT tables. Common grouping columns include:

        **Tree Characteristics:**
        - 'SPCD': Species code (see REF_SPECIES table)
        - 'SPGRPCD': Species group code (hardwood/softwood groups)
        - 'DIA': Diameter at breast height (continuous, use with caution)
        - 'HT': Total tree height in feet
        - 'CR': Crown ratio (percent of bole with live crown)
        - 'CCLCD': Crown class code (1=Open grown, 2=Dominant, 3=Codominant,
          4=Intermediate, 5=Overtopped)
        - 'TREECLCD': Tree class code (2=Growing stock, 3=Rough cull, 4=Rotten cull)
        - 'DECAYCD': Decay class for standing dead trees

        **Ownership and Management:**
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'OWNCD': Detailed ownership code (see REF_RESEARCH_STATION)
        - 'ADFORCD': Administrative forest code
        - 'RESERVCD': Reserved status (0=Not reserved, 1=Reserved)

        **Forest Characteristics:**
        - 'FORTYPCD': Forest type code (see REF_FOREST_TYPE)
        - 'STDSZCD': Stand size class (1=Large diameter, 2=Medium diameter,
          3=Small diameter, 4=Seedling/sapling, 5=Nonstocked)
        - 'STDORGCD': Stand origin (0=Natural, 1=Planted)
        - 'STDAGE': Stand age in years

        **Site Characteristics:**
        - 'SITECLCD': Site productivity class (1=225+ cu ft/ac/yr,
          2=165-224, 3=120-164, 4=85-119, 5=50-84, 6=20-49, 7=0-19)
        - 'PHYSCLCD': Physiographic class code
        - 'SLOPE': Slope in percent
        - 'ASPECT': Aspect in degrees (0-360)

        **Location:**
        - 'STATECD': State FIPS code
        - 'UNITCD': FIA survey unit code
        - 'COUNTYCD': County code
        - 'INVYR': Inventory year

        **Disturbance and Treatment:**
        - 'DSTRBCD1', 'DSTRBCD2', 'DSTRBCD3': Disturbance codes
        - 'TRTCD1', 'TRTCD2', 'TRTCD3': Treatment codes

        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). This is a convenience
        parameter equivalent to adding 'SPCD' to grp_by.
    by_size_class : bool, default False
        If True, group results by diameter size classes. Size classes are
        defined as: 1.0-4.9", 5.0-9.9", 10.0-19.9", 20.0-29.9", 30.0+".
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland (COND_STATUS_CD = 1)
        - 'timber': Timberland only (unreserved, productive forestland with
          SITECLCD < 7 and RESERVCD = 0)
        - 'all': All land types including non-forest
    tree_type : {'live', 'dead', 'gs', 'all'}, default 'live'
        Tree type to include:

        - 'live': All live trees (STATUSCD = 1)
        - 'dead': Standing dead trees (STATUSCD = 2)
        - 'gs': Growing stock trees (live, TREECLCD = 2, no defects)
        - 'all': All trees regardless of status
    vol_type : {'net', 'gross', 'sound', 'sawlog'}, default 'net'
        Volume type to estimate:

        - 'net': Net cubic foot volume (VOLCFNET) - gross minus defects
        - 'gross': Gross cubic foot volume (VOLCFGRS) - total stem volume
        - 'sound': Sound cubic foot volume (VOLCFSND) - gross minus rot
        - 'sawlog': Sawlog board foot volume (VOLBFNET) - net board feet
    tree_domain : str, optional
        SQL-like filter expression for tree-level attributes. Examples:

        - "DIA >= 10.0": Trees 10 inches DBH and larger
        - "SPCD IN (131, 110)": Specific species (loblolly and Virginia pine)
        - "DIA BETWEEN 10 AND 20": Mid-sized trees
        - "HT > 50 AND CR > 30": Tall trees with good crowns
    area_domain : str, optional
        SQL-like filter expression for COND-level attributes. Examples:

        - "STDAGE > 50": Stands older than 50 years
        - "FORTYPCD IN (161, 162)": Specific forest types
        - "OWNGRPCD == 40": Private lands only
        - "SLOPE < 30 AND ASPECT BETWEEN 135 AND 225": Gentle south-facing slopes
    totals : bool, default True
        If True, include total volume estimates expanded to population level.
        If False, only return per-acre values.
    variance : bool, default False
        If True, return variance instead of standard error.
    most_recent : bool, default False
        If True, automatically select the most recent evaluation for each
        state/region. Equivalent to calling db.clip_most_recent() first.
    eval_type : str, optional
        Evaluation type to select if most_recent=True. Options:
        'ALL', 'VOL', 'GROW', 'MORT', 'REMV', 'CHANGE', 'DWM', 'INV'.
        Default is 'VOL' for volume estimation.

    Returns
    -------
    pl.DataFrame
        Volume estimates with the following columns:

        - **YEAR** : int
            Inventory year
        - **[grouping columns]** : varies
            Any columns specified in grp_by parameter
        - **VOLCFNET_ACRE** : float (if vol_type='net')
            Net cubic foot volume per acre
        - **VOLCFGRS_ACRE** : float (if vol_type='gross')
            Gross cubic foot volume per acre
        - **VOLCFSND_ACRE** : float (if vol_type='sound')
            Sound cubic foot volume per acre
        - **VOLBFNET_ACRE** : float (if vol_type='sawlog')
            Net board foot volume per acre
        - **VOLCFNET_ACRE_SE** : float (if variance=False)
            Standard error of per-acre volume estimate
        - **VOLCFNET_ACRE_VAR** : float (if variance=True)
            Variance of per-acre volume estimate
        - **N_PLOTS** : int
            Number of plots in estimate
        - **N_TREES** : int
            Number of trees in estimate
        - **AREA_TOTAL** : float
            Total area (acres) represented by the estimation
        - **VOLCFNET_TOTAL** : float (if totals=True)
            Total volume expanded to population level
        - **VOLCFNET_TOTAL_SE** : float (if totals=True and variance=False)
            Standard error of total volume

    See Also
    --------
    pyfia.area : Estimate forest area
    pyfia.biomass : Estimate tree biomass
    pyfia.tpa : Estimate trees per acre
    pyfia.mortality : Estimate annual mortality
    pyfia.growth : Estimate annual growth
    pyfia.constants.SpeciesCodes : Species code definitions
    pyfia.constants.ForestTypes : Forest type code definitions
    pyfia.constants.StateCodes : State FIPS code definitions
    pyfia.utils.reference_tables : Functions for adding species/forest type names

    Notes
    -----
    The volume estimation follows USDA FIA's design-based estimation procedures
    as described in Bechtold & Patterson (2005). The basic formula is:

    Volume per acre = Σ(VOLCFNET × TPA_UNADJ × ADJ_FACTOR × EXPNS) / Σ(AREA)

    Where:
    - VOLCFNET: Net cubic foot volume per tree (or VOLCFGRS, VOLCFSND, VOLBFNET)
    - TPA_UNADJ: Unadjusted trees per acre factor
    - ADJ_FACTOR: Size-based adjustment factor (MICR, SUBP, or MACR)
    - EXPNS: Expansion factor from stratification
    - AREA: Total area from condition proportions

    **Adjustment Factors:**
    Trees are adjusted based on their diameter and sampling method:
    - Trees < 5.0" DBH: Microplot adjustment (ADJ_FACTOR_MICR)
    - Trees 5.0" to MACRO_BREAKPOINT_DIA: Subplot adjustment (ADJ_FACTOR_SUBP)
    - Trees ≥ MACRO_BREAKPOINT_DIA: Macroplot adjustment (ADJ_FACTOR_MACR)

    **EVALID Handling:**
    If no EVALID is specified, the function automatically selects the most
    recent EXPVOL evaluation to prevent overcounting from multiple evaluations.
    For explicit control, use db.clip_by_evalid() before calling volume().

    **Valid Grouping Columns:**
    The function joins TREE, COND, and PLOT tables, so any column from these
    tables can be used for grouping. Not all columns are suitable - continuous
    variables like DIA should be used with caution or binned first.

    **NULL Value Handling:**
    Some grouping columns may contain NULL values (e.g., HT ~30% NULL for
    some species). NULL values are handled safely by Polars and will appear
    as a separate group in results if present.

    **Growing Stock Definition:**
    Growing stock trees (tree_type='gs') are defined as live trees of
    commercial species that meet minimum merchantability standards:
    - Must be live (STATUSCD = 1)
    - Must be growing stock (TREECLCD = 2)
    - Excludes rough and rotten culls
    - Typically ≥ 5.0" DBH for hardwoods and softwoods

    **Board Foot Conversion:**
    Sawlog volume (vol_type='sawlog') uses board foot measurements which
    apply only to sawtimber-sized trees:
    - Softwoods: ≥ 9.0" DBH
    - Hardwoods: ≥ 11.0" DBH

    Trees below these thresholds will have NULL or 0 board foot volume.

    Examples
    --------
    Basic net volume on forestland:

    >>> from pyfia import FIA, volume
    >>> with FIA("path/to/fia.duckdb") as db:
    ...     db.clip_by_state(37)  # North Carolina
    ...     results = volume(db, land_type="forest", vol_type="net")

    Volume by species on timberland:

    >>> results = volume(
    ...     db,
    ...     by_species=True,
    ...     land_type="timber",
    ...     tree_type="gs"  # Growing stock only
    ... )
    >>> # Find top species by volume
    >>> if not results.is_empty():
    ...     top_species = results.sort(by='VOLCFNET_ACRE', descending=True).head(5)

    Large tree volume by ownership:

    >>> results = volume(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     tree_domain="DIA >= 20.0",
    ...     variance=True
    ... )

    Sawlog volume by forest type:

    >>> results = volume(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     vol_type="sawlog",
    ...     tree_type="gs",
    ...     tree_domain="DIA >= 11.0"  # Hardwood sawtimber size
    ... )

    Volume by multiple grouping variables:

    >>> results = volume(
    ...     db,
    ...     grp_by=["STATECD", "OWNGRPCD", "STDSZCD"],
    ...     land_type="forest",
    ...     totals=True
    ... )

    Complex filtering with domain expressions:

    >>> # High-value timber on productive sites
    >>> results = volume(
    ...     db,
    ...     grp_by="SPCD",
    ...     land_type="timber",
    ...     tree_domain="DIA >= 16.0 AND TREECLCD == 2",
    ...     area_domain="SITECLCD <= 3 AND SLOPE < 35"
    ... )

    Dead tree volume assessment:

    >>> results = volume(
    ...     db,
    ...     tree_type="dead",
    ...     by_species=True,
    ...     tree_domain="DIA >= 10.0 AND DECAYCD IN (1, 2)"  # Sound dead trees
    ... )

    Warnings
    --------
    The current implementation uses a simplified variance calculation
    (10% CV as placeholder). Full stratified variance calculation following
    Bechtold & Patterson (2005) will be implemented in a future release.
    For applications requiring precise variance estimates, consider using
    the FIA EVALIDator tool or other specialized FIA analysis software.

    Raises
    ------
    ValueError
        If invalid parameter values are provided or if required tables
        (TREE, COND, PLOT) are not found in the database.
    KeyError
        If specified columns in grp_by don't exist in the joined tables.
    """
    # Ensure db is a FIA instance
    if isinstance(db, str):
        db = FIA(db)
        owns_db = True
    else:
        owns_db = False

    # Handle EVALID selection for volume estimation
    if db.evalid is None and most_recent:
        db.clip_most_recent(eval_type=eval_type or "VOL")
    elif db.evalid is None:
        # Auto-select most recent EXPVOL evaluation for volume
        import warnings
        warnings.warn(
            "No EVALID specified. Automatically selecting most recent EXPVOL evaluations. "
            "For explicit control, use db.clip_most_recent() or db.clip_by_evalid() before calling volume()."
        )
        db.clip_most_recent(eval_type="VOL")

        # If still no EVALID, warn about potential issues
        if db.evalid is None:
            warnings.warn(
                "WARNING: No EXPVOL evaluations found. Results may be incorrect due to "
                "inclusion of multiple overlapping evaluations. Consider using db.clip_by_evalid() "
                "to explicitly select appropriate EVALIDs."
            )

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

    try:
        # Create and run estimator
        estimator = VolumeEstimator(db, config)
        return estimator.estimate()
    finally:
        # Clean up if we created the db
        if owns_db and hasattr(db, 'close'):
            db.close()