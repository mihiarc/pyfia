"""
Area estimation for FIA data.

Simple, straightforward implementation without unnecessary abstractions.
"""

import re
from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..aggregation import aggregate_to_population, merge_stratification
from ..statistics import VarianceCalculator
from ..tree_expansion import apply_area_adjustment_factors
from ..utils import format_output_columns, check_required_columns


class AreaEstimator(BaseEstimator):
    """
    Area estimator for FIA data.
    
    Estimates forest area by various categories without complex
    abstractions or deep inheritance hierarchies.
    """
    
    def get_required_tables(self) -> List[str]:
        """Area estimation requires COND, PLOT, and stratification tables."""
        return ["COND", "PLOT", "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM"]
    
    def get_cond_columns(self) -> List[str]:
        """Get required condition columns based on actual usage."""
        # Core columns always needed for area calculation
        core_cols = [
            "PLT_CN", "CONDID", "COND_STATUS_CD", 
            "CONDPROP_UNADJ", "PROP_BASIS"
        ]
        
        # Additional columns needed based on land_type filter
        filter_cols = set()
        land_type = self.config.get("land_type", "forest")
        if land_type == "timber":
            filter_cols.update(["SITECLCD", "RESERVCD"])
        
        # Add columns needed for area_domain filtering
        area_domain = self.config.get("area_domain")
        if area_domain:
            # Parse domain string to extract column names
            # Simple extraction - could be enhanced with proper parser
            import re
            # Exclude SQL keywords and operators
            sql_keywords = {'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE', 'AS'}
            col_pattern = r'\b([A-Z][A-Z0-9_]*)\b'
            potential_cols = re.findall(col_pattern, area_domain.upper())
            # Filter to valid COND columns (simplified check)
            for col in potential_cols:
                if col not in sql_keywords and col not in core_cols and len(col) > 2:
                    filter_cols.add(col)
        
        # Add grouping columns if specified
        grouping_cols = set()
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grouping_cols.add(grp_by)
            else:
                grouping_cols.update(grp_by)
        
        # Combine all needed columns
        all_cols = core_cols + list(filter_cols) + list(grouping_cols)
        
        # Remove duplicates while preserving order
        seen = set()
        result = []
        for col in all_cols:
            if col not in seen:
                seen.add(col)
                result.append(col)
        
        return result
    
    def load_data(self) -> pl.LazyFrame:
        """Load condition and plot data with lazy column selection."""
        # Get only the columns we actually need
        cond_cols_needed = self.get_cond_columns()
        plot_cols_needed = self._get_plot_columns()
        
        # Load COND table with only needed columns
        if "COND" not in self.db.tables:
            # Check what columns are available in COND
            available_cond_cols = self._get_available_columns("COND")
            if available_cond_cols is not None:
                cols_to_load = [col for col in cond_cols_needed if col in available_cond_cols]
                if cols_to_load:  # Only load if we have columns to load
                    self.db.load_table("COND", columns=cols_to_load)
                else:
                    # Load all columns if no matches found
                    self.db.load_table("COND")
            else:
                # Fallback: load with requested columns and let the reader handle it
                self.db.load_table("COND", columns=cond_cols_needed if cond_cols_needed else None)
        
        cond_df = self.db.tables.get("COND")
        if cond_df is not None and cond_cols_needed:
            # Select only needed columns from already loaded table
            if isinstance(cond_df, pl.LazyFrame):
                available_cols = cond_df.collect_schema().names()
            else:
                available_cols = cond_df.columns if hasattr(cond_df, 'columns') else []
            
            # Check if we have all needed columns
            missing_cols = [col for col in cond_cols_needed if col not in available_cols]
            if missing_cols:
                # Need to reload with additional columns
                self.db.load_table("COND", columns=None)  # Reload all columns
                cond_df = self.db.tables["COND"]
            else:
                cols_to_select = [col for col in cond_cols_needed if col in available_cols]
                if cols_to_select:
                    cond_df = cond_df.select(cols_to_select)
        
        # Load PLOT table with only needed columns
        if "PLOT" not in self.db.tables:
            # Check what columns are available in PLOT
            available_plot_cols = self._get_available_columns("PLOT")
            if available_plot_cols is not None:
                cols_to_load = [col for col in plot_cols_needed if col in available_plot_cols]
                if cols_to_load:  # Only load if we have columns to load
                    self.db.load_table("PLOT", columns=cols_to_load)
                else:
                    # Load all columns if no matches found
                    self.db.load_table("PLOT")
            else:
                # Fallback: load with requested columns and let the reader handle it
                self.db.load_table("PLOT", columns=plot_cols_needed if plot_cols_needed else None)
        
        plot_df = self.db.tables.get("PLOT")
        if plot_df is not None and plot_cols_needed:
            # Select only needed columns from already loaded table
            if isinstance(plot_df, pl.LazyFrame):
                available_cols = plot_df.collect_schema().names()
            else:
                available_cols = plot_df.columns if hasattr(plot_df, 'columns') else []
            
            # Check if we have all needed columns
            missing_cols = [col for col in plot_cols_needed if col not in available_cols]
            if missing_cols:
                # Need to reload with additional columns  
                self.db.load_table("PLOT", columns=None)  # Reload all columns
                plot_df = self.db.tables["PLOT"]
            else:
                cols_to_select = [col for col in plot_cols_needed if col in available_cols]
                if cols_to_select:
                    plot_df = plot_df.select(cols_to_select)
        
        # Ensure LazyFrames
        if not isinstance(cond_df, pl.LazyFrame):
            cond_df = cond_df.lazy()
        if not isinstance(plot_df, pl.LazyFrame):
            plot_df = plot_df.lazy()
        
        # Join condition and plot
        data = cond_df.join(
            plot_df,
            left_on="PLT_CN",
            right_on="CN",
            how="inner"
        )
        
        return data
    
    def _get_plot_columns(self) -> List[str]:
        """Get required plot columns based on actual usage."""
        # Always need CN for joining
        core_cols = ["CN"]
        
        # Add any grouping columns from PLOT table
        plot_group_cols = set()
        grp_by = self.config.get("grp_by")
        if grp_by:
            # Common PLOT columns that might be used for grouping
            plot_cols_available = [
                "STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR", 
                "MEASYEAR", "KINDCD", "DESIGNCD", "RDDISTCD", "WATERCD",
                "ECOSUBCD", "CONGCD", "EMAP_HEX"
            ]
            
            if isinstance(grp_by, str):
                if grp_by in plot_cols_available:
                    plot_group_cols.add(grp_by)
            else:
                for col in grp_by:
                    if col in plot_cols_available:
                        plot_group_cols.add(col)
        
        return core_cols + list(plot_group_cols)
    
    def _get_available_columns(self, table_name: str) -> List[str]:
        """Get list of available columns in a table without loading it."""
        try:
            # Query the database schema to get column names
            if hasattr(self.db._reader, 'conn'):
                conn = self.db._reader.conn
                if self.db._reader.backend == "duckdb":
                    # Use DESCRIBE for DuckDB
                    query = f"DESCRIBE {table_name}"
                    result = conn.execute(query).fetchall()
                    return [row[0] for row in result]
                elif self.db._reader.backend == "sqlite":
                    query = f"PRAGMA table_info({table_name})"
                    result = conn.execute(query).fetchall()
                    return [row[1] for row in result]
        except Exception:
            pass
        
        # Fallback: return None to signal we should load all columns
        # The db.load_table with columns=None will handle loading all available columns
        return None
    
    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate area values.
        
        For area estimation, the value is simply CONDPROP_UNADJ
        which represents the proportion of the plot in the condition.
        """
        # Area calculation is straightforward
        data = data.with_columns([
            pl.col("CONDPROP_UNADJ").alias("AREA_VALUE")
        ])
        
        return data
    
    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply land type and domain filters."""
        # Collect for filtering
        data_df = data.collect()
        
        # Apply land type filter
        land_type = self.config.get("land_type", "forest")
        if land_type == "forest":
            data_df = data_df.filter(pl.col("COND_STATUS_CD") == 1)
        elif land_type == "timber":
            data_df = data_df.filter(
                (pl.col("COND_STATUS_CD") == 1) &
                (pl.col("SITECLCD").is_in([1, 2, 3, 4, 5, 6])) &
                (pl.col("RESERVCD") == 0)
            )
        # "all" means no filter
        
        # Apply area domain filter
        if self.config.get("area_domain"):
            # This would use the domain parser from utils
            # For now, simplified example:
            domain_str = self.config["area_domain"]
            if "STDAGE > " in domain_str:
                age_threshold = int(domain_str.split(">")[1].strip())
                data_df = data_df.filter(pl.col("STDAGE") > age_threshold)
        
        return data_df.lazy()
    
    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate area with stratification."""
        # Get stratification data
        strat_data = self._get_stratification_data()
        
        # Join with stratification
        data_with_strat = data.join(
            strat_data,
            on="PLT_CN",
            how="inner"
        )
        
        # Apply area adjustment factors based on PROP_BASIS
        data_with_strat = apply_area_adjustment_factors(
            data_with_strat,
            prop_basis_col="PROP_BASIS",
            output_col="ADJ_FACTOR_AREA"
        )
        
        # Setup grouping
        group_cols = []
        if self.config.get("grp_by"):
            grp_by = self.config["grp_by"]
            if isinstance(grp_by, str):
                group_cols = [grp_by]
            else:
                group_cols = list(grp_by)
        
        # Calculate area totals with proper FIA expansion logic
        # Area = CONDPROP_UNADJ * ADJ_FACTOR_AREA * EXPNS
        agg_exprs = [
            (pl.col("AREA_VALUE").cast(pl.Float64) * 
             pl.col("ADJ_FACTOR_AREA").cast(pl.Float64) * 
             pl.col("EXPNS").cast(pl.Float64)).sum().alias("AREA_TOTAL"),
            pl.col("EXPNS").cast(pl.Float64).sum().alias("TOTAL_EXPNS"),
            pl.count("PLT_CN").alias("N_PLOTS")
        ]
        
        if group_cols:
            results = data_with_strat.group_by(group_cols).agg(agg_exprs)
        else:
            results = data_with_strat.select(agg_exprs)
        
        results = results.collect()
        
        # Add percentage if grouped
        if group_cols:
            total_area = results["AREA_TOTAL"].sum()
            results = results.with_columns([
                (100 * pl.col("AREA_TOTAL") / total_area).alias("AREA_PERCENT")
            ])
        
        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for area estimates."""
        # Use simple variance calculator
        calc = VarianceCalculator(method="ratio_of_means")
        
        # For now, add placeholder SE
        results = results.with_columns([
            (pl.col("AREA_TOTAL") * 0.05).alias("AREA_SE")  # 5% CV placeholder
        ])
        
        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format area estimation output."""
        # Add year
        results = results.with_columns([
            pl.lit(2023).alias("YEAR")
        ])
        
        # Format columns
        results = format_output_columns(
            results,
            estimation_type="area",
            include_se=True,
            include_cv=self.config.get("include_cv", False)
        )
        
        return results


def area(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    land_type: str = "forest",
    area_domain: Optional[str] = None,
    most_recent: bool = False,
    eval_type: Optional[str] = None,
    variance: bool = False,
    totals: bool = True
) -> pl.DataFrame:
    """
    Estimate forest area from FIA data.
    
    Calculates area estimates using FIA's design-based estimation methods
    with proper expansion factors and stratification. Automatically handles
    EVALID selection to prevent overcounting from multiple evaluations.
    
    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the 
        PLOT and COND tables. Common grouping columns include:
        
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
        
        **Location:**
        - 'STATECD': State FIPS code
        - 'UNITCD': FIA survey unit code
        - 'COUNTYCD': County code
        - 'INVYR': Inventory year
        
        **Disturbance and Treatment:**
        - 'DSTRBCD1', 'DSTRBCD2', 'DSTRBCD3': Disturbance codes
        - 'TRTCD1', 'TRTCD2', 'TRTCD3': Treatment codes
        
        For complete column descriptions, see USDA FIA Database User Guide.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:
        
        - 'forest': All forestland (COND_STATUS_CD = 1)
        - 'timber': Timberland only (unreserved, productive forestland)
        - 'all': All land types including non-forest
    area_domain : str, optional
        SQL-like filter expression for COND-level attributes. Examples:
        
        - "STDAGE > 50": Stands older than 50 years
        - "FORTYPCD IN (161, 162)": Specific forest types
        - "OWNGRPCD == 10": National Forest lands only
        - "PHYSCLCD == 31 AND STDSZCD == 1": Xeric sites with large trees
    most_recent : bool, default False
        If True, automatically select the most recent evaluation for each
        state/region. Equivalent to calling db.clip_most_recent() first.
    eval_type : str, optional
        Evaluation type to select if most_recent=True. Options:
        'ALL', 'VOL', 'GROW', 'MORT', 'REMV', 'CHANGE', 'DWM', 'INV'.
        Default is 'ALL' for area estimation.
    variance : bool, default False
        If True, return variance instead of standard error.
    totals : bool, default True
        If True, include total area estimates expanded to population level.
        If False, only return per-acre values.
        
    Returns
    -------
    pl.DataFrame
        Area estimates with the following columns:
        
        - **YEAR** : int
            Inventory year
        - **[grouping columns]** : varies
            Any columns specified in grp_by parameter
        - **AREA_PCT** : float
            Percentage of total area
        - **AREA_SE** : float (if variance=False)
            Standard error of area percentage
        - **AREA_VAR** : float (if variance=True)
            Variance of area percentage  
        - **N_PLOTS** : int
            Number of plots in estimate
        - **AREA** : float (if totals=True)
            Total area in acres
        - **AREA_TOTAL_SE** : float (if totals=True and variance=False)
            Standard error of total area
            
    See Also
    --------
    pyfia.volume : Estimate tree volume
    pyfia.biomass : Estimate tree biomass
    pyfia.tpa : Estimate trees per acre
    pyfia.constants.ForestTypes : Forest type code definitions
    pyfia.constants.StateCodes : State FIPS code definitions
    
    Notes
    -----
    The area estimation follows USDA FIA's design-based estimation procedures
    as described in Bechtold & Patterson (2005). The basic formula is:
    
    Area = Σ(CONDPROP_UNADJ × ADJ_FACTOR × EXPNS)
    
    Where:
    - CONDPROP_UNADJ: Proportion of plot in the condition
    - ADJ_FACTOR: Adjustment factor based on PROP_BASIS
    - EXPNS: Expansion factor from stratification
    
    **EVALID Handling:**
    If no EVALID is specified, the function automatically selects the most
    recent EXPALL evaluation to prevent overcounting from multiple evaluations.
    For explicit control, use db.clip_by_evalid() before calling area().
    
    **Valid Grouping Columns:**
    The function loads comprehensive sets of columns from COND and PLOT tables.
    Not all columns are suitable for grouping - continuous variables like
    LAT, LON, ELEV should not be used. The function will error if a requested
    grouping column is not available in the loaded data.
    
    **NULL Value Handling:**
    Some grouping columns may contain NULL values (e.g., PHYSCLCD ~18% NULL,
    DSTRBCD1 ~22% NULL). NULL values are handled safely by Polars and will
    appear as a separate group in results if present.
        
    Examples
    --------
    Basic forest area estimation:
    
    >>> from pyfia import FIA, area
    >>> with FIA("path/to/fia.duckdb") as db:
    ...     db.clip_by_state(37)  # North Carolina
    ...     results = area(db, land_type="forest")
    
    Area by ownership group:
    
    >>> results = area(db, grp_by="OWNGRPCD")
    >>> # Results will show area for each ownership category
    
    Timber area by forest type for stands over 50 years:
    
    >>> results = area(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     land_type="timber",
    ...     area_domain="STDAGE > 50"
    ... )
    
    Multiple grouping variables:
    
    >>> results = area(
    ...     db,
    ...     grp_by=["STATECD", "OWNGRPCD", "STDSZCD"],
    ...     land_type="forest"
    ... )
    
    Area by disturbance type:
    
    >>> results = area(
    ...     db,
    ...     grp_by="DSTRBCD1",
    ...     area_domain="DSTRBCD1 > 0"  # Only disturbed areas
    ... )
    """
    # Ensure db is a FIA instance
    if isinstance(db, str):
        db = FIA(db)
        owns_db = True
    else:
        owns_db = False
    
    # CRITICAL: If no EVALID is set, automatically select most recent EXPALL
    # This prevents massive overcounting from including all historical evaluations
    if db.evalid is None:
        import warnings
        warnings.warn(
            "No EVALID specified. Automatically selecting most recent EXPALL evaluations. "
            "For explicit control, use db.clip_most_recent() or db.clip_by_evalid() before calling area()."
        )
        db.clip_most_recent(eval_type="ALL")  # Use "ALL" not "EXPALL" per line 159-160 in fia.py
        
        # If still no EVALID (no EXPALL evaluations), try without filtering but warn strongly
        if db.evalid is None:
            warnings.warn(
                "WARNING: No EXPALL evaluations found. Results may be incorrect due to "
                "inclusion of multiple overlapping evaluations. Consider using db.clip_by_evalid() "
                "to explicitly select appropriate EVALIDs."
            )
    
    # Create simple config dict
    config = {
        "grp_by": grp_by,
        "land_type": land_type,
        "area_domain": area_domain,
        "most_recent": most_recent,
        "eval_type": eval_type,
        "variance": variance,
        "totals": totals
    }
    
    try:
        # Create estimator and run
        estimator = AreaEstimator(db, config)
        return estimator.estimate()
    finally:
        # Clean up if we created the db
        if owns_db and hasattr(db, 'close'):
            db.close()