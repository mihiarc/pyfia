"""
Biomass estimation for FIA data.

Simple implementation for calculating tree biomass and carbon
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
        """Aggregate biomass with two-stage aggregation for correct per-acre estimates.

        CRITICAL FIX: This method implements two-stage aggregation following FIA
        methodology. The previous single-stage approach caused ~20x underestimation
        by having each tree contribute its condition proportion to the denominator.

        Stage 1: Aggregate trees to plot-condition level
        Stage 2: Apply expansion factors and calculate ratio-of-means
        """
        # Validate required columns exist
        data_schema = data.collect_schema()
        required_cols = ["PLT_CN", "BIOMASS_ACRE", "CARBON_ACRE"]
        missing_cols = [col for col in required_cols if col not in data_schema.names()]
        if missing_cols:
            raise ValueError(f"Required columns missing from data: {missing_cols}")

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
            (pl.col("BIOMASS_ACRE") * pl.col("ADJ_FACTOR")).alias("BIOMASS_ADJ"),
            (pl.col("CARBON_ACRE") * pl.col("ADJ_FACTOR")).alias("CARBON_ADJ")
        ])

        # Setup grouping
        group_cols = self._setup_grouping()

        # Use shared two-stage aggregation method
        metric_mappings = {
            "BIOMASS_ADJ": "CONDITION_BIOMASS",
            "CARBON_ADJ": "CONDITION_CARBON"
        }

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=False
        )

        # Rename columns to match expected output
        results = results.rename({
            "BIOMASS_ACRE": "BIO_ACRE",
            "CARBON_ACRE": "CARB_ACRE",
            "BIOMASS_TOTAL": "BIO_TOTAL",
            "CARBON_TOTAL": "CARB_TOTAL"
        })

        # Handle totals based on config
        if not self.config.get("totals", True):
            # Remove total columns if not requested
            cols_to_drop = ["BIO_TOTAL", "CARB_TOTAL"]
            cols_to_drop = [col for col in cols_to_drop if col in results.columns]
            if cols_to_drop:
                results = results.drop(cols_to_drop)

        return results
    
    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for biomass estimates.

        WARNING: This is a simplified placeholder implementation that returns
        10% of the estimate as standard error. This does NOT follow proper
        FIA variance calculation methodology. See Warnings in biomass()
        docstring for details.

        TODO: Implement proper stratified variance calculation following
        Bechtold & Patterson (2005) methodology:
        1. Calculate variance by stratum
        2. Apply finite population correction
        3. Combine stratum variances properly
        4. Calculate standard errors from variance
        """
        # Placeholder calculation - 10% CV
        # This is NOT statistically valid - see docstring warnings
        results = results.with_columns([
            (pl.col("BIO_ACRE") * 0.1).alias("BIO_ACRE_SE"),
            (pl.col("CARB_ACRE") * 0.1).alias("CARB_ACRE_SE"),
            (pl.col("BIO_TOTAL") * 0.1).alias("BIO_TOTAL_SE"),
            (pl.col("CARB_TOTAL") * 0.1).alias("CARB_TOTAL_SE")
        ])

        return results
    
    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format biomass estimation output."""
        # Extract year from evaluation data
        # IMPORTANT: Use EVALID year (evaluation reference year) not INVYR
        # EVALID year represents the complete statistical estimate for that year
        # while INVYR varies by plot due to FIA's rotating panel design
        year = None

        # Primary source: EVALID encodes the evaluation reference year
        if hasattr(self.db, 'evalids') and self.db.evalids:
            # EVALIDs are 6-digit codes: SSYYTT where YY is the evaluation year
            # Example: 482300 = State 48, Year 2023 evaluation, Type 00
            # This evaluation might include plots measured 2019-2023, but
            # statistically represents forest conditions as of 2023
            try:
                evalid = self.db.evalids[0]  # Use first EVALID
                year_part = int(str(evalid)[2:4])  # Extract YY portion

                # Handle century correctly - FIA data starts in 1990s
                if year_part >= 90:  # Years 90-99 are 1990-1999
                    year = 1900 + year_part
                else:  # Years 00-89 are 2000-2089
                    year = 2000 + year_part

                # Validate year is reasonable (1990-2050)
                if year < 1990 or year > 2050:
                    year = None  # Fall back to other methods
            except (IndexError, ValueError, TypeError):
                pass

        # Fallback: If no EVALID, use most recent INVYR as approximation
        # Note: This is less accurate as plots have different INVYR values
        if year is None and "PLOT" in self.db.tables:
            try:
                plot_years = self.db.tables["PLOT"].select("INVYR").collect()
                if not plot_years.is_empty():
                    # Use max year as it best represents the evaluation period
                    year = int(plot_years["INVYR"].max())
            except Exception:
                pass

        # Default to current year minus 2 (typical FIA processing lag)
        if year is None:
            from datetime import datetime
            year = datetime.now().year - 2

        results = results.with_columns([
            pl.lit(year).alias("YEAR")
        ])

        # Standard column order
        col_order = [
            "YEAR", "BIO_ACRE", "BIO_TOTAL", "CARB_ACRE", "CARB_TOTAL",
            "BIO_ACRE_SE", "BIO_TOTAL_SE", "CARB_ACRE_SE", "CARB_TOTAL_SE",
            "N_PLOTS", "N_TREES"
        ]

        # Add any grouping columns at the beginning after YEAR
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
    Estimate tree biomass and carbon from FIA data.

    Calculates dry weight biomass (in tons) and carbon content using FIA's
    standard biomass equations and expansion factors. Implements two-stage
    aggregation following FIA methodology for statistically valid per-acre
    and total estimates.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        FIA tables used in the estimation (PLOT, COND, TREE). Common
        grouping columns include:

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
        - 'BALIVE': Basal area of live trees (from COND)

        For complete column descriptions, see USDA FIA Database User Guide.
    by_species : bool, default False
        If True, group results by species code (SPCD). This is a convenience
        parameter equivalent to adding 'SPCD' to grp_by.
    by_size_class : bool, default False
        If True, group results by diameter size classes. Size classes are
        defined as: 1.0-4.9", 5.0-9.9", 10.0-19.9", 20.0-29.9", 30.0+".
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land type to include in estimation:

        - 'forest': All forestland (land at least 10% stocked with forest
          trees of any size, or formerly having such tree cover)
        - 'timber': Productive timberland only (unreserved forestland capable
          of producing 20 cubic feet per acre per year)
        - 'all': All land conditions including non-forest
    tree_type : {'live', 'dead', 'gs', 'all'}, default 'live'
        Tree type to include:

        - 'live': All live trees (STATUSCD == 1)
        - 'dead': Standing dead trees (STATUSCD == 2)
        - 'gs': Growing stock trees (live, merchantable, STATUSCD == 1 with
          specific quality requirements)
        - 'all': All trees regardless of status
    component : str, default 'AG'
        Biomass component to estimate. Valid options include:

        - 'AG': Aboveground biomass (stem, bark, branches, foliage)
        - 'BG': Belowground biomass (coarse roots)
        - 'TOTAL': Total biomass (AG + BG)
        - 'BOLE': Main stem wood and bark
        - 'BRANCH': Live and dead branches
        - 'FOLIAGE': Leaves/needles
        - 'ROOTS': Coarse roots (same as BG)
        - 'STUMP': Stump biomass
        - 'SAPLING': Sapling biomass
        - 'TOP': Top and branches above merchantable height

        Note: Not all components may be available for all species or regions.
        Check TREE table for available DRYBIO_* columns.
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering. Applied to
        TREE table. Example: "DIA >= 10.0 AND SPCD == 131".
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
        Applied to COND table. Example: "OWNGRPCD == 40 AND FORTYPCD == 161".
    totals : bool, default True
        If True, include population-level total estimates in addition to
        per-acre values. Totals are expanded using FIA expansion factors.
    variance : bool, default False
        If True, calculate and include variance and standard error estimates.
        Note: Currently uses simplified variance calculation (10% of estimate).
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation for
        each state in the database before estimation.

    Returns
    -------
    pl.DataFrame
        Biomass and carbon estimates with the following columns:

        - **BIO_ACRE** : float
            Biomass per acre in tons (dry weight)
        - **BIO_TOTAL** : float (if totals=True)
            Total biomass in tons expanded to population level
        - **CARB_ACRE** : float
            Carbon per acre in tons (47% of biomass)
        - **CARB_TOTAL** : float (if totals=True)
            Total carbon in tons expanded to population level
        - **BIO_ACRE_SE** : float (if variance=True)
            Standard error of per-acre biomass estimate
        - **BIO_TOTAL_SE** : float (if variance=True and totals=True)
            Standard error of total biomass estimate
        - **CARB_ACRE_SE** : float (if variance=True)
            Standard error of per-acre carbon estimate
        - **CARB_TOTAL_SE** : float (if variance=True and totals=True)
            Standard error of total carbon estimate
        - **AREA_TOTAL** : float
            Total area (acres) represented by the estimation
        - **N_PLOTS** : int
            Number of FIA plots included in the estimation
        - **N_TREES** : int
            Number of individual tree records
        - **YEAR** : int
            Evaluation reference year (from EVALID). This represents the year
            of the complete statistical estimate, not individual plot measurement
            years (INVYR) which vary due to FIA's rotating panel design
        - **[grouping columns]** : various
            Any columns specified in grp_by or from by_species

    See Also
    --------
    volume : Estimate volume per acre (current inventory)
    tpa : Estimate trees per acre (current inventory)
    mortality : Estimate annual mortality using GRM tables
    growth : Estimate annual growth using GRM tables
    area : Estimate forestland area
    pyfia.constants.TreeStatus : Tree status code definitions
    pyfia.constants.OwnershipGroup : Ownership group code definitions
    pyfia.constants.ForestType : Forest type code definitions
    pyfia.utils.reference_tables : Functions for adding species/forest type names

    Notes
    -----
    Biomass is calculated using FIA's standard dry weight equations stored
    in the DRYBIO_* columns of the TREE table. These values are in pounds
    and are converted to tons by dividing by 2000.

    Carbon content is estimated as 47% of dry biomass following IPCC
    guidelines and FIA standard practice. This percentage may vary slightly
    by species and component but 47% is the standard factor.

    **Evaluation Year vs. Inventory Year**: The YEAR in output represents
    the evaluation reference year from EVALID, not individual plot inventory
    years (INVYR). Due to FIA's rotating panel design, plots within an
    evaluation are measured across multiple years (typically 5-7 year cycle),
    but the evaluation statistically represents forest conditions as of the
    reference year. For example, EVALID 482300 represents Texas forest
    conditions as of 2023, even though it includes plots measured 2019-2023.

    The function implements two-stage aggregation following FIA methodology:

    1. **Stage 1**: Aggregate trees to plot-condition level to ensure each
       condition's area proportion is counted exactly once.
    2. **Stage 2**: Apply expansion factors and calculate ratio-of-means
       for per-acre estimates and population totals.

    This approach prevents the ~20x underestimation that would occur with
    single-stage aggregation where each tree contributes its condition
    proportion to the denominator.

    Required FIA tables and columns:

    - TREE: CN, PLT_CN, CONDID, STATUSCD, SPCD, DIA, TPA_UNADJ, DRYBIO_*
    - COND: PLT_CN, CONDID, COND_STATUS_CD, CONDPROP_UNADJ, OWNGRPCD, etc.
    - PLOT: CN, STATECD, INVYR, MACRO_BREAKPOINT_DIA
    - POP_PLOT_STRATUM_ASSGN: PLT_CN, STRATUM_CN
    - POP_STRATUM: CN, EXPNS, ADJ_FACTOR_*

    Valid grouping columns depend on which tables are included in the
    estimation query. For a complete list of available columns and their
    meanings, refer to:

    - USDA FIA Database User Guide, Version 9.1
    - pyFIA documentation: https://mihiarc.github.io/pyfia/
    - FIA DataMart: https://apps.fs.usda.gov/fia/datamart/

    Biomass components availability varies by FIA region and evaluation type.
    Check your database for available DRYBIO_* columns using:

    >>> import duckdb
    >>> conn = duckdb.connect("your_database.duckdb")
    >>> columns = conn.execute("PRAGMA table_info(TREE)").fetchall()
    >>> biomass_cols = [c[1] for c in columns if 'DRYBIO' in c[1]]

    Warnings
    --------
    The current implementation uses a simplified variance calculation that
    returns 10% of the estimate as standard error. This is a placeholder
    and does not follow the full stratified variance calculation described
    in Bechtold & Patterson (2005). For applications requiring precise
    variance estimates, consider using the FIA EVALIDator tool or other
    specialized FIA analysis software.

    Some biomass components may not be available for all species or in all
    FIA regions. If a requested component is not available, the function
    will raise an error. Always verify component availability in your
    specific database.

    Raises
    ------
    ValueError
        If the specified biomass component column does not exist in the
        TREE table, or if grp_by contains invalid column names.
    KeyError
        If specified columns in grp_by don't exist in the joined tables.
    RuntimeError
        If no data matches the specified filters and domains.

    Examples
    --------
    Basic aboveground biomass on forestland:

    >>> results = biomass(db, component="AG", land_type="forest")
    >>> if not results.is_empty():
    ...     print(f"Aboveground biomass: {results['BIO_ACRE'][0]:.1f} tons/acre")
    ...     print(f"Carbon storage: {results['CARB_ACRE'][0]:.1f} tons/acre")
    ... else:
    ...     print("No biomass data available")

    Total biomass (above + below ground) by species:

    >>> results = biomass(db, by_species=True, component="TOTAL")
    >>> # Sort by biomass to find dominant species
    >>> if not results.is_empty():
    ...     top_species = results.sort(by='BIO_ACRE', descending=True).head(5)
    ...     print("Top 5 species by biomass per acre:")
    ...     for row in top_species.iter_rows(named=True):
    ...         print(f"  SPCD {row['SPCD']}: {row['BIO_ACRE']:.1f} tons/acre")

    Biomass by ownership on timberland:

    >>> results = biomass(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     land_type="timber",
    ...     component="AG",
    ...     tree_type="gs",
    ...     variance=True
    ... )
    >>> # Display with standard errors
    >>> for row in results.iter_rows(named=True):
    ...     ownership = {10: "National Forest", 20: "Other Federal",
    ...                  30: "State/Local", 40: "Private"}
    ...     name = ownership.get(row['OWNGRPCD'], f"Code {row['OWNGRPCD']}")
    ...     print(f"{name}: {row['BIO_ACRE']:.1f} ± {row['BIO_ACRE_SE']:.1f} tons/acre")

    Large tree biomass by forest type:

    >>> results = biomass(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     tree_domain="DIA >= 20.0",
    ...     component="AG",
    ...     totals=True
    ... )
    >>> # Show both per-acre and total biomass
    >>> for row in results.iter_rows(named=True):
    ...     print(f"Forest Type {row['FORTYPCD']}:")
    ...     print(f"  Per acre: {row['BIO_ACRE']:.1f} tons")
    ...     print(f"  Total: {row['BIO_TOTAL']/1e6:.2f} million tons")

    Carbon storage by multiple grouping variables:

    >>> results = biomass(
    ...     db,
    ...     grp_by=["STATECD", "OWNGRPCD"],
    ...     component="TOTAL",
    ...     most_recent=True
    ... )
    >>> # Calculate total carbon by state
    >>> state_carbon = results.group_by("STATECD").agg([
    ...     pl.col("CARB_TOTAL").sum()
    ... ])

    Standing dead tree biomass:

    >>> results = biomass(
    ...     db,
    ...     tree_type="dead",
    ...     component="AG",
    ...     by_size_class=True
    ... )
    >>> print("Dead tree biomass by size class:")
    >>> for row in results.iter_rows(named=True):
    ...     print(f"  {row['SIZE_CLASS']}: {row['BIO_ACRE']:.1f} tons/acre")
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