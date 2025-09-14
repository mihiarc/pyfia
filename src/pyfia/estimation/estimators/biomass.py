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

        # ========================================================================
        # CRITICAL FIX: Two-stage aggregation following FIA methodology
        # ========================================================================

        # STAGE 1: Aggregate trees to plot-condition level
        # This ensures each condition's area proportion is counted exactly once
        condition_group_cols = ["PLT_CN", "CONDID", "STRATUM_CN", "EXPNS", "CONDPROP_UNADJ"]
        if group_cols:
            # Add user-specified grouping columns if they exist at condition level
            for col in group_cols:
                if col in data_with_strat.collect_schema().names() and col not in condition_group_cols:
                    condition_group_cols.append(col)

        # Aggregate biomass and carbon at condition level
        condition_agg = data_with_strat.group_by(condition_group_cols).agg([
            # Sum biomass and carbon within each condition
            pl.col("BIOMASS_ADJ").sum().alias("CONDITION_BIOMASS"),
            pl.col("CARBON_ADJ").sum().alias("CONDITION_CARBON"),
            # Count trees per condition for diagnostics
            pl.len().alias("TREES_PER_CONDITION")
        ])

        # STAGE 2: Apply expansion factors and calculate population estimates
        if group_cols:
            # Group by user-specified columns for final aggregation
            final_group_cols = [col for col in group_cols if col in condition_agg.collect_schema().names()]
            if final_group_cols:
                results = condition_agg.group_by(final_group_cols).agg([
                    # Numerator: Sum of expanded condition biomass/carbon
                    (pl.col("CONDITION_BIOMASS") * pl.col("EXPNS")).sum().alias("BIOMASS_NUM"),
                    (pl.col("CONDITION_CARBON") * pl.col("EXPNS")).sum().alias("CARBON_NUM"),
                    # Denominator: Sum of expanded condition areas
                    (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
                    # Totals (for totals=True)
                    (pl.col("CONDITION_BIOMASS") * pl.col("EXPNS")).sum().alias("BIOMASS_TOTAL"),
                    (pl.col("CONDITION_CARBON") * pl.col("EXPNS")).sum().alias("CARBON_TOTAL"),
                    # Diagnostic counts
                    pl.n_unique("PLT_CN").alias("N_PLOTS"),
                    pl.col("TREES_PER_CONDITION").sum().alias("N_TREES"),
                    pl.len().alias("N_CONDITIONS")
                ])
            else:
                # No valid grouping columns at condition level
                results = condition_agg.select([
                    (pl.col("CONDITION_BIOMASS") * pl.col("EXPNS")).sum().alias("BIOMASS_NUM"),
                    (pl.col("CONDITION_CARBON") * pl.col("EXPNS")).sum().alias("CARBON_NUM"),
                    (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
                    (pl.col("CONDITION_BIOMASS") * pl.col("EXPNS")).sum().alias("BIOMASS_TOTAL"),
                    (pl.col("CONDITION_CARBON") * pl.col("EXPNS")).sum().alias("CARBON_TOTAL"),
                    pl.n_unique("PLT_CN").alias("N_PLOTS"),
                    pl.col("TREES_PER_CONDITION").sum().alias("N_TREES"),
                    pl.len().alias("N_CONDITIONS")
                ])
        else:
            # No grouping - aggregate all conditions
            results = condition_agg.select([
                (pl.col("CONDITION_BIOMASS") * pl.col("EXPNS")).sum().alias("BIOMASS_NUM"),
                (pl.col("CONDITION_CARBON") * pl.col("EXPNS")).sum().alias("CARBON_NUM"),
                (pl.col("CONDPROP_UNADJ") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
                (pl.col("CONDITION_BIOMASS") * pl.col("EXPNS")).sum().alias("BIOMASS_TOTAL"),
                (pl.col("CONDITION_CARBON") * pl.col("EXPNS")).sum().alias("CARBON_TOTAL"),
                pl.n_unique("PLT_CN").alias("N_PLOTS"),
                pl.col("TREES_PER_CONDITION").sum().alias("N_TREES"),
                pl.len().alias("N_CONDITIONS")
            ])

        results = results.collect()

        # Calculate per-acre values using ratio-of-means
        # This is now correct because each condition contributes exactly once to denominator
        # Add protection against division by zero
        results = results.with_columns([
            pl.when(pl.col("AREA_TOTAL") > 0)
            .then(pl.col("BIOMASS_NUM") / pl.col("AREA_TOTAL"))
            .otherwise(0.0)
            .alias("BIO_ACRE"),

            pl.when(pl.col("AREA_TOTAL") > 0)
            .then(pl.col("CARBON_NUM") / pl.col("AREA_TOTAL"))
            .otherwise(0.0)
            .alias("CARB_ACRE")
        ])

        # Rename totals
        results = results.rename({
            "BIOMASS_TOTAL": "BIO_TOTAL",
            "CARBON_TOTAL": "CARB_TOTAL"
        })

        # Clean up intermediate columns
        results = results.drop(["BIOMASS_NUM", "CARBON_NUM", "N_CONDITIONS"])

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