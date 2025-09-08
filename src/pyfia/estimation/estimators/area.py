"""
Area estimation for FIA data.

Simple, straightforward implementation without unnecessary abstractions.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..config import EstimatorConfig
from ..aggregation import aggregate_to_population, merge_stratification
from ..statistics import VarianceCalculator
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
        """Required condition columns."""
        return [
            "PLT_CN", "CONDID", "COND_STATUS_CD", 
            "CONDPROP_UNADJ", "OWNGRPCD", "FORTYPCD",
            "SITECLCD", "RESERVCD", "STDAGE"
        ]
    
    def load_data(self) -> pl.LazyFrame:
        """Load condition and plot data."""
        # Load COND table
        if "COND" not in self.db.tables:
            self.db.load_table("COND")
        cond_df = self.db.tables["COND"]
        
        # Load PLOT table  
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")
        plot_df = self.db.tables["PLOT"]
        
        # Ensure LazyFrames
        if not isinstance(cond_df, pl.LazyFrame):
            cond_df = cond_df.lazy()
        if not isinstance(plot_df, pl.LazyFrame):
            plot_df = plot_df.lazy()
        
        # Apply EVALID filtering
        if self.db.evalid:
            cond_df = cond_df.filter(pl.col("EVALID").is_in(self.db.evalid))
            plot_df = plot_df.filter(pl.col("EVALID").is_in(self.db.evalid))
        
        # Select needed columns
        cond_cols = self.get_cond_columns()
        cond_df = cond_df.select([col for col in cond_cols if col in cond_df.columns])
        
        plot_cols = ["CN", "STATECD", "COUNTYCD", "PLOT", "LAT", "LON", "ELEV"]
        plot_df = plot_df.select([col for col in plot_cols if col in plot_df.columns])
        
        # Join condition and plot
        data = cond_df.join(
            plot_df,
            left_on="PLT_CN",
            right_on="CN",
            how="inner"
        )
        
        return data
    
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
        
        # Setup grouping
        group_cols = []
        if self.config.get("grp_by"):
            grp_by = self.config["grp_by"]
            if isinstance(grp_by, str):
                group_cols = [grp_by]
            else:
                group_cols = list(grp_by)
        
        # Calculate area totals
        agg_exprs = [
            (pl.col("AREA_VALUE") * pl.col("EXPNS")).sum().alias("AREA_TOTAL"),
            pl.col("EXPNS").sum().alias("TOTAL_EXPNS"),
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
    variance: bool = False
) -> pl.DataFrame:
    """
    Estimate forest area from FIA data.
    
    Simple function interface that creates an estimator and runs it
    without complex parameter validation or transformation.
    
    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path
    grp_by : Optional[Union[str, List[str]]]
        Columns to group by
    land_type : str
        Land type: "forest", "timber", or "all"
    area_domain : Optional[str]
        SQL-like filter condition
    most_recent : bool
        Use most recent evaluation
    eval_type : Optional[str]
        Evaluation type (EXPALL, etc.)
    variance : bool
        Return variance instead of SE
        
    Returns
    -------
    pl.DataFrame
        Area estimates
        
    Examples
    --------
    >>> # Basic forest area
    >>> results = area(db, land_type="forest")
    
    >>> # Area by ownership
    >>> results = area(db, grp_by="OWNGRPCD")
    
    >>> # Timber area by forest type
    >>> results = area(db, grp_by="FORTYPCD", land_type="timber")
    """
    # Create simple config dict
    config = {
        "grp_by": grp_by,
        "land_type": land_type,
        "area_domain": area_domain,
        "most_recent": most_recent,
        "eval_type": eval_type,
        "variance": variance
    }
    
    # Create estimator and run
    estimator = AreaEstimator(db, config)
    return estimator.estimate()