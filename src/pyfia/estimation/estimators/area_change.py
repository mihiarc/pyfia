"""
Area change estimation for FIA data.

Estimates net change in forest land area using remeasured plots from the
SUBP_COND_CHNG_MTRX table. Follows FIA methodology for tracking land use
transitions between measurement periods.

References
----------
Bechtold & Patterson (2005), Chapter 4: Area Change Estimation
FIA Database User Guide, SUBP_COND_CHNG_MTRX table documentation
"""

from typing import List, Literal, Optional, Union

import duckdb
import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..utils import format_output_columns


class AreaChangeEstimator(BaseEstimator):
    """
    Area change estimator for FIA data.

    Estimates annual net change in forest or timberland area using remeasured
    plots. The SUBP_COND_CHNG_MTRX table tracks condition transitions at the
    subplot level between measurement periods.

    Area change is calculated as:
        Net change = (Gains from non-forest) - (Losses to non-forest)

    Results are annualized by dividing by the remeasurement period (REMPER).

    Parameters (via config)
    -----------------------
    land_type : {'forest', 'timber'}, default 'forest'
        Land classification to track changes for
    change_type : {'net', 'gross_gain', 'gross_loss'}, default 'net'
        Type of change to calculate:
        - 'net': Net change (gains - losses)
        - 'gross_gain': Only gains (non-forest to forest)
        - 'gross_loss': Only losses (forest to non-forest)
    annual : bool, default True
        If True, return annualized rate (acres/year)
        If False, return total change over remeasurement period
    grp_by : str or list of str, optional
        Column(s) to group results by
    variance : bool, default False
        Whether to calculate sampling variance
    totals : bool, default False
        Whether to include state totals

    Notes
    -----
    COND_STATUS_CD values:
        1 = Forest land
        2 = Non-forest land
        3 = Non-census water
        4 = Census water
        5 = Denied access

    Forest transitions:
        - Gain: Previous COND_STATUS_CD != 1, Current COND_STATUS_CD == 1
        - Loss: Previous COND_STATUS_CD == 1, Current COND_STATUS_CD != 1
    """

    def __init__(self, db: FIA, config: dict):
        """Initialize with storage for variance calculation."""
        super().__init__(db, config)
        self.plot_change_data: Optional[pl.DataFrame] = None

    def get_required_tables(self) -> List[str]:
        """Area change requires SUBP_COND_CHNG_MTRX, COND, PLOT, and stratification."""
        return [
            "SUBP_COND_CHNG_MTRX",
            "COND",
            "PLOT",
            "POP_PLOT_STRATUM_ASSGN",
            "POP_STRATUM",
        ]

    def get_cond_columns(self) -> List[str]:
        """Get required condition columns."""
        core_cols = [
            "CN",
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
        ]

        # Add timberland columns if needed
        land_type = self.config.get("land_type", "forest")
        if land_type == "timber":
            core_cols.extend(["SITECLCD", "RESERVCD"])

        # Add grouping columns
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            for col in grp_by:
                if col not in core_cols:
                    core_cols.append(col)

        return core_cols

    def load_data(self) -> Optional[pl.LazyFrame]:
        """
        Load and join tables for area change estimation.

        Join sequence:
        1. SUBP_COND_CHNG_MTRX (base - one row per subplot-condition change)
        2. COND (current) - get current condition status
        3. COND (previous) - get previous condition status via PREV_PLT_CN/PREVCOND
        4. PLOT - get REMPER and other plot attributes
        5. Stratification data - for expansion factors
        """
        # Load SUBP_COND_CHNG_MTRX
        if "SUBP_COND_CHNG_MTRX" not in self.db.tables:
            self.db.load_table("SUBP_COND_CHNG_MTRX")

        chng = self.db.tables["SUBP_COND_CHNG_MTRX"]
        if not isinstance(chng, pl.LazyFrame):
            chng = chng.lazy()

        # Select needed columns from change matrix
        data = chng.select([
            "PLT_CN",
            "CONDID",
            "PREV_PLT_CN",
            "PREVCOND",
            "SUBP",
            "SUBPTYP",
            "SUBPTYP_PROP_CHNG",
        ])

        # Load COND table
        if "COND" not in self.db.tables:
            self.db.load_table("COND")

        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        cond_cols = self.get_cond_columns()

        # Get available columns
        try:
            available_cols = cond.collect_schema().names()
            cond_select = [c for c in cond_cols if c in available_cols]
        except Exception:
            cond_select = cond_cols

        # Join current condition
        cond_current = cond.select(cond_select)
        data = data.join(
            cond_current,
            left_on=["PLT_CN", "CONDID"],
            right_on=["PLT_CN", "CONDID"],
            how="inner",
        )

        # Rename current status column
        data = data.rename({"COND_STATUS_CD": "CURR_COND_STATUS_CD"})

        # Join previous condition to get previous status
        # IMPORTANT: Load the FULL COND table (without EVALID filter) because
        # PREV_PLT_CN references plots from previous inventory cycles
        conn = duckdb.connect(str(self.db.db_path), read_only=True)
        cond_prev = conn.execute(
            "SELECT PLT_CN, CONDID, COND_STATUS_CD FROM COND"
        ).pl().lazy()
        conn.close()

        # Alias the status column for previous condition
        cond_prev = cond_prev.select([
            pl.col("PLT_CN"),
            pl.col("CONDID"),
            pl.col("COND_STATUS_CD").alias("PREV_COND_STATUS_CD"),
        ])

        data = data.join(
            cond_prev,
            left_on=["PREV_PLT_CN", "PREVCOND"],
            right_on=["PLT_CN", "CONDID"],
            how="left",
        )

        # Load PLOT table for REMPER
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")

        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        plot_cols = ["CN", "STATECD", "INVYR", "REMPER"]
        plot = plot.select(plot_cols)

        data = data.join(
            plot,
            left_on="PLT_CN",
            right_on="CN",
            how="inner",
        )

        # Filter to plots with valid REMPER (remeasured plots only)
        data = data.filter(
            pl.col("REMPER").is_not_null() & (pl.col("REMPER") > 0)
        )

        # Join stratification data for expansion factors
        strat_data = self._get_stratification_data()
        data = data.join(strat_data, on="PLT_CN", how="inner")

        return data

    def _is_forest_condition(self, status_col: str) -> pl.Expr:
        """Create expression to check if condition is forest land."""
        land_type = self.config.get("land_type", "forest")

        if land_type == "forest":
            # Forest land: COND_STATUS_CD == 1
            return pl.col(status_col) == 1
        else:
            # Timberland: more complex criteria would need additional columns
            # For now, use same forest definition
            return pl.col(status_col) == 1

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate area change values.

        For each subplot-condition, determine if it represents:
        - Gain: non-forest → forest
        - Loss: forest → non-forest
        - No change: same status

        The change value is weighted by SUBPTYP_PROP_CHNG.
        """
        change_type = self.config.get("change_type", "net")

        # Create forest indicator expressions
        curr_is_forest = self._is_forest_condition("CURR_COND_STATUS_CD")
        prev_is_forest = self._is_forest_condition("PREV_COND_STATUS_CD")

        # Calculate change indicators
        # Gain: was not forest, now is forest
        gain_expr = (~prev_is_forest & curr_is_forest).cast(pl.Float64)
        # Loss: was forest, now is not forest
        loss_expr = (prev_is_forest & ~curr_is_forest).cast(pl.Float64)

        # Weight by subplot proportion
        prop_col = pl.col("SUBPTYP_PROP_CHNG").fill_null(1.0)

        if change_type == "gross_gain":
            # Only gains (positive values)
            data = data.with_columns([
                (gain_expr * prop_col).alias("CHANGE_VALUE")
            ])
        elif change_type == "gross_loss":
            # Only losses (as positive values for magnitude)
            data = data.with_columns([
                (loss_expr * prop_col).alias("CHANGE_VALUE")
            ])
        else:
            # Net change: gains - losses
            data = data.with_columns([
                ((gain_expr - loss_expr) * prop_col).alias("CHANGE_VALUE")
            ])

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply filters to area change data.

        For area change, we keep all remeasured subplot-conditions
        but the change calculation already handles the forest/non-forest logic.
        """
        # Filter to valid transitions (both statuses must be known)
        data = data.filter(
            pl.col("CURR_COND_STATUS_CD").is_not_null() &
            pl.col("PREV_COND_STATUS_CD").is_not_null()
        )

        # Apply any area domain filter if specified
        area_domain = self.config.get("area_domain")
        if area_domain:
            from ...filtering import apply_area_filters
            data = apply_area_filters(data, area_domain)

        return data

    def aggregate_to_plot(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Aggregate subplot-level changes to plot level.

        Sum change values across all subplots within each plot,
        then apply expansion factors.
        """
        # Determine grouping columns
        group_cols = ["PLT_CN", "STATECD", "INVYR", "REMPER"]

        # Add stratification columns for variance (use columns that exist)
        strat_cols = ["STRATUM_CN", "EXPNS", "ADJ_FACTOR_SUBP"]
        for col in strat_cols:
            if col not in group_cols:
                group_cols.append(col)

        # Add user grouping columns
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            for col in grp_by:
                if col not in group_cols:
                    group_cols.append(col)

        # Aggregate to plot level
        # Each subplot contributes 1/4 of the plot area (4 subplots per plot)
        # SUBPTYP 1 = subplot (larger), SUBPTYP 2 = microplot (smaller)
        # For simplicity, we sum the change values and will apply expansion later
        agg_exprs = [
            pl.col("CHANGE_VALUE").sum().alias("PLOT_CHANGE_VALUE"),
            pl.len().alias("N_SUBPLOTS"),
        ]

        data = data.group_by(group_cols).agg(agg_exprs)

        # Normalize by number of subplots (typically 4 per plot)
        # Each subplot represents 1/4 of the plot
        data = data.with_columns([
            (pl.col("PLOT_CHANGE_VALUE") / 4.0).alias("PLOT_CHANGE_NORM")
        ])

        return data

    def apply_expansion_factors(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply expansion factors to convert to acres.

        For area change, we multiply by EXPNS * ADJ_FACTOR_SUBP.
        If annual=True (default), we also divide by REMPER.
        """
        annual = self.config.get("annual", True)

        # Apply expansion factor
        data = data.with_columns([
            (pl.col("PLOT_CHANGE_NORM") * pl.col("EXPNS") * pl.col("ADJ_FACTOR_SUBP"))
            .alias("CHANGE_EXPANDED")
        ])

        # Annualize if requested
        if annual:
            data = data.with_columns([
                (pl.col("CHANGE_EXPANDED") / pl.col("REMPER"))
                .alias("CHANGE_ANNUAL")
            ])
            value_col = "CHANGE_ANNUAL"
        else:
            value_col = "CHANGE_EXPANDED"

        # Rename to standard column
        data = data.with_columns([
            pl.col(value_col).alias("AREA_CHANGE")
        ])

        return data

    def calculate_totals(self, data: pl.LazyFrame) -> pl.DataFrame:
        """
        Calculate area change totals by grouping.

        Sum expanded area change values, optionally by grouping columns.
        """
        # Store for variance calculation
        self.plot_change_data = data.collect()

        # Determine grouping
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            group_cols = list(grp_by)
        else:
            group_cols = []

        # Always include STATECD for state-level results
        if "STATECD" not in group_cols:
            group_cols.insert(0, "STATECD")

        # Aggregate
        if group_cols:
            result = self.plot_change_data.group_by(group_cols).agg([
                pl.col("AREA_CHANGE").sum().alias("AREA_CHANGE_TOTAL"),
                pl.col("PLT_CN").n_unique().alias("N_PLOTS"),
            ])
        else:
            result = self.plot_change_data.select([
                pl.col("AREA_CHANGE").sum().alias("AREA_CHANGE_TOTAL"),
                pl.col("PLT_CN").n_unique().alias("N_PLOTS"),
            ])

        return result

    def calculate_variance(self, result: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate variance for area change estimates.

        Uses stratified variance estimation following Bechtold & Patterson.
        """
        if self.plot_change_data is None:
            return result

        # Get grouping columns
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            group_cols = list(grp_by)
        else:
            group_cols = []

        if "STATECD" not in group_cols:
            group_cols.insert(0, "STATECD")

        # Calculate stratified variance
        from ..variance import calculate_grouped_domain_total_variance

        var_result = calculate_grouped_domain_total_variance(
            plot_data=self.plot_change_data,
            value_col="AREA_CHANGE",
            group_cols=group_cols if group_cols else None,
        )

        # Join variance to result
        if group_cols:
            result = result.join(var_result, on=group_cols, how="left")
        else:
            # Single row result
            if len(var_result) > 0:
                result = result.with_columns([
                    pl.lit(var_result["VARIANCE"][0]).alias("VARIANCE"),
                    pl.lit(var_result["SE"][0]).alias("SE"),
                ])

        return result

    def estimate(self) -> pl.DataFrame:
        """
        Run the area change estimation pipeline.

        Returns
        -------
        pl.DataFrame
            Area change estimates with columns:
            - STATECD: State code
            - AREA_CHANGE_TOTAL: Total area change (acres or acres/year)
            - N_PLOTS: Number of remeasured plots
            - SE, VARIANCE: (if variance=True) Standard error and variance
            - Additional grouping columns if grp_by specified
        """
        # Load data
        data = self.load_data()
        if data is None:
            raise ValueError("Failed to load area change data")

        # Apply filters
        data = self.apply_filters(data)

        # Calculate change values
        data = self.calculate_values(data)

        # Aggregate to plot level
        data = self.aggregate_to_plot(data)

        # Apply expansion factors
        data = self.apply_expansion_factors(data)

        # Calculate totals
        result = self.calculate_totals(data)

        # Calculate variance if requested
        if self.config.get("variance", False):
            result = self.calculate_variance(result)

        # Format output columns
        result = format_output_columns(result, "area_change")

        return result


def area_change(
    db: FIA,
    land_type: Literal["forest", "timber"] = "forest",
    change_type: Literal["net", "gross_gain", "gross_loss"] = "net",
    annual: bool = True,
    grp_by: Optional[Union[str, List[str]]] = None,
    area_domain: Optional[str] = None,
    variance: bool = False,
    totals: bool = False,
) -> pl.DataFrame:
    """
    Estimate area change for forest or timberland.

    Calculates net or gross change in forest/timberland area using remeasured
    plots from the SUBP_COND_CHNG_MTRX table. Only plots measured at two time
    points contribute to the estimate.

    Parameters
    ----------
    db : FIA
        FIA database connection with EVALID set
    land_type : {'forest', 'timber'}, default 'forest'
        Land classification to track changes for:
        - 'forest': All forest land (COND_STATUS_CD = 1)
        - 'timber': Timberland only (productive, unreserved forest)
    change_type : {'net', 'gross_gain', 'gross_loss'}, default 'net'
        Type of change to calculate:
        - 'net': Net change (gains minus losses)
        - 'gross_gain': Only area gained (non-forest to forest)
        - 'gross_loss': Only area lost (forest to non-forest)
    annual : bool, default True
        If True, return annualized rate in acres/year
        If False, return total change over remeasurement period
    grp_by : str or list of str, optional
        Column(s) to group results by (e.g., 'OWNGRPCD', 'FORTYPCD')
    area_domain : str, optional
        SQL-like filter expression for conditions
    variance : bool, default False
        Whether to calculate sampling variance and standard error
    totals : bool, default False
        Whether to include totals row

    Returns
    -------
    pl.DataFrame
        Area change estimates with columns:
        - STATECD: State FIPS code
        - AREA_CHANGE_TOTAL: Area change (acres/year if annual=True, else acres)
        - N_PLOTS: Number of remeasured plots
        - SE: Standard error (if variance=True)
        - VARIANCE: Sampling variance (if variance=True)
        - Grouping columns (if grp_by specified)

    Examples
    --------
    >>> from pyfia import FIA, area_change
    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_most_recent()
    ...     # Net annual forest area change
    ...     result = area_change(db, land_type="forest")
    ...     print(f"Annual change: {result['AREA_CHANGE_TOTAL'][0]:+,.0f} acres/year")

    >>> # Gross forest loss by ownership
    >>> result = area_change(
    ...     db,
    ...     change_type="gross_loss",
    ...     grp_by="OWNGRPCD",
    ...     variance=True
    ... )

    Notes
    -----
    Area change estimation requires remeasured plots (plots with both current
    and previous measurements). States with newer FIA programs may have fewer
    remeasured plots, resulting in higher sampling errors.

    The REMPER (remeasurement period) varies by plot but averages approximately
    5-7 years in most states.

    References
    ----------
    Bechtold & Patterson (2005), "The Enhanced Forest Inventory and Analysis
    Program - National Sampling Design and Estimation Procedures", Chapter 4.
    """
    config = {
        "land_type": land_type,
        "change_type": change_type,
        "annual": annual,
        "grp_by": grp_by,
        "area_domain": area_domain,
        "variance": variance,
        "totals": totals,
    }

    estimator = AreaChangeEstimator(db, config)
    return estimator.estimate()
