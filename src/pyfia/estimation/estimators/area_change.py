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

from __future__ import annotations

from typing import Literal

import polars as pl

from ...core import FIA
from ..base import AggregationResult, BaseEstimator
from ..columns import collect_referenced_columns, columns_in_table
from ..utils import apply_variance_columns, format_output_columns


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
    area_domain : str, optional
        Filter on the current (time-2) condition
    variance : bool, default False
        If True, also return the AREA_CHANGE_VARIANCE column alongside
        AREA_CHANGE_SE (which is always returned). Variance = SE squared.
    totals : bool, default False
        Whether to include state totals

    Notes
    -----
    COND_STATUS_CD values:
        1 = Forest land
        2 = Non-forest land
        3 = Non-census water
        4 = Census water
        5 = Nonsampled, possibility of forest land

    Forest transitions:
        - Gain: Previous COND_STATUS_CD != 1, Current COND_STATUS_CD == 1
        - Loss: Previous COND_STATUS_CD == 1, Current COND_STATUS_CD != 1

    Conditions nonsampled at either measurement are excluded.
    """

    def __init__(self, db: str | FIA, config: dict) -> None:
        """Initialize with storage for variance calculation."""
        super().__init__(db, config)
        self.plot_change_data: pl.DataFrame | None = None

    def get_required_tables(self) -> list[str]:
        """Area change requires SUBP_COND_CHNG_MTRX, COND, PLOT, and stratification."""
        return [
            "SUBP_COND_CHNG_MTRX",
            "COND",
            "PLOT",
            "POP_PLOT_STRATUM_ASSGN",
            "POP_STRATUM",
        ]

    def get_cond_columns(self) -> list[str]:
        """Get required condition columns."""
        core_cols = [
            "CN",
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "PROP_BASIS",
            "COND_NONSAMPLE_REASN_CD",
        ]

        # Add timberland columns if needed
        land_type = self.config.get("land_type", "forest")
        if land_type == "timber":
            core_cols.extend(["SITECLCD", "RESERVCD"])

        # Add the COND columns that grp_by and area_domain reference
        referenced = collect_referenced_columns(
            self.config.get("grp_by"), self.config.get("area_domain")
        )
        for col in columns_in_table(self.db, "COND", referenced):
            if col not in core_cols:
                core_cols.append(col)

        return core_cols

    def load_data(self) -> pl.LazyFrame | None:
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
        data = chng.select(
            [
                "PLT_CN",
                "CONDID",
                "PREV_PLT_CN",
                "PREVCOND",
                "SUBP",
                "SUBPTYP",
                "SUBPTYP_PROP_CHNG",
            ]
        )

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
        except pl.exceptions.ComputeError:
            # Schema unavailable (e.g., invalid query); fall back to all requested columns
            cond_select = cond_cols

        # Join current condition
        cond_current = cond.select(cond_select)
        data = data.join(
            cond_current,
            left_on=["PLT_CN", "CONDID"],
            right_on=["PLT_CN", "CONDID"],
            how="inner",
        )

        # Name the current status explicitly, keeping COND_STATUS_CD for
        # area_domain expressions, which describe the current condition
        data = data.with_columns(pl.col("COND_STATUS_CD").alias("CURR_COND_STATUS_CD"))

        # Join previous condition to get previous status
        # IMPORTANT: Load the FULL COND table (without EVALID filter) because
        # PREV_PLT_CN references plots from previous inventory cycles
        cond_prev = self.db._reader.read_table(
            "COND",
            columns=["PLT_CN", "CONDID", "COND_STATUS_CD", "COND_NONSAMPLE_REASN_CD"],
            lazy=True,
        )

        # Alias the previous condition's columns
        cond_prev = cond_prev.select(
            [
                pl.col("PLT_CN"),
                pl.col("CONDID"),
                pl.col("COND_STATUS_CD").alias("PREV_COND_STATUS_CD"),
                pl.col("COND_NONSAMPLE_REASN_CD").alias("PREV_COND_NONSAMPLE_REASN_CD"),
            ]
        )

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

        # An annual rate needs a remeasurement period. The change over the
        # period doesn't, so plots without REMPER still count when annual=False.
        if self.config.get("annual", True):
            data = data.filter(pl.col("REMPER").is_not_null() & (pl.col("REMPER") > 0))

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

        The change value is weighted by SUBPTYP_PROP_CHNG and by the
        adjustment factor of the condition's footprint (ADJ_FACTOR_MACR when
        PROP_BASIS is 'MACR', ADJ_FACTOR_SUBP otherwise), as in EVALIDator.
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

        # Weight by the adjusted subplot proportion; a null proportion
        # contributes nothing
        adj_factor = (
            pl.when(pl.col("PROP_BASIS") == "MACR")
            .then(pl.col("ADJ_FACTOR_MACR"))
            .otherwise(pl.col("ADJ_FACTOR_SUBP"))
        )
        prop_col = (pl.col("SUBPTYP_PROP_CHNG") * adj_factor).fill_null(0.0)

        if change_type == "gross_gain":
            # Only gains (positive values)
            data = data.with_columns([(gain_expr * prop_col).alias("CHANGE_VALUE")])
        elif change_type == "gross_loss":
            # Only losses (as positive values for magnitude)
            data = data.with_columns([(loss_expr * prop_col).alias("CHANGE_VALUE")])
        else:
            # Net change: gains - losses
            data = data.with_columns(
                [((gain_expr - loss_expr) * prop_col).alias("CHANGE_VALUE")]
            )

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply filters to area change data.

        Keeps one change-matrix row per subplot-condition pair: the row for
        the footprint the condition's area is based on (SUBPTYP 1 when
        PROP_BASIS is 'SUBP', SUBPTYP 3 when it is 'MACR'). SUBP_COND_CHNG_MTRX
        repeats each subplot's proportions for the microplot (SUBPTYP 2) and,
        in macroplot states, the macroplot, so summing every row would count
        each transition more than once. Conditions nonsampled at either
        measurement are dropped. These are the row filters EVALIDator applies
        to its area change estimates.
        """
        # Filter to valid transitions (both statuses must be known)
        data = data.filter(
            pl.col("CURR_COND_STATUS_CD").is_not_null()
            & pl.col("PREV_COND_STATUS_CD").is_not_null()
        )

        footprint = ((pl.col("SUBPTYP") == 1) & (pl.col("PROP_BASIS") == "SUBP")) | (
            (pl.col("SUBPTYP") == 3) & (pl.col("PROP_BASIS") == "MACR")
        )
        # COND_NONSAMPLE_REASN_CD is stored as text in some state databases
        sampled_at_both = (
            pl.col("COND_NONSAMPLE_REASN_CD").cast(pl.Int64, strict=False).fill_null(0)
            == 0
        ) & (
            pl.col("PREV_COND_NONSAMPLE_REASN_CD")
            .cast(pl.Int64, strict=False)
            .fill_null(0)
            == 0
        )
        data = data.filter(
            footprint & sampled_at_both & pl.col("CONDPROP_UNADJ").is_not_null()
        )

        # area_domain filters the current (time-2) condition, as in panel()
        # and the GRM estimators
        area_domain = self.config.get("area_domain")
        if area_domain:
            from ...filtering import apply_area_filters

            data = apply_area_filters(data, area_domain=area_domain)

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
        strat_cols = ["STRATUM_CN", "EXPNS"]
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
        # Each subplot contributes 1/4 of the plot area (4 subplots per plot).
        # CHANGE_VALUE is already adjusted; EXPNS is applied later.
        agg_exprs = [
            pl.col("CHANGE_VALUE").sum().alias("PLOT_CHANGE_VALUE"),
            pl.len().alias("N_SUBPLOTS"),
        ]

        data = data.group_by(group_cols).agg(agg_exprs)

        # Normalize by number of subplots (typically 4 per plot)
        # Each subplot represents 1/4 of the plot
        data = data.with_columns(
            [(pl.col("PLOT_CHANGE_VALUE") / 4.0).alias("PLOT_CHANGE_NORM")]
        )

        return data

    def apply_expansion_factors(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply expansion factors to convert to acres.

        The plot's adjusted change, divided by REMPER when annual=True
        (default), is kept as ``y_i`` for the variance; ``AREA_CHANGE`` is
        ``y_i`` times EXPNS.
        """
        annual = self.config.get("annual", True)

        y = pl.col("PLOT_CHANGE_NORM")
        if annual:
            y = y / pl.col("REMPER")

        return data.with_columns(y.alias("y_i")).with_columns(
            (pl.col("y_i") * pl.col("EXPNS")).alias("AREA_CHANGE")
        )

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
            result = self.plot_change_data.group_by(group_cols).agg(
                [
                    pl.col("AREA_CHANGE").sum().alias("AREA_CHANGE_TOTAL"),
                    pl.col("PLT_CN").n_unique().alias("N_PLOTS"),
                ]
            )
        else:
            result = self.plot_change_data.select(
                [
                    pl.col("AREA_CHANGE").sum().alias("AREA_CHANGE_TOTAL"),
                    pl.col("PLT_CN").n_unique().alias("N_PLOTS"),
                ]
            )

        return result

    def calculate_variance(
        self, result: AggregationResult | pl.DataFrame
    ) -> pl.DataFrame:
        """
        Calculate the standard error of each area change total.

        Uses the exact Bechtold & Patterson post-stratified variance of a
        domain total on unexpanded plot values (``y_i``). Every plot in the
        evaluation enters, with zero change where it has none in the group.
        """
        # Handle AggregationResult from base class signature
        if isinstance(result, AggregationResult):
            result = result.results

        if self.plot_change_data is None:
            return result

        # Get grouping columns
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]
            group_cols: list[str] = list(grp_by)
        else:
            group_cols = []

        if "STATECD" not in group_cols:
            group_cols.insert(0, "STATECD")

        from ..variance import calculate_domain_total_variance

        all_plots = (
            self._get_stratification_data()
            .select(
                [
                    "PLT_CN",
                    "STRATUM_CN",
                    "EXPNS",
                    "ESTN_UNIT_CN",
                    "STRATUM_WGT",
                    "AREA_USED",
                    "P2POINTCNT",
                ]
            )
            .collect()
        )

        se_values = []
        for row in result.iter_rows(named=True):
            in_group = pl.all_horizontal(
                pl.col(col).is_null() if row[col] is None else pl.col(col) == row[col]
                for col in group_cols
            )
            group_y = (
                self.plot_change_data.filter(in_group)
                .group_by("PLT_CN")
                .agg(pl.col("y_i").sum())
            )
            plots = all_plots.join(group_y, on="PLT_CN", how="left").with_columns(
                pl.col("y_i").fill_null(0.0)
            )
            se_values.append(calculate_domain_total_variance(plots, "y_i")["se_total"])

        # One SE per result row, in row order. Attached positionally because a
        # join on the group keys would drop the SE of a null group. The
        # matching AREA_CHANGE_VARIANCE column (when variance=True) is added
        # by apply_variance_columns at the end of estimate().
        return result.with_columns(
            pl.Series("AREA_CHANGE_SE", se_values, dtype=pl.Float64)
        )

    def estimate(self) -> pl.DataFrame:
        """
        Run the area change estimation pipeline.

        Returns
        -------
        pl.DataFrame
            Area change estimates with columns:
            - STATECD: State code
            - AREA_CHANGE_TOTAL: Total area change (acres or acres/year)
            - AREA_CHANGE_SE: Standard error of the total
            - N_PLOTS: Number of remeasured plots
            - AREA_CHANGE_VARIANCE: (if variance=True) Variance of the total
              (= AREA_CHANGE_SE squared)
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

        # Standard errors are always computed (AREA_CHANGE_SE); the matching
        # AREA_CHANGE_VARIANCE column is added only when variance=True, so the
        # contract matches every other estimator.
        result = self.calculate_variance(result)

        # Format output columns
        result = format_output_columns(result, "area_change")

        return apply_variance_columns(result, self.config.get("variance", False))


def area_change(
    db: FIA,
    land_type: Literal["forest", "timber"] = "forest",
    change_type: Literal["net", "gross_gain", "gross_loss"] = "net",
    annual: bool = True,
    grp_by: str | list[str] | None = None,
    area_domain: str | None = None,
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
        SQL-like filter on the current (time-2) condition, e.g.
        ``"OWNGRPCD == 40"``. A transition counts when its current condition
        meets the filter, as in ``panel()`` and the GRM estimators. FIADB
        records some attributes only on forest conditions (FORTYPCD
        everywhere; OWNGRPCD and RESERVCD in many states), so a domain on one
        of them leaves out losses to nonforest land, whose current condition
        has no value. For a filter on the previous condition, or on both, use
        condition-level remeasurement data.
    variance : bool, default False
        If True, also return the AREA_CHANGE_VARIANCE column alongside
        AREA_CHANGE_SE (which is always returned). Variance = SE squared.
    totals : bool, default False
        Whether to include totals row

    Returns
    -------
    pl.DataFrame
        Area change estimates with columns:
        - STATECD: State FIPS code
        - AREA_CHANGE_TOTAL: Area change (acres/year if annual=True, else acres)
        - AREA_CHANGE_SE: Standard error of the total
        - N_PLOTS: Number of remeasured plots
        - AREA_CHANGE_VARIANCE: Variance of the total, = AREA_CHANGE_SE squared
          (if variance=True)
        - Grouping columns (if grp_by specified)

    Examples
    --------
    >>> from pyfia import FIA, area_change
    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_most_recent(eval_type="CHNG")
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
    remeasured plots, resulting in higher sampling errors. Clip the database to
    a change evaluation (``db.clip_most_recent(eval_type="CHNG")``) so plots
    are expanded by that evaluation's strata.

    Each subplot's transition counts once, on the footprint the condition's
    area is based on: the subplot row of SUBP_COND_CHNG_MTRX when PROP_BASIS
    is 'SUBP', the macroplot row when it is 'MACR', with the matching
    adjustment factor. Conditions nonsampled at either measurement are left
    out. These are EVALIDator's rules, so ``gross_gain + gross_loss`` equals
    EVALIDator's forest area where either measurement is forest land minus
    the area where both are (snum 128 minus 127, or 137 minus 136 per year).
    With ``annual=False``, plots without a remeasurement period still count.

    ``AREA_CHANGE_SE`` is the exact Bechtold & Patterson post-stratified
    standard error of a domain total, computed over every plot in the
    evaluation (zero change where a plot has none). On the same plot values
    it reproduces EVALIDator's sampling errors for its area change estimates.

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
