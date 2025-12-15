"""
Growth estimation for FIA data using GRM methodology.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree growth using TREE_GRM_COMPONENT, TREE_GRM_MIDPT, and
TREE_GRM_BEGIN tables following EVALIDator approach.
"""

from typing import List, Literal, Optional, Union

import polars as pl

from ...core import FIA
from ..base import GRMBaseEstimator


class GrowthEstimator(GRMBaseEstimator):
    """
    Growth estimator for FIA data using GRM methodology.

    Estimates annual tree growth in terms of volume, biomass, or trees per acre
    using the TREE_GRM_COMPONENT, TREE_GRM_MIDPT, and TREE_GRM_BEGIN tables.
    Follows EVALIDator methodology with component-based calculations.
    """

    @property
    def component_type(self) -> Literal["growth", "mortality", "removals"]:
        """Return 'growth' as the GRM component type."""
        return "growth"

    def get_cond_columns(self) -> List[str]:
        """Required condition columns for growth estimation."""
        base_cols = [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "OWNGRPCD",
            "FORTYPCD",
            "SITECLCD",
            "RESERVCD",
            "ALSTKCD",
        ]

        if self.config.get("grp_by"):
            grp_cols = self.config["grp_by"]
            if isinstance(grp_cols, str):
                grp_cols = [grp_cols]
            for col in grp_cols:
                if col not in base_cols:
                    base_cols.append(col)

        return base_cols

    def load_data(self) -> Optional[pl.LazyFrame]:
        """
        Load and join tables following EVALIDator SQL join sequence.

        Growth requires complex join pattern with BEGINEND cross-join.
        Cannot use the simple GRM loading pattern.
        """
        from ..grm import load_grm_begin, load_grm_component, load_grm_midpt

        measure = self.config.get("measure", "volume")

        # Resolve GRM column names
        grm_cols = self._resolve_grm_columns()

        # Load TREE table first - this is our anchor
        if "TREE" not in self.db.tables:
            self.db.load_table("TREE")

        tree = self.db.tables["TREE"]
        if not isinstance(tree, pl.LazyFrame):
            tree = tree.lazy()

        # Select TREE columns
        if measure == "volume":
            tree_cols = [
                "CN",
                "PLT_CN",
                "CONDID",
                "PREVCOND",
                "PREV_TRE_CN",
                "VOLCFNET",
            ]
            tree_vol_col = "VOLCFNET"
        elif measure == "biomass":
            tree_cols = [
                "CN",
                "PLT_CN",
                "CONDID",
                "PREVCOND",
                "PREV_TRE_CN",
                "DRYBIO_AG",
            ]
            tree_vol_col = "DRYBIO_AG"
        else:
            tree_cols = ["CN", "PLT_CN", "CONDID", "PREVCOND", "PREV_TRE_CN"]
            tree_vol_col = None

        data = tree.select(tree_cols)
        if tree_vol_col:
            data = data.rename({tree_vol_col: f"TREE_{tree_vol_col}"})

        # Load and join GRM_COMPONENT
        grm_component = load_grm_component(
            self.db,
            grm_cols,
            include_dia_end=True,
        )

        data = data.join(
            grm_component,
            left_on="CN",
            right_on="TRE_CN",
            how="inner",
        )

        # Load and join TREE_GRM_MIDPT
        if measure == "volume":
            midpt_cols = ["VOLCFNET"]
        elif measure == "biomass":
            midpt_cols = ["DRYBIO_AG"]
        else:
            midpt_cols = None

        grm_midpt = load_grm_midpt(
            self.db, measure=measure, include_additional_cols=midpt_cols
        )

        if measure in ("volume", "biomass"):
            vol_col = "VOLCFNET" if measure == "volume" else "DRYBIO_AG"
            grm_midpt = grm_midpt.rename({vol_col: f"MIDPT_{vol_col}"})

        data = data.join(grm_midpt, left_on="CN", right_on="TRE_CN", how="left")

        # Load and join TREE_GRM_BEGIN
        grm_begin = load_grm_begin(self.db, measure=measure)
        if measure in ("volume", "biomass"):
            vol_col = "VOLCFNET" if measure == "volume" else "DRYBIO_AG"
            grm_begin = grm_begin.rename({vol_col: f"BEGIN_{vol_col}"})

        data = data.join(grm_begin, left_on="CN", right_on="TRE_CN", how="left")

        # Join PTREE for fallback
        if measure in ("volume", "biomass") and tree_vol_col is not None:
            ptree = tree.select(["CN", tree_vol_col]).rename(
                {tree_vol_col: f"PTREE_{tree_vol_col}"}
            )
            data = data.join(ptree, left_on="PREV_TRE_CN", right_on="CN", how="left")

        # Join PLOT
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")

        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        plot = plot.select(
            ["CN", "STATECD", "INVYR", "PREV_PLT_CN", "MACRO_BREAKPOINT_DIA", "REMPER"]
        )

        data = data.join(plot, left_on="PLT_CN", right_on="CN", how="inner")

        # Join COND
        if "COND" not in self.db.tables:
            self.db.load_table("COND")

        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        cond_cols = self.get_cond_columns()
        try:
            cond = cond.select(cond_cols)
        except Exception:
            available = cond.collect_schema().names()
            cond = cond.select([c for c in cond_cols if c in available])

        data = data.join(
            cond,
            left_on=["PLT_CN", "CONDID"],
            right_on=["PLT_CN", "CONDID"],
            how="inner",
        )

        # Join stratification for expansion factors
        strat_data = self._get_stratification_data()
        data = data.join(strat_data, on="PLT_CN", how="inner")

        # Join BEGINEND (cross-join)
        if "BEGINEND" not in self.db.tables:
            try:
                self.db.load_table("BEGINEND")
            except Exception as e:
                raise ValueError(f"BEGINEND not found: {e}")

        beginend = self.db.tables["BEGINEND"]
        if not isinstance(beginend, pl.LazyFrame):
            beginend = beginend.lazy()

        if hasattr(self.db, "_state_filter") and self.db._state_filter:
            beginend = beginend.filter(
                pl.col("STATE_ADDED").is_in(self.db._state_filter)
            )

        beginend = beginend.select(["ONEORTWO"]).unique()

        data = data.join(beginend, how="cross")

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply growth-specific filters.

        CRITICAL: Include ALL components for BEGINEND cross-join methodology.
        Do NOT filter by COND_STATUS_CD - GRM columns already handle land basis.
        """
        from ...filtering import apply_area_filters

        area_domain = self.config.get("area_domain")
        tree_domain = self.config.get("tree_domain")

        if area_domain:
            # apply_area_filters expects DataFrame, collect and convert back
            data_df = apply_area_filters(data.collect(), area_domain)
            data = data_df.lazy()

        if tree_domain:
            try:
                if "DIA_MIDPT >= 5.0" in tree_domain:
                    data = data.filter(pl.col("DIA_MIDPT") >= 5.0)
            except Exception:
                pass

        # Filter to records with non-null TPA_UNADJ
        data = data.filter(pl.col("TPA_UNADJ").is_not_null())

        return data

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate growth values using BEGINEND ONEORTWO methodology.

        Implements EVALIDator's ONEORTWO logic:
        - ONEORTWO=2: Add ending volumes (positive contribution)
        - ONEORTWO=1: Subtract beginning volumes (negative contribution)
        Sum across ONEORTWO rows gives NET growth = ending - beginning
        """
        measure = self.config.get("measure", "volume")

        if measure == "volume":
            tree_col = "TREE_VOLCFNET"
            midpt_col = "MIDPT_VOLCFNET"
            begin_col = "BEGIN_VOLCFNET"
            ptree_col = "PTREE_VOLCFNET"
        elif measure == "biomass":
            tree_col = "TREE_DRYBIO_AG"
            midpt_col = "MIDPT_DRYBIO_AG"
            begin_col = "BEGIN_DRYBIO_AG"
            ptree_col = "PTREE_DRYBIO_AG"
        else:
            # For count, use TPA_UNADJ directly with ONEORTWO logic
            data = data.with_columns(
                [
                    pl.when(pl.col("ONEORTWO") == 2)
                    .then(pl.col("TPA_UNADJ").cast(pl.Float64))
                    .when(pl.col("ONEORTWO") == 1)
                    .then(-pl.col("TPA_UNADJ").cast(pl.Float64))
                    .otherwise(0.0)
                    .alias("GROWTH_VALUE")
                ]
            )
            return data

        # ONEORTWO = 2: Add ending volumes
        # MORTALITY gets 0 for ending volumes - they died
        ending_volume = (
            pl.when(
                (pl.col("COMPONENT") == "SURVIVOR")
                | (pl.col("COMPONENT") == "INGROWTH")
                | (pl.col("COMPONENT").str.starts_with("REVERSION"))
            )
            .then(pl.col(tree_col).fill_null(0) / pl.col("REMPER").fill_null(5.0))
            .when(
                (pl.col("COMPONENT").str.starts_with("CUT"))
                | (pl.col("COMPONENT").str.starts_with("DIVERSION"))
            )
            .then(pl.col(midpt_col).fill_null(0) / pl.col("REMPER").fill_null(5.0))
            .otherwise(0.0)
        )

        # ONEORTWO = 1: Subtract beginning volumes
        beginning_volume = (
            pl.when(
                (pl.col("COMPONENT") == "SURVIVOR")
                | (pl.col("COMPONENT") == "CUT1")
                | (pl.col("COMPONENT") == "DIVERSION1")
                | (pl.col("COMPONENT") == "MORTALITY1")
            )
            .then(
                pl.when(pl.col(begin_col).is_not_null())
                .then(-(pl.col(begin_col) / pl.col("REMPER").fill_null(5.0)))
                .otherwise(
                    -(pl.col(ptree_col).fill_null(0) / pl.col("REMPER").fill_null(5.0))
                )
            )
            .otherwise(0.0)
        )

        # Apply ONEORTWO logic
        data = data.with_columns(
            [
                pl.when(pl.col("ONEORTWO") == 2)
                .then(ending_volume)
                .when(pl.col("ONEORTWO") == 1)
                .then(beginning_volume)
                .otherwise(0.0)
                .alias("volume_contribution")
            ]
        )

        # Convert biomass from pounds to tons
        conversion_factor = 1.0 / 2000.0 if measure == "biomass" else 1.0

        data = data.with_columns(
            [
                (
                    pl.col("TPA_UNADJ").cast(pl.Float64)
                    * pl.col("volume_contribution").cast(pl.Float64)
                    * conversion_factor
                ).alias("GROWTH_VALUE")
            ]
        )

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:  # type: ignore[override]
        """Aggregate growth with two-stage aggregation."""
        from ..grm import apply_grm_adjustment

        # Apply GRM-specific adjustment factors
        data_with_strat = apply_grm_adjustment(data)

        # Apply adjustment to growth values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("GROWTH_VALUE") * pl.col("ADJ_FACTOR")).alias("GROWTH_ADJ")]
        )

        # Setup grouping
        group_cols = self._setup_grouping()
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")
        self.group_cols = group_cols

        # Store plot-tree level data for variance calculation
        self.plot_tree_data, data_with_strat = self._preserve_plot_tree_data(
            data_with_strat,
            metric_cols=["GROWTH_ADJ"],
            group_cols=group_cols,
        )

        # Use shared two-stage aggregation method
        metric_mappings = {"GROWTH_ADJ": "CONDITION_GROWTH"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,
        )

        # Rename columns
        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_GROWTH_TREES"})

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for growth estimates using ratio-of-means formula.

        Implements Bechtold & Patterson (2005) stratified variance calculation.
        """
        results = self._calculate_grm_variance(
            results,
            adjusted_col="GROWTH_ADJ",
            acre_se_col="GROWTH_ACRE_SE",
            total_se_col="GROWTH_TOTAL_SE",
        )

        # Add CV if requested
        if self.config.get("include_cv", False):
            results = results.with_columns(
                [
                    pl.when(pl.col("GROWTH_ACRE") > 0)
                    .then(pl.col("GROWTH_ACRE_SE") / pl.col("GROWTH_ACRE") * 100)
                    .otherwise(None)
                    .alias("GROWTH_ACRE_CV"),
                    pl.when(pl.col("GROWTH_TOTAL") > 0)
                    .then(pl.col("GROWTH_TOTAL_SE") / pl.col("GROWTH_TOTAL") * 100)
                    .otherwise(None)
                    .alias("GROWTH_TOTAL_CV"),
                ]
            )

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format growth estimation output."""
        return self._format_grm_output(
            results,
            estimation_type="growth",
            include_cv=self.config.get("include_cv", False),
        )


def growth(
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
) -> pl.DataFrame:
    """
    Estimate annual tree growth from FIA data using GRM methodology.

    Calculates annual growth of tree volume, biomass, or tree count using
    FIA's Growth-Removal-Mortality (GRM) tables following EVALIDator
    methodology.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database.
    grp_by : str or list of str, optional
        Column name(s) to group results by.
    by_species : bool, default False
        If True, group results by species code (SPCD).
    by_size_class : bool, default False
        If True, group results by diameter size classes.
    land_type : {'forest', 'timber'}, default 'forest'
        Land type to include in estimation.
    tree_type : {'gs', 'al', 'sl'}, default 'gs'
        Tree type to include.
    measure : {'volume', 'biomass', 'count'}, default 'volume'
        What to measure in the growth estimation.
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
    totals : bool, default True
        If True, include population-level total estimates.
    variance : bool, default False
        If True, calculate and include variance and standard error estimates.
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation.

    Returns
    -------
    pl.DataFrame
        Growth estimates with columns:
        - GROWTH_ACRE: Annual growth per acre
        - GROWTH_TOTAL: Total annual growth (if totals=True)
        - GROWTH_ACRE_SE: Standard error of per-acre estimate (if variance=True)
        - Additional grouping columns if specified

    See Also
    --------
    mortality : Estimate annual mortality using GRM tables
    removals : Estimate annual removals/harvest using GRM tables

    Examples
    --------
    Basic volume growth on forestland:

    >>> results = growth(db, measure="volume", land_type="forest")

    Growth by species (tree count):

    >>> results = growth(db, by_species=True, measure="count")

    Notes
    -----
    This function uses FIA's GRM tables which contain component-level tree
    data for calculating annual growth. The implementation follows
    EVALIDator methodology for statistically valid estimation.
    """
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,
        validate_tree_type,
    )

    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
    measure = validate_mortality_measure(measure)
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

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
        "include_cv": False,
    }

    estimator = GrowthEstimator(db, config)
    return estimator.estimate()
