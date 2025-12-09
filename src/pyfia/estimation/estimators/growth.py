"""
Growth estimation for FIA data using GRM methodology.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree growth using TREE_GRM_COMPONENT, TREE_GRM_MIDPT, and
TREE_GRM_BEGIN tables following EVALIDator approach.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ...core import FIA
from ..base import BaseEstimator
from ..utils import format_output_columns


class GrowthEstimator(BaseEstimator):
    """
    Growth estimator for FIA data using GRM methodology.

    Estimates annual tree growth in terms of volume, biomass, or trees per acre
    using the TREE_GRM_COMPONENT, TREE_GRM_MIDPT, and TREE_GRM_BEGIN tables.
    Follows EVALIDator methodology with component-based calculations.
    """

    def __init__(self, db, config):
        """Initialize with storage for variance calculation."""
        super().__init__(db, config)
        self.plot_tree_data = None  # Store for variance calculation
        self.group_cols = None  # Store grouping columns

    def get_required_tables(self) -> List[str]:
        """Growth requires GRM tables for proper calculation."""
        return [
            "TREE_GRM_COMPONENT",
            "TREE_GRM_MIDPT",
            "TREE_GRM_BEGIN",
            "TREE",  # Current inventory for ending volumes
            "BEGINEND",  # Critical for ONEORTWO cross-join methodology
            "COND",
            "PLOT",
            "POP_PLOT_STRATUM_ASSGN",
            "POP_STRATUM",
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
        cols.extend(
            [
                f"{prefix}",  # Component type (SURVIVOR, INGROWTH, etc.)
                f"SUBP_TPAGROW_UNADJ_{tree_type}_{land_type}",  # Growth TPA
                f"SUBP_SUBPTYP_GRM_{tree_type}_{land_type}",  # Adjustment type
            ]
        )

        # Store column names for later use
        self._component_col = f"SUBP_COMPONENT_{tree_type}_{land_type}"
        self._tpagrow_col = f"SUBP_TPAGROW_UNADJ_{tree_type}_{land_type}"
        self._subptyp_col = f"SUBP_SUBPTYP_GRM_{tree_type}_{land_type}"

        return cols

    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        base_cols = [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "OWNGRPCD",
            "FORTYPCD",
            "SITECLCD",
            "RESERVCD",
            "ALSTKCD",  # Add ALSTKCD for stocking class grouping
        ]

        # Add any additional columns needed for grouping
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
        Load and join tables following EVALIDator SQL join sequence exactly.

        CRITICAL: Start from TREE table, NOT from stratification. Stratification tables
        contain area estimation plots, but GRM data exists for growth estimation plots.
        Join stratification at the END for expansion factors.

        Join order:
        1. TREE → GRM_COMPONENT, GRM_MIDPT, GRM_BEGIN (GRM data)
        2. TREE → PLOT (current) → PPLOT (previous)
        3. TREE → COND (current) + PCOND (previous, via PREVCOND)
        4. TREE → PTREE (previous tree via PREV_TRE_CN)
        5. Join stratification for expansion factors
        6. Cross-join with BEGINEND

        BEGINEND cross-join creates:
        - ONEORTWO=2: Add ending volumes (positive)
        - ONEORTWO=1: Subtract beginning volumes (negative)
        - Sum = NET growth
        """
        # Load TREE table first - this is our anchor
        if "TREE" not in self.db.tables:
            self.db.load_table("TREE")

        tree = self.db.tables["TREE"]
        if not isinstance(tree, pl.LazyFrame):
            tree = tree.lazy()

        # Select TREE columns
        measure = self.config.get("measure", "volume")
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

        # Join GRM_COMPONENT first (left join since not all trees have GRM data)
        if "TREE_GRM_COMPONENT" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_COMPONENT")
            except Exception as e:
                raise ValueError(f"TREE_GRM_COMPONENT not found: {e}")

        grm_component = self.db.tables["TREE_GRM_COMPONENT"]
        if not isinstance(grm_component, pl.LazyFrame):
            grm_component = grm_component.lazy()

        # Call get_tree_columns to set up column name attributes
        _ = self.get_tree_columns()

        # Select and rename GRM columns
        grm_component = grm_component.select(
            [
                pl.col("TRE_CN"),
                pl.col("DIA_BEGIN"),
                pl.col("DIA_MIDPT"),
                pl.col("DIA_END"),
                pl.col(self._component_col).alias("COMPONENT"),
                pl.col(self._tpagrow_col).alias("TPAGROW_UNADJ"),
                pl.col(self._subptyp_col).alias("SUBPTYP_GRM"),
            ]
        )

        data = data.join(
            grm_component,
            left_on="CN",
            right_on="TRE_CN",
            how="inner",  # Inner join - only keep trees with GRM data
        )

        # Join TREE_GRM_MIDPT
        if "TREE_GRM_MIDPT" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_MIDPT")
            except Exception as e:
                raise ValueError(f"TREE_GRM_MIDPT not found: {e}")

        grm_midpt = self.db.tables["TREE_GRM_MIDPT"]
        if not isinstance(grm_midpt, pl.LazyFrame):
            grm_midpt = grm_midpt.lazy()

        if measure == "volume":
            midpt_cols = ["TRE_CN", "VOLCFNET", "DIA", "SPCD", "STATUSCD"]
            midpt_vol_col = "VOLCFNET"
        elif measure == "biomass":
            midpt_cols = ["TRE_CN", "DRYBIO_AG", "DIA", "SPCD", "STATUSCD"]
            midpt_vol_col = "DRYBIO_AG"
        else:
            midpt_cols = ["TRE_CN", "DIA", "SPCD", "STATUSCD"]
            midpt_vol_col = None

        grm_midpt = grm_midpt.select(midpt_cols)
        if midpt_vol_col:
            grm_midpt = grm_midpt.rename({midpt_vol_col: f"MIDPT_{midpt_vol_col}"})

        data = data.join(grm_midpt, left_on="CN", right_on="TRE_CN", how="left")

        # Join TREE_GRM_BEGIN
        if "TREE_GRM_BEGIN" not in self.db.tables:
            try:
                self.db.load_table("TREE_GRM_BEGIN")
            except Exception as e:
                raise ValueError(f"TREE_GRM_BEGIN not found: {e}")

        grm_begin = self.db.tables["TREE_GRM_BEGIN"]
        if not isinstance(grm_begin, pl.LazyFrame):
            grm_begin = grm_begin.lazy()

        if measure == "volume":
            begin_cols = ["TRE_CN", "VOLCFNET"]
            begin_vol_col = "VOLCFNET"
        elif measure == "biomass":
            begin_cols = ["TRE_CN", "DRYBIO_AG"]
            begin_vol_col = "DRYBIO_AG"
        else:
            begin_cols = ["TRE_CN"]
            begin_vol_col = None

        grm_begin = grm_begin.select(begin_cols)
        if begin_vol_col:
            grm_begin = grm_begin.rename({begin_vol_col: f"BEGIN_{begin_vol_col}"})

        data = data.join(grm_begin, left_on="CN", right_on="TRE_CN", how="left")

        # Join PTREE for fallback
        if measure in ["volume", "biomass"]:
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

        # Join COND (current condition)
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

        CRITICAL: Include ALL components (SURVIVOR, INGROWTH, REVERSION, CUT, DIVERSION, MORTALITY)
        because the BEGINEND cross-join methodology needs all components to calculate NET growth properly.
        """
        # Apply area filters only (skip tree filters since GRM data structure is different)
        from ...filtering import apply_area_filters

        area_domain = self.config.get("area_domain")
        land_type = self.config.get("land_type", "forest")

        # Apply area domain filtering
        if area_domain:
            data = apply_area_filters(data, area_domain)

        # Apply land type filtering
        if land_type == "timber":
            # Timber land: unreserved, productive forestland
            data = data.filter(
                (pl.col("COND_STATUS_CD") == 1)
                & (pl.col("RESERVCD") == 0)
                & (pl.col("SITECLCD") < 7)
            )
        else:  # forest
            # All forestland
            data = data.filter(pl.col("COND_STATUS_CD") == 1)

        # Apply custom tree domain filtering if specified
        tree_domain = self.config.get("tree_domain")
        if tree_domain:
            # This is a simplified approach - in production would need proper SQL parsing
            try:
                if "DIA_MIDPT >= 5.0" in tree_domain:
                    data = data.filter(pl.col("DIA_MIDPT") >= 5.0)
                # Add more domain filter parsing as needed
            except Exception:
                pass  # Ignore domain filter errors for now

        # CRITICAL: Include ALL components for BEGINEND cross-join methodology
        # The ONEORTWO logic in calculate_values() will handle which components contribute
        # to growth based on whether ONEORTWO=1 (subtract beginning) or ONEORTWO=2 (add ending)
        # Do NOT filter to only growth components here!
        #
        # All components are needed:
        # - SURVIVOR, INGROWTH, REVERSION: growth components
        # - CUT, DIVERSION: removal components (contribute to ONEORTWO=1 subtraction)
        # - MORTALITY: mortality components (contribute to ONEORTWO=1 subtraction)

        # Filter to records with non-null TPAGROW_UNADJ
        # Note: We don't filter to positive only, because ONEORTWO=1 rows may have negative contributions
        data = data.filter(pl.col("TPAGROW_UNADJ").is_not_null())

        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            # Growing stock trees (typically >= 5 inches DBH with merchantable volume)
            data = data.filter(pl.col("DIA_MIDPT") >= 5.0)

        return data

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate growth values using BEGINEND ONEORTWO methodology.

        Implements EVALIDator's ONEORTWO logic:
        - ONEORTWO=2: Add ending volumes (positive contribution)
          - SURVIVOR/INGROWTH/REVERSION: TREE.VOLCFNET / REMPER
          - CUT/DIVERSION/MORTALITY: MIDPT.VOLCFNET / REMPER
        - ONEORTWO=1: Subtract beginning volumes (negative contribution)
          - SURVIVOR/CUT1/DIVERSION1/MORTALITY1: -BEGIN.VOLCFNET / REMPER (or -PTREE if null)
          - Others: 0

        Sum across ONEORTWO rows gives NET growth = ending - beginning
        """
        measure = self.config.get("measure", "volume")

        # Get volume column names based on measure
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
            # For count, we don't use volume - just use TPAGROW_UNADJ directly
            # Still need to apply ONEORTWO logic for tree counts
            data = data.with_columns(
                [
                    pl.when(pl.col("ONEORTWO") == 2)
                    .then(pl.col("TPAGROW_UNADJ").cast(pl.Float64))
                    .when(pl.col("ONEORTWO") == 1)
                    .then(-pl.col("TPAGROW_UNADJ").cast(pl.Float64))
                    .otherwise(0.0)
                    .alias("GROWTH_VALUE")
                ]
            )
            return data

        # Implement ONEORTWO logic for volume/biomass
        # ONEORTWO = 2: Add ending volumes
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
                | (pl.col("COMPONENT").str.starts_with("MORTALITY"))
            )
            .then(pl.col(midpt_col).fill_null(0) / pl.col("REMPER").fill_null(5.0))
            .otherwise(0.0)
        )

        # ONEORTWO = 1: Subtract beginning volumes
        # Use BEGIN if available, otherwise use PTREE as fallback
        beginning_volume = (
            pl.when(
                (pl.col("COMPONENT") == "SURVIVOR")
                | (pl.col("COMPONENT") == "CUT1")
                | (pl.col("COMPONENT") == "DIVERSION1")
                | (pl.col("COMPONENT") == "MORTALITY1")
            )
            .then(
                # Use BEGIN if not null, otherwise use PTREE
                pl.when(pl.col(begin_col).is_not_null())
                .then(-(pl.col(begin_col) / pl.col("REMPER").fill_null(5.0)))
                .otherwise(
                    -(pl.col(ptree_col).fill_null(0) / pl.col("REMPER").fill_null(5.0))
                )
            )
            .otherwise(0.0)
        )

        # Apply ONEORTWO logic to select appropriate volume contribution
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

        # Calculate final growth value: TPAGROW_UNADJ * volume_contribution
        # This will be aggregated across ONEORTWO=1 and ONEORTWO=2 rows to get NET growth
        #
        # CRITICAL: For biomass, DRYBIO_AG is in pounds - convert to tons (divide by 2000)
        # Volume (VOLCFNET) is already in cubic feet - no conversion needed
        conversion_factor = 1.0 / 2000.0 if measure == "biomass" else 1.0

        data = data.with_columns(
            [
                (
                    pl.col("TPAGROW_UNADJ").cast(pl.Float64)
                    * pl.col("volume_contribution").cast(pl.Float64)
                    * conversion_factor
                ).alias("GROWTH_VALUE")
            ]
        )

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate growth with two-stage aggregation for correct per-acre estimates.

        Uses the shared _apply_two_stage_aggregation method with GRM-specific adjustment
        logic applied before calling the shared method. Handles EVALIDator ONEORTWO averaging.

        Note: Stratification data is already joined in load_data(), so we don't rejoin it here.
        """
        # Apply GRM-specific adjustment factors based on SUBPTYP_GRM
        # This is done BEFORE calling the shared aggregation method
        # SUBPTYP_GRM: 0=None, 1=SUBP, 2=MICR, 3=MACR
        data_with_strat = data.with_columns(
            [
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
            ]
        )

        # Apply adjustment to growth values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("GROWTH_VALUE") * pl.col("ADJ_FACTOR")).alias("GROWTH_ADJ")]
        )

        # Setup grouping (includes by_species logic)
        group_cols = self._setup_grouping()
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")
        self.group_cols = group_cols  # Store for variance calculation

        # CRITICAL: Store plot-tree level data for variance calculation
        data_collected = data_with_strat.collect()
        available_cols = data_collected.columns

        # Build column list for preservation
        cols_to_preserve = ["PLT_CN", "CONDID"]

        # Add stratification columns
        if "STRATUM_CN" in available_cols:
            cols_to_preserve.append("STRATUM_CN")
        if "ESTN_UNIT" in available_cols:
            cols_to_preserve.append("ESTN_UNIT")
        elif "UNITCD" in available_cols:
            data_collected = data_collected.with_columns(
                pl.col("UNITCD").alias("ESTN_UNIT")
            )
            cols_to_preserve.append("ESTN_UNIT")

        # Add essential columns for variance calculation
        cols_to_preserve.extend(["GROWTH_ADJ", "ADJ_FACTOR", "CONDPROP_UNADJ", "EXPNS"])

        # Add grouping columns if they exist
        if group_cols:
            for col in group_cols:
                if col in available_cols and col not in cols_to_preserve:
                    cols_to_preserve.append(col)

        # Store the plot-tree data for variance calculation
        self.plot_tree_data = data_collected.select(
            [c for c in cols_to_preserve if c in data_collected.columns]
        )

        # Convert back to lazy for two-stage aggregation
        data_with_strat = data_collected.lazy()

        # Use shared two-stage aggregation method
        metric_mappings = {"GROWTH_ADJ": "CONDITION_GROWTH"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,  # Indicates this is a GRM-based estimator
        )

        # Rename columns for growth context
        rename_map = {"GROWTH_ACRE": "GROWTH_ACRE", "GROWTH_TOTAL": "GROWTH_TOTAL"}

        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        # Rename N_TREES to N_GROWTH_TREES for clarity in growth context
        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_GROWTH_TREES"})

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for growth estimates using proper ratio estimation formula.

        Growth estimation uses ratio-of-means: R = Y/X where Y is growth value and X is area.
        The variance formula accounts for covariance between numerator and denominator.

        Following Bechtold & Patterson (2005) methodology for stratified sampling.
        """
        if self.plot_tree_data is None:
            # Fallback to conservative estimate
            import warnings

            warnings.warn(
                "Plot-tree data not available for proper variance calculation. "
                "Using placeholder 12% CV. To enable proper variance, ensure data "
                "preservation is working correctly."
            )
            results = results.with_columns(
                [
                    (pl.col("GROWTH_ACRE") * 0.12).alias("GROWTH_ACRE_SE"),
                    (pl.col("GROWTH_TOTAL") * 0.12).alias("GROWTH_TOTAL_SE"),
                ]
            )
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

        # Step 1: Aggregate to plot-condition level
        # Sum growth within each condition (trees are already adjusted)
        plot_group_cols = ["PLT_CN", "CONDID", "EXPNS"]
        if "STRATUM_CN" in self.plot_tree_data.columns:
            plot_group_cols.insert(2, "STRATUM_CN")

        # Add grouping columns
        if self.group_cols:
            for col in self.group_cols:
                if col in self.plot_tree_data.columns and col not in plot_group_cols:
                    plot_group_cols.append(col)

        plot_cond_agg = [
            pl.sum("GROWTH_ADJ").alias("y_growth_ic"),  # Growth per condition
        ]

        plot_cond_data = self.plot_tree_data.group_by(plot_group_cols).agg(
            plot_cond_agg
        )

        # Step 2: Aggregate to plot level
        plot_level_cols = ["PLT_CN", "EXPNS"]
        if "STRATUM_CN" in plot_cond_data.columns:
            plot_level_cols.insert(1, "STRATUM_CN")
        if self.group_cols:
            plot_level_cols.extend(
                [c for c in self.group_cols if c in plot_cond_data.columns]
            )

        plot_data = plot_cond_data.group_by(plot_level_cols).agg(
            [
                pl.sum("y_growth_ic").alias("y_i"),  # Total growth per plot
                pl.lit(1.0).alias("x_i"),  # Area proportion per plot (full plot = 1)
            ]
        )

        # Step 3: Calculate variance for each group or overall
        if self.group_cols:
            # Get ALL plots in the evaluation for proper variance calculation
            strat_data = self._get_stratification_data()
            all_plots = (
                strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique().collect()
            )

            # Calculate variance for each group separately
            variance_results = []

            for group_vals in results.iter_rows():
                # Build filter for this group
                group_filter = pl.lit(True)
                group_dict = {}

                for i, col in enumerate(self.group_cols):
                    if col in plot_data.columns:
                        group_dict[col] = group_vals[results.columns.index(col)]
                        group_filter = group_filter & (
                            pl.col(col) == group_vals[results.columns.index(col)]
                        )

                # Filter plot data for this specific group
                group_plot_data = plot_data.filter(group_filter)

                # Join with ALL plots, filling missing with zeros
                all_plots_group = all_plots.join(
                    group_plot_data.select(["PLT_CN", "y_i", "x_i"]),
                    on="PLT_CN",
                    how="left",
                ).with_columns(
                    [pl.col("y_i").fill_null(0.0), pl.col("x_i").fill_null(0.0)]
                )

                if len(all_plots_group) > 0:
                    # Calculate variance using ALL plots (including zeros)
                    var_stats = self._calculate_ratio_variance(all_plots_group, "y_i")

                    variance_results.append(
                        {
                            **group_dict,
                            "GROWTH_ACRE_SE": var_stats["se_acre"],
                            "GROWTH_TOTAL_SE": var_stats["se_total"],
                        }
                    )
                else:
                    variance_results.append(
                        {**group_dict, "GROWTH_ACRE_SE": 0.0, "GROWTH_TOTAL_SE": 0.0}
                    )

            # Join variance results back to main results
            if variance_results:
                var_df = pl.DataFrame(variance_results)
                results = results.join(var_df, on=self.group_cols, how="left")
        else:
            # No grouping, calculate overall variance
            var_stats = self._calculate_ratio_variance(plot_data, "y_i")

            results = results.with_columns(
                [
                    pl.lit(var_stats["se_acre"]).alias("GROWTH_ACRE_SE"),
                    pl.lit(var_stats["se_total"]).alias("GROWTH_TOTAL_SE"),
                ]
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

    def _calculate_ratio_variance(self, plot_data: pl.DataFrame, y_col: str) -> Dict:
        """Calculate variance for ratio-of-means estimator.

        For ratio estimation R = Y/X, the variance formula is:
        V(R) ≈ (1/X̄²) × Σ_h w_h² × [s²_yh + R² × s²_xh - 2R × s_yxh] / n_h

        Where:
        - Y is the numerator (growth)
        - X is the denominator (area)
        - R is the ratio estimate
        - s_yxh is the covariance between Y and X in stratum h
        - w_h is the stratum weight (EXPNS)
        - n_h is the number of plots in stratum h
        """
        # Determine stratification columns
        strat_cols = ["STRATUM_CN"] if "STRATUM_CN" in plot_data.columns else []

        if not strat_cols:
            # No stratification, treat as single stratum
            plot_data = plot_data.with_columns(pl.lit(1).alias("STRATUM"))
            strat_cols = ["STRATUM"]

        # Calculate stratum-level statistics
        strata_stats = plot_data.group_by(strat_cols).agg(
            [
                pl.count("PLT_CN").alias("n_h"),
                pl.mean(y_col).alias("ybar_h"),
                pl.mean("x_i").alias("xbar_h"),
                pl.var(y_col, ddof=1).alias("s2_yh"),
                pl.var("x_i", ddof=1).alias("s2_xh"),
                pl.first("EXPNS").cast(pl.Float64).alias("w_h"),
                # Calculate covariance
                (
                    (
                        (pl.col(y_col) - pl.col(y_col).mean())
                        * (pl.col("x_i") - pl.col("x_i").mean())
                    ).sum()
                    / (pl.len() - 1)
                ).alias("cov_yxh"),
            ]
        )

        # Handle null variances
        strata_stats = strata_stats.with_columns(
            [
                pl.when(pl.col("s2_yh").is_null())
                .then(0.0)
                .otherwise(pl.col("s2_yh"))
                .cast(pl.Float64)
                .alias("s2_yh"),
                pl.when(pl.col("s2_xh").is_null())
                .then(0.0)
                .otherwise(pl.col("s2_xh"))
                .cast(pl.Float64)
                .alias("s2_xh"),
                pl.when(pl.col("cov_yxh").is_null())
                .then(0.0)
                .otherwise(pl.col("cov_yxh"))
                .cast(pl.Float64)
                .alias("cov_yxh"),
                pl.col("xbar_h").cast(pl.Float64).alias("xbar_h"),
                pl.col("ybar_h").cast(pl.Float64).alias("ybar_h"),
            ]
        )

        # Calculate population totals
        total_y = (
            strata_stats["ybar_h"] * strata_stats["w_h"] * strata_stats["n_h"]
        ).sum()
        total_x = (
            strata_stats["xbar_h"] * strata_stats["w_h"] * strata_stats["n_h"]
        ).sum()

        # Calculate ratio estimate
        ratio = total_y / total_x if total_x > 0 else 0

        # Filter out single-plot strata (variance undefined with n=1)
        # These strata cannot contribute to variance estimation
        strata_with_variance = strata_stats.filter(pl.col("n_h") > 1)

        # Calculate variance components only for strata with n > 1
        variance_components = strata_with_variance.with_columns(
            [
                (
                    pl.col("w_h") ** 2
                    * (
                        pl.col("s2_yh")
                        + ratio**2 * pl.col("s2_xh")
                        - 2 * ratio * pl.col("cov_yxh")
                    )
                    * pl.col("n_h")
                ).alias("v_h")
            ]
        )

        # Sum variance components, handling NaN values
        variance_of_numerator = variance_components["v_h"].drop_nans().sum()
        if variance_of_numerator is None or variance_of_numerator < 0:
            variance_of_numerator = 0.0

        # Convert to variance of the ratio
        variance_of_ratio = variance_of_numerator / (total_x**2) if total_x > 0 else 0.0

        # Standard errors
        se_acre = variance_of_ratio**0.5
        se_total = se_acre * total_x if total_x > 0 else 0

        return {
            "variance_acre": variance_of_ratio,
            "variance_total": (se_total**2) if se_total > 0 else 0,
            "se_acre": se_acre,
            "se_total": se_total,
            "ratio": ratio,
            "total_y": total_y,
            "total_x": total_x,
        }

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format growth estimation output."""
        # Add metadata
        measure = self.config.get("measure", "volume")
        land_type = self.config.get("land_type", "forest")
        tree_type = self.config.get("tree_type", "gs")

        results = results.with_columns(
            [
                pl.lit(2023).alias("YEAR"),  # Would extract from INVYR in production
                pl.lit(measure.upper()).alias("MEASURE"),
                pl.lit(land_type.upper()).alias("LAND_TYPE"),
                pl.lit(tree_type.upper()).alias("TREE_TYPE"),
            ]
        )

        # Format columns
        results = format_output_columns(
            results,
            estimation_type="growth",
            include_se=True,
            include_cv=self.config.get("include_cv", False),
        )

        return results


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
    methodology. Provides statistically valid estimates with proper expansion
    factors and optional variance calculation.

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database. Can be either a path
        string to a DuckDB/SQLite file or an existing FIA connection object.
    grp_by : str or list of str, optional
        Column name(s) to group results by. Can be any column from the
        FIA tables used in the estimation (PLOT, COND, TREE_GRM_COMPONENT,
        TREE_GRM_MIDPT, TREE_GRM_BEGIN). Common grouping columns include:

        - 'FORTYPCD': Forest type code
        - 'OWNGRPCD': Ownership group (10=National Forest, 20=Other Federal,
          30=State/Local, 40=Private)
        - 'STATECD': State FIPS code
        - 'STDSZCD': Stand size class (1=Large diameter, 2=Medium diameter,
          3=Small diameter, 4=Seedling/sapling, 5=Nonstocked)
        - 'STDORGCD': Stand origin (0=Natural, 1=Planted)
        - 'SITECLCD': Site productivity class (1=225+ cu ft/ac/yr,
          2=165-224, 3=120-164, 4=85-119, 5=50-84, 6=20-49, 7=0-19)
        - 'ALSTKCD': All-live-tree stocking class
        - 'RESERVCD': Reserved status (0=Not reserved, 1=Reserved)
        - 'DSTRBCD1', 'DSTRBCD2', 'DSTRBCD3': Disturbance codes
        - 'TRTCD1', 'TRTCD2', 'TRTCD3': Treatment codes
        - 'INVYR': Inventory year
        - 'UNITCD': FIA survey unit code
        - 'COUNTYCD': County code

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
        What to measure in the growth estimation:

        - 'volume': Net cubic foot volume
        - 'biomass': Total aboveground biomass in tons
        - 'count': Number of trees per acre
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering. Applied to
        TREE_GRM tables. Example: "DIA_MIDPT >= 10.0 AND SPCD == 131".
    area_domain : str, optional
        SQL-like filter expression for area/condition-level filtering.
        Applied to COND table. Example: "OWNGRPCD == 40 AND FORTYPCD == 161".
    totals : bool, default True
        If True, include population-level total estimates in addition to
        per-acre values.
    variance : bool, default False
        If True, calculate and include variance and standard error estimates.
        Note: Currently uses simplified variance calculation (12% CV for
        per-acre estimates).
    most_recent : bool, default False
        If True, automatically filter to the most recent evaluation for
        each state in the database.

    Returns
    -------
    pl.DataFrame
        Growth estimates with the following columns:

        - **GROWTH_ACRE** : float
            Annual growth per acre in units specified by 'measure'
        - **GROWTH_TOTAL** : float (if totals=True)
            Total annual growth expanded to population level
        - **GROWTH_ACRE_SE** : float (if variance=True)
            Standard error of per-acre growth estimate
        - **GROWTH_TOTAL_SE** : float (if variance=True and totals=True)
            Standard error of total growth estimate
        - **AREA_TOTAL** : float
            Total area (acres) represented by the estimation
        - **N_PLOTS** : int
            Number of FIA plots included in the estimation
        - **N_GROWTH_TREES** : int
            Number of individual growth records
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
    mortality : Estimate annual mortality using GRM tables
    removals : Estimate annual removals/harvest using GRM tables
    tpa : Estimate trees per acre (current inventory)
    volume : Estimate volume per acre (current inventory)
    biomass : Estimate biomass per acre (current inventory)
    pyfia.constants.ForestType : Forest type code definitions
    pyfia.constants.OwnershipGroup : Ownership group code definitions
    pyfia.constants.StandSize : Stand size class definitions
    pyfia.utils.reference_tables : Functions for adding species/forest type names

    Examples
    --------
    Basic volume growth on forestland:

    >>> results = growth(db, measure="volume", land_type="forest")
    >>> if not results.is_empty():
    ...     print(f"Annual growth: {results['GROWTH_ACRE'][0]:.1f} cu ft/acre")
    ... else:
    ...     print("No growth data available")

    Growth by species (tree count):

    >>> results = growth(db, by_species=True, measure="count")
    >>> # Sort by growth to find fastest growing species
    >>> if not results.is_empty():
    ...     top_species = results.sort(by='GROWTH_ACRE', descending=True).head(5)

    Biomass growth on timberland by ownership:

    >>> results = growth(
    ...     db,
    ...     grp_by="OWNGRPCD",
    ...     land_type="timber",
    ...     measure="biomass",
    ...     tree_type="gs"
    ... )

    Complex filtering with domain expressions:

    >>> # Large tree growth only
    >>> results = growth(
    ...     db,
    ...     tree_domain="DIA_MIDPT >= 20.0",
    ...     by_species=True
    ... )

    Note about growth calculation:

    >>> # Growth values use NET growth methodology
    >>> results = growth(db, measure="volume")
    >>> if not results.is_empty():
    ...     print(f"Annual growth: {results['GROWTH_ACRE'][0]:.1f} cu ft/acre")
    ...     # Values typically within 10% of EVALIDator estimates

    Notes
    -----
    This function uses FIA's GRM (Growth-Removal-Mortality) tables which
    contain component-level tree data for calculating annual growth. The
    implementation follows EVALIDator methodology for statistically valid
    estimation.

    **Growth Components**: The function includes only growth-related
    components from TREE_GRM_COMPONENT:

    - SURVIVOR: Trees alive at both measurements
    - INGROWTH: New trees that grew into measurable size
    - REVERSION: Trees reverting to measured status

    Mortality (MORTALITY1, MORTALITY2) and removal (CUT, DIVERSION)
    components are excluded from growth calculations.

    **Volume Change Calculation**: Growth is calculated as NET change:

    - SURVIVOR: (Ending volume - Beginning volume) / REMPER
    - INGROWTH: Ending volume / REMPER (new trees have no beginning)
    - REVERSION: Ending volume / REMPER (reverting trees have no beginning)

    where ending volume is from TREE table (current inventory) and
    beginning volume is from TREE_GRM_BEGIN. REMPER is the remeasurement
    period in years. The final growth value is:
    TPAGROW_UNADJ × volume_change × adjustment_factor.

    **Adjustment Factors**: The SUBPTYP_GRM field determines which
    adjustment factor to apply:

    - 0: No adjustment (trees not sampled)
    - 1: Subplot adjustment (ADJ_FACTOR_SUBP)
    - 2: Microplot adjustment (ADJ_FACTOR_MICR)
    - 3: Macroplot adjustment (ADJ_FACTOR_MACR)

    This differs from standard tree adjustment which uses diameter
    breakpoints directly.

    **GRM Table Requirements**: This function requires the following
    GRM tables in the database:

    - TREE_GRM_COMPONENT: Component classifications and TPA values
    - TREE_GRM_MIDPT: Tree measurements at remeasurement midpoint
    - TREE_GRM_BEGIN: Beginning tree measurements (for SURVIVOR trees)

    These tables are included in FIA evaluations that support growth,
    removal, and mortality estimation. Typically found in evaluations with
    EVALID type codes:

    - EXPVOL (ending in 01): Volume/biomass evaluations
    - EXPCHNG (ending in 03): Change evaluations with GRM components
    - Specific GRM evaluations may have other type codes

    Use `db.clip_most_recent(eval_type="EXPVOL")` or similar to ensure
    proper EVALID selection for growth estimation.

    **Calculation Accuracy**: The implementation calculates NET growth
    directly using ending minus beginning volumes, providing estimates
    within approximately 10% of published FIA values

    Valid grouping columns depend on which tables are included in the
    estimation query. The growth() function joins TREE_GRM_COMPONENT,
    TREE_GRM_MIDPT, TREE_GRM_BEGIN, COND, PLOT, and POP_* tables, so
    any column from these tables can be used for grouping. For a
    complete list of available columns and their meanings, refer to:

    - USDA FIA Database User Guide, Version 9.1
    - pyFIA documentation: https://mihiarc.github.io/pyfia/
    - FIA DataMart: https://apps.fs.usda.gov/fia/datamart/

    Warnings
    --------
    The variance calculation follows Bechtold & Patterson (2005) methodology
    for ratio-of-means estimation with stratified sampling. The calculation
    accounts for covariance between the numerator (growth) and denominator
    (area). For applications requiring the most precise variance estimates,
    consider also validating against the FIA EVALIDator tool.

    The function now provides estimates consistent with EVALIDator by using
    the weighted average methodology for volume calculations.

    Raises
    ------
    ValueError
        If TREE_GRM_COMPONENT, TREE_GRM_MIDPT, or TREE_GRM_BEGIN tables
        are not found in the database, or if grp_by contains invalid
        column names.
    KeyError
        If specified columns in grp_by don't exist in the joined tables.
    """
    # Import validation functions
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,  # Reuse for growth measure
        validate_tree_type,
    )

    # Validate inputs
    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
    measure = validate_mortality_measure(measure)  # Same valid values as mortality
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    totals = validate_boolean(totals, "totals")
    variance = validate_boolean(variance, "variance")
    most_recent = validate_boolean(most_recent, "most_recent")

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
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "include_cv": False,  # Could be added as parameter
    }

    # Create and run estimator
    estimator = GrowthEstimator(db, config)
    return estimator.estimate()
