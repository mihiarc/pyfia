"""
Mortality estimation for FIA data using GRM tables.

Implements FIA's Growth-Removal-Mortality methodology for calculating
annual tree mortality using TREE_GRM_COMPONENT and TREE_GRM_MIDPT tables.
"""

from typing import List, Optional, Union

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
            "TREE_GRM_COMPONENT",
            "TREE_GRM_MIDPT",
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
        # Default to timber for mortality (matches EVALIDator approach)
        land_type = self.config.get("land_type", "timber").upper()
        tree_type = self.config.get("tree_type", "gs").upper()

        # Map tree_type to FIA convention
        if tree_type in ["LIVE", "AL"]:
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
                f"{prefix}",  # Component type (MORTALITY1, MORTALITY2, etc.)
                f"SUBP_TPAMORT_UNADJ_{tree_type}_{land_type}",  # Mortality TPA
                f"SUBP_SUBPTYP_GRM_{tree_type}_{land_type}",  # Adjustment type
            ]
        )

        # Store column names for later use
        self._component_col = f"SUBP_COMPONENT_{tree_type}_{land_type}"
        self._tpamort_col = f"SUBP_TPAMORT_UNADJ_{tree_type}_{land_type}"
        self._subptyp_col = f"SUBP_SUBPTYP_GRM_{tree_type}_{land_type}"

        return cols

    def get_cond_columns(self) -> List[str]:
        """Required condition columns."""
        return [
            "PLT_CN",
            "CONDID",
            "COND_STATUS_CD",
            "CONDPROP_UNADJ",
            "OWNGRPCD",
            "FORTYPCD",
            "SITECLCD",
            "RESERVCD",
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

        # Select and rename columns for cleaner processing
        grm_component = grm_component.select(
            [
                pl.col("TRE_CN"),
                pl.col("PLT_CN"),
                pl.col("DIA_BEGIN"),
                pl.col("DIA_MIDPT"),
                pl.col("DIA_END"),
                pl.col(self._component_col).alias("COMPONENT"),
                pl.col(self._tpamort_col).alias("TPAMORT_UNADJ"),
                pl.col(self._subptyp_col).alias("SUBPTYP_GRM"),
            ]
        )

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
        elif measure == "sawlog":
            # For sawlog measure, use VOLCSNET (sawlog net volume)
            midpt_cols = ["TRE_CN", "VOLCSNET", "DIA", "SPCD", "STATUSCD"]
        elif measure == "biomass":
            midpt_cols = [
                "TRE_CN",
                "DRYBIO_BOLE",
                "DRYBIO_BRANCH",
                "DIA",
                "SPCD",
                "STATUSCD",
            ]
        elif measure in ["tpa", "count", "basal_area"]:
            midpt_cols = ["TRE_CN", "DIA", "SPCD", "STATUSCD"]
        else:  # Default case - include both volume fields for safety
            midpt_cols = ["TRE_CN", "VOLCFNET", "VOLCSNET", "DIA", "SPCD", "STATUSCD"]

        grm_midpt = grm_midpt.select(midpt_cols)

        # Join GRM tables
        data = grm_component.join(grm_midpt, on="TRE_CN", how="inner")

        # Apply EVALID filtering if set (similar to base class)
        if hasattr(self.db, "evalid") and self.db.evalid:
            # Load POP_PLOT_STRATUM_ASSGN to get plots for the EVALID
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")

            ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            if not isinstance(ppsa, pl.LazyFrame):
                ppsa = ppsa.lazy()

            # Filter to get PLT_CNs for the specified EVALID(s)
            valid_plots = (
                ppsa.filter(
                    pl.col("EVALID").is_in(self.db.evalid)
                    if isinstance(self.db.evalid, list)
                    else pl.col("EVALID") == self.db.evalid
                )
                .select("PLT_CN")
                .unique()
            )

            # Filter data to only include these plots
            data = data.join(
                valid_plots,
                on="PLT_CN",
                how="inner",  # This filters to only plots in the EVALID
            )

        # Load and join COND table
        # Note: TREE_GRM tables don't have CONDID, so we need to aggregate COND to plot level
        if "COND" not in self.db.tables:
            self.db.load_table("COND")

        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        # Aggregate COND to plot level since GRM tables don't have CONDID
        # For mortality, we'll use plot-level aggregates
        cond_agg = cond.group_by("PLT_CN").agg(
            [
                pl.col("COND_STATUS_CD").first().alias("COND_STATUS_CD"),
                pl.col("CONDPROP_UNADJ")
                .sum()
                .alias("CONDPROP_UNADJ"),  # Sum of condition proportions
                pl.col("OWNGRPCD")
                .first()
                .alias("OWNGRPCD"),  # Use first/dominant condition
                pl.col("FORTYPCD").first().alias("FORTYPCD"),
                pl.col("SITECLCD").first().alias("SITECLCD"),
                pl.col("RESERVCD").first().alias("RESERVCD"),
                pl.lit(1).alias("CONDID"),  # Dummy CONDID since we're aggregating
            ]
        )

        # Join with aggregated conditions
        data = data.join(
            cond_agg,
            on="PLT_CN",
            how="left",  # Use left join in case some plots don't have COND records
        )

        # Add PLOT data for additional info if needed
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")

        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        # Select minimal plot columns
        plot = plot.select(["CN", "STATECD", "INVYR", "MACRO_BREAKPOINT_DIA"])

        data = data.join(plot, left_on="PLT_CN", right_on="CN", how="left")

        return data

    def apply_filters(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply mortality-specific filters."""
        # For mortality, we need to handle filtering differently since GRM tables
        # have different column names (TPAMORT_UNADJ instead of TPA_UNADJ)

        # Collect to DataFrame for filtering
        data_df = data.collect()

        # Apply area domain filter if specified
        if self.config.get("area_domain"):
            from pyfia.filtering.area.filters import apply_area_filters

            data_df = apply_area_filters(
                data_df, area_domain=self.config["area_domain"]
            )

        # Apply tree domain filter if specified, using the domain parser directly
        # to avoid the TPA_UNADJ check in apply_tree_filters
        if self.config.get("tree_domain"):
            from pyfia.filtering.core.parser import DomainExpressionParser

            data_df = DomainExpressionParser.apply_to_dataframe(
                data_df, self.config["tree_domain"], "tree"
            )

        # Filter to mortality components only
        data_df = data_df.filter(pl.col("COMPONENT").str.starts_with("MORTALITY"))

        # Filter to records with positive mortality
        data_df = data_df.filter(
            (pl.col("TPAMORT_UNADJ").is_not_null()) & (pl.col("TPAMORT_UNADJ") > 0)
        )

        # Apply tree type filter if specified
        tree_type = self.config.get("tree_type", "gs")
        if tree_type == "gs":
            # Growing stock trees (typically >= 5 inches DBH with merchantable volume)
            data_df = data_df.filter(pl.col("DIA_MIDPT") >= 5.0)
            if "VOLCFNET" in data_df.columns:
                data_df = data_df.filter(pl.col("VOLCFNET") > 0)
        elif tree_type == "sawtimber":
            # Sawtimber trees: softwood >= 9.0", hardwood >= 11.0" DBH with sawlog volume
            # Standard FIA sawtimber definition
            data_df = data_df.filter(
                ((pl.col("SPCD") < 300) & (pl.col("DIA_MIDPT") >= 9.0))
                | ((pl.col("SPCD") >= 300) & (pl.col("DIA_MIDPT") >= 11.0))
            )
            # Also require sawlog volume > 0 if available
            if "VOLCSNET" in data_df.columns:
                data_df = data_df.filter(pl.col("VOLCSNET") > 0)

        # Convert back to LazyFrame
        return data_df.lazy()

    def calculate_values(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate mortality values per acre.

        TPAMORT_UNADJ is already annualized, so no remeasurement period adjustment needed.
        """
        measure = self.config.get("measure", "volume")

        if measure == "volume":
            # Mortality volume per acre = TPAMORT * Volume
            data = data.with_columns(
                [
                    (
                        pl.col("TPAMORT_UNADJ").cast(pl.Float64)
                        * pl.col("VOLCFNET").cast(pl.Float64)
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "sawlog":
            # Sawlog volume mortality - use VOLCSNET (sawlog net volume)
            data = data.with_columns(
                [
                    (
                        pl.col("TPAMORT_UNADJ").cast(pl.Float64)
                        * pl.col("VOLCSNET").cast(pl.Float64)
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "biomass":
            # Mortality biomass per acre (total biomass in tons)
            # DRYBIO fields are in pounds, convert to tons
            data = data.with_columns(
                [
                    (
                        pl.col("TPAMORT_UNADJ").cast(pl.Float64)
                        * (pl.col("DRYBIO_BOLE") + pl.col("DRYBIO_BRANCH")).cast(
                            pl.Float64
                        )
                        / 2000.0
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "basal_area":
            # Mortality basal area per acre
            # Basal area = π * (DIA/2)^2 / 144 = DIA^2 * 0.005454154
            data = data.with_columns(
                [
                    (
                        pl.col("TPAMORT_UNADJ").cast(pl.Float64)
                        * (pl.col("DIA").cast(pl.Float64) ** 2 * 0.005454154)
                    ).alias("MORT_VALUE")
                ]
            )
        elif measure == "tpa":
            # Mortality trees per acre (same as count)
            data = data.with_columns(
                [pl.col("TPAMORT_UNADJ").cast(pl.Float64).alias("MORT_VALUE")]
            )
        else:  # Default to tpa/count
            # Mortality trees per acre
            data = data.with_columns(
                [pl.col("TPAMORT_UNADJ").cast(pl.Float64).alias("MORT_VALUE")]
            )

        # TPAMORT_UNADJ is already annual, so no division by remeasurement period
        data = data.with_columns([pl.col("MORT_VALUE").alias("MORT_ANNUAL")])

        return data

    def aggregate_results(self, data: pl.LazyFrame) -> pl.DataFrame:
        """Aggregate mortality with two-stage aggregation for correct per-acre estimates.

        Uses the shared _apply_two_stage_aggregation method with GRM-specific adjustment
        logic applied before calling the shared method.
        """
        # Get stratification data
        strat_data = self._get_stratification_data()

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply GRM-specific adjustment factors based on SUBPTYP_GRM
        # This is done BEFORE calling the shared aggregation method
        # SUBPTYP_GRM: 0=None, 1=SUBP, 2=MICR, 3=MACR
        data_with_strat = data_with_strat.with_columns(
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

        # Apply adjustment to mortality values
        data_with_strat = data_with_strat.with_columns(
            [(pl.col("MORT_ANNUAL") * pl.col("ADJ_FACTOR")).alias("MORT_ADJ")]
        )

        # Setup grouping (includes by_species logic)
        group_cols = self._setup_grouping()
        if self.config.get("by_species", False) and "SPCD" not in group_cols:
            group_cols.append("SPCD")

        # Use shared two-stage aggregation method
        metric_mappings = {"MORT_ADJ": "CONDITION_MORTALITY"}

        results = self._apply_two_stage_aggregation(
            data_with_strat=data_with_strat,
            metric_mappings=metric_mappings,
            group_cols=group_cols,
            use_grm_adjustment=True,  # Indicates this is a GRM-based estimator
        )

        # The shared method returns MORTALITY_ACRE and MORTALITY_TOTAL
        # Rename to match mortality-specific naming convention
        rename_map = {"MORTALITY_ACRE": "MORT_ACRE", "MORTALITY_TOTAL": "MORT_TOTAL"}

        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        # Rename N_TREES to N_DEAD_TREES for clarity in mortality context
        if "N_TREES" in results.columns:
            results = results.rename({"N_TREES": "N_DEAD_TREES"})

        # Calculate mortality rate if requested
        if self.config.get("as_rate", False):
            # This would require live tree data for proper rate calculation
            # For now, add as a percentage of mortality per acre
            results = results.with_columns([pl.col("MORT_ACRE").alias("MORT_RATE")])

        return results

    def calculate_variance(self, results: pl.DataFrame) -> pl.DataFrame:
        """Calculate variance for mortality estimates using stratified sampling formulas.

        Uses the FIA post-stratified variance estimation following Bechtold & Patterson (2005).
        For domain (subset) estimation in stratified sampling:
        V(Ŷ_D) = Σ_h [N_h² × (1 - n_h/N_h) × s²_yh / n_h]

        Where:
        - N_h = total plots in stratum (population)
        - n_h = sampled plots in stratum
        - s²_yh = sample variance of mortality values in stratum
        - (1 - n_h/N_h) = finite population correction (FPC)

        For FIA, we typically assume n_h/N_h is small, so FPC ≈ 1.
        The expansion factor EXPNS = N_h × acres_per_plot / n_h
        """

        # Get stratification data for variance calculation
        strat_data = self._get_stratification_data()

        # Load the raw mortality data for variance calculation
        data = self.load_data()
        if data is None:
            # If no data, return results with zero variance
            results = results.with_columns(
                [pl.lit(0.0).alias("MORT_ACRE_SE"), pl.lit(0.0).alias("MORT_TOTAL_SE")]
            )
            return results

        # Apply filters to get the same subset used in estimation
        data = self.apply_filters(data)
        data = self.calculate_values(data)

        # Join with stratification
        data_with_strat = data.join(strat_data, on="PLT_CN", how="inner")

        # Apply adjustment factors based on SUBPTYP_GRM
        data_with_strat = data_with_strat.with_columns(
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

        # Calculate plot-level mortality values (including zeros for plots without mortality)
        # First, get all plots in the evaluation
        all_plots = strat_data.select("PLT_CN", "STRATUM_CN", "EXPNS").unique()

        # Aggregate mortality to plot level
        plot_mortality = data_with_strat.group_by(
            ["PLT_CN", "STRATUM_CN", "EXPNS"]
        ).agg(
            [
                (pl.col("MORT_ANNUAL") * pl.col("ADJ_FACTOR"))
                .sum()
                .alias("plot_mort_value")
            ]
        )

        # Join to include all plots (with zeros for non-mortality plots)
        all_plots_mort = all_plots.join(
            plot_mortality.select(["PLT_CN", "plot_mort_value"]),
            on="PLT_CN",
            how="left",
        ).with_columns([pl.col("plot_mort_value").fill_null(0.0)])

        # Calculate stratum-level statistics
        strat_stats = all_plots_mort.group_by("STRATUM_CN").agg(
            [
                pl.count("PLT_CN").alias("n_h"),  # Number of plots in stratum
                pl.mean("plot_mort_value").alias("ybar_h"),  # Mean mortality per plot
                pl.var("plot_mort_value", ddof=1).alias("s2_yh"),  # Sample variance
                pl.first("EXPNS").alias(
                    "w_h"
                ),  # Expansion factor (same for all plots in stratum)
            ]
        )

        # Handle single-plot strata (variance = 0)
        strat_stats = strat_stats.with_columns(
            [
                pl.when(pl.col("s2_yh").is_null() | (pl.col("n_h") == 1))
                .then(0.0)
                .otherwise(pl.col("s2_yh"))
                .alias("s2_yh")
            ]
        )

        # Calculate variance components for total estimation
        # For domain totals in stratified sampling:
        # V(total) = Σ_h [w_h² × s²_yh × n_h]
        # We multiply by n_h because we're estimating a total, not a mean
        variance_components = strat_stats.with_columns(
            [
                (
                    pl.col("w_h").cast(pl.Float64) ** 2
                    * pl.col("s2_yh")
                    * pl.col("n_h")
                ).alias("v_h")
            ]
        )

        # Sum variance components
        total_variance = variance_components.collect()["v_h"].sum()
        if total_variance is None or total_variance < 0:
            total_variance = 0.0

        # Calculate standard errors
        se_total = total_variance**0.5

        # For per-acre estimate, we need to divide by total area
        # Get total area (sum of all EXPNS × n_h for all strata)
        total_area_df = strat_stats.select(
            [(pl.col("w_h") * pl.col("n_h")).alias("stratum_area")]
        ).collect()
        total_area = total_area_df["stratum_area"].sum()

        if total_area > 0:
            se_acre = se_total / total_area
        else:
            se_acre = 0.0

        # Update results with calculated variance
        results = results.with_columns(
            [
                pl.lit(se_acre).alias("MORT_ACRE_SE"),
                pl.lit(se_total).alias("MORT_TOTAL_SE"),
            ]
        )

        if "MORT_RATE" in results.columns:
            # For rates, use a higher CV (more uncertainty in rates)
            results = results.with_columns(
                [(pl.col("MORT_RATE") * 0.20).alias("MORT_RATE_SE")]
            )

        # Add CV if requested
        if self.config.get("include_cv", False):
            results = results.with_columns(
                [
                    pl.when(pl.col("MORT_ACRE") > 0)
                    .then(pl.col("MORT_ACRE_SE") / pl.col("MORT_ACRE") * 100)
                    .otherwise(None)
                    .alias("MORT_ACRE_CV"),
                    pl.when(pl.col("MORT_TOTAL") > 0)
                    .then(pl.col("MORT_TOTAL_SE") / pl.col("MORT_TOTAL") * 100)
                    .otherwise(None)
                    .alias("MORT_TOTAL_CV"),
                ]
            )

        return results

    def format_output(self, results: pl.DataFrame) -> pl.DataFrame:
        """Format mortality estimation output."""
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
            estimation_type="mortality",
            include_se=True,
            include_cv=self.config.get("include_cv", False),
        )

        return results


def mortality(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "timber",  # Default to timber to match EVALIDator
    tree_type: str = "gs",
    measure: str = "volume",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    as_rate: bool = False,
    totals: bool = True,
    variance: bool = False,
    most_recent: bool = False,
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
    land_type : {'forest', 'timber'}, default 'timber'
        Land type to include in estimation:

        - 'forest': All forestland
        - 'timber': Productive timberland only (unreserved, productive)
    tree_type : {'gs', 'al', 'sawtimber', 'live'}, default 'gs'
        Tree type to include:

        - 'gs': Growing stock trees (live, merchantable)
        - 'al' or 'live': All live trees
        - 'sawtimber': Sawtimber trees (softwood ≥9.0", hardwood ≥11.0" DBH)
    measure : {'volume', 'sawlog', 'biomass', 'tpa', 'count', 'basal_area'}, default 'volume'
        What to measure in the mortality estimation:

        - 'volume': Net cubic foot volume (VOLCFNET)
        - 'sawlog': Sawlog net cubic foot volume (VOLCSNET)
        - 'biomass': Total aboveground biomass in tons
        - 'tpa': Trees per acre (same as 'count')
        - 'count': Number of trees per acre
        - 'basal_area': Basal area in square feet per acre
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
    # Import validation functions
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_grp_by,
        validate_land_type,
        validate_mortality_measure,
        validate_tree_type,
    )

    # Validate inputs
    land_type = validate_land_type(land_type)
    tree_type = validate_tree_type(tree_type)
    measure = validate_mortality_measure(measure)
    grp_by = validate_grp_by(grp_by)
    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    by_species = validate_boolean(by_species, "by_species")
    by_size_class = validate_boolean(by_size_class, "by_size_class")
    as_rate = validate_boolean(as_rate, "as_rate")
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
        "as_rate": as_rate,
        "totals": totals,
        "variance": variance,
        "most_recent": most_recent,
        "include_cv": False,  # Could be added as parameter
    }

    # Create and run estimator
    estimator = MortalityEstimator(db, config)
    return estimator.estimate()
