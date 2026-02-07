"""
Remeasurement panel creation for FIA data.

Creates t1/t2 (time 1/time 2) linked panel datasets from FIA remeasurement data.
Supports both condition-level and tree-level panels for harvest analysis,
growth tracking, and change detection.

Tree-level panels use GRM (Growth-Removal-Mortality) tables for authoritative
tree fate classification. This provides consistent definitions aligned with
FIA's official GRM estimation methodology.

References
----------
Bechtold & Patterson (2005), Chapter 4: Change Estimation
FIA Database User Guide, PLOT and TREE table documentation
FIA GRM methodology documentation
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from ...core import FIA
from ...validation import (
    validate_boolean,
    validate_domain_expression,
    validate_land_type,
)
from ..aggregation import apply_two_stage_aggregation
from ..grm import (
    apply_grm_adjustment,
    resolve_grm_columns,
)


class PanelBuilder:
    """
    Builder for creating t1/t2 remeasurement panels from FIA data.

    Creates linked datasets where each row represents a measurement pair:
    - t1 (time 1): Previous measurement
    - t2 (time 2): Current measurement

    Supports condition-level panels for area/harvest analysis and tree-level
    panels for individual tree tracking.

    Parameters (via config)
    -----------------------
    level : {'condition', 'tree'}
        Level of panel to create
    columns : list of str, optional
        Additional columns to include beyond defaults
    land_type : {'forest', 'timber', 'all'}
        Land classification filter
    tree_type : {'all', 'live', 'gs'}
        Tree type filter (tree-level only)
    tree_domain : str, optional
        SQL-like filter for trees
    area_domain : str, optional
        SQL-like filter for conditions
    expand_chains : bool
        If True, expand multi-remeasurement chains into all pairs
    min_remper : float
        Minimum remeasurement period (years)
    max_remper : float, optional
        Maximum remeasurement period (years)
    harvest_only : bool
        If True, return only records where harvest was detected
    """

    # Default columns for plot-level data (used in both condition and tree panels)
    DEFAULT_PLOT_COLUMNS = [
        "LAT",
        "LON",
        "ELEV",
    ]

    # Default columns for condition-level panels
    DEFAULT_COND_COLUMNS = [
        # Identification & Status
        "COND_STATUS_CD",
        # Ownership
        "OWNCD",  # Detailed ownership (11-46)
        "OWNGRPCD",  # Ownership group (10-40)
        "RESERVCD",
        # Stand characteristics
        "FORTYPCD",
        "STDAGE",
        "STDSZCD",
        "SICOND",  # Site index
        "SITECLCD",  # Site productivity class
        "BALIVE",  # Basal area of live trees
        # Topography
        "SLOPE",
        "ASPECT",
        "PHYSCLCD",  # Physiographic class
        # Treatment codes (for harvest detection)
        "TRTCD1",
        "TRTCD2",
        "TRTCD3",
        "TRTYR1",
        # Disturbance codes
        "DSTRBCD1",
        "DSTRBCD2",
        "DSTRBCD3",
        "DSTRBYR1",
    ]

    # Default columns for tree-level panels (from GRM tables)
    # Note: GRM-specific columns like DIA_BEGIN, DIA_MIDPT come from TREE_GRM_COMPONENT
    DEFAULT_TREE_COLUMNS = [
        # Species
        "SPCD",
        "SPGRPCD",
        # Size (from GRM - beginning/midpoint/end measurements)
        "DIA_BEGIN",
        "DIA_MIDPT",
        "DIA_END",
        # Volume estimates (from TREE_GRM_MIDPT)
        "VOLCFNET",  # Net cubic foot volume
        # Biomass (from TREE_GRM_MIDPT)
        "DRYBIO_AG",
        "DRYBIO_BOLE",
        # Expansion factors (computed from GRM)
        "TPA_UNADJ",
        # Status (from TREE_GRM_MIDPT)
        "STATUSCD",
    ]

    # Treatment codes indicating harvest (used for condition-level panels only)
    HARVEST_TRTCD = {10, 20}  # 10=Cutting, 20=Site preparation

    # GRM component to tree fate mapping
    # These are the authoritative classifications from TREE_GRM_COMPONENT
    GRM_FATE_MAPPING = {
        "SURVIVOR": "survivor",
        "MORTALITY1": "mortality",
        "MORTALITY2": "mortality",
        "CUT1": "cut",
        "CUT2": "cut",
        "DIVERSION1": "diversion",
        "DIVERSION2": "diversion",
        "INGROWTH": "ingrowth",
    }

    def __init__(self, db: FIA, config: dict):
        """Initialize panel builder with database and configuration."""
        self.db = db
        self.config = config
        self.level = config.get("level", "condition")

    def build(self) -> pl.DataFrame:
        """
        Build the remeasurement panel.

        Returns
        -------
        pl.DataFrame
            Panel dataset with t1/t2 measurement pairs
        """
        if self.level == "condition":
            return self._build_condition_panel()
        elif self.level == "tree":
            return self._build_tree_panel()
        else:
            raise ValueError(
                f"Invalid level: {self.level}. Must be 'condition' or 'tree'"
            )

    def _build_condition_panel(self) -> pl.DataFrame:
        """Build condition-level remeasurement panel."""
        # Load required tables
        self._ensure_tables_loaded(["PLOT", "COND"])

        # For chain expansion, load ALL plots (not just current EVALID)
        # This captures all measurement pairs in the database
        expand_chains = self.config.get("expand_chains", True)

        if expand_chains:
            # Load full PLOT table without EVALID filter
            # Include location data (LAT, LON, ELEV) for spatial analysis
            plot_cols_to_load = [
                "CN",
                "STATECD",
                "COUNTYCD",
                "INVYR",
                "PREV_PLT_CN",
                "REMPER",
                "CYCLE",
            ] + self.DEFAULT_PLOT_COLUMNS
            plot = self.db._reader.read_table(
                "PLOT",
                columns=plot_cols_to_load,
                lazy=True,
            )
        else:
            # Use EVALID-filtered plots (most recent evaluation only)
            plot = self.db.tables["PLOT"]
            if not isinstance(plot, pl.LazyFrame):
                plot = plot.lazy()

        # Load COND table (also full table for chain expansion)
        if expand_chains:
            cond_cols = self._get_condition_columns()
            cond = self.db._reader.read_table("COND", columns=cond_cols, lazy=True)
        else:
            cond = self.db.tables["COND"]
            if not isinstance(cond, pl.LazyFrame):
                cond = cond.lazy()

        # Get plot columns for current measurement (t2)
        plot_cols = [
            "CN",
            "STATECD",
            "COUNTYCD",
            "INVYR",
            "PREV_PLT_CN",
            "REMPER",
            "CYCLE",
        ] + self.DEFAULT_PLOT_COLUMNS
        plot_schema = plot.collect_schema().names()
        plot_cols = [c for c in plot_cols if c in plot_schema]

        # Filter to plots with previous measurements
        plot_t2 = plot.select(plot_cols).filter(
            pl.col("PREV_PLT_CN").is_not_null() & (pl.col("REMPER") > 0)
        )

        # Apply REMPER filters
        min_remper = self.config.get("min_remper", 0)
        max_remper = self.config.get("max_remper")

        if min_remper > 0:
            plot_t2 = plot_t2.filter(pl.col("REMPER") >= min_remper)
        if max_remper is not None:
            plot_t2 = plot_t2.filter(pl.col("REMPER") <= max_remper)

        # Apply min_invyr filter (default 2000 for post-annual inventory methodology)
        min_invyr = self.config.get("min_invyr", 2000)
        if min_invyr is not None and min_invyr > 0:
            plot_t2 = plot_t2.filter(pl.col("INVYR") >= min_invyr)

        # Get condition columns
        cond_cols = self._get_condition_columns()
        cond_schema = cond.collect_schema().names()
        cond_cols = [c for c in cond_cols if c in cond_schema]

        # Ensure required columns are present
        required = ["CN", "PLT_CN", "CONDID"]
        for col in required:
            if col not in cond_cols:
                cond_cols.append(col)

        # Get current conditions (t2)
        cond_t2 = cond.select(cond_cols)

        # Join plot and condition for t2
        data = plot_t2.join(
            cond_t2,
            left_on="CN",
            right_on="PLT_CN",
            how="inner",
        )

        # Rename CN to PLT_CN for clarity
        data = data.rename({"CN": "PLT_CN"})

        # Rename t2 columns with prefix
        t2_rename = {}
        for col in self.DEFAULT_COND_COLUMNS:
            if col in data.collect_schema().names():
                t2_rename[col] = f"t2_{col}"
        data = data.rename(t2_rename)

        # Load previous conditions (t1) - need full table without EVALID filter
        cond_prev = self.db._reader.read_table("COND", columns=cond_cols, lazy=True)

        # Rename t1 columns with prefix
        t1_rename = {"PLT_CN": "t1_PLT_CN", "CN": "t1_COND_CN", "CONDID": "t1_CONDID"}
        for col in self.DEFAULT_COND_COLUMNS:
            if col in cond_prev.collect_schema().names():
                t1_rename[col] = f"t1_{col}"
        cond_prev = cond_prev.rename(t1_rename)

        # Join to get t1 data
        # Need PREVCOND from current COND to link properly
        if "PREVCOND" in cond.collect_schema().names():
            # Get PREVCOND mapping - select only PREVCOND to avoid duplicate columns
            prevcond_map = cond.select(["PLT_CN", "CONDID", "PREVCOND"])
            data = data.join(
                prevcond_map,
                left_on=["PLT_CN", "CONDID"],  # CN was renamed to PLT_CN at line 233
                right_on=["PLT_CN", "CONDID"],
                how="left",
            )
            # Join previous condition
            data = data.join(
                cond_prev,
                left_on=["PREV_PLT_CN", "PREVCOND"],
                right_on=["t1_PLT_CN", "t1_CONDID"],
                how="left",
            )
        else:
            # Fall back to same CONDID assumption
            data = data.join(
                cond_prev,
                left_on=["PREV_PLT_CN", "CONDID"],
                right_on=["t1_PLT_CN", "t1_CONDID"],
                how="left",
            )

        # Apply land type filter
        data = self._apply_land_type_filter(data)

        # Apply area domain filter
        data = self._apply_area_domain_filter(data)

        # Detect harvest
        data = self._detect_harvest(data)

        # Filter to harvest only if requested
        if self.config.get("harvest_only", False):
            data = data.filter(pl.col("HARVEST") == 1)

        # Note: Chain expansion is handled earlier by loading ALL plots with
        # remeasurement data (when expand_chains=True), not just current EVALID.
        # This ensures all measurement pairs (t1,t2), (t2,t3), etc. are captured.

        # Clean up and format output
        result = data.collect()
        result = self._format_condition_output(result)

        return result

    def _build_tree_panel(self) -> pl.DataFrame:
        """
        Build tree-level remeasurement panel using GRM tables.

        Uses TREE_GRM_COMPONENT for authoritative tree fate classification,
        providing consistent definitions aligned with FIA's official
        GRM estimation methodology.

        The GRM component classification includes:
        - SURVIVOR: Tree alive at both t1 and t2
        - MORTALITY1/2: Tree died naturally
        - CUT1/2: Tree removed by harvest
        - DIVERSION1/2: Tree removed due to land use change
        - INGROWTH: New tree crossing size threshold
        """
        # Determine if we need stratification data for expansion
        expand = self.config.get("expand", False)

        # Load required GRM tables
        required_tables = ["PLOT", "TREE_GRM_COMPONENT", "TREE_GRM_MIDPT", "COND"]
        if expand:
            required_tables.extend(["POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"])
        self._ensure_tables_loaded(required_tables)

        # Resolve GRM column names based on tree_type and land_type
        tree_type = self.config.get("tree_type", "gs")
        land_type = self.config.get("land_type", "forest")

        # Default to "gs" for GRM (more restrictive, matches removals behavior)
        if tree_type == "all":
            tree_type = "gs"
        elif tree_type == "live":
            tree_type = "al"  # All live = AL in GRM terminology

        grm_cols = resolve_grm_columns("removals", tree_type, land_type)

        # Load TREE_GRM_COMPONENT
        grm_component = self.db.tables["TREE_GRM_COMPONENT"]
        if not isinstance(grm_component, pl.LazyFrame):
            grm_component = grm_component.lazy()

        # Select columns from GRM_COMPONENT
        component_cols = [
            "TRE_CN",
            "PLT_CN",
            "DIA_BEGIN",
            "DIA_MIDPT",
            "DIA_END",
        ]

        # Add the resolved component and TPA columns
        grm_schema = grm_component.collect_schema().names()
        if grm_cols.component in grm_schema:
            component_cols.append(grm_cols.component)
        if grm_cols.tpa in grm_schema:
            component_cols.append(grm_cols.tpa)
        if grm_cols.subptyp in grm_schema:
            component_cols.append(grm_cols.subptyp)

        # Filter to valid columns
        component_cols = [c for c in component_cols if c in grm_schema]
        grm_data = grm_component.select(component_cols)

        # Rename component column to standard COMPONENT
        if grm_cols.component in grm_data.collect_schema().names():
            grm_data = grm_data.rename({grm_cols.component: "COMPONENT"})
        if grm_cols.tpa in grm_data.collect_schema().names():
            grm_data = grm_data.rename({grm_cols.tpa: "TPA_UNADJ"})
        if grm_cols.subptyp in grm_data.collect_schema().names():
            grm_data = grm_data.rename({grm_cols.subptyp: "SUBPTYP_GRM"})

        # Load TREE_GRM_MIDPT for additional tree attributes
        grm_midpt = self.db.tables["TREE_GRM_MIDPT"]
        if not isinstance(grm_midpt, pl.LazyFrame):
            grm_midpt = grm_midpt.lazy()

        midpt_cols = ["TRE_CN", "DIA", "SPCD", "STATUSCD", "VOLCFNET"]
        midpt_schema = grm_midpt.collect_schema().names()

        # Add biomass columns if available
        for col in ["DRYBIO_AG", "DRYBIO_BOLE", "SPGRPCD"]:
            if col in midpt_schema:
                midpt_cols.append(col)

        midpt_cols = [c for c in midpt_cols if c in midpt_schema]
        grm_midpt = grm_midpt.select(midpt_cols)

        # Join GRM_COMPONENT with GRM_MIDPT
        data = grm_data.join(
            grm_midpt,
            on="TRE_CN",
            how="left",
        )

        # Load PLOT for remeasurement metadata
        plot = self.db.tables["PLOT"]
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        plot_cols = [
            "CN",
            "STATECD",
            "COUNTYCD",
            "INVYR",
            "PREV_PLT_CN",
            "REMPER",
            "CYCLE",
        ] + self.DEFAULT_PLOT_COLUMNS
        plot_schema = plot.collect_schema().names()
        plot_cols = [c for c in plot_cols if c in plot_schema]

        plot = plot.select(plot_cols)

        # Apply REMPER filters
        min_remper = self.config.get("min_remper", 0)
        max_remper = self.config.get("max_remper")

        if min_remper > 0:
            plot = plot.filter(pl.col("REMPER") >= min_remper)
        if max_remper is not None:
            plot = plot.filter(pl.col("REMPER") <= max_remper)

        # Apply min_invyr filter
        min_invyr = self.config.get("min_invyr", 2000)
        if min_invyr is not None and min_invyr > 0:
            plot = plot.filter(pl.col("INVYR") >= min_invyr)

        # Join with PLOT data
        data = data.join(
            plot,
            left_on="PLT_CN",
            right_on="CN",
            how="inner",
        )

        # Join with stratification data if expansion is requested
        if expand:
            data = self._join_stratification_data(data)

        # Calculate tree fate from GRM component
        data = self._calculate_tree_fate(data)

        # Rename columns with t2_ prefix for consistency
        # (GRM MIDPT values are midpoint estimates between t1 and t2)
        data = data.rename(
            {
                "DIA": "t2_DIA",
                "STATUSCD": "t2_STATUSCD",
                "VOLCFNET": "t2_VOLCFNET",
            }
        )

        # Rename biomass columns if present
        schema = data.collect_schema().names()
        rename_map = {}
        for col in ["DRYBIO_AG", "DRYBIO_BOLE"]:
            if col in schema:
                rename_map[col] = f"t2_{col}"
        if rename_map:
            data = data.rename(rename_map)

        # Create t1_ columns from DIA_BEGIN
        if "DIA_BEGIN" in data.collect_schema().names():
            data = data.with_columns(
                [
                    pl.col("DIA_BEGIN").alias("t1_DIA"),
                ]
            )

        # Apply tree domain filter
        data = self._apply_tree_domain_filter(data)

        # Filter to harvest only if requested
        # Include both cut AND diversion for comprehensive removal analysis
        if self.config.get("harvest_only", False):
            data = data.filter(pl.col("TREE_FATE").is_in(["cut", "diversion"]))

        # Apply expansion if requested
        if expand:
            return self._apply_expansion(data)

        # Clean up and format output (non-expanded)
        result = data.collect()
        result = self._format_tree_output(result)

        return result

    def _ensure_tables_loaded(self, tables: list[str]) -> None:
        """Ensure required tables are loaded."""
        for table in tables:
            if table not in self.db.tables:
                self.db.load_table(table)

    def _join_stratification_data(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Join stratification data for expansion factors.

        Loads and joins:
        - POP_STRATUM: EXPNS, ADJ_FACTOR_SUBP/MICR/MACR
        - POP_PLOT_STRATUM_ASSGN: Maps plots to strata
        - COND: CONDPROP_UNADJ for ratio-of-means denominator
        """
        # Load POP_PLOT_STRATUM_ASSGN
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
        if not isinstance(ppsa, pl.LazyFrame):
            ppsa = ppsa.lazy()

        ppsa = ppsa.select(["PLT_CN", "STRATUM_CN"])

        # Load POP_STRATUM
        pop_stratum = self.db.tables["POP_STRATUM"]
        if not isinstance(pop_stratum, pl.LazyFrame):
            pop_stratum = pop_stratum.lazy()

        strat_cols = [
            "CN",
            "EXPNS",
            "ADJ_FACTOR_SUBP",
            "ADJ_FACTOR_MICR",
            "ADJ_FACTOR_MACR",
        ]
        strat_schema = pop_stratum.collect_schema().names()
        strat_cols = [c for c in strat_cols if c in strat_schema]
        pop_stratum = pop_stratum.select(strat_cols)

        # Join PPSA with POP_STRATUM
        strat_data = ppsa.join(
            pop_stratum,
            left_on="STRATUM_CN",
            right_on="CN",
            how="inner",
        )

        # Join with main data
        data = data.join(strat_data, on="PLT_CN", how="inner")

        # Load COND for CONDPROP_UNADJ (aggregate to plot level)
        cond = self.db.tables["COND"]
        if not isinstance(cond, pl.LazyFrame):
            cond = cond.lazy()

        # Aggregate CONDPROP to plot level (sum of all condition proportions)
        cond_agg = cond.group_by("PLT_CN").agg(
            [
                pl.col("CONDPROP_UNADJ").sum().alias("CONDPROP_UNADJ"),
            ]
        )

        data = data.join(cond_agg, on="PLT_CN", how="left")

        # Fill null CONDPROP with 1.0 (assume full plot if missing)
        data = data.with_columns(
            [
                pl.col("CONDPROP_UNADJ").fill_null(1.0).alias("CONDPROP_UNADJ"),
            ]
        )

        return data

    def _apply_expansion(self, data: pl.LazyFrame) -> pl.DataFrame:
        """
        Apply expansion factors to produce per-acre estimates.

        Uses the same methodology as removals():
        1. Apply GRM adjustment based on SUBPTYP_GRM
        2. Calculate adjusted TPA: TPA_UNADJ × ADJ_FACTOR
        3. Apply two-stage aggregation with EXPNS

        Returns
        -------
        pl.DataFrame
            Expanded estimates with per-acre and total values
        """
        # Add dummy CONDID (GRM data is plot-level, not condition-level)
        # The aggregation functions expect CONDID for condition-level grouping
        data = data.with_columns([pl.lit(1).alias("CONDID")])

        # Apply GRM-specific adjustment factors
        data = apply_grm_adjustment(data)

        # Calculate adjusted TPA and volume
        measure = self.config.get("measure", "tpa")

        if measure == "volume":
            value_col: str | None = "t2_VOLCFNET"
            # Check if volume column exists
            schema = data.collect_schema().names()
            if value_col not in schema:
                value_col = "VOLCFNET" if "VOLCFNET" in schema else None
            if value_col is not None:
                data = data.with_columns(
                    [
                        (
                            pl.col("TPA_UNADJ").cast(pl.Float64)
                            * pl.col(value_col).cast(pl.Float64)
                            * pl.col("ADJ_FACTOR").cast(pl.Float64)
                        ).alias("VALUE_ADJ")
                    ]
                )
            else:
                # Fall back to TPA if no volume available
                data = data.with_columns(
                    [
                        (
                            pl.col("TPA_UNADJ").cast(pl.Float64)
                            * pl.col("ADJ_FACTOR").cast(pl.Float64)
                        ).alias("VALUE_ADJ")
                    ]
                )
        else:  # TPA
            data = data.with_columns(
                [
                    (
                        pl.col("TPA_UNADJ").cast(pl.Float64)
                        * pl.col("ADJ_FACTOR").cast(pl.Float64)
                    ).alias("VALUE_ADJ")
                ]
            )

        # Apply two-stage aggregation
        metric_mappings = {"VALUE_ADJ": "CONDITION_VALUE"}

        # Get grouping columns from config
        grp_by = self.config.get("grp_by", [])
        if isinstance(grp_by, str):
            grp_by = [grp_by]

        # Add TREE_FATE to grouping if requested
        if self.config.get("by_fate", False):
            if "TREE_FATE" not in grp_by:
                grp_by.append("TREE_FATE")

        results = apply_two_stage_aggregation(
            data_with_strat=data,
            metric_mappings=metric_mappings,
            group_cols=grp_by,
            use_grm_adjustment=True,
        )

        # Rename columns for clarity
        rename_map = {
            "VALUE_ACRE": "PANEL_ACRE",
            "VALUE_TOTAL": "PANEL_TOTAL",
        }
        for old, new in rename_map.items():
            if old in results.columns:
                results = results.rename({old: new})

        return results

    def _get_condition_columns(self) -> list[str]:
        """Get columns to include for condition panel."""
        cols = ["CN", "PLT_CN", "CONDID", "CONDPROP_UNADJ"]
        cols.extend(self.DEFAULT_COND_COLUMNS)

        # Add user-specified columns
        extra_cols = self.config.get("columns", [])
        if extra_cols:
            for col in extra_cols:
                if col not in cols:
                    cols.append(col)

        return cols

    def _get_tree_columns(self) -> list[str]:
        """Get columns to include for tree panel."""
        cols = ["CN", "PLT_CN", "CONDID", "TREE", "SUBP"]
        cols.extend(self.DEFAULT_TREE_COLUMNS)

        # Add user-specified columns
        extra_cols = self.config.get("columns", [])
        if extra_cols:
            for col in extra_cols:
                if col not in cols:
                    cols.append(col)

        return cols

    def _apply_land_type_filter(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply land type filter to data."""
        land_type = self.config.get("land_type", "forest")

        if land_type == "all":
            return data

        # Forest land: COND_STATUS_CD == 1
        if land_type == "forest":
            # Filter on current (t2) condition
            if "t2_COND_STATUS_CD" in data.collect_schema().names():
                data = data.filter(pl.col("t2_COND_STATUS_CD") == 1)

        # Timberland: forest + productive + unreserved
        elif land_type == "timber":
            schema = data.collect_schema().names()
            filters = []

            if "t2_COND_STATUS_CD" in schema:
                filters.append(pl.col("t2_COND_STATUS_CD") == 1)
            if "t2_SITECLCD" in schema:
                filters.append(pl.col("t2_SITECLCD") < 7)
            if "t2_RESERVCD" in schema:
                filters.append(pl.col("t2_RESERVCD") == 0)

            if filters:
                combined = filters[0]
                for f in filters[1:]:
                    combined = combined & f
                data = data.filter(combined)

        return data

    def _apply_tree_type_filter(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply tree type filter to tree data."""
        tree_type = self.config.get("tree_type", "all")

        if tree_type == "all":
            return data

        schema = data.collect_schema().names()

        if tree_type == "live":
            if "t2_STATUSCD" in schema:
                data = data.filter(pl.col("t2_STATUSCD") == 1)
        elif tree_type == "gs":
            # Growing stock: live trees with merchantable volume
            filters = []
            if "t2_STATUSCD" in schema:
                filters.append(pl.col("t2_STATUSCD") == 1)
            if "t2_TREECLCD" in schema:
                filters.append(pl.col("t2_TREECLCD") == 2)

            if filters:
                combined = filters[0]
                for f in filters[1:]:
                    combined = combined & f
                data = data.filter(combined)

        return data

    def _apply_area_domain_filter(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply area domain filter."""
        area_domain = self.config.get("area_domain")
        if not area_domain:
            return data

        from ...filtering import apply_area_filters

        return apply_area_filters(data, area_domain)

    def _apply_tree_domain_filter(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Apply tree domain filter."""
        tree_domain = self.config.get("tree_domain")
        if not tree_domain:
            return data

        from ...filtering import apply_tree_filters

        return apply_tree_filters(data, tree_domain)

    def _detect_harvest(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Detect harvest events between t1 and t2.

        Harvest is identified using:
        1. Primary: Treatment codes (TRTCD1, TRTCD2, TRTCD3) in {10, 20}
        2. Secondary: Volume reduction > 25% (following Dennis 1989, Singh 2010)
        """
        schema = data.collect_schema().names()

        # Primary method: Treatment codes
        trtcd_cols = [
            f"t2_{c}" for c in ["TRTCD1", "TRTCD2", "TRTCD3"] if f"t2_{c}" in schema
        ]

        if trtcd_cols:
            # Check if any treatment code indicates harvest
            harvest_exprs = []
            for col in trtcd_cols:
                harvest_exprs.append(pl.col(col).is_in(list(self.HARVEST_TRTCD)))

            # Combine with OR
            trtcd_harvest = harvest_exprs[0]
            for expr in harvest_exprs[1:]:
                trtcd_harvest = trtcd_harvest | expr

            data = data.with_columns(
                [trtcd_harvest.fill_null(False).alias("HARVEST_TRTCD")]
            )
        else:
            data = data.with_columns([pl.lit(False).alias("HARVEST_TRTCD")])

        # Secondary method: Volume reduction > 25%
        # This would require aggregating tree-level volume, which we don't have here
        # For condition-level, we use only TRTCD
        # TODO: Add volume-based detection when tree data is joined

        # Final harvest indicator
        data = data.with_columns(
            [pl.col("HARVEST_TRTCD").cast(pl.Int8).alias("HARVEST")]
        )

        # Calculate harvest intensity if we have treatment data
        # TRTCD 10 = cutting (partial or clearcut)
        # We don't have intensity without tree-level data at condition level

        return data

    def _calculate_tree_fate(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate tree fate from GRM COMPONENT classification.

        Maps GRM component types to tree fate categories:
        - survivor: SURVIVOR component
        - mortality: MORTALITY1, MORTALITY2 components
        - cut: CUT1, CUT2 components (harvest)
        - diversion: DIVERSION1, DIVERSION2 components (land use change)
        - ingrowth: INGROWTH component
        - other: Any unrecognized component
        """
        schema = data.collect_schema().names()

        if "COMPONENT" not in schema:
            # Fall back to unknown if no component column
            return data.with_columns([pl.lit("unknown").alias("TREE_FATE")])

        # Map GRM components to tree fates
        # Use str.starts_with for flexibility with component naming
        return data.with_columns(
            [
                pl.when(pl.col("COMPONENT") == "SURVIVOR")
                .then(pl.lit("survivor"))
                .when(pl.col("COMPONENT").str.starts_with("MORTALITY"))
                .then(pl.lit("mortality"))
                .when(pl.col("COMPONENT").str.starts_with("CUT"))
                .then(pl.lit("cut"))
                .when(pl.col("COMPONENT").str.starts_with("DIVERSION"))
                .then(pl.lit("diversion"))
                .when(pl.col("COMPONENT") == "INGROWTH")
                .then(pl.lit("ingrowth"))
                .otherwise(pl.lit("other"))
                .alias("TREE_FATE")
            ]
        )

    # Note: _infer_cut_from_harvest() has been removed.
    # Tree fate classification is now handled entirely by GRM COMPONENT,
    # which provides authoritative cut/mortality/diversion classification.

    def _format_condition_output(self, result: pl.DataFrame) -> pl.DataFrame:
        """Format condition panel output with clean column ordering."""
        # Drop internal/temporary columns
        drop_cols = [
            c
            for c in result.columns
            if c.endswith("_right") or c in ("HARVEST_TRTCD", "PREVCOND", "t1_COND_CN")
        ]
        if drop_cols:
            result = result.drop(drop_cols)

        # Define column order priority
        priority_cols = [
            "PLT_CN",
            "PREV_PLT_CN",
            "CONDID",
            "STATECD",
            "COUNTYCD",
            "INVYR",
            "REMPER",
            "CYCLE",
            "HARVEST",
            # Location data
            "LAT",
            "LON",
            "ELEV",
        ]

        # Get t1 and t2 columns
        t1_cols = sorted([c for c in result.columns if c.startswith("t1_")])
        t2_cols = sorted([c for c in result.columns if c.startswith("t2_")])

        # Other columns (excluding internal ones)
        exclude = set(priority_cols) | set(t1_cols) | set(t2_cols)
        other_cols = [c for c in result.columns if c not in exclude]

        # Build final column order
        final_cols = []
        for col in priority_cols:
            if col in result.columns:
                final_cols.append(col)

        # Interleave t1/t2 columns for easier comparison
        t1_base = {c.replace("t1_", ""): c for c in t1_cols}
        t2_base = {c.replace("t2_", ""): c for c in t2_cols}
        all_bases = sorted(set(t1_base.keys()) | set(t2_base.keys()))

        for base in all_bases:
            if base in t1_base:
                final_cols.append(t1_base[base])
            if base in t2_base:
                final_cols.append(t2_base[base])

        final_cols.extend(other_cols)

        # Select in order (only columns that exist)
        final_cols = [c for c in final_cols if c in result.columns]

        return result.select(final_cols)

    def _format_tree_output(self, result: pl.DataFrame) -> pl.DataFrame:
        """Format tree panel output with clean column ordering."""
        # Drop internal/temporary columns
        drop_cols = [
            c
            for c in result.columns
            if c.endswith("_right") or c in ("t1_TRE_CN", "t1_PLT_CN", "t1_CONDID")
        ]
        if drop_cols:
            result = result.drop(drop_cols)

        priority_cols = [
            "PLT_CN",
            "PREV_PLT_CN",
            "TRE_CN",
            "PREV_TRE_CN",
            "CONDID",
            "TREE",
            "SUBP",
            "STATECD",
            "COUNTYCD",
            "INVYR",
            "REMPER",
            "CYCLE",
            "TREE_FATE",
            # Location data
            "LAT",
            "LON",
            "ELEV",
        ]

        # Get t1 and t2 columns
        t1_cols = sorted([c for c in result.columns if c.startswith("t1_")])
        t2_cols = sorted([c for c in result.columns if c.startswith("t2_")])

        # Other columns (excluding internal)
        exclude = set(priority_cols) | set(t1_cols) | set(t2_cols)
        other_cols = [c for c in result.columns if c not in exclude]

        # Build final column order
        final_cols = []
        for col in priority_cols:
            if col in result.columns:
                final_cols.append(col)

        # Interleave t1/t2 columns
        t1_base = {c.replace("t1_", ""): c for c in t1_cols}
        t2_base = {c.replace("t2_", ""): c for c in t2_cols}
        all_bases = sorted(set(t1_base.keys()) | set(t2_base.keys()))

        for base in all_bases:
            if base in t1_base:
                final_cols.append(t1_base[base])
            if base in t2_base:
                final_cols.append(t2_base[base])

        final_cols.extend(other_cols)

        # Select in order (only columns that exist)
        final_cols = [c for c in final_cols if c in result.columns]

        return result.select(final_cols)


def panel(
    db: str | FIA,
    level: Literal["condition", "tree"] = "condition",
    columns: list[str] | None = None,
    land_type: str = "forest",
    tree_type: str = "gs",
    tree_domain: str | None = None,
    area_domain: str | None = None,
    expand_chains: bool = True,
    min_remper: float = 0,
    max_remper: float | None = None,
    min_invyr: int = 2000,
    harvest_only: bool = False,
    expand: bool = False,
    measure: Literal["tpa", "volume"] = "tpa",
    grp_by: list[str] | None = None,
    by_fate: bool = False,
) -> pl.DataFrame:
    """
    Create a t1/t2 remeasurement panel from FIA data.

    Returns a DataFrame where each row represents a measurement pair:
    - t1 (time 1): Previous measurement
    - t2 (time 2): Current measurement

    This panel data is useful for:
    - Harvest probability modeling
    - Forest change detection
    - Growth and mortality analysis
    - Land use transition studies

    Tree-level panels use GRM (Growth-Removal-Mortality) tables for
    authoritative tree fate classification, providing consistent
    definitions aligned with FIA's official estimation methodology.

    Parameters
    ----------
    db : str | FIA
        Database connection or path to FIA database.
    level : {'condition', 'tree'}, default 'condition'
        Level of panel to create:
        - 'condition': Condition-level panel for area/harvest analysis.
          Each row is a condition measured at two time points.
        - 'tree': Tree-level panel for individual tree tracking.
          Each row is a tree with GRM component classification.
    columns : list of str, optional
        Additional columns to include beyond defaults. Useful for adding
        specific attributes needed for analysis.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land classification filter:
        - 'forest': All forest land (COND_STATUS_CD = 1)
        - 'timber': Timberland (productive, unreserved forest)
        - 'all': No land type filtering
    tree_type : {'all', 'live', 'gs'}, default 'gs'
        Tree type filter (tree-level only). Maps to GRM column suffixes:
        - 'gs': Growing stock (merchantable trees) - uses GS columns
        - 'all': All trees - uses GS columns (default GRM behavior)
        - 'live': All live trees - uses AL columns
    tree_domain : str, optional
        SQL-like filter expression for tree-level filtering.
        Example: "SPCD == 131" (loblolly pine only)
    area_domain : str, optional
        SQL-like filter expression for condition-level filtering.
        Example: "OWNGRPCD == 40" (private land only)
    expand_chains : bool, default True
        If True and multiple remeasurements exist (t1->t2->t3),
        creates pairs (t1,t2) and (t2,t3). If False, only returns
        the most recent pair for each location.
    min_remper : float, default 0
        Minimum remeasurement period in years. Filters out pairs
        with shorter intervals.
    max_remper : float, optional
        Maximum remeasurement period in years. Filters out pairs
        with longer intervals.
    min_invyr : int, default 2000
        Minimum inventory year for t2 (current measurement). Defaults to 2000
        to use only the enhanced annual inventory methodology. FIA transitioned
        from periodic to annual inventory around 1999-2000, with significant
        methodology changes. Set to None or 0 to include all years.
    harvest_only : bool, default False
        If True, return only records where harvest was detected.
        For condition-level: uses TRTCD treatment codes.
        For tree-level: returns trees with TREE_FATE in ['cut', 'diversion'].
    expand : bool, default False
        If True, apply expansion factors to produce per-acre estimates
        comparable to removals(). Requires level='tree'.
        Uses three-layer expansion:
        - TPA_UNADJ: Base trees-per-acre
        - ADJ_FACTOR: Plot-type adjustment (subplot/microplot/macroplot)
        - EXPNS: Stratum expansion to total acres
        When expand=True, returns aggregated per-acre estimates instead of
        tree-level data.
    measure : {'tpa', 'volume'}, default 'tpa'
        Measure to expand (only used when expand=True):
        - 'tpa': Trees per acre
        - 'volume': Cubic foot volume per acre
    grp_by : list of str, optional
        Grouping columns for expanded estimates (only used when expand=True).
        Example: ['SPCD'] for by-species estimates.
    by_fate : bool, default False
        If True and expand=True, include TREE_FATE in grouping columns
        to get separate estimates for survivors, mortality, cut, etc.

    Returns
    -------
    pl.DataFrame
        Panel dataset with columns:

        For condition-level:
        - PLT_CN: Current plot control number
        - PREV_PLT_CN: Previous plot control number
        - CONDID: Condition identifier
        - STATECD, COUNTYCD: Geographic identifiers
        - INVYR: Current inventory year
        - REMPER: Remeasurement period (years)
        - HARVEST: Harvest indicator (1=harvest detected, 0=no harvest)
        - t1_*/t2_*: Attributes at time 1 and time 2

        For tree-level:
        - PLT_CN: Plot control number
        - TRE_CN: Tree control number
        - TREE_FATE: Tree fate from GRM classification:
          - 'survivor': Tree alive at both measurements
          - 'mortality': Tree died naturally
          - 'cut': Tree removed by harvest
          - 'diversion': Tree removed due to land use change
          - 'ingrowth': New tree crossing size threshold
        - COMPONENT: Raw GRM component (SURVIVOR, CUT1, etc.)
        - DIA_BEGIN, DIA_MIDPT, DIA_END: Diameter measurements
        - TPA_UNADJ: Trees per acre expansion factor
        - t1_*/t2_*: Tree attributes at time 1 and time 2

    See Also
    --------
    removals : Estimate harvest removals (uses same GRM methodology)
    mortality : Estimate tree mortality
    growth : Estimate forest growth
    area_change : Estimate forest area change

    Examples
    --------
    Basic condition-level panel for harvest analysis:

    >>> from pyfia import FIA, panel
    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_by_state(37)  # North Carolina
    ...     data = panel(db, level="condition", land_type="timber")
    ...     print(f"Panel has {len(data)} condition pairs")
    ...     print(f"Harvest rate: {data['HARVEST'].mean():.1%}")

    Tree-level panel with GRM-based fate classification:

    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_by_state(37)
    ...     trees = panel(db, level="tree", tree_type="gs")
    ...     fate_counts = trees.group_by("TREE_FATE").len()
    ...     print(fate_counts)

    Get all removals (cut + diversion):

    >>> data = panel(
    ...     db,
    ...     level="tree",
    ...     harvest_only=True,  # Returns cut and diversion trees
    ... )

    Filter remeasurement period to 4-8 years:

    >>> data = panel(
    ...     db,
    ...     level="condition",
    ...     min_remper=4,
    ...     max_remper=8,
    ... )

    Notes
    -----
    Tree-level panels use GRM (Growth-Removal-Mortality) tables:

    Tree fate is determined from TREE_GRM_COMPONENT, which provides
    authoritative classification pre-computed by FIA. This ensures
    consistency with the `removals()` function and EVALIDator.

    GRM component types:
    - SURVIVOR: Tree alive at beginning and end of period
    - MORTALITY1/2: Tree died during measurement period
    - CUT1/2: Tree harvested during measurement period
    - DIVERSION1/2: Tree removed due to land use change
    - INGROWTH: New tree crossing 5" DBH threshold

    Condition-level harvest detection uses treatment codes (TRTCD):
    - 10 = Cutting (harvest)
    - 20 = Site preparation (implies prior harvest)

    Remeasurement availability varies by region:
    - Southern states typically have 5-7 year remeasurement cycles
    - Western states may have 10-year cycles
    - Some plots have 3+ remeasurements (t1->t2->t3)

    References
    ----------
    Bechtold & Patterson (2005), "The Enhanced Forest Inventory and
    Analysis Program", Chapter 4: Change Estimation.

    FIA Database User Guide, TREE_GRM_COMPONENT table documentation.
    """
    # Validate inputs
    if level not in ("condition", "tree"):
        raise ValueError(f"Invalid level '{level}'. Must be 'condition' or 'tree'")

    land_type = validate_land_type(land_type)

    if tree_type not in ("all", "live", "gs"):
        raise ValueError(
            f"Invalid tree_type '{tree_type}'. Must be 'all', 'live', or 'gs'"
        )

    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    expand_chains = validate_boolean(expand_chains, "expand_chains")
    harvest_only = validate_boolean(harvest_only, "harvest_only")
    expand = validate_boolean(expand, "expand")
    by_fate = validate_boolean(by_fate, "by_fate")

    if expand and level != "tree":
        raise ValueError("expand=True requires level='tree'")

    if measure not in ("tpa", "volume"):
        raise ValueError(f"Invalid measure '{measure}'. Must be 'tpa' or 'volume'")

    if min_remper < 0:
        raise ValueError(f"min_remper must be non-negative, got {min_remper}")
    if max_remper is not None and max_remper < min_remper:
        raise ValueError(
            f"max_remper ({max_remper}) must be >= min_remper ({min_remper})"
        )
    if min_invyr is not None and min_invyr < 0:
        raise ValueError(f"min_invyr must be non-negative, got {min_invyr}")

    # Handle database connection - convert path string to FIA instance
    if isinstance(db, str):
        db = FIA(db)

    # Build config
    config = {
        "level": level,
        "columns": columns or [],
        "land_type": land_type,
        "tree_type": tree_type,
        "tree_domain": tree_domain,
        "area_domain": area_domain,
        "expand_chains": expand_chains,
        "min_remper": min_remper,
        "max_remper": max_remper,
        "min_invyr": min_invyr,
        "harvest_only": harvest_only,
        "expand": expand,
        "measure": measure,
        "grp_by": grp_by or [],
        "by_fate": by_fate,
    }

    builder = PanelBuilder(db, config)
    return builder.build()
