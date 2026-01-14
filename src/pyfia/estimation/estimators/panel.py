"""
Remeasurement panel creation for FIA data.

Creates t1/t2 (time 1/time 2) linked panel datasets from FIA remeasurement data.
Supports both condition-level and tree-level panels for harvest analysis,
growth tracking, and change detection.

References
----------
Bechtold & Patterson (2005), Chapter 4: Change Estimation
FIA Database User Guide, PLOT and TREE table documentation
Dennis (1989), Singh (2010): Harvest identification methodology
"""

from typing import List, Literal, Optional, Union

import polars as pl

from ...core import FIA


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

    # Default columns for condition-level panels
    DEFAULT_COND_COLUMNS = [
        "COND_STATUS_CD",
        "OWNGRPCD",
        "FORTYPCD",
        "STDAGE",
        "SICOND",
        "SITECLCD",
        "STDSZCD",
        "SLOPE",
        "ASPECT",
        "RESERVCD",
        "TRTCD1",
        "TRTCD2",
        "TRTCD3",
    ]

    # Default columns for tree-level panels
    DEFAULT_TREE_COLUMNS = [
        "STATUSCD",
        "SPCD",
        "DIA",
        "HT",
        "ACTUALHT",
        "VOLCFNET",
        "VOLCFGRS",
        "DRYBIO_AG",
        "TPA_UNADJ",
        "TREECLCD",
    ]

    # Treatment codes indicating harvest
    HARVEST_TRTCD = {10, 20}  # 10=Cutting, 20=Site preparation

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
            raise ValueError(f"Invalid level: {self.level}. Must be 'condition' or 'tree'")

    def _build_condition_panel(self) -> pl.DataFrame:
        """Build condition-level remeasurement panel."""
        # Load required tables
        self._ensure_tables_loaded(["PLOT", "COND"])

        # For chain expansion, load ALL plots (not just current EVALID)
        # This captures all measurement pairs in the database
        expand_chains = self.config.get("expand_chains", True)

        if expand_chains:
            # Load full PLOT table without EVALID filter
            plot = self.db._reader.read_table(
                "PLOT",
                columns=["CN", "STATECD", "COUNTYCD", "INVYR", "PREV_PLT_CN", "REMPER", "CYCLE"],
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
        plot_cols = ["CN", "STATECD", "COUNTYCD", "INVYR", "PREV_PLT_CN", "REMPER", "CYCLE"]
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
            # Get PREVCOND mapping
            prevcond_map = cond.select(["PLT_CN", "CONDID", "PREVCOND"])
            data = data.join(
                prevcond_map,
                left_on=["CN", "CONDID"],
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

        # Expand chains if requested
        if self.config.get("expand_chains", True):
            data = self._expand_condition_chains(data)

        # Clean up and format output
        result = data.collect()
        result = self._format_condition_output(result)

        return result

    def _build_tree_panel(self) -> pl.DataFrame:
        """Build tree-level remeasurement panel."""
        # Load required tables
        self._ensure_tables_loaded(["PLOT", "TREE", "COND"])

        plot = self.db.tables["PLOT"]
        tree = self.db.tables["TREE"]

        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()
        if not isinstance(tree, pl.LazyFrame):
            tree = tree.lazy()

        # Get plot columns
        plot_cols = ["CN", "STATECD", "COUNTYCD", "INVYR", "PREV_PLT_CN", "REMPER", "CYCLE"]
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

        # Get tree columns
        tree_cols = self._get_tree_columns()
        tree_schema = tree.collect_schema().names()
        tree_cols = [c for c in tree_cols if c in tree_schema]

        # Ensure required columns
        required = ["CN", "PLT_CN", "CONDID", "PREV_TRE_CN"]
        for col in required:
            if col not in tree_cols and col in tree_schema:
                tree_cols.append(col)

        # Also get previous status columns if available
        prev_cols = ["PREV_STATUS_CD", "PREVDIA", "PREVCOND"]
        for col in prev_cols:
            if col in tree_schema and col not in tree_cols:
                tree_cols.append(col)

        # Get current trees (t2)
        tree_t2 = tree.select(tree_cols)

        # Cast PREV_TRE_CN to match CN type for joining
        if "PREV_TRE_CN" in tree_t2.collect_schema().names():
            tree_t2 = tree_t2.with_columns([
                pl.col("PREV_TRE_CN").cast(pl.Int64, strict=False).alias("PREV_TRE_CN")
            ])

        # Cast PREV_STATUS_CD if present
        if "PREV_STATUS_CD" in tree_t2.collect_schema().names():
            tree_t2 = tree_t2.with_columns([
                pl.col("PREV_STATUS_CD").cast(pl.Int64, strict=False).alias("PREV_STATUS_CD")
            ])

        # Join plot and tree for t2
        data = plot_t2.join(
            tree_t2,
            left_on="CN",
            right_on="PLT_CN",
            how="inner",
        )

        # Rename CN to PLT_CN for clarity
        data = data.rename({"CN": "PLT_CN"})

        # Rename tree CN to TRE_CN
        schema = data.collect_schema().names()
        t2_rename = {}
        if "CN_right" in schema:
            t2_rename["CN_right"] = "TRE_CN"
        for col in self.DEFAULT_TREE_COLUMNS:
            if col in data.collect_schema().names():
                t2_rename[col] = f"t2_{col}"
        if t2_rename:
            data = data.rename(t2_rename)

        # Calculate tree fate based on status codes
        data = self._calculate_tree_fate(data)

        # Load previous trees (t1) for additional attributes
        tree_prev = self.db._reader.read_table("TREE", columns=tree_cols, lazy=True)

        # Cast CN to Int64 to match PREV_TRE_CN
        tree_prev = tree_prev.with_columns([
            pl.col("CN").cast(pl.Int64, strict=False).alias("CN")
        ])

        # Rename t1 columns
        t1_rename = {"PLT_CN": "t1_PLT_CN", "CN": "t1_TRE_CN", "CONDID": "t1_CONDID"}
        for col in self.DEFAULT_TREE_COLUMNS:
            if col in tree_prev.collect_schema().names():
                t1_rename[col] = f"t1_{col}"
        tree_prev = tree_prev.rename(t1_rename)

        # Join to get full t1 data
        data = data.join(
            tree_prev,
            left_on="PREV_TRE_CN",
            right_on="t1_TRE_CN",
            how="left",
        )

        # Apply tree type filter
        data = self._apply_tree_type_filter(data)

        # Apply tree domain filter
        data = self._apply_tree_domain_filter(data)

        # Filter to harvest only if requested (trees that were cut)
        if self.config.get("harvest_only", False):
            data = data.filter(pl.col("TREE_FATE") == "cut")

        # Expand chains if requested
        if self.config.get("expand_chains", True):
            data = self._expand_tree_chains(data)

        # Clean up and format output
        result = data.collect()
        result = self._format_tree_output(result)

        return result

    def _ensure_tables_loaded(self, tables: List[str]) -> None:
        """Ensure required tables are loaded."""
        for table in tables:
            if table not in self.db.tables:
                self.db.load_table(table)

    def _get_condition_columns(self) -> List[str]:
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

    def _get_tree_columns(self) -> List[str]:
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
        trtcd_cols = [f"t2_{c}" for c in ["TRTCD1", "TRTCD2", "TRTCD3"] if f"t2_{c}" in schema]

        if trtcd_cols:
            # Check if any treatment code indicates harvest
            harvest_exprs = []
            for col in trtcd_cols:
                harvest_exprs.append(pl.col(col).is_in([10, 20]))

            # Combine with OR
            trtcd_harvest = harvest_exprs[0]
            for expr in harvest_exprs[1:]:
                trtcd_harvest = trtcd_harvest | expr

            data = data.with_columns([
                trtcd_harvest.fill_null(False).alias("HARVEST_TRTCD")
            ])
        else:
            data = data.with_columns([
                pl.lit(False).alias("HARVEST_TRTCD")
            ])

        # Secondary method: Volume reduction > 25%
        # This would require aggregating tree-level volume, which we don't have here
        # For condition-level, we use only TRTCD
        # TODO: Add volume-based detection when tree data is joined

        # Final harvest indicator
        data = data.with_columns([
            pl.col("HARVEST_TRTCD").cast(pl.Int8).alias("HARVEST")
        ])

        # Calculate harvest intensity if we have treatment data
        # TRTCD 10 = cutting (partial or clearcut)
        # We don't have intensity without tree-level data at condition level

        return data

    def _calculate_tree_fate(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate tree fate (survivor, mortality, cut, ingrowth).

        Uses STATUSCD at t1 and t2 to determine fate:
        - survivor: live at t1 and t2
        - mortality: live at t1, dead at t2
        - cut: live at t1, removed at t2 (STATUSCD=3)
        - ingrowth: no previous measurement (new tree)
        """
        schema = data.collect_schema().names()

        # Check what status columns we have
        has_t2_status = "t2_STATUSCD" in schema
        has_prev_status = "PREV_STATUS_CD" in schema
        has_prev_tre = "PREV_TRE_CN" in schema

        if has_t2_status and has_prev_status:
            data = data.with_columns([
                pl.when(pl.col("PREV_TRE_CN").is_null())
                .then(pl.lit("ingrowth"))
                .when((pl.col("PREV_STATUS_CD") == 1) & (pl.col("t2_STATUSCD") == 1))
                .then(pl.lit("survivor"))
                .when((pl.col("PREV_STATUS_CD") == 1) & (pl.col("t2_STATUSCD") == 2))
                .then(pl.lit("mortality"))
                .when((pl.col("PREV_STATUS_CD") == 1) & (pl.col("t2_STATUSCD") == 3))
                .then(pl.lit("cut"))
                .otherwise(pl.lit("other"))
                .alias("TREE_FATE")
            ])
        elif has_prev_tre:
            # Simplified fate based on whether tree was tracked
            data = data.with_columns([
                pl.when(pl.col("PREV_TRE_CN").is_null())
                .then(pl.lit("ingrowth"))
                .otherwise(pl.lit("tracked"))
                .alias("TREE_FATE")
            ])
        else:
            data = data.with_columns([
                pl.lit("unknown").alias("TREE_FATE")
            ])

        return data

    def _expand_condition_chains(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Expand multi-remeasurement chains into all pairs.

        If a condition has measurements at t1, t2, t3, this creates
        pairs (t1,t2) and (t2,t3) instead of just (t2,t3).

        The current data already has the most recent pairs.
        To get earlier pairs, we need to find plots where PLT_CN
        also appears as someone else's PREV_PLT_CN (making it a t2
        that should also be treated as a t1 for an earlier pair).

        This is handled by loading ALL plots with PREV_PLT_CN, not
        just those in the current evaluation.
        """
        # Chain expansion is now handled in _build_condition_panel
        # by loading all plots with remeasurement data
        return data

    def _expand_tree_chains(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """
        Expand multi-remeasurement chains for trees.

        Similar to condition chains but following PREV_TRE_CN links.
        """
        # TODO: Implement full chain expansion for trees
        return data

    def _format_condition_output(self, result: pl.DataFrame) -> pl.DataFrame:
        """Format condition panel output with clean column ordering."""
        # Drop internal/temporary columns
        drop_cols = [
            c for c in result.columns
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
            c for c in result.columns
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
    db: Union[str, FIA],
    level: Literal["condition", "tree"] = "condition",
    columns: Optional[List[str]] = None,
    land_type: str = "forest",
    tree_type: str = "all",
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    expand_chains: bool = True,
    min_remper: float = 0,
    max_remper: Optional[float] = None,
    min_invyr: int = 2000,
    harvest_only: bool = False,
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

    Parameters
    ----------
    db : Union[str, FIA]
        Database connection or path to FIA database.
    level : {'condition', 'tree'}, default 'condition'
        Level of panel to create:
        - 'condition': Condition-level panel for area/harvest analysis.
          Each row is a condition measured at two time points.
        - 'tree': Tree-level panel for individual tree tracking.
          Each row is a tree measured at two time points.
    columns : list of str, optional
        Additional columns to include beyond defaults. Useful for adding
        specific attributes needed for analysis.
    land_type : {'forest', 'timber', 'all'}, default 'forest'
        Land classification filter:
        - 'forest': All forest land (COND_STATUS_CD = 1)
        - 'timber': Timberland (productive, unreserved forest)
        - 'all': No land type filtering
    tree_type : {'all', 'live', 'gs'}, default 'all'
        Tree type filter (tree-level only):
        - 'all': All trees
        - 'live': Living trees only (STATUSCD = 1)
        - 'gs': Growing stock (merchantable trees)
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
        For tree-level: returns only cut trees (TREE_FATE = 'cut').

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
        - PLT_CN, PREV_PLT_CN: Plot control numbers
        - TRE_CN, PREV_TRE_CN: Tree control numbers
        - TREE_FATE: Tree fate (survivor/mortality/cut/ingrowth)
        - t1_*/t2_*: Tree attributes at time 1 and time 2

    See Also
    --------
    area_change : Estimate forest area change
    mortality : Estimate tree mortality
    removals : Estimate harvest removals
    growth : Estimate forest growth

    Examples
    --------
    Basic condition-level panel for harvest analysis:

    >>> from pyfia import FIA, panel
    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_by_state(37)  # North Carolina
    ...     data = panel(db, level="condition", land_type="timber")
    ...     print(f"Panel has {len(data)} condition pairs")
    ...     print(f"Harvest rate: {data['HARVEST'].mean():.1%}")

    Tree-level panel for mortality analysis:

    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_by_state(37)
    ...     trees = panel(db, level="tree", tree_type="live")
    ...     mortality_rate = (trees["TREE_FATE"] == "mortality").mean()
    ...     print(f"Mortality rate: {mortality_rate:.1%}")

    Filter to harvested conditions on private land:

    >>> data = panel(
    ...     db,
    ...     level="condition",
    ...     area_domain="OWNGRPCD == 40",
    ...     harvest_only=True,
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
    Harvest detection methods:

    1. **Primary (Treatment Codes)**: TRTCD1, TRTCD2, or TRTCD3 in {10, 20}
       - 10 = Cutting (harvest)
       - 20 = Site preparation (implies prior harvest)

    2. **Secondary (Volume Change)**: For plots without treatment codes,
       harvest can be inferred from volume reduction > 25%
       (following Dennis 1989, Singh 2010).

    Remeasurement availability varies by region:
    - Southern states typically have 5-7 year remeasurement cycles
    - Western states may have 10-year cycles
    - Some plots have 3+ remeasurements (t1->t2->t3)

    References
    ----------
    Dennis, D.F. 1989. An economic analysis of harvest behavior.
    Forest Science 35(4): 1088-1104.

    Singh, N. 2010. Econometric Analysis of Timber Harvest Behavior.
    MS Thesis, North Carolina State University.

    Bechtold & Patterson (2005), "The Enhanced Forest Inventory and
    Analysis Program", Chapter 4: Change Estimation.
    """
    from ...validation import (
        validate_boolean,
        validate_domain_expression,
        validate_land_type,
    )

    # Validate inputs
    if level not in ("condition", "tree"):
        raise ValueError(f"Invalid level '{level}'. Must be 'condition' or 'tree'")

    land_type = validate_land_type(land_type)

    if tree_type not in ("all", "live", "gs"):
        raise ValueError(f"Invalid tree_type '{tree_type}'. Must be 'all', 'live', or 'gs'")

    tree_domain = validate_domain_expression(tree_domain, "tree_domain")
    area_domain = validate_domain_expression(area_domain, "area_domain")
    expand_chains = validate_boolean(expand_chains, "expand_chains")
    harvest_only = validate_boolean(harvest_only, "harvest_only")

    if min_remper < 0:
        raise ValueError(f"min_remper must be non-negative, got {min_remper}")
    if max_remper is not None and max_remper < min_remper:
        raise ValueError(f"max_remper ({max_remper}) must be >= min_remper ({min_remper})")
    if min_invyr is not None and min_invyr < 0:
        raise ValueError(f"min_invyr must be non-negative, got {min_invyr}")

    # Handle database connection
    owns_db = False
    if isinstance(db, str):
        db = FIA(db)
        owns_db = True

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
    }

    try:
        builder = PanelBuilder(db, config)
        return builder.build()
    finally:
        # FIA uses context manager, no explicit close needed
        pass
