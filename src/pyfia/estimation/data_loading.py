"""
Data loading utilities for FIA estimation.

This module provides a DataLoader class that handles all data loading operations
for FIA estimators, keeping the BaseEstimator class focused on estimation logic.

Design principles:
- Simple composition over inheritance
- Polars LazyFrames for memory efficiency
- Minimal caching (only where it provides clear benefit)
- No complex patterns - just load data
"""

from __future__ import annotations

import logging
from functools import lru_cache

import polars as pl

from ..core import FIA
from ..filtering import apply_plot_filters

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Data loader for FIA estimation.

    Handles loading and joining FIA tables with proper filtering and column
    selection. Uses Polars LazyFrames for memory efficiency.

    Parameters
    ----------
    db : FIA
        FIA database connection
    config : dict
        Configuration dictionary with estimation parameters

    Attributes
    ----------
    db : FIA
        The FIA database connection
    config : dict
        Configuration dictionary
    """

    def __init__(self, db: FIA, config: dict) -> None:
        """
        Initialize the data loader.

        Parameters
        ----------
        db : FIA
            FIA database connection
        config : dict
            Configuration dictionary with estimation parameters
        """
        self.db = db
        self.config = config

        # Validate grp_by columns early during initialization
        self._validate_grp_by_columns()

    def _validate_grp_by_columns(self) -> None:
        """
        Validate that grp_by columns exist in available table schemas.

        This method checks grp_by columns against TREE, COND, and PLOT schemas
        to catch invalid column names early before query execution.

        Raises
        ------
        ValueError
            If any grp_by column does not exist in TREE, COND, or PLOT tables.
        """
        grp_by = self.config.get("grp_by")
        if not grp_by:
            return

        if isinstance(grp_by, str):
            grp_by = [grp_by]

        # Try to get table schemas; skip validation if not available (e.g., in tests)
        try:
            tree_schema_result = self.db._reader.get_table_schema("TREE")
            cond_schema_result = self.db._reader.get_table_schema("COND")
            plot_schema_result = self.db._reader.get_table_schema("PLOT")

            # Handle case where schema returns are not proper dicts (e.g., Mock objects)
            if not isinstance(tree_schema_result, dict):
                return
            if not isinstance(cond_schema_result, dict):
                return
            if not isinstance(plot_schema_result, dict):
                return

            tree_schema = list(tree_schema_result.keys())
            cond_schema = list(cond_schema_result.keys())
            plot_schema = list(plot_schema_result.keys())
        except (AttributeError, TypeError):
            # Skip validation if schema access fails (e.g., mock db in tests)
            return

        # Check for polygon attributes if they exist
        polygon_attr_cols: list[str] = []
        if (
            hasattr(self.db, "_polygon_attributes")
            and self.db._polygon_attributes is not None
        ):
            try:
                polygon_attr_cols = list(self.db._polygon_attributes.columns)
            except (AttributeError, TypeError):
                pass

        # Combine all available columns
        all_available_cols = (
            set(tree_schema)
            | set(cond_schema)
            | set(plot_schema)
            | set(polygon_attr_cols)
        )

        # Find invalid columns
        invalid_cols = [col for col in grp_by if col not in all_available_cols]

        if invalid_cols:
            # Build helpful error message with common grouping options
            from .columns import (
                COND_GROUPING_COLUMNS,
                TREE_GROUPING_COLUMNS,
            )

            common_grouping_cols = sorted(
                set(TREE_GROUPING_COLUMNS) | set(COND_GROUPING_COLUMNS)
            )
            raise ValueError(
                f"Invalid grp_by column(s): {invalid_cols}. "
                f"Column(s) not found in TREE, COND, or PLOT tables. "
                f"Common grouping columns include: {common_grouping_cols}"
            )

    def load_data(
        self,
        required_tables: list[str],
        tree_columns: list[str] | None = None,
        cond_columns: list[str] | None = None,
    ) -> pl.LazyFrame | None:
        """
        Load and join required tables.

        Parameters
        ----------
        required_tables : list[str]
            List of required table names
        tree_columns : list[str] | None
            Columns to load from TREE table
        cond_columns : list[str] | None
            Columns to load from COND table

        Returns
        -------
        pl.LazyFrame | None
            Joined data or None if no tree data needed
        """
        # Handle area-only estimations (no tree data)
        if "TREE" not in required_tables:
            return self._load_area_data(cond_columns)

        # Load tree and condition data
        return self._load_tree_cond_data(tree_columns, cond_columns)

    def _prepare_column_lists(
        self,
        tree_columns: list[str] | None,
        cond_columns: list[str] | None,
    ) -> tuple[list[str] | None, list[str] | None]:
        """
        Prepare column lists for TREE and COND tables including grouping columns.

        Parameters
        ----------
        tree_columns : list[str] | None
            Base tree columns required
        cond_columns : list[str] | None
            Base condition columns required

        Returns
        -------
        tuple[list[str] | None, list[str] | None]
            (tree_cols, cond_cols) with grouping columns added

        Raises
        ------
        ValueError
            If any grp_by column does not exist in TREE, COND, or PLOT tables.
        """
        # Make copies to avoid modifying original lists
        tree_cols = list(tree_columns) if tree_columns else None
        cond_cols = list(cond_columns) if cond_columns else None

        # Get table schemas to determine where grp_by columns live
        tree_schema = list(self.db._reader.get_table_schema("TREE").keys())
        cond_schema = list(self.db._reader.get_table_schema("COND").keys())

        # Add grouping columns from config if specified
        # Note: Validation is done in __init__ via _validate_grp_by_columns()
        grp_by = self.config.get("grp_by")
        if grp_by:
            if isinstance(grp_by, str):
                grp_by = [grp_by]

            for col in grp_by:
                in_tree = col in tree_schema and (
                    tree_cols is None or col not in tree_cols
                )
                in_cond = col in cond_schema and (
                    cond_cols is None or col not in cond_cols
                )
                if tree_cols is not None and in_tree:
                    tree_cols.append(col)
                elif cond_cols is not None and in_cond:
                    cond_cols.append(col)

        return tree_cols, cond_cols

    def _load_table_with_cache_check(
        self,
        table_name: str,
        columns: list[str] | None,
        where: str | None = None,
    ) -> pl.LazyFrame:
        """
        Load a table with cache invalidation if required columns are missing.

        Parameters
        ----------
        table_name : str
            Name of the table to load
        columns : list[str] | None
            Required columns
        where : str | None
            SQL WHERE clause

        Returns
        -------
        pl.LazyFrame
            Loaded table as LazyFrame
        """
        # Check if cached table has all required columns
        if table_name in self.db.tables:
            cached = self.db.tables[table_name]
            cached_cols = set(
                cached.collect_schema().names()
                if isinstance(cached, pl.LazyFrame)
                else cached.columns
            )
            required_cols = set(columns) if columns else set()
            if not required_cols.issubset(cached_cols):
                del self.db.tables[table_name]

        if table_name not in self.db.tables:
            self.db.load_table(table_name, columns=columns, where=where)

        df = self.db.tables[table_name]
        if not isinstance(df, pl.LazyFrame):
            df = df.lazy()

        return df

    def _get_valid_plots_filter(self) -> pl.LazyFrame | None:
        """
        Get valid plot CNs based on EVALID and plot_domain filters.

        Returns
        -------
        pl.LazyFrame | None
            LazyFrame with valid PLT_CN values, or None if no filters
        """
        valid_plots = None

        # Apply EVALID filtering
        if self.db.evalid:
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")

            ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            if not isinstance(ppsa, pl.LazyFrame):
                ppsa = ppsa.lazy()

            valid_plots = (
                ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
                .select("PLT_CN")
                .unique()
            )

        # Apply plot domain filter if specified
        if self.config.get("plot_domain"):
            if "PLOT" not in self.db.tables:
                self.db.load_table("PLOT")
            plot_df = self.db.tables["PLOT"]
            if not isinstance(plot_df, pl.LazyFrame):
                plot_df = plot_df.lazy()

            plot_df = apply_plot_filters(
                plot_df, plot_domain=self.config["plot_domain"]
            )
            plot_filtered_plots = plot_df.select(pl.col("CN").alias("PLT_CN")).unique()

            # Combine with EVALID filter if both exist
            if valid_plots is not None:
                valid_plots = valid_plots.join(
                    plot_filtered_plots, on="PLT_CN", how="inner"
                )
            else:
                valid_plots = plot_filtered_plots

        return valid_plots

    def _apply_plot_filter_and_select_columns(
        self,
        df: pl.LazyFrame,
        valid_plots: pl.LazyFrame | None,
        columns: list[str] | None,
    ) -> pl.LazyFrame:
        """
        Apply plot filter and select required columns from a dataframe.

        Parameters
        ----------
        df : pl.LazyFrame
            Input dataframe
        valid_plots : pl.LazyFrame | None
            Valid plot CNs to filter by
        columns : list[str] | None
            Columns to select

        Returns
        -------
        pl.LazyFrame
            Filtered and projected dataframe
        """
        # Apply plot filter if set
        if valid_plots is not None:
            df = df.join(valid_plots, on="PLT_CN", how="inner")

        # Select only needed columns
        if columns:
            schema_names = df.collect_schema().names()
            available_cols = [c for c in columns if c in schema_names]
            df = df.select(available_cols)

        return df

    def _load_tree_cond_data(
        self,
        tree_columns: list[str] | None,
        cond_columns: list[str] | None,
    ) -> pl.LazyFrame:
        """Load and join tree and condition data.

        Memory optimization:
        1. Column projection: Load only required columns at SQL level
        2. Database-side filtering: Push tree_type and land_type filters to SQL

        This reduces memory footprint by 60-80% for large TREE tables.

        Parameters
        ----------
        tree_columns : list[str] | None
            Columns to load from TREE table
        cond_columns : list[str] | None
            Columns to load from COND table

        Returns
        -------
        pl.LazyFrame
            Joined tree and condition data
        """
        # Prepare column lists with grouping columns
        tree_cols, cond_cols = self._prepare_column_lists(tree_columns, cond_columns)

        # Build SQL WHERE clauses for database-side filtering
        tree_where = self._build_tree_sql_filter()
        cond_where = self._build_cond_sql_filter()

        # Load tables with cache invalidation
        tree_df = self._load_table_with_cache_check("TREE", tree_cols, tree_where)
        cond_df = self._load_table_with_cache_check("COND", cond_cols, cond_where)

        # Get valid plots based on EVALID and plot_domain filters
        valid_plots = self._get_valid_plots_filter()

        # Apply filters and column selection
        tree_df = self._apply_plot_filter_and_select_columns(
            tree_df, valid_plots, tree_cols
        )
        cond_df = self._apply_plot_filter_and_select_columns(
            cond_df, valid_plots, cond_cols
        )

        # Join tree and condition
        return tree_df.join(cond_df, on=["PLT_CN", "CONDID"], how="inner")

    def _load_area_data(
        self,
        cond_columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Load condition and plot data for area estimation.

        Parameters
        ----------
        cond_columns : list[str] | None
            Columns to load from COND table

        Returns
        -------
        pl.LazyFrame
            Joined condition and plot data
        """
        # Get required columns for area estimation
        cond_cols = list(cond_columns) if cond_columns else None

        # Load COND table with cache invalidation
        if "COND" in self.db.tables:
            cached = self.db.tables["COND"]
            cached_cols = set(
                cached.collect_schema().names()
                if isinstance(cached, pl.LazyFrame)
                else cached.columns
            )
            required_cols = set(cond_cols) if cond_cols else set()
            if not required_cols.issubset(cached_cols):
                # Reload with all required columns
                del self.db.tables["COND"]
        if "COND" not in self.db.tables:
            self.db.load_table("COND", columns=cond_cols)
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

        # Apply EVALID filtering through POP_PLOT_STRATUM_ASSGN
        if self.db.evalid:
            # Load POP_PLOT_STRATUM_ASSGN to get plots for the EVALID
            if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
                self.db.load_table("POP_PLOT_STRATUM_ASSGN")

            ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]
            if not isinstance(ppsa, pl.LazyFrame):
                ppsa = ppsa.lazy()

            # Filter to get PLT_CNs for the specified EVALID(s)
            valid_plots = (
                ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
                .select("PLT_CN")
                .unique()
            )

            # Filter cond and plot to only include these plots
            cond_df = cond_df.join(valid_plots, on="PLT_CN", how="inner")
            # For plot table, join on CN not PLT_CN
            valid_plot_cns = valid_plots.rename({"PLT_CN": "CN"})
            plot_df = plot_df.join(valid_plot_cns, on="CN", how="inner")

        # Apply plot domain filter BEFORE joining with COND
        # This allows filtering by PLOT-level attributes like COUNTYCD
        if self.config.get("plot_domain"):
            plot_df = apply_plot_filters(
                plot_df, plot_domain=self.config["plot_domain"]
            )

        # Join condition and plot (all PLOT columns for grouping flexibility)
        data = cond_df.join(plot_df, left_on="PLT_CN", right_on="CN", how="inner")

        return data

    def _build_tree_sql_filter(self) -> str | None:
        """Build SQL WHERE clause for TREE table based on config.

        This pushes common filters to the database level to reduce memory usage.

        Returns
        -------
        str | None
            SQL WHERE clause (without WHERE keyword) or None if no filters
        """
        filters = []

        # Tree type filter (most common optimization)
        tree_type = self.config.get("tree_type", "live")
        if tree_type == "live":
            filters.append("STATUSCD = 1")
        elif tree_type == "dead":
            filters.append("STATUSCD = 2")
        elif tree_type == "gs":
            # Growing stock: live trees with valid tree class
            # Note: TREECLCD filter applied in Polars since it's conditional
            filters.append("STATUSCD = 1")
        # "all" means no STATUSCD filter

        # Basic validity filters (these are always applied in apply_tree_filters)
        filters.append("DIA IS NOT NULL")
        filters.append("TPA_UNADJ > 0")

        if filters:
            return " AND ".join(filters)
        return None

    def _build_cond_sql_filter(self) -> str | None:
        """Build SQL WHERE clause for COND table based on config.

        This pushes land type filters to the database level to reduce memory usage.

        Returns
        -------
        str | None
            SQL WHERE clause (without WHERE keyword) or None if no filters
        """
        filters = []

        # Land type filter
        land_type = self.config.get("land_type", "forest")
        if land_type == "forest":
            filters.append("COND_STATUS_CD = 1")
        elif land_type == "timber":
            # Timberland: forest, productive, not reserved
            filters.append("COND_STATUS_CD = 1")
            filters.append("SITECLCD IN (1, 2, 3, 4, 5, 6)")
            filters.append("RESERVCD = 0")
        # "all" means no COND_STATUS_CD filter

        if filters:
            return " AND ".join(filters)
        return None

    @lru_cache(maxsize=1)
    def get_stratification_data(self) -> pl.LazyFrame:
        """
        Get stratification data with simple caching.

        Returns
        -------
        pl.LazyFrame
            Joined PPSA, POP_STRATUM, and PLOT data including MACRO_BREAKPOINT_DIA
        """
        # Load PPSA
        if "POP_PLOT_STRATUM_ASSGN" not in self.db.tables:
            self.db.load_table("POP_PLOT_STRATUM_ASSGN")
        ppsa = self.db.tables["POP_PLOT_STRATUM_ASSGN"]

        # Load POP_STRATUM
        if "POP_STRATUM" not in self.db.tables:
            self.db.load_table("POP_STRATUM")
        pop_stratum = self.db.tables["POP_STRATUM"]

        # Load PLOT table for MACRO_BREAKPOINT_DIA
        if "PLOT" not in self.db.tables:
            self.db.load_table("PLOT")
        plot = self.db.tables["PLOT"]

        # Ensure LazyFrames
        if not isinstance(ppsa, pl.LazyFrame):
            ppsa = ppsa.lazy()
        if not isinstance(pop_stratum, pl.LazyFrame):
            pop_stratum = pop_stratum.lazy()
        if not isinstance(plot, pl.LazyFrame):
            plot = plot.lazy()

        # Apply EVALID filter
        if self.db.evalid:
            ppsa = ppsa.filter(pl.col("EVALID").is_in(self.db.evalid))
            pop_stratum = pop_stratum.filter(pl.col("EVALID").is_in(self.db.evalid))

        # CRITICAL: Remove duplicates from both tables
        # Texas has duplicate rows in both POP_PLOT_STRATUM_ASSGN and POP_STRATUM
        # Each plot-stratum pair and each stratum appears exactly twice
        ppsa_unique = ppsa.unique(subset=["PLT_CN", "STRATUM_CN"])
        pop_stratum_unique = pop_stratum.unique(subset=["CN"])

        # Select only necessary columns from PPSA to avoid duplicate columns
        # when joining with other tables that also have STATECD, INVYR, etc.
        ppsa_selected = ppsa_unique.select(["PLT_CN", "STRATUM_CN"])

        # Load POP_ESTN_UNIT for exact B&P variance formula
        if "POP_ESTN_UNIT" not in self.db.tables:
            self.db.load_table("POP_ESTN_UNIT")
        pop_estn_unit = self.db.tables["POP_ESTN_UNIT"]
        if not isinstance(pop_estn_unit, pl.LazyFrame):
            pop_estn_unit = pop_estn_unit.lazy()

        # Apply EVALID filter to POP_ESTN_UNIT
        if self.db.evalid:
            pop_estn_unit = pop_estn_unit.filter(pl.col("EVALID").is_in(self.db.evalid))

        # Deduplicate POP_ESTN_UNIT (same reason as POP_STRATUM)
        pop_estn_unit_unique = pop_estn_unit.unique(subset=["CN"])

        # Select columns from POP_ESTN_UNIT
        pop_estn_unit_selected = pop_estn_unit_unique.select(
            [
                pl.col("CN").alias("ESTN_UNIT_CN"),
                "AREA_USED",
                "P1PNTCNT_EU",
            ]
        )

        # Select necessary columns from POP_STRATUM
        # Include ESTN_UNIT_CN, P1POINTCNT, P2POINTCNT for exact B&P variance
        pop_stratum_selected = pop_stratum_unique.select(
            [
                pl.col("CN").alias("STRATUM_CN"),
                "ESTN_UNIT_CN",
                "EXPNS",
                "ADJ_FACTOR_MICR",
                "ADJ_FACTOR_SUBP",
                "ADJ_FACTOR_MACR",
                "P1POINTCNT",
                "P2POINTCNT",
            ]
        )

        # Join POP_STRATUM with POP_ESTN_UNIT to get AREA_USED and P1PNTCNT_EU
        pop_stratum_selected = pop_stratum_selected.join(
            pop_estn_unit_selected, on="ESTN_UNIT_CN", how="left"
        )

        # Compute STRATUM_WGT = P1POINTCNT / P1PNTCNT_EU
        # Guard against null or zero P1PNTCNT_EU to avoid inf/NaN propagation
        pop_stratum_selected = pop_stratum_selected.with_columns(
            pl.when(
                pl.col("P1PNTCNT_EU").is_not_null()
                & (pl.col("P1PNTCNT_EU").cast(pl.Float64) > 0)
            )
            .then(
                pl.col("P1POINTCNT").cast(pl.Float64)
                / pl.col("P1PNTCNT_EU").cast(pl.Float64)
            )
            .otherwise(0.0)
            .alias("STRATUM_WGT")
        )

        # Select MACRO_BREAKPOINT_DIA from PLOT table
        # This is CRITICAL for correct adjustment factor selection in states with macroplots
        plot_cols = [pl.col("CN").alias("PLT_CN"), "MACRO_BREAKPOINT_DIA"]

        # Include polygon attributes if they exist (from intersect_polygons)
        # This allows grp_by to use polygon attribute columns
        if (
            hasattr(self.db, "_polygon_attributes")
            and self.db._polygon_attributes is not None
            and isinstance(self.db._polygon_attributes, pl.DataFrame)
        ):
            # Get column names from polygon attributes (excluding CN which is the join key)
            polygon_attr_cols = [
                col for col in self.db._polygon_attributes.columns if col != "CN"
            ]
            # Add these columns to the selection if they exist in the plot schema
            plot_schema = plot.collect_schema().names()
            for col in polygon_attr_cols:
                if col in plot_schema:
                    plot_cols.append(col)

        plot_selected = plot.select(plot_cols)

        # Join PPSA with POP_STRATUM
        strat_data = ppsa_selected.join(
            pop_stratum_selected, on="STRATUM_CN", how="inner"
        )

        # Join with PLOT to get MACRO_BREAKPOINT_DIA
        strat_data = strat_data.join(plot_selected, on="PLT_CN", how="left")

        return strat_data
