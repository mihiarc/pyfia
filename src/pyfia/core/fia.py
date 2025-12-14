"""
Core FIA database class and functionality for pyFIA.

This module provides the main FIA class that handles database connections,
EVALID-based filtering, and common FIA data operations.
"""

import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union

# import duckdb  # No longer needed - handled by backends
import polars as pl

from .data_reader import FIADataReader


def _add_parsed_evalid_columns(
    df: pl.DataFrame | pl.LazyFrame,
) -> pl.DataFrame | pl.LazyFrame:
    """
    Add parsed EVALID columns to a DataFrame for sorting.

    EVALID format: SSYYTT (State, Year 2-digit, Type)
    Year uses Y2K windowing: 00-30 -> 2000-2030, 31-99 -> 1931-1999
    """
    return df.with_columns(
        [
            pl.when(pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32) <= 30)
            .then(2000 + pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32))
            .otherwise(
                1900 + pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32)
            )
            .alias("EVALID_YEAR"),
            pl.col("EVALID")
            .cast(pl.Utf8)
            .str.slice(0, 2)
            .cast(pl.Int32)
            .alias("EVALID_STATE"),
            pl.col("EVALID")
            .cast(pl.Utf8)
            .str.slice(4, 2)
            .cast(pl.Int32)
            .alias("EVALID_TYPE"),
        ]
    )


class FIA:
    """
    Main FIA database class for working with Forest Inventory and Analysis data.

    This class provides methods for loading FIA data from DuckDB databases,
    filtering by EVALID, and preparing data for estimation functions.

    Attributes
    ----------
    db_path : Path
        Path to the DuckDB database.
    tables : Dict[str, pl.LazyFrame]
        Loaded FIA tables as lazy frames.
    evalid : list of int or None
        Active EVALID filter.
    most_recent : bool
        Whether to use most recent evaluations.
    """

    def __init__(self, db_path: Union[str, Path], engine: Optional[str] = None):
        """
        Initialize FIA database connection.

        Parameters
        ----------
        db_path : str or Path
            Path to FIA database (DuckDB or SQLite).
        engine : str, optional
            Database engine ('duckdb', 'sqlite', or None for auto-detect).
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        # Initialize with appropriate engine
        self.tables: Dict[str, pl.LazyFrame] = {}
        self.evalid: Optional[List[int]] = None
        self.most_recent: bool = False
        self.state_filter: Optional[List[int]] = None  # Add state filter
        self._valid_plot_cns: Optional[List[str]] = (
            None  # Cache for EVALID plot filtering
        )
        # Connection managed by FIADataReader
        self._reader = FIADataReader(db_path, engine=engine)

    @classmethod
    def from_download(
        cls,
        states: Union[str, List[str]],
        dir: Optional[Union[str, Path]] = None,
        common: bool = True,
        tables: Optional[List[str]] = None,
        force: bool = False,
        show_progress: bool = True,
    ) -> "FIA":
        """
        Download FIA data and return a connected FIA instance.

        This is a convenience method that combines downloading data from
        the FIA DataMart with opening a database connection.

        Parameters
        ----------
        states : str or list of str
            State abbreviations (e.g., 'GA', 'NC') or 'REF' for reference tables.
            Supports multiple states: ['GA', 'FL', 'SC']
        dir : str or Path, optional
            Directory to save downloaded data. Defaults to ~/.pyfia/data/
        common : bool, default True
            If True, download only tables required for pyFIA functions.
        tables : list of str, optional
            Specific tables to download. Overrides `common` parameter.
        force : bool, default False
            If True, re-download even if files exist locally.
        show_progress : bool, default True
            Show download progress bars.

        Returns
        -------
        FIA
            Connected FIA database instance.

        Examples
        --------
        >>> # Download and open Georgia data
        >>> db = FIA.from_download("GA")
        >>> db.clip_most_recent()
        >>> result = db.area()
        >>>
        >>> # Download multiple states
        >>> db = FIA.from_download(["GA", "FL", "SC"])
        """
        from pyfia.downloader import download

        db_path = download(
            states=states,
            dir=dir,
            common=common,
            tables=tables,
            force=force,
            show_progress=show_progress,
        )

        return cls(db_path)

    def __enter__(self):
        """Context manager entry."""
        # Connection managed by FIADataReader
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        # Connection cleanup handled by FIADataReader
        pass

    # Connection management moved to FIADataReader with backend support

    def _get_valid_plot_cns(self) -> Optional[List[str]]:
        """
        Get plot CNs valid for the current EVALID filter.

        This method caches the result to avoid repeated database queries.

        Returns
        -------
        list of str or None
            List of plot CNs or None if no EVALID filter is set.
        """
        if self.evalid is None:
            return None

        if self._valid_plot_cns is not None:
            return self._valid_plot_cns

        # Query PLT_CNs from POP_PLOT_STRATUM_ASSGN for the EVALID
        evalid_str = ", ".join(str(e) for e in self.evalid)
        ppsa = self._reader.read_table(
            "POP_PLOT_STRATUM_ASSGN",
            columns=["PLT_CN"],
            where=f"EVALID IN ({evalid_str})",
            lazy=True,  # Get as LazyFrame
        ).collect()  # Then collect

        self._valid_plot_cns = ppsa["PLT_CN"].unique().to_list()
        return self._valid_plot_cns

    def load_table(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> pl.LazyFrame:
        """
        Load a table from the FIA database as a lazy frame.

        Parameters
        ----------
        table_name : str
            Name of the FIA table to load (e.g., 'PLOT', 'TREE', 'COND').
        columns : list of str, optional
            Columns to load. Loads all columns if None.

        Returns
        -------
        pl.LazyFrame
            Polars LazyFrame of the requested table.
        """
        # Build base WHERE clause for state filter
        base_where_clause = None
        if self.state_filter and table_name in ["PLOT", "COND", "TREE"]:
            state_list = ", ".join(str(s) for s in self.state_filter)
            base_where_clause = f"STATECD IN ({state_list})"

        # EVALID filter via PLT_CN for TREE, COND tables
        # This is a critical optimization - it reduces data load by 90%+ for GRM estimates
        if self.evalid and table_name in ["TREE", "COND"]:
            valid_plot_cns = self._get_valid_plot_cns()
            if valid_plot_cns:
                # Batch the PLT_CN values to avoid SQL query limits
                batch_size = 900
                dfs = []

                for i in range(0, len(valid_plot_cns), batch_size):
                    batch = valid_plot_cns[i : i + batch_size]
                    cn_str = ", ".join(f"'{cn}'" for cn in batch)
                    plt_cn_where = f"PLT_CN IN ({cn_str})"

                    # Combine with base where clause if present
                    if base_where_clause:
                        where_clause = f"{base_where_clause} AND {plt_cn_where}"
                    else:
                        where_clause = plt_cn_where

                    df = self._reader.read_table(
                        table_name,
                        columns=columns,
                        where=where_clause,
                        lazy=True,
                    )
                    dfs.append(df)

                # Concatenate all batches
                if len(dfs) == 1:
                    result = dfs[0]
                else:
                    result = pl.concat(dfs)

                self.tables[table_name] = result
                return self.tables[table_name]

        # Default path - no EVALID filtering or not a filterable table
        df = self._reader.read_table(
            table_name,
            columns=columns,
            where=base_where_clause,
            lazy=True,
        )

        self.tables[table_name] = df
        return self.tables[table_name]

    def find_evalid(
        self,
        most_recent: bool = True,
        state: Optional[Union[int, List[int]]] = None,
        year: Optional[Union[int, List[int]]] = None,
        eval_type: Optional[str] = None,
    ) -> List[int]:
        """
        Find EVALID values matching criteria.

        Identify evaluation IDs for filtering FIA data based on specific criteria.

        Parameters
        ----------
        most_recent : bool, default True
            If True, return only most recent evaluations per state.
        state : int or list of int, optional
            State FIPS code(s) to filter by.
        year : int or list of int, optional
            End inventory year(s) to filter by.
        eval_type : str, optional
            Evaluation type ('VOL', 'GRM', 'CHNG', 'ALL', etc.).

        Returns
        -------
        list of int
            EVALID values matching the specified criteria.
        """
        # Load required tables if not already loaded
        try:
            if "POP_EVAL" not in self.tables:
                self.load_table("POP_EVAL")
            if "POP_EVAL_TYP" not in self.tables:
                self.load_table("POP_EVAL_TYP")

            # Get the data
            pop_eval = self.tables["POP_EVAL"].collect()
            pop_eval_typ = self.tables["POP_EVAL_TYP"].collect()

            # Check if EVALID exists in POP_EVAL
            if "EVALID" not in pop_eval.columns:
                raise ValueError(
                    f"EVALID column not found in POP_EVAL table. Available columns: {pop_eval.columns}"
                )

            # Join on CN = EVAL_CN
            df = pop_eval.join(
                pop_eval_typ, left_on="CN", right_on="EVAL_CN", how="left"
            )
        except Exception as e:
            # If tables don't exist or join fails, return empty list with warning
            warnings.warn(f"Could not load evaluation tables: {e}")
            return []

        # Apply filters
        if state is not None:
            if isinstance(state, int):
                state = [state]
            df = df.filter(pl.col("STATECD").is_in(state))

        if year is not None:
            if isinstance(year, int):
                year = [year]
            df = df.filter(pl.col("END_INVYR").is_in(year))

        if eval_type is not None:
            # FIA uses 'EXP' prefix for evaluation types
            # Special case: "ALL" maps to "EXPALL" for area estimation
            if eval_type.upper() == "ALL":
                eval_type_full = "EXPALL"
            else:
                eval_type_full = f"EXP{eval_type}"
            df = df.filter(pl.col("EVAL_TYP") == eval_type_full)

        if most_recent:
            # Add parsed EVALID columns for robust year sorting
            df = _add_parsed_evalid_columns(df)  # type: ignore[assignment]

            # Special handling for Texas (STATECD=48)
            # Texas has separate East/West evaluations, but we want the full state
            # Prefer evaluations with "Texas" (not "Texas(EAST)" or "Texas(West)")
            df_texas = df.filter(pl.col("STATECD") == 48)
            df_other = df.filter(pl.col("STATECD") != 48)

            if not df_texas.is_empty():
                # For Texas, prefer full state evaluations over regional ones
                # Check LOCATION_NM to identify full state vs regional
                if "LOCATION_NM" in df_texas.columns:
                    # Mark full state evaluations (just "Texas" without parentheses)
                    df_texas = df_texas.with_columns(
                        pl.when(pl.col("LOCATION_NM") == "Texas")
                        .then(1)
                        .otherwise(0)
                        .alias("IS_FULL_STATE")
                    )
                    # Sort using parsed year for robust chronological ordering
                    df_texas = (
                        df_texas.sort(
                            ["EVAL_TYP", "IS_FULL_STATE", "EVALID_YEAR", "EVALID_TYPE"],
                            descending=[False, True, True, False],
                        )
                        .group_by(["STATECD", "EVAL_TYP"])
                        .first()
                        .drop(
                            [
                                "IS_FULL_STATE",
                                "EVALID_YEAR",
                                "EVALID_STATE",
                                "EVALID_TYPE",
                            ]
                        )
                    )
                else:
                    # Fallback if LOCATION_NM not available - use parsed year
                    df_texas = (
                        df_texas.sort(
                            ["STATECD", "EVAL_TYP", "EVALID_YEAR", "EVALID_TYPE"],
                            descending=[False, False, True, False],
                        )
                        .group_by(["STATECD", "EVAL_TYP"])
                        .first()
                        .drop(["EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
                    )

            # For other states, use robust year sorting
            if not df_other.is_empty():
                df_other = (
                    df_other.sort(
                        ["STATECD", "EVAL_TYP", "EVALID_YEAR", "EVALID_TYPE"],
                        descending=[False, False, True, False],
                    )
                    .group_by(["STATECD", "EVAL_TYP"])
                    .first()
                    .drop(["EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
                )

            # Combine Texas and other states
            df_list = []
            if not df_texas.is_empty():
                df_list.append(df_texas)
            if not df_other.is_empty():
                df_list.append(df_other)

            if df_list:
                df = pl.concat(df_list)

        # Extract unique EVALIDs
        evalids = df.select("EVALID").unique().sort("EVALID")["EVALID"].to_list()

        return evalids

    def clip_by_evalid(self, evalid: Union[int, List[int]]) -> "FIA":
        """
        Filter FIA data by EVALID (evaluation ID).

        This is the core filtering method that ensures statistically valid
        plot groupings by evaluation.

        Parameters
        ----------
        evalid : int or list of int
            Single EVALID or list of EVALIDs to filter by.

        Returns
        -------
        FIA
            Self for method chaining.
        """
        if isinstance(evalid, int):
            evalid = [evalid]

        self.evalid = evalid
        # Clear plot CN cache when EVALID changes
        self._valid_plot_cns = None
        # Clear loaded tables to ensure they use the new filter
        self.tables.clear()
        return self

    def clip_by_state(
        self,
        state: Union[int, List[int]],
        most_recent: bool = True,
        eval_type: Optional[str] = "ALL",
    ) -> "FIA":
        """
        Filter FIA data by state code(s).

        This method efficiently filters data at the database level by:
        1. Setting a state filter for direct table queries
        2. Finding appropriate EVALIDs for the state(s)
        3. Combining both filters for optimal performance

        Parameters
        ----------
        state : int or list of int
            Single state FIPS code or list of codes.
        most_recent : bool, default True
            If True, use only most recent evaluations.
        eval_type : str, optional, default 'ALL'
            Evaluation type to use. Default 'ALL' for EXPALL which is
            appropriate for area estimation. Use None to get all types.

        Returns
        -------
        FIA
            Self for method chaining.
        """
        if isinstance(state, int):
            state = [state]

        self.state_filter = state

        # Find EVALIDs for proper statistical grouping
        if eval_type is not None:
            # Get specific evaluation type (e.g., "ALL" for EXPALL)
            evalids = self.find_evalid(
                state=state, most_recent=most_recent, eval_type=eval_type
            )
            if evalids:
                # Use only the first EVALID to ensure single evaluation
                self.clip_by_evalid([evalids[0]] if len(evalids) > 1 else evalids)
        else:
            # Get all evaluation types (old behavior - can cause overcounting)
            evalids = self.find_evalid(state=state, most_recent=most_recent)
            if evalids:
                self.clip_by_evalid(evalids)

        return self

    def clip_most_recent(self, eval_type: str = "VOL") -> "FIA":
        """
        Filter to most recent evaluation of specified type.

        Parameters
        ----------
        eval_type : str, default 'VOL'
            Evaluation type ('VOL' for volume, 'GRM' for growth, etc.).

        Returns
        -------
        FIA
            Self for method chaining.
        """
        self.most_recent = True
        # Include state filter if it exists
        state_filter = getattr(self, "state_filter", None)
        evalids = self.find_evalid(
            most_recent=True,
            eval_type=eval_type,
            state=state_filter,  # Pass state filter to find_evalid
        )

        if not evalids:
            warnings.warn(f"No evaluations found for type {eval_type}")
            return self

        # When most_recent is True, we get one EVALID per state
        # This is correct - we want the most recent evaluation for EACH state
        return self.clip_by_evalid(evalids)

    def get_plots(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get PLOT table filtered by current EVALID and state settings.

        Parameters
        ----------
        columns : list of str, optional
            Columns to return. Returns all columns if None.

        Returns
        -------
        pl.DataFrame
            Filtered PLOT dataframe.
        """
        # Load PLOT table if needed (with state filter applied)
        if "PLOT" not in self.tables:
            self.load_table("PLOT")

        # If we have EVALID filter, we need to join with assignments
        if self.evalid:
            # Load assignment table with EVALID filter directly
            evalid_str = ", ".join(str(e) for e in self.evalid)
            ppsa = self._reader.read_table(
                "POP_PLOT_STRATUM_ASSGN",
                columns=["PLT_CN", "STRATUM_CN", "EVALID"],
                where=f"EVALID IN ({evalid_str})",
                lazy=True,
            )

            # Filter plots to those in the evaluation
            plots = self.tables["PLOT"].join(
                ppsa.select(["PLT_CN", "EVALID"]).unique(),
                left_on="CN",
                right_on="PLT_CN",
                how="inner",
            )
        else:
            plots = self.tables["PLOT"]

        # Select columns if specified
        if columns:
            plots = plots.select(columns)

        # Materialize results
        plots_df = plots.collect()

        # Ensure PLT_CN is always available for downstream joins
        if "PLT_CN" not in plots_df.columns and "CN" in plots_df.columns:
            plots_df = plots_df.with_columns(pl.col("CN").alias("PLT_CN"))

        return plots_df

    def get_trees(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get TREE table filtered by current EVALID and state settings.

        Parameters
        ----------
        columns : list of str, optional
            Columns to return. Returns all columns if None.

        Returns
        -------
        pl.DataFrame
            Filtered TREE dataframe.
        """
        # Load TREE table if needed (with state filter applied)
        if "TREE" not in self.tables:
            self.load_table("TREE")

        # If we need additional filtering by plot CNs
        if self.evalid:
            # Get plot CNs efficiently
            plot_query = self.tables["PLOT"].select("CN")
            if self.evalid:
                evalid_str = ", ".join(str(e) for e in self.evalid)
                ppsa = self._reader.read_table(
                    "POP_PLOT_STRATUM_ASSGN",
                    columns=["PLT_CN"],
                    where=f"EVALID IN ({evalid_str})",
                    lazy=True,
                ).unique()
                plot_query = plot_query.join(
                    ppsa, left_on="CN", right_on="PLT_CN", how="inner"
                )

            # Filter trees to those plots
            trees = self.tables["TREE"].join(
                plot_query.select("CN"), left_on="PLT_CN", right_on="CN", how="inner"
            )
        else:
            trees = self.tables["TREE"]

        # Select columns if specified
        if columns:
            trees = trees.select(columns)

        return trees.collect()

    def get_conditions(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get COND table filtered by current EVALID and state settings.

        Parameters
        ----------
        columns : list of str, optional
            Columns to return. Returns all columns if None.

        Returns
        -------
        pl.DataFrame
            Filtered COND dataframe.
        """
        # Load COND table if needed (with state filter applied)
        if "COND" not in self.tables:
            self.load_table("COND")

        # If we need additional filtering by plot CNs
        if self.evalid:
            # Get plot CNs efficiently
            plot_query = self.tables["PLOT"].select("CN")
            if self.evalid:
                evalid_str = ", ".join(str(e) for e in self.evalid)
                ppsa = self._reader.read_table(
                    "POP_PLOT_STRATUM_ASSGN",
                    columns=["PLT_CN"],
                    where=f"EVALID IN ({evalid_str})",
                    lazy=True,
                ).unique()
                plot_query = plot_query.join(
                    ppsa, left_on="CN", right_on="PLT_CN", how="inner"
                )

            # Filter conditions to those plots
            conds = self.tables["COND"].join(
                plot_query.select("CN"), left_on="PLT_CN", right_on="CN", how="inner"
            )
        else:
            conds = self.tables["COND"]

        # Select columns if specified
        if columns:
            conds = conds.select(columns)

        return conds.collect()

    def prepare_estimation_data(self) -> Dict[str, pl.DataFrame]:
        """
        Prepare standard set of tables for estimation functions.

        This method loads and filters the core tables needed for most
        FIA estimation procedures, properly filtered by EVALID.

        Returns
        -------
        Dict[str, pl.DataFrame]
            Dictionary with filtered dataframes for estimation containing:
            'plot', 'tree', 'cond', 'pop_plot_stratum_assgn',
            'pop_stratum', 'pop_estn_unit'.
        """
        # Ensure we have an EVALID filter
        if not self.evalid and not self.most_recent:
            warnings.warn("No EVALID filter set. Using most recent volume evaluation.")
            self.clip_most_recent(eval_type="VOL")

        # Load population tables
        if "POP_STRATUM" not in self.tables:
            self.load_table("POP_STRATUM")
        if "POP_PLOT_STRATUM_ASSGN" not in self.tables:
            self.load_table("POP_PLOT_STRATUM_ASSGN")
        if "POP_ESTN_UNIT" not in self.tables:
            self.load_table("POP_ESTN_UNIT")

        # Get filtered core tables
        plots = self.get_plots()
        trees = self.get_trees()
        conds = self.get_conditions()

        # Get stratum assignments for filtered plots
        plot_cns = plots["CN"].to_list()
        if self.evalid is None:
            raise ValueError("No EVALID specified or found")
        ppsa = (
            self.tables["POP_PLOT_STRATUM_ASSGN"]
            .filter(pl.col("PLT_CN").is_in(plot_cns))
            .filter(pl.col("EVALID").is_in(self.evalid))
            .collect()
        )

        # Get strata for these assignments
        stratum_cns = ppsa["STRATUM_CN"].unique().to_list()
        pop_stratum = (
            self.tables["POP_STRATUM"].filter(pl.col("CN").is_in(stratum_cns)).collect()
        )

        # Get estimation units
        estn_unit_cns = pop_stratum["ESTN_UNIT_CN"].unique().to_list()
        pop_estn_unit = (
            self.tables["POP_ESTN_UNIT"]
            .filter(pl.col("CN").is_in(estn_unit_cns))
            .collect()
        )

        return {
            "plot": plots,
            "tree": trees,
            "cond": conds,
            "pop_plot_stratum_assgn": ppsa,
            "pop_stratum": pop_stratum,
            "pop_estn_unit": pop_estn_unit,
        }

    def tpa(self, **kwargs) -> pl.DataFrame:
        """
        Estimate trees per acre.

        See tpa() function for full parameter documentation.
        """
        from pyfia.estimation.tpa import tpa

        result: pl.DataFrame = tpa(self, **kwargs)
        return result

    def biomass(self, **kwargs) -> pl.DataFrame:
        """
        Estimate biomass.

        See biomass() function for full parameter documentation.
        """
        from pyfia.estimation.biomass import biomass

        result: pl.DataFrame = biomass(self, **kwargs)
        return result

    def volume(self, **kwargs) -> pl.DataFrame:
        """
        Estimate volume.

        See volume() function for full parameter documentation.
        """
        from pyfia.estimation.volume import volume

        result: pl.DataFrame = volume(self, **kwargs)
        return result

    def mortality(self, **kwargs) -> pl.DataFrame:
        """
        Estimate mortality.

        See mortality() function for full parameter documentation.
        """
        from pyfia.estimation.estimators.mortality import mortality

        return mortality(self, **kwargs)

    def area(self, **kwargs) -> pl.DataFrame:
        """
        Estimate forest area.

        See area() function for full parameter documentation.
        """
        from pyfia.estimation.estimators.area import area

        return area(self, **kwargs)

    def growth(self, **kwargs) -> pl.DataFrame:
        """
        Estimate annual growth.

        See growth() function for full parameter documentation.
        """
        from pyfia.estimation.estimators.growth import growth

        return growth(self, **kwargs)

    def removals(self, **kwargs) -> pl.DataFrame:
        """
        Estimate annual removals/harvest.

        See removals() function for full parameter documentation.
        """
        from pyfia.estimation.estimators.removals import removals

        return removals(self, **kwargs)

    def carbon_flux(self, **kwargs) -> pl.DataFrame:
        """
        Estimate annual net carbon flux.

        Calculates net carbon sequestration as:
            Net Carbon Flux = Growth_carbon - Mortality_carbon - Removals_carbon

        Positive values indicate net carbon sequestration (carbon sink).
        Negative values indicate net carbon emission (carbon source).

        See carbon_flux() function for full parameter documentation.
        """
        from pyfia.estimation.estimators.carbon_flux import carbon_flux

        return carbon_flux(self, **kwargs)
