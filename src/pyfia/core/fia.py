"""
Core FIA database class and functionality for pyFIA.

This module provides the main FIA class that handles database connections,
EVALID-based filtering, and common operations following rFIA patterns.
"""

import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union

import duckdb
import polars as pl

from .data_reader import FIADataReader


class FIA:
    """
    Main FIA database class for working with Forest Inventory and Analysis data.

    This class provides methods for loading FIA data from DuckDB databases,
    filtering by EVALID, and preparing data for estimation functions.

    Attributes:
        db_path (Path): Path to the DuckDB database
        tables (Dict[str, pl.LazyFrame]): Loaded FIA tables as lazy frames
        evalid (Optional[List[int]]): Active EVALID filter
        most_recent (bool): Whether to use most recent evaluations
    """

    def __init__(self, db_path: Union[str, Path], engine: str = "duckdb"):
        """
        Initialize FIA database connection.

        Args:
            db_path: Path to FIA DuckDB database
            engine: Database engine (kept for compatibility, always uses DuckDB)
        """
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")

        # Always use DuckDB regardless of engine parameter
        self.tables: Dict[str, pl.LazyFrame] = {}
        self.evalid: Optional[List[int]] = None
        self.most_recent: bool = False
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._reader = FIADataReader(db_path, engine="duckdb")

    def __enter__(self):
        """Context manager entry."""
        self._conn = duckdb.connect(str(self.db_path), read_only=True)
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get database connection, creating if needed."""
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path), read_only=True)
        return self._conn

    def load_table(
        self, table_name: str, columns: Optional[List[str]] = None
    ) -> pl.LazyFrame:
        """
        Load a table from the FIA database as a lazy frame.

        Args:
            table_name: Name of the FIA table to load
            columns: Optional list of columns to load (None loads all)

        Returns:
            Polars LazyFrame of the table
        """
        # Use the data reader which handles types properly
        df = self._reader.read_table(table_name, columns=columns, lazy=False)

        # Store as lazy frame
        self.tables[table_name] = df.lazy()

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

        This mirrors rFIA's findEVALID function to identify evaluation IDs
        for filtering FIA data.

        Args:
            most_recent: If True, return only most recent evaluations
            state: State code(s) to filter by
            year: End inventory year(s) to filter by
            eval_type: Evaluation type ('VOL', 'GRM', 'CHNG', etc.)

        Returns:
            List of EVALID values matching criteria
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

            # Check if EVALID exists in POP_EVAL, if not we need to handle differently
            if "EVALID" not in pop_eval.columns:
                # Some FIA databases might have different schema
                # Try to extract EVALID from CN or other columns
                raise ValueError(
                    f"EVALID column not found in POP_EVAL table. "
                    f"Available columns: {pop_eval.columns}"
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
            eval_type_full = f"EXP{eval_type}"
            df = df.filter(pl.col("EVAL_TYP") == eval_type_full)

        if most_recent:
            # Group by state and eval type, get max year
            df = (
                df.group_by(["STATECD", "EVAL_TYP"])
                .agg(pl.col("END_INVYR").max())
                .join(df, on=["STATECD", "EVAL_TYP", "END_INVYR"])
            )

        # Extract unique EVALIDs
        evalids = df.select("EVALID").unique().sort("EVALID")["EVALID"].to_list()

        return evalids

    def clip_by_evalid(self, evalid: Union[int, List[int]]) -> "FIA":
        """
        Filter FIA data by EVALID (evaluation ID).

        This is the core filtering method that ensures statistically valid
        plot groupings by evaluation.

        Args:
            evalid: Single EVALID or list of EVALIDs to filter by

        Returns:
            Self for method chaining
        """
        if isinstance(evalid, int):
            evalid = [evalid]

        self.evalid = evalid
        return self

    def clip_most_recent(self, eval_type: str = "VOL") -> "FIA":
        """
        Filter to most recent evaluation of specified type.

        Args:
            eval_type: Evaluation type (default 'VOL' for volume)

        Returns:
            Self for method chaining
        """
        self.most_recent = True
        evalids = self.find_evalid(most_recent=True, eval_type=eval_type)

        if not evalids:
            warnings.warn(f"No evaluations found for type {eval_type}")
            return self

        return self.clip_by_evalid(evalids)

    def get_plots(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get PLOT table filtered by current EVALID settings.

        Args:
            columns: Optional list of columns to return

        Returns:
            Filtered PLOT dataframe
        """
        # Load tables if needed
        if "PLOT" not in self.tables:
            self.load_table("PLOT")
        if "POP_PLOT_STRATUM_ASSGN" not in self.tables:
            self.load_table(
                "POP_PLOT_STRATUM_ASSGN", ["PLT_CN", "STRATUM_CN", "EVALID"]
            )

        # Start with PLOT table
        plots = self.tables["PLOT"]

        # If EVALID filter is active, apply it
        if self.evalid or self.most_recent:
            if self.most_recent and not self.evalid:
                # Find most recent EVALIDs
                self.evalid = self.find_evalid(most_recent=True, eval_type="VOL")

            # Get plot-stratum assignments for these EVALIDs
            if self.evalid is None:
                raise ValueError("No EVALID specified or found")
            ppsa = (
                self.tables["POP_PLOT_STRATUM_ASSGN"]
                .filter(pl.col("EVALID").is_in(self.evalid))
                .select(["PLT_CN", "EVALID"])
                .unique()
            )

            # Filter plots to those in the evaluation
            plots = plots.join(ppsa, left_on="CN", right_on="PLT_CN", how="inner")

        # Select columns if specified
        if columns:
            plots = plots.select(columns)

        return plots.collect()

    def get_trees(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get TREE table filtered by current EVALID settings.

        Args:
            columns: Optional list of columns to return

        Returns:
            Filtered TREE dataframe
        """
        # Get filtered plots
        plot_cns = self.get_plots(["CN"])["CN"].to_list()

        # Load TREE table if needed
        if "TREE" not in self.tables:
            self.load_table("TREE")

        # Filter trees to those on filtered plots
        trees = self.tables["TREE"].filter(pl.col("PLT_CN").is_in(plot_cns))

        # Select columns if specified
        if columns:
            trees = trees.select(columns)

        return trees.collect()

    def get_conditions(self, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Get COND table filtered by current EVALID settings.

        Args:
            columns: Optional list of columns to return

        Returns:
            Filtered COND dataframe
        """
        # Get filtered plots
        plot_cns = self.get_plots(["CN"])["CN"].to_list()

        # Load COND table if needed
        if "COND" not in self.tables:
            self.load_table("COND")

        # Filter conditions to those on filtered plots
        conds = self.tables["COND"].filter(pl.col("PLT_CN").is_in(plot_cns))

        # Select columns if specified
        if columns:
            conds = conds.select(columns)

        return conds.collect()

    def prepare_estimation_data(self) -> Dict[str, pl.DataFrame]:
        """
        Prepare standard set of tables for estimation functions.

        This method loads and filters the core tables needed for most
        FIA estimation procedures, properly filtered by EVALID.

        Returns:
            Dictionary with filtered dataframes for estimation
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

        return tpa(self, **kwargs)

    def biomass(self, **kwargs) -> pl.DataFrame:
        """
        Estimate biomass.

        See biomass() function for full parameter documentation.
        """
        from pyfia.estimation.biomass import biomass

        return biomass(self, **kwargs)

    def volume(self, **kwargs) -> pl.DataFrame:
        """
        Estimate volume.

        See volume() function for full parameter documentation.
        """
        from pyfia.estimation.volume import volume

        return volume(self, **kwargs)

    def mortality(self, **kwargs) -> pl.DataFrame:
        """
        Estimate mortality.

        See mortality() function for full parameter documentation.
        """
        from pyfia.estimation.mortality import mortality

        return mortality(self, **kwargs)

    def area(self, **kwargs) -> pl.DataFrame:
        """
        Estimate forest area.

        See area() function for full parameter documentation.
        """
        from pyfia.estimation.area import area

        return area(self, **kwargs)
