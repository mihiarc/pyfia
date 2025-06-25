"""
Database schema mapper for handling differences between SQLite and DuckDB FIA databases.

This module provides mappings and utilities to handle naming and type differences
between different FIA database implementations.
"""

from typing import Optional


class SchemaMapper:
    """Maps table and column names between different database schemas."""

    # Table name mappings (SQLite/standard -> DuckDB)
    TABLE_MAPPINGS = {
        "PLOT": "plot",
        "TREE": "tree",
        "COND": "cond",
        "POP_EVAL": "pop_eval_grp",  # DuckDB uses pop_eval_grp instead
        "POP_STRATUM": None,  # Not available in this DuckDB version
        "POP_ESTN_UNIT": "POP_ESTN_UNIT",
        "POP_PLOT_STRATUM_ASSGN": "POP_PLOT_STRATUM_ASSGN",
        "REF_SPECIES": "ref_species",
        "REF_FOREST_TYPE": "ref_forest_type",
        "TREE_GRM_ESTN": "tree_grm_estn",
    }

    # Reverse mapping (DuckDB -> SQLite/standard)
    REVERSE_TABLE_MAPPINGS = {
        "plot": "PLOT",
        "tree": "TREE",
        "cond": "COND",
        "pop_eval_grp": "POP_EVAL",
        "POP_ESTN_UNIT": "POP_ESTN_UNIT",
        "POP_PLOT_STRATUM_ASSGN": "POP_PLOT_STRATUM_ASSGN",
        "ref_species": "REF_SPECIES",
        "ref_forest_type": "REF_FOREST_TYPE",
        "tree_grm_estn": "TREE_GRM_ESTN",
    }

    # Column type differences (table -> column -> expected type)
    COLUMN_TYPE_OVERRIDES = {
        "plot": {
            "CN": "BIGINT",  # DuckDB uses BIGINT instead of VARCHAR
            "SRV_CN": "BIGINT",
            "CTY_CN": "BIGINT",
            "PREV_PLT_CN": "BIGINT",
        },
        "tree": {"CN": "BIGINT", "PLT_CN": "BIGINT", "PREV_TRE_CN": "BIGINT"},
        "cond": {"CN": "BIGINT", "PLT_CN": "BIGINT"},
    }

    def __init__(self, engine: str = "sqlite"):
        """
        Initialize schema mapper.

        Args:
            engine: Database engine ("sqlite" or "duckdb")
        """
        self.engine = engine.lower()

    def get_table_name(self, standard_name: str) -> Optional[str]:
        """
        Get the actual table name for the database engine.

        Args:
            standard_name: Standard FIA table name (usually uppercase)

        Returns:
            Actual table name for the engine, or None if not available
        """
        if self.engine == "sqlite":
            return standard_name
        else:  # duckdb
            return self.TABLE_MAPPINGS.get(standard_name, standard_name)

    def get_standard_name(self, actual_name: str) -> str:
        """
        Get the standard table name from engine-specific name.

        Args:
            actual_name: Engine-specific table name

        Returns:
            Standard FIA table name
        """
        if self.engine == "sqlite":
            return actual_name
        else:  # duckdb
            return self.REVERSE_TABLE_MAPPINGS.get(actual_name, actual_name)

    def is_cn_field_bigint(self, table_name: str, column_name: str) -> bool:
        """
        Check if a CN field should be treated as BIGINT.

        Args:
            table_name: Table name (engine-specific)
            column_name: Column name

        Returns:
            True if the field is BIGINT in DuckDB
        """
        if self.engine == "sqlite":
            return False

        table_overrides = self.COLUMN_TYPE_OVERRIDES.get(table_name, {})
        return table_overrides.get(column_name) == "BIGINT"

    def get_evalid_query(self) -> str:
        """
        Get the appropriate EVALID query for the engine.

        Returns:
            SQL query to get evaluation information
        """
        if self.engine == "sqlite":
            return """
            SELECT
                pe.EVALID,
                pe.EVAL_DESCR,
                pe.STATECD,
                pe.START_INVYR,
                pe.END_INVYR,
                pet.EVAL_TYP,
                COUNT(DISTINCT ppsa.PLT_CN) as plot_count
            FROM POP_EVAL pe
            LEFT JOIN POP_EVAL_TYP pet ON pe.EVALID = pet.EVALID
            LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
            GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD, pe.START_INVYR, pe.END_INVYR, pet.EVAL_TYP
            ORDER BY pe.EVALID DESC
            """
        else:  # duckdb
            # Use pop_eval_grp table structure
            return """
            SELECT
                peg.EVAL_GRP as EVALID,
                peg.EVAL_GRP_DESCR as EVAL_DESCR,
                peg.STATECD,
                peg.RSCD,
                peu.EVALID as ACTUAL_EVALID,
                COUNT(DISTINCT ppsa.PLT_CN) as plot_count
            FROM pop_eval_grp peg
            LEFT JOIN POP_ESTN_UNIT peu ON peg.CN = peu.EVAL_CN
            LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON peu.EVALID = ppsa.EVALID
            GROUP BY peg.EVAL_GRP, peg.EVAL_GRP_DESCR, peg.STATECD, peg.RSCD, peu.EVALID
            ORDER BY peg.EVAL_GRP DESC
            """

    def adapt_query(self, query: str) -> str:
        """
        Adapt a standard SQL query for the specific engine.

        Args:
            query: Standard SQL query with uppercase table names

        Returns:
            Adapted query for the engine
        """
        if self.engine == "sqlite":
            return query

        # For DuckDB, replace table names
        adapted = query
        for standard, actual in self.TABLE_MAPPINGS.items():
            if actual is not None:
                # Replace table names (case-sensitive)
                adapted = adapted.replace(f" {standard} ", f" {actual} ")
                adapted = adapted.replace(f" {standard}.", f" {actual}.")
                adapted = adapted.replace(f"FROM {standard}", f"FROM {actual}")
                adapted = adapted.replace(f"JOIN {standard}", f"JOIN {actual}")

        return adapted
