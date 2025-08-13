"""
Data validation utilities for FIA database conversion.

This module provides comprehensive validation of converted FIA databases,
including schema validation, referential integrity checks, data completeness,
and statistical validation against source databases.
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import duckdb
import polars as pl

from .models import (
    STANDARD_FIA_TABLES,
    IntegrityError,
    ValidationError,
    ValidationLevel,
    ValidationResult,
)

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Validates data integrity during and after FIA database conversion.

    Features:
    - Schema validation against FIA standards
    - Referential integrity checks
    - Data completeness validation
    - Statistical validation
    - Cross-database comparison
    """

    # Required columns for key FIA tables
    REQUIRED_COLUMNS = {
        "PLOT": {
            "CN", "STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR",
            "LAT", "LON"  # PLOT_STATUS_CD can be null in real FIA data
        },
        "TREE": {
            "CN", "PLT_CN", "STATUSCD", "SPCD"
        },
        "COND": {
            "CN", "PLT_CN", "CONDID", "COND_STATUS_CD", "CONDPROP_UNADJ"
        },
        "POP_EVAL": {
            "CN", "EVALID", "STATECD", "END_INVYR"
        },
        "POP_PLOT_STRATUM_ASSGN": {
            "CN", "PLT_CN", "EVALID", "STRATUM_CN"
        },
        "POP_STRATUM": {
            "CN", "EVALID", "EXPNS"
        }
    }

    # Expected data ranges for validation
    DATA_RANGES = {
        "STATECD": (1, 56),
        "COUNTYCD": (1, 999),
        "INVYR": (1930, 2040),  # FIA historical data goes back to 1930s, extends to future inventories
        "DIA": (0, 200),  # Diameter in inches
        "HT": (0, 500),  # Height in feet
        "LAT": (15, 72),  # US latitude range
        "LON": (-180, -60),  # US longitude range (negative values)
        "TPA_UNADJ": (0, 150),  # Trees per acre (adjusted for real data range)
        "CONDPROP_UNADJ": (0, 1),  # Condition proportion
    }

    # Required referential integrity constraints
    FOREIGN_KEY_CONSTRAINTS = [
        ("TREE", "PLT_CN", "PLOT", "CN"),
        ("COND", "PLT_CN", "PLOT", "CN"),
        ("SUBPLOT", "PLT_CN", "PLOT", "CN"),
        ("POP_PLOT_STRATUM_ASSGN", "PLT_CN", "PLOT", "CN"),
        ("POP_PLOT_STRATUM_ASSGN", "STRATUM_CN", "POP_STRATUM", "CN"),
        ("POP_PLOT_STRATUM_ASSGN", "EVALID", "POP_EVAL", "EVALID"),
        ("POP_STRATUM", "EVALID", "POP_EVAL", "EVALID"),
    ]

    def __init__(self):
        """Initialize the data validator."""
        self.validation_cache = {}

    def validate_database(
        self,
        db_path: Path,
        validation_level: ValidationLevel = ValidationLevel.STANDARD,
        source_db_path: Optional[Path] = None
    ) -> ValidationResult:
        """
        Validate a converted FIA database.

        Parameters
        ----------
        db_path : Path
            Path to DuckDB database to validate
        validation_level : ValidationLevel
            Level of validation to perform
        source_db_path : Path, optional
            Path to source SQLite database for comparison

        Returns
        -------
        ValidationResult
            Validation results with errors and warnings
        """
        start_time = datetime.now()
        logger.info(f"Starting validation of {db_path} at level {validation_level.value}")

        result = ValidationResult(
            is_valid=True,
            validation_time=start_time,
            validation_duration_seconds=0.0
        )

        try:
            with duckdb.connect(str(db_path), read_only=True) as conn:
                # Get list of tables
                tables = self._get_table_list(conn)
                result.tables_validated = len(tables)

                # Perform validation based on level
                if validation_level in [ValidationLevel.BASIC, ValidationLevel.STANDARD, ValidationLevel.COMPREHENSIVE]:
                    self._validate_schema(conn, tables, result)

                if validation_level in [ValidationLevel.STANDARD, ValidationLevel.COMPREHENSIVE]:
                    self._validate_data_integrity(conn, tables, result)
                    self._validate_data_completeness(conn, tables, result)

                if validation_level == ValidationLevel.COMPREHENSIVE:
                    self._validate_referential_integrity(conn, tables, result)
                    self._validate_statistical_consistency(conn, tables, result)

                    # Compare with source if provided
                    if source_db_path and source_db_path.exists():
                        self._validate_against_source(conn, source_db_path, result)

        except Exception as e:
            logger.error(f"Validation failed with exception: {e}")
            result.add_error(ValidationError(
                error_type="validation_exception",
                message=f"Validation failed: {str(e)}",
                severity="error"
            ))

        # Finalize results
        end_time = datetime.now()
        result.validation_duration_seconds = (end_time - start_time).total_seconds()

        logger.info(
            f"Validation completed: {len(result.errors)} errors, "
            f"{len(result.warnings)} warnings in "
            f"{result.validation_duration_seconds:.2f} seconds"
        )

        return result

    def validate_schema(
        self,
        df: pl.DataFrame,
        expected_schema: Dict[str, str],
        table_name: str = "unknown"
    ) -> ValidationResult:
        """
        Validate dataframe schema against expected structure.

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to validate
        expected_schema : Dict[str, str]
            Expected column names and types
        table_name : str
            Name of the table for error reporting

        Returns
        -------
        ValidationResult
            Schema validation results
        """
        start_time = datetime.now()
        result = ValidationResult(
            is_valid=True,
            validation_time=start_time,
            validation_duration_seconds=0.0
        )

        # Check for missing columns
        missing_columns = set(expected_schema.keys()) - set(df.columns)
        for col in missing_columns:
            result.add_error(ValidationError(
                error_type="missing_column",
                table_name=table_name,
                column_name=col,
                message=f"Required column '{col}' missing from table '{table_name}'",
                severity="error"
            ))

        # Check for unexpected columns
        unexpected_columns = set(df.columns) - set(expected_schema.keys())
        for col in unexpected_columns:
            result.add_error(ValidationError(
                error_type="unexpected_column",
                table_name=table_name,
                column_name=col,
                message=f"Unexpected column '{col}' found in table '{table_name}'",
                severity="warning"
            ))

        # Check data types (simplified)
        for col in df.columns:
            if col in expected_schema:
                actual_type = str(df[col].dtype)
                expected_type = expected_schema[col]

                # Basic type compatibility check
                if not self._types_compatible(actual_type, expected_type):
                    result.add_error(ValidationError(
                        error_type="type_mismatch",
                        table_name=table_name,
                        column_name=col,
                        message=f"Column '{col}' has type '{actual_type}', expected '{expected_type}'",
                        severity="warning"
                    ))

        result.validation_duration_seconds = (datetime.now() - start_time).total_seconds()
        return result

    def check_referential_integrity(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str]
    ) -> List[IntegrityError]:
        """
        Check referential integrity constraints.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Database connection
        tables : List[str]
            List of table names

        Returns
        -------
        List[IntegrityError]
            List of integrity violations
        """
        integrity_errors = []

        for child_table, child_col, parent_table, parent_col in self.FOREIGN_KEY_CONSTRAINTS:
            if child_table in tables and parent_table in tables:
                try:
                    # Check for orphaned references
                    query = f"""
                    SELECT COUNT(*) as violation_count
                    FROM {child_table} c
                    LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                    WHERE c.{child_col} IS NOT NULL AND p.{parent_col} IS NULL
                    """

                    result = conn.execute(query).fetchone()
                    violation_count = result[0] if result else 0

                    if violation_count > 0:
                        # Get example values
                        example_query = f"""
                        SELECT DISTINCT c.{child_col}
                        FROM {child_table} c
                        LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                        WHERE c.{child_col} IS NOT NULL AND p.{parent_col} IS NULL
                        LIMIT 5
                        """

                        examples = conn.execute(example_query).fetchall()
                        example_values = [str(row[0]) for row in examples]

                        integrity_errors.append(IntegrityError(
                            constraint_type="foreign_key",
                            parent_table=parent_table,
                            child_table=child_table,
                            parent_column=parent_col,
                            child_column=child_col,
                            violation_count=violation_count,
                            example_values=example_values
                        ))

                except Exception as e:
                    logger.debug(f"Failed to check FK constraint {child_table}.{child_col} -> {parent_table}.{parent_col}: {e}")

        return integrity_errors

    def validate_statistics(
        self,
        source_stats: Dict[str, Any],
        target_stats: Dict[str, Any],
        tolerance: float = 0.05
    ) -> bool:
        """
        Validate that statistical measures are consistent between source and target.

        Parameters
        ----------
        source_stats : Dict[str, Any]
            Statistics from source database
        target_stats : Dict[str, Any]
            Statistics from target database
        tolerance : float
            Acceptable relative difference (5% by default)

        Returns
        -------
        bool
            True if statistics are consistent within tolerance
        """
        for stat_name, source_value in source_stats.items():
            if stat_name in target_stats:
                target_value = target_stats[stat_name]

                if isinstance(source_value, (int, float)) and isinstance(target_value, (int, float)):
                    if source_value == 0:
                        # Handle zero values
                        if abs(target_value) > tolerance:
                            logger.warning(f"Statistic {stat_name}: source=0, target={target_value}")
                            return False
                    else:
                        # Calculate relative difference
                        rel_diff = abs(target_value - source_value) / abs(source_value)
                        if rel_diff > tolerance:
                            logger.warning(
                                f"Statistic {stat_name} differs by {rel_diff:.2%}: "
                                f"source={source_value}, target={target_value}"
                            )
                            return False

        return True

    def _validate_schema(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str],
        result: ValidationResult
    ) -> None:
        """Validate database schema structure."""
        for table_name in tables:
            if table_name in STANDARD_FIA_TABLES:
                try:
                    # Get table schema
                    schema_query = f"DESCRIBE {table_name}"
                    schema_result = conn.execute(schema_query).fetchall()

                    actual_columns = {row[0] for row in schema_result}

                    # Check required columns
                    if table_name in self.REQUIRED_COLUMNS:
                        required_columns = self.REQUIRED_COLUMNS[table_name]
                        missing_columns = required_columns - actual_columns

                        for col in missing_columns:
                            result.add_error(ValidationError(
                                error_type="missing_required_column",
                                table_name=table_name,
                                column_name=col,
                                message=f"Required column '{col}' missing from table '{table_name}'",
                                severity="error"
                            ))

                except Exception as e:
                    result.add_error(ValidationError(
                        error_type="schema_check_failed",
                        table_name=table_name,
                        message=f"Failed to check schema for table '{table_name}': {str(e)}",
                        severity="error"
                    ))

    def _validate_data_integrity(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str],
        result: ValidationResult
    ) -> None:
        """Validate data integrity constraints."""
        for table_name in tables:
            if table_name not in STANDARD_FIA_TABLES:
                continue

            try:
                # Check for null values in key columns
                if table_name in self.REQUIRED_COLUMNS:
                    for col in self.REQUIRED_COLUMNS[table_name]:
                        null_query = f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
                        null_count = conn.execute(null_query).fetchone()[0]

                        if null_count > 0:
                            result.add_error(ValidationError(
                                error_type="null_values",
                                table_name=table_name,
                                column_name=col,
                                message=f"Found {null_count} null values in required column '{col}'",
                                severity="error",
                                record_count=null_count
                            ))

                # Check data ranges
                self._check_data_ranges(conn, table_name, result)

                # Check for duplicate primary keys
                if table_name != "REF_SPECIES":  # Reference tables may have duplicates
                    self._check_primary_key_duplicates(conn, table_name, result)

            except Exception as e:
                result.add_error(ValidationError(
                    error_type="data_integrity_check_failed",
                    table_name=table_name,
                    message=f"Failed to check data integrity for table '{table_name}': {str(e)}",
                    severity="error"
                ))

    def _validate_data_completeness(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str],
        result: ValidationResult
    ) -> None:
        """Validate data completeness."""
        for table_name in tables:
            if table_name not in STANDARD_FIA_TABLES:
                continue

            try:
                # Check if table is empty
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                row_count = conn.execute(count_query).fetchone()[0]
                result.records_validated += row_count

                if row_count == 0:
                    result.add_error(ValidationError(
                        error_type="empty_table",
                        table_name=table_name,
                        message=f"Table '{table_name}' is empty",
                        severity="warning"
                    ))
                    continue

                # Check for reasonable data distribution
                if "STATECD" in self._get_table_columns(conn, table_name):
                    state_query = f"SELECT COUNT(DISTINCT STATECD) FROM {table_name}"
                    state_count = conn.execute(state_query).fetchone()[0]

                    if state_count == 0:
                        result.add_error(ValidationError(
                            error_type="no_state_data",
                            table_name=table_name,
                            message=f"Table '{table_name}' has no state data",
                            severity="error"
                        ))

            except Exception as e:
                result.add_error(ValidationError(
                    error_type="completeness_check_failed",
                    table_name=table_name,
                    message=f"Failed to check completeness for table '{table_name}': {str(e)}",
                    severity="error"
                ))

    def _validate_referential_integrity(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str],
        result: ValidationResult
    ) -> None:
        """Validate referential integrity constraints."""
        integrity_errors = self.check_referential_integrity(conn, tables)
        result.referential_integrity_checks = len(self.FOREIGN_KEY_CONSTRAINTS)

        for error in integrity_errors:
            result.add_error(ValidationError(
                error_type="referential_integrity",
                table_name=error.child_table,
                column_name=error.child_column,
                message=str(error),
                severity="error",
                record_count=error.violation_count
            ))

    def _validate_statistical_consistency(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str],
        result: ValidationResult
    ) -> None:
        """Validate statistical consistency of the data."""
        try:
            # Basic statistical checks
            for table_name in ["PLOT", "TREE", "COND"]:
                if table_name not in tables:
                    continue

                # Check for reasonable plot distribution
                if table_name == "PLOT":
                    stats_query = """
                    SELECT
                        COUNT(*) as total_plots,
                        COUNT(DISTINCT STATECD) as state_count,
                        MIN(INVYR) as min_year,
                        MAX(INVYR) as max_year
                    FROM PLOT
                    """

                    stats = conn.execute(stats_query).fetchone()
                    total_plots, state_count, min_year, max_year = stats

                    if total_plots < 100:
                        result.add_error(ValidationError(
                            error_type="insufficient_data",
                            table_name="PLOT",
                            message=f"Only {total_plots} plots found, expected more for valid analysis",
                            severity="warning"
                        ))

                    if max_year and min_year and (max_year - min_year) > 50:
                        result.add_error(ValidationError(
                            error_type="suspicious_year_range",
                            table_name="PLOT",
                            message=f"Year range {min_year}-{max_year} seems unusually large",
                            severity="warning"
                        ))

        except Exception as e:
            result.add_error(ValidationError(
                error_type="statistical_check_failed",
                message=f"Failed to check statistical consistency: {str(e)}",
                severity="error"
            ))

    def _validate_against_source(
        self,
        duck_conn: duckdb.DuckDBPyConnection,
        source_db_path: Path,
        result: ValidationResult
    ) -> None:
        """Validate converted database against source SQLite database."""
        try:
            sqlite_conn = sqlite3.connect(str(source_db_path))

            # Compare record counts for key tables
            for table_name in ["PLOT", "TREE", "COND"]:
                try:
                    # Get DuckDB count
                    duck_count = duck_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

                    # Get SQLite count
                    sqlite_cursor = sqlite_conn.cursor()
                    sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    sqlite_count = sqlite_cursor.fetchone()[0]

                    # Allow for small differences due to filtering
                    if abs(duck_count - sqlite_count) > max(sqlite_count * 0.01, 10):
                        result.add_error(ValidationError(
                            error_type="record_count_mismatch",
                            table_name=table_name,
                            message=(
                                f"Record count mismatch in {table_name}: "
                                f"source={sqlite_count}, target={duck_count}"
                            ),
                            severity="warning"
                        ))

                except Exception as e:
                    logger.debug(f"Failed to compare counts for {table_name}: {e}")

            sqlite_conn.close()

        except Exception as e:
            result.add_error(ValidationError(
                error_type="source_comparison_failed",
                message=f"Failed to compare with source database: {str(e)}",
                severity="error"
            ))

    def _check_data_ranges(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        result: ValidationResult
    ) -> None:
        """Check if data values are within expected ranges."""
        table_columns = self._get_table_columns(conn, table_name)

        for col_name, (min_val, max_val) in self.DATA_RANGES.items():
            if col_name in table_columns:
                try:
                    out_of_range_query = f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE {col_name} < {min_val} OR {col_name} > {max_val}
                    """

                    out_of_range_count = conn.execute(out_of_range_query).fetchone()[0]

                    if out_of_range_count > 0:
                        result.add_error(ValidationError(
                            error_type="out_of_range_values",
                            table_name=table_name,
                            column_name=col_name,
                            message=(
                                f"Found {out_of_range_count} values outside expected range "
                                f"[{min_val}, {max_val}] in column '{col_name}'"
                            ),
                            severity="warning",
                            record_count=out_of_range_count
                        ))

                except Exception as e:
                    logger.debug(f"Failed to check range for {table_name}.{col_name}: {e}")

    def _check_primary_key_duplicates(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        result: ValidationResult
    ) -> None:
        """Check for duplicate primary key values."""
        if "CN" not in self._get_table_columns(conn, table_name):
            return

        try:
            duplicate_query = f"""
            SELECT COUNT(*) FROM (
                SELECT CN, COUNT(*) as cnt
                FROM {table_name}
                GROUP BY CN
                HAVING cnt > 1
            )
            """

            duplicate_count = conn.execute(duplicate_query).fetchone()[0]

            if duplicate_count > 0:
                result.add_error(ValidationError(
                    error_type="duplicate_primary_keys",
                    table_name=table_name,
                    column_name="CN",
                    message=f"Found {duplicate_count} duplicate primary keys in table '{table_name}'",
                    severity="error",
                    record_count=duplicate_count
                ))

        except Exception as e:
            logger.debug(f"Failed to check for duplicate PKs in {table_name}: {e}")

    def _get_table_list(self, conn: duckdb.DuckDBPyConnection) -> List[str]:
        """Get list of tables in the database."""
        try:
            result = conn.execute("SHOW TABLES").fetchall()
            return [row[0] for row in result if not row[0].startswith('__')]
        except Exception as e:
            logger.error(f"Failed to get table list: {e}")
            return []

    def _get_table_columns(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> Set[str]:
        """Get set of column names for a table."""
        try:
            result = conn.execute(f"DESCRIBE {table_name}").fetchall()
            return {row[0] for row in result}
        except Exception as e:
            logger.debug(f"Failed to get columns for {table_name}: {e}")
            return set()

    def _types_compatible(self, actual_type: str, expected_type: str) -> bool:
        """
        Check if actual and expected data types are compatible.

        This is a simplified type compatibility check.
        """
        # Normalize type names
        actual = actual_type.upper()
        expected = expected_type.upper()

        # Direct match
        if actual == expected:
            return True

        # Integer type compatibility
        if "INT" in expected and "INT" in actual:
            return True

        # Float/decimal compatibility
        if any(t in expected for t in ["FLOAT", "DOUBLE", "DECIMAL"]) and \
           any(t in actual for t in ["FLOAT", "DOUBLE", "DECIMAL"]):
            return True

        # String type compatibility
        if any(t in expected for t in ["VARCHAR", "TEXT", "STRING"]) and \
           any(t in actual for t in ["VARCHAR", "TEXT", "STRING"]):
            return True

        return False
