"""
Schema optimization utilities for FIA database conversion.

This module provides tools for optimizing FIA table schemas when converting
from SQLite to DuckDB, including data type optimization, indexing strategies,
partitioning, and compression configuration.
"""

import logging
from typing import Any, Dict, Optional

import duckdb
import polars as pl
from rich.progress import Progress, TaskID

from .models import CompressionLevel, OptimizedSchema
from .schema_loader import get_schema_loader

logger = logging.getLogger(__name__)


class SchemaOptimizer:
    """
    Optimizes FIA table schemas for DuckDB columnar storage.

    Provides data type optimization, indexing strategies, partitioning,
    and compression configuration tailored for FIA data patterns.
    """

    # Optimized data types for common FIA columns
    OPTIMIZED_TYPES = {
        # CN (Control Number) fields - Use BIGINT for better performance
        "CN": "BIGINT",
        "PLT_CN": "BIGINT",
        "PREV_PLT_CN": "BIGINT",
        "TREE_CN": "BIGINT",
        "COND_CN": "BIGINT",
        "SUBP_CN": "BIGINT",
        "EVAL_CN": "BIGINT",
        "STRATUM_CN": "BIGINT",
        "ESTN_UNIT_CN": "BIGINT",

        # State and administrative codes
        "STATECD": "TINYINT",
        "UNITCD": "TINYINT",
        "COUNTYCD": "SMALLINT",
        "PLOT": "INTEGER",
        "SUBPLOT": "TINYINT",

        # Status and classification codes
        "STATUSCD": "TINYINT",
        "COND_STATUS_CD": "TINYINT",
        "PLOT_STATUS_CD": "TINYINT",
        "TREECLCD": "TINYINT",
        "CCLCD": "TINYINT",
        "OWNGRPCD": "TINYINT",
        "RESERVCD": "TINYINT",
        "SITECLCD": "TINYINT",
        "LANDCLCD": "TINYINT",

        # Species and forest type codes
        "SPCD": "SMALLINT",
        "FORTYPCD": "SMALLINT",
        "SPGRPCD": "TINYINT",

        # Measurements with appropriate precision
        "DIA": "DECIMAL(6,2)",
        "HT": "DECIMAL(7,2)",
        "ACTUALHT": "DECIMAL(7,2)",
        "CR": "DECIMAL(4,1)",
        "CDENCD": "DECIMAL(4,1)",

        # Coordinates with sufficient precision
        "LAT": "DECIMAL(9,6)",
        "LON": "DECIMAL(10,6)",
        "ELEV": "DECIMAL(8,2)",

        # Area and proportion fields
        "CONDPROP_UNADJ": "DECIMAL(8,6)",
        "SUBPPROP_UNADJ": "DECIMAL(8,6)",
        "MICRPROP_UNADJ": "DECIMAL(8,6)",
        "MACRPROP_UNADJ": "DECIMAL(8,6)",

        # Biomass and volume measurements
        "DRYBIO_AG": "DECIMAL(10,3)",
        "DRYBIO_BG": "DECIMAL(10,3)",
        "DRYBIO_BOLE": "DECIMAL(10,3)",
        "DRYBIO_STUMP": "DECIMAL(10,3)",
        "DRYBIO_SAPLING": "DECIMAL(10,3)",
        "DRYBIO_WDLD_SPP": "DECIMAL(10,3)",

        "VOLCFNET": "DECIMAL(10,3)",
        "VOLCSNET": "DECIMAL(10,3)",
        "VOLBFNET": "DECIMAL(10,3)",
        "VOLCFGRS": "DECIMAL(10,3)",
        "VOLCSGRS": "DECIMAL(10,3)",
        "VOLBFGRS": "DECIMAL(10,3)",

        # Carbon measurements
        "CARBON_AG": "DECIMAL(10,3)",
        "CARBON_BG": "DECIMAL(10,3)",
        "CARBON_DOWN_DEAD": "DECIMAL(10,3)",
        "CARBON_LITTER": "DECIMAL(10,3)",
        "CARBON_SOIL_ORG": "DECIMAL(10,3)",

        # TPA and expansion factors
        "TPA_UNADJ": "DECIMAL(8,4)",
        "EXPNS": "DECIMAL(12,4)",
        "ADJ_FACTOR_SUBP": "DECIMAL(8,6)",
        "ADJ_FACTOR_MICR": "DECIMAL(8,6)",
        "ADJ_FACTOR_MACR": "DECIMAL(8,6)",

        # Date and time fields
        "INVYR": "SMALLINT",
        "MEASYEAR": "SMALLINT",
        "MEASMON": "TINYINT",
        "MEASDAY": "TINYINT",

        # Evaluation fields
        "EVALID": "INTEGER",
        "EVAL_GRP": "VARCHAR(50)",
        "EVAL_TYP": "VARCHAR(20)",
        "EVAL_DESCR": "VARCHAR(200)",
        "START_INVYR": "SMALLINT",
        "END_INVYR": "SMALLINT",

        # Point counts and statistics
        "P2POINTCNT": "INTEGER",
        "P1POINTCNT": "INTEGER",
        "P1PNTCNT_EU": "INTEGER",
        "AREA_USED": "DECIMAL(15,4)",

        # Agent and damage codes
        "AGENTCD": "TINYINT",
        "DAMAGE_AGENT_CD1": "TINYINT",
        "DAMAGE_AGENT_CD2": "TINYINT",
        "DAMAGE_AGENT_CD3": "TINYINT",
        "DSTRBCD1": "TINYINT",
        "DSTRBCD2": "TINYINT",
        "DSTRBCD3": "TINYINT",

        # Text fields
        "LOCATION_NM": "VARCHAR(100)",
        "COMMON_NAME": "VARCHAR(100)",
        "SCIENTIFIC_NAME": "VARCHAR(100)",
        "GENUS": "VARCHAR(50)",
        "SPECIES": "VARCHAR(50)",
    }

    # Index configurations for FIA tables
    INDEX_CONFIGS = {
        "PLOT": [
            "CN",
            "STATECD",
            "INVYR",
            "PLOT_STATUS_CD",
            "(STATECD, COUNTYCD, PLOT)",
            "(LAT, LON)",  # Spatial index
            "(STATECD, INVYR)"
        ],
        "TREE": [
            "CN",
            "PLT_CN",
            "STATUSCD",
            "SPCD",
            "(PLT_CN, STATUSCD)",
            "(SPCD, STATUSCD)",
            "(PLT_CN, SPCD)",
            "DIA",
            "(STATUSCD, DIA)"
        ],
        "COND": [
            "CN",
            "PLT_CN",
            "COND_STATUS_CD",
            "FORTYPCD",
            "OWNGRPCD",
            "(PLT_CN, COND_STATUS_CD)",
            "(COND_STATUS_CD, FORTYPCD)"
        ],
        "POP_PLOT_STRATUM_ASSGN": [
            "CN",
            "PLT_CN",
            "EVALID",
            "STRATUM_CN",
            "(EVALID, PLT_CN)",
            "(PLT_CN, EVALID)",
            "(STRATUM_CN, EVALID)"
        ],
        "POP_EVAL": [
            "CN",
            "EVALID",
            "STATECD",
            "END_INVYR",
            "(STATECD, END_INVYR)",
            "(EVALID, STATECD)"
        ],
        "POP_STRATUM": [
            "CN",
            "EVALID",
            "(EVALID, STRATUMCD)"
        ],
        "POP_ESTN_UNIT": [
            "CN",
            "EVALID"
        ],
        "REF_SPECIES": [
            "SPCD",
            "COMMON_NAME",
            "SCIENTIFIC_NAME",
            "GENUS"
        ]
    }

    # Note: DuckDB partitioning is handled differently - these are for documentation
    PARTITIONING_CONFIGS = {
        "PLOT": "State-based partitioning recommended",
        "TREE": "State and year partitioning recommended",
        "COND": "State-based partitioning recommended",
        "POP_EVAL": "State-based partitioning recommended",
        "POP_PLOT_STRATUM_ASSGN": "Evaluation-based partitioning recommended",
        "POP_STRATUM": "Evaluation-based partitioning recommended"
    }

    def __init__(self):
        """Initialize the schema optimizer."""
        self.compression_configs = self._get_compression_configs()
        self.schema_loader = get_schema_loader()

    def optimize_table_schema(
        self,
        table_name: str,
        df: pl.DataFrame,
        compression_level: CompressionLevel = CompressionLevel.MEDIUM
    ) -> OptimizedSchema:
        """
        Optimize schema for a specific FIA table.

        Parameters
        ----------
        table_name : str
            Name of the FIA table
        df : pl.DataFrame
            Sample dataframe to analyze
        compression_level : CompressionLevel
            Desired compression level

        Returns
        -------
        OptimizedSchema
            Optimized schema configuration
        """
        logger.info(f"Optimizing schema for table: {table_name}")

        # Try to get predefined schema from YAML first
        yaml_duckdb_schema = self.schema_loader.get_table_duckdb_schema(table_name)

        optimized_types = {}
        original_types = dict(zip(df.columns, [str(dtype) for dtype in df.dtypes]))

        for column in df.columns:
            # Priority 1: Use hardcoded optimized types (these are DuckDB-optimized)
            if column in self.OPTIMIZED_TYPES:
                optimized_types[column] = self.OPTIMIZED_TYPES[column]
                logger.debug(f"Using optimized schema for {table_name}.{column}: {self.OPTIMIZED_TYPES[column]}")
            # Priority 2: Use YAML-defined type if no optimization exists
            elif yaml_duckdb_schema and column in yaml_duckdb_schema:
                optimized_types[column] = yaml_duckdb_schema[column]
                logger.debug(f"Using YAML schema for {table_name}.{column}: {yaml_duckdb_schema[column]}")
            # Priority 3: Infer optimal type from data
            else:
                optimized_types[column] = self._infer_optimal_type(
                    column, df.select(column), original_types[column]
                )
                logger.debug(f"Inferred type for {table_name}.{column}: {optimized_types[column]}")

        # Get indexes for this table
        indexes = self.INDEX_CONFIGS.get(table_name, ["CN"])

        # Get partitioning strategy (for documentation only)
        partitioning = self.PARTITIONING_CONFIGS.get(table_name)

        # Get compression configuration
        compression_config = self.compression_configs[compression_level]

        # Estimate size reduction
        size_reduction = self._estimate_size_reduction(
            df, original_types, optimized_types, compression_level
        )

        schema = OptimizedSchema(
            table_name=table_name,
            optimized_types=optimized_types,
            indexes=indexes,
            partitioning=partitioning,
            compression_config=compression_config,
            estimated_size_reduction=size_reduction
        )

        logger.info(
            f"Schema optimized for {table_name}: "
            f"{len(optimized_types)} columns, "
            f"{len(indexes)} indexes, "
            f"{size_reduction:.2f}x estimated reduction"
        )

        return schema

    def create_indexes(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        progress: Optional[Progress] = None,
        task_id: Optional[TaskID] = None
    ) -> None:
        """
        Create optimized indexes for a table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of the table
        progress : Progress, optional
            Rich progress tracker
        task_id : TaskID, optional
            Progress task ID
        """
        indexes = self.INDEX_CONFIGS.get(table_name, [])

        for i, index_def in enumerate(indexes):
            try:
                index_name = f"idx_{table_name.lower()}_{i+1}"

                if "(" in index_def and ")" in index_def:
                    # Composite index
                    sql = f"CREATE INDEX {index_name} ON {table_name} {index_def}"
                else:
                    # Single column index
                    sql = f"CREATE INDEX {index_name} ON {table_name} ({index_def})"

                logger.debug(f"Creating index: {sql}")
                conn.execute(sql)

                if progress and task_id:
                    progress.advance(task_id)

            except Exception as e:
                logger.warning(f"Failed to create index {index_def} on {table_name}: {e}")

    def setup_partitioning(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str
    ) -> None:
        """
        Setup table partitioning if supported.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of the table
        """
        partitioning = self.PARTITIONING_CONFIGS.get(table_name)

        if partitioning:
            try:
                # Note: DuckDB partitioning syntax may vary by version
                # This is a placeholder for future partitioning support
                logger.info(f"Partitioning strategy for {table_name}: {partitioning}")
                # TODO: Implement when DuckDB fully supports table partitioning

            except Exception as e:
                logger.warning(f"Failed to setup partitioning for {table_name}: {e}")

    def configure_compression(
        self,
        conn: duckdb.DuckDBPyConnection,
        compression_level: CompressionLevel = CompressionLevel.MEDIUM
    ) -> None:
        """
        Configure DuckDB compression settings.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        compression_level : CompressionLevel
            Desired compression level
        """
        config = self.compression_configs[compression_level]

        try:
            for setting, value in config.items():
                sql = f"SET {setting} = {value}"
                conn.execute(sql)
                logger.debug(f"Applied compression setting: {sql}")

        except Exception as e:
            logger.warning(f"Failed to configure compression: {e}")

    def analyze_table_statistics(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Analyze table statistics for optimization insights.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of the table

        Returns
        -------
        Dict[str, Any]
            Table statistics
        """
        try:
            # Get basic table stats
            stats_query = f"""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT STATECD) as state_count,
                MIN(INVYR) as min_year,
                MAX(INVYR) as max_year
            FROM {table_name}
            WHERE STATECD IS NOT NULL AND INVYR IS NOT NULL
            """

            result = conn.execute(stats_query).fetchone()

            stats = {
                "row_count": result[0] if result else 0,
                "state_count": result[1] if result else 0,
                "year_range": (result[2], result[3]) if result and result[2] else None
            }

            # Get column statistics
            column_stats = self._analyze_column_statistics(conn, table_name)
            stats["columns"] = column_stats

            return stats

        except Exception as e:
            logger.warning(f"Failed to analyze statistics for {table_name}: {e}")
            return {}

    def _infer_optimal_type(
        self,
        column_name: str,
        column_data: pl.DataFrame,
        original_type: str
    ) -> str:
        """
        Infer optimal data type for a column.

        Parameters
        ----------
        column_name : str
            Name of the column
        column_data : pl.DataFrame
            Column data sample
        original_type : str
            Original data type

        Returns
        -------
        str
            Optimal DuckDB data type
        """
        # If we have a predefined optimization, use it
        if column_name in self.OPTIMIZED_TYPES:
            return self.OPTIMIZED_TYPES[column_name]

        # Analyze data characteristics
        col = column_data.get_column(column_name)

        try:
            # Check for null values
            null_count = col.null_count()
            total_count = len(col)

            if null_count == total_count:
                return "VARCHAR"  # All nulls, keep as text

            # Try to infer numeric types
            if col.dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]:
                min_val = col.min()
                max_val = col.max()

                if min_val >= -128 and max_val <= 127:
                    return "TINYINT"
                elif min_val >= -32768 and max_val <= 32767:
                    return "SMALLINT"
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    return "INTEGER"
                else:
                    return "BIGINT"

            elif col.dtype in [pl.Float32, pl.Float64]:
                # Use DECIMAL for measurements, DOUBLE for ratios/factors
                if any(keyword in column_name.upper() for keyword in
                       ["DIA", "HT", "LAT", "LON", "ELEV", "BIOMASS", "VOLUME", "CARBON"]):
                    return "DECIMAL(12,4)"
                else:
                    return "DOUBLE"

            elif col.dtype == pl.Utf8:
                # Analyze string length for VARCHAR sizing
                try:
                    max_length = col.str.n_chars().max()
                    if max_length <= 10:
                        return "VARCHAR(10)"
                    elif max_length <= 50:
                        return "VARCHAR(50)"
                    elif max_length <= 100:
                        return "VARCHAR(100)"
                    else:
                        return "VARCHAR(255)"
                except:
                    return "VARCHAR(255)"

            elif col.dtype == pl.Boolean:
                return "BOOLEAN"

            elif col.dtype in [pl.Date, pl.Datetime]:
                return "DATE"

        except Exception as e:
            logger.debug(f"Type inference failed for {column_name}: {e}")

        # Fallback to VARCHAR for unknown types
        return "VARCHAR(255)"

    def _estimate_size_reduction(
        self,
        df: pl.DataFrame,
        original_types: Dict[str, str],
        optimized_types: Dict[str, str],
        compression_level: CompressionLevel
    ) -> float:
        """
        Estimate storage size reduction from optimization.

        Parameters
        ----------
        df : pl.DataFrame
            Sample dataframe
        original_types : Dict[str, str]
            Original data types
        optimized_types : Dict[str, str]
            Optimized data types
        compression_level : CompressionLevel
            Compression level

        Returns
        -------
        float
            Estimated size reduction ratio
        """
        # Simple heuristic-based estimation
        type_reduction = 1.0

        for col in df.columns:
            orig = original_types.get(col, "VARCHAR")
            opt = optimized_types.get(col, "VARCHAR")

            # Estimate per-type reduction
            if "BIGINT" in orig and "INT" in opt and "BIG" not in opt:
                type_reduction *= 0.5  # 8 bytes -> 4 bytes
            elif "VARCHAR" in orig and "TINYINT" in opt:
                type_reduction *= 0.1  # String -> 1 byte
            elif "VARCHAR" in orig and ("SMALLINT" in opt or "INTEGER" in opt):
                type_reduction *= 0.2  # String -> 2-4 bytes
            elif "DOUBLE" in orig and "DECIMAL" in opt:
                type_reduction *= 0.8  # Precision optimization

        # Apply compression factor
        compression_factors = {
            CompressionLevel.NONE: 1.0,
            CompressionLevel.LOW: 1.2,
            CompressionLevel.MEDIUM: 1.5,
            CompressionLevel.HIGH: 2.0,
            CompressionLevel.ADAPTIVE: 1.8
        }

        compression_factor = compression_factors[compression_level]

        # Columnar storage benefit (rough estimate)
        columnar_factor = 1.3

        return type_reduction * compression_factor * columnar_factor

    def _analyze_column_statistics(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Analyze statistics for individual columns.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of the table

        Returns
        -------
        Dict[str, Dict[str, Any]]
            Column statistics
        """
        try:
            # Get column information
            columns_query = f"DESCRIBE {table_name}"
            columns_result = conn.execute(columns_query).fetchall()

            column_stats = {}

            for col_info in columns_result:
                col_name = col_info[0]
                col_type = col_info[1]

                try:
                    # Basic statistics per column
                    stats_query = f"""
                    SELECT
                        COUNT(*) as total_count,
                        COUNT({col_name}) as non_null_count,
                        COUNT(DISTINCT {col_name}) as distinct_count
                    FROM {table_name}
                    """

                    result = conn.execute(stats_query).fetchone()

                    column_stats[col_name] = {
                        "type": col_type,
                        "total_count": result[0] if result else 0,
                        "non_null_count": result[1] if result else 0,
                        "distinct_count": result[2] if result else 0,
                        "null_percentage": (
                            (result[0] - result[1]) / result[0] * 100
                            if result and result[0] > 0 else 0
                        )
                    }

                except Exception as e:
                    logger.debug(f"Failed to get stats for column {col_name}: {e}")
                    column_stats[col_name] = {"type": col_type, "error": str(e)}

            return column_stats

        except Exception as e:
            logger.warning(f"Failed to analyze column statistics: {e}")
            return {}

    def _get_compression_configs(self) -> Dict[CompressionLevel, Dict[str, Any]]:
        """
        Get compression configurations for different levels.

        Returns
        -------
        Dict[CompressionLevel, Dict[str, Any]]
            Compression configurations
        """
        return {
            CompressionLevel.NONE: {
                "default_null_order": "'NULLS_FIRST'",
                "enable_object_cache": "true"
            },
            CompressionLevel.LOW: {
                "default_null_order": "'NULLS_FIRST'",
                "enable_object_cache": "true",
                "force_compression": "'auto'"
            },
            CompressionLevel.MEDIUM: {
                "default_null_order": "'NULLS_FIRST'",
                "enable_object_cache": "true",
                "force_compression": "'auto'",
                "enable_fsst_vectors": "true"
            },
            CompressionLevel.HIGH: {
                "default_null_order": "'NULLS_FIRST'",
                "enable_object_cache": "true",
                "force_compression": "'auto'",
                "enable_fsst_vectors": "true",
                "compress_bitpacking": "true"
            },
            CompressionLevel.ADAPTIVE: {
                "default_null_order": "'NULLS_FIRST'",
                "enable_object_cache": "true",
                "force_compression": "'auto'",
                "enable_fsst_vectors": "true",
                "compress_bitpacking": "true",
                "compression_level": "6"
            }
        }

    def get_create_table_sql(
        self,
        table_name: str,
        schema: OptimizedSchema
    ) -> str:
        """
        Generate CREATE TABLE SQL with optimized schema.

        Parameters
        ----------
        table_name : str
            Name of the table
        schema : OptimizedSchema
            Optimized schema configuration

        Returns
        -------
        str
            CREATE TABLE SQL statement
        """
        columns = []
        for col_name, col_type in schema.optimized_types.items():
            columns.append(f"    {col_name} {col_type}")

        sql = f"CREATE TABLE {table_name} (\n"
        sql += ",\n".join(columns)
        sql += "\n)"

        # Note: DuckDB doesn't support table partitioning syntax yet
        # Partitioning is handled through other optimization methods

        return sql
