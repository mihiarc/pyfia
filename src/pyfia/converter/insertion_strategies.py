"""
Data insertion strategies for DuckDB conversion.

This module provides different strategies for inserting data into DuckDB tables,
including handling of ART operator conflicts through staging table approaches.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import List

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class InsertionStrategy(ABC):
    """Abstract base class for data insertion strategies."""

    @abstractmethod
    def insert(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        data: pl.DataFrame,
        **kwargs
    ) -> None:
        """Insert data into DuckDB table."""
        pass


class DirectInsertionStrategy(InsertionStrategy):
    """Direct batch insertion strategy for new tables."""

    def __init__(self, batch_size: int = 100_000):
        self.batch_size = batch_size

    def insert(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        data: pl.DataFrame,
        **kwargs
    ) -> None:
        """Insert data using direct batch approach."""
        total_rows = len(data)
        logger.debug(f"Using direct insertion strategy for {table_name} ({total_rows:,} rows)")

        for i in range(0, total_rows, self.batch_size):
            try:
                batch = data.slice(i, self.batch_size)
                conn.register("batch_data", batch)
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM batch_data")

                # Log progress for large tables
                if total_rows > 100_000:
                    progress = min(100, ((i + self.batch_size) / total_rows) * 100)
                    batch_num = i // self.batch_size + 1
                    total_batches = (total_rows + self.batch_size - 1) // self.batch_size
                    logger.info(f"Inserted batch {batch_num}/{total_batches} for {table_name} ({progress:.1f}%)")

            except Exception as e:
                batch_num = i // self.batch_size + 1
                logger.error(f"Batch insertion failed for {table_name} (batch {batch_num}): {e}")
                raise RuntimeError(f"Failed to insert batch {batch_num} for {table_name}: {e}")


class StagingTableStrategy(InsertionStrategy):
    """
    Staging table approach to avoid DuckDB ART operator conflicts.
    
    This strategy is used for append operations where we need to add data to
    an existing table. It creates a staging table, inserts new data there,
    then uses UNION ALL to merge with the existing table atomically.
    This avoids ART index conflicts that can occur with direct insertions.
    """

    def insert(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        data: pl.DataFrame,
        **kwargs
    ) -> None:
        """Insert data using staging table and UNION ALL to avoid ART conflicts."""
        total_rows = len(data)
        logger.debug(f"Using staging table strategy for {table_name} ({total_rows:,} rows)")

        staging_table = f"{table_name}_staging_{int(time.time())}"
        temp_merged_table = f"{table_name}_merged_{int(time.time())}"

        try:
            # Create staging table with same schema as target
            conn.execute(f"CREATE TABLE {staging_table} AS SELECT * FROM {table_name} WHERE 1=0")
            logger.debug(f"Created staging table {staging_table}")

            # Insert all data into staging table at once (avoids batching issues)
            conn.register("staging_data", data)
            conn.execute(f"INSERT INTO {staging_table} SELECT * FROM staging_data")
            logger.info(f"Loaded {total_rows:,} records into staging table {staging_table}")

            # Create merged table using UNION ALL (avoids ART conflicts)
            conn.execute(f"""
                CREATE TABLE {temp_merged_table} AS
                SELECT * FROM {table_name}
                UNION ALL
                SELECT * FROM {staging_table}
            """)
            logger.debug(f"Created merged table {temp_merged_table}")

            # Replace the original table atomically
            conn.execute(f"DROP TABLE {table_name}")
            conn.execute(f"ALTER TABLE {temp_merged_table} RENAME TO {table_name}")
            logger.info(f"Replaced {table_name} with merged data ({total_rows:,} new records)")

            # Clean up staging table
            conn.execute(f"DROP TABLE {staging_table}")
            logger.debug(f"Dropped staging table {staging_table}")

        except Exception as e:
            # Clean up temporary tables on error
            self._cleanup_temp_tables(conn, [staging_table, temp_merged_table])
            logger.error(f"Staging table insertion failed for {table_name}: {e}")
            raise Exception(f"Failed to update {table_name} using staging approach: {e}")

    def _cleanup_temp_tables(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: List[str]
    ) -> None:
        """Clean up temporary tables."""
        for table in tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                logger.warning(f"Could not clean up temporary table {table}: {e}")


class InsertionStrategyFactory:
    """Factory for creating appropriate insertion strategy."""

    @staticmethod
    def create_strategy(
        append_mode: bool,
        table_exists: bool,
        batch_size: int = 100_000
    ) -> InsertionStrategy:
        """Create appropriate insertion strategy based on conditions."""
        if append_mode and table_exists:
            logger.debug("Selected staging table strategy for append mode")
            return StagingTableStrategy()
        else:
            logger.debug(f"Selected direct insertion strategy (batch_size={batch_size})")
            return DirectInsertionStrategy(batch_size)
