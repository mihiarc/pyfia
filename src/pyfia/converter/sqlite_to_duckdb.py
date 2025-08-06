"""
Main FIA SQLite to DuckDB converter implementation.

This module provides the core FIAConverter class that handles conversion
of FIA DataMart SQLite databases to optimized DuckDB format with support
for single state conversion, multi-state merging, and incremental updates.
"""

import json
import logging
import shutil
import sqlite3
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import duckdb
import polars as pl
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from .models import (
    STANDARD_FIA_TABLES,
    ConversionMetadata,
    ConversionResult,
    ConversionStats,
    ConversionStatus,
    ConverterConfig,
    UpdateResult,
)
from .schema_loader import get_schema_loader
from .schema_optimizer import SchemaOptimizer
from .state_merger import StateMerger
from .validation import DataValidator

logger = logging.getLogger(__name__)


class FIAConverter:
    """
    Main converter for FIA SQLite to DuckDB transformation.

    Features:
    - Single state conversion with schema optimization
    - Multi-state merging with conflict resolution
    - Incremental updates for new data
    - Progress tracking and error recovery
    - Data validation and integrity checks
    """

    def __init__(self, config: ConverterConfig):
        """
        Initialize the FIA converter.

        Parameters
        ----------
        config : ConverterConfig
            Converter configuration
        """
        self.config = config
        self.console = Console()

        # Initialize components
        self.schema_loader = get_schema_loader()
        self.schema_optimizer = SchemaOptimizer()
        self.state_merger = StateMerger()
        self.validator = DataValidator()

        # Setup logging
        self._setup_logging()

        # Prepare temporary directory
        self.temp_dir = self._setup_temp_directory()

        logger.info(f"FIAConverter initialized with config: {config.target_path}")

    def convert_state(
        self,
        sqlite_path: Path,
        state_code: int,
        target_path: Optional[Path] = None
    ) -> ConversionResult:
        """
        Convert a single state SQLite database to DuckDB format.

        Parameters
        ----------
        sqlite_path : Path
            Path to source SQLite database
        state_code : int
            FIPS state code
        target_path : Path, optional
            Target DuckDB path (uses config default if None)

        Returns
        -------
        ConversionResult
            Results of the conversion operation
        """
        if target_path is None:
            target_path = self.config.target_path

        logger.info(f"Starting conversion of state {state_code} from {sqlite_path}")

        # Initialize conversion tracking
        stats = ConversionStats(
            start_time=datetime.now(),
            source_file_count=1,
            source_total_size_bytes=sqlite_path.stat().st_size,
            source_tables_processed=0,
            source_records_processed=0
        )

        try:
            # Create conversion pipeline
            pipeline = ConversionPipeline(
                converter=self,
                source_paths=[sqlite_path],
                target_path=target_path,
                stats=stats
            )

            # Execute conversion with progress tracking
            if self.config.show_progress:
                result = self._convert_with_progress(pipeline, [state_code])
            else:
                result = pipeline.execute([state_code])

            logger.info(f"State {state_code} conversion completed: {result.status}")
            return result

        except Exception as e:
            logger.error(f"Conversion failed for state {state_code}: {e}")

            # Create failure result
            stats.end_time = datetime.now()
            stats.calculate_derived_metrics()

            return ConversionResult(
                status=ConversionStatus.FAILED,
                config=self.config,
                stats=stats,
                source_paths=[sqlite_path],
                target_path=target_path,
                error_message=str(e),
                error_traceback=traceback.format_exc()
            )

    def merge_states(
        self,
        state_paths: List[Path],
        target_path: Optional[Path] = None
    ) -> ConversionResult:
        """
        Merge multiple state SQLite databases into a single DuckDB.

        Parameters
        ----------
        state_paths : List[Path]
            List of SQLite database paths to merge
        target_path : Path, optional
            Target DuckDB path (uses config default if None)

        Returns
        -------
        ConversionResult
            Results of the merge operation
        """
        if target_path is None:
            target_path = self.config.target_path

        logger.info(f"Starting merge of {len(state_paths)} states to {target_path}")

        # Extract state codes from file paths or database
        state_codes = []
        total_size = 0

        for path in state_paths:
            if not path.exists():
                raise FileNotFoundError(f"Source file not found: {path}")

            total_size += path.stat().st_size

            # Try to extract state code from filename (e.g., "OR_FIA.db" -> 41)
            state_code = self._extract_state_code_from_path(path)
            if state_code is None:
                # Fallback: query database for state code
                state_code = self._get_state_code_from_db(path)

            if state_code:
                state_codes.append(state_code)

        # Initialize conversion tracking
        stats = ConversionStats(
            start_time=datetime.now(),
            source_file_count=len(state_paths),
            source_total_size_bytes=total_size,
            source_tables_processed=0,
            source_records_processed=0
        )

        try:
            # Create conversion pipeline for multi-state
            pipeline = ConversionPipeline(
                converter=self,
                source_paths=state_paths,
                target_path=target_path,
                stats=stats,
                is_merge=True
            )

            # Execute merge with progress tracking
            if self.config.show_progress:
                result = self._convert_with_progress(pipeline, state_codes)
            else:
                result = pipeline.execute(state_codes)

            logger.info(f"Multi-state merge completed: {result.status}")
            return result

        except Exception as e:
            logger.error(f"Multi-state merge failed: {e}")

            # Create failure result
            stats.end_time = datetime.now()
            stats.calculate_derived_metrics()

            return ConversionResult(
                status=ConversionStatus.FAILED,
                config=self.config,
                stats=stats,
                source_paths=state_paths,
                target_path=target_path,
                error_message=str(e),
                error_traceback=traceback.format_exc()
            )

    def update_state(
        self,
        duckdb_path: Path,
        sqlite_path: Path,
        state_code: int
    ) -> UpdateResult:
        """
        Perform incremental update of a state in existing DuckDB.

        Parameters
        ----------
        duckdb_path : Path
            Existing DuckDB database path
        sqlite_path : Path
            Updated SQLite database path
        state_code : int
            FIPS state code

        Returns
        -------
        UpdateResult
            Results of the update operation
        """
        logger.info(f"Starting incremental update for state {state_code}")

        update_start = datetime.now()

        try:
            # Connect to existing DuckDB
            with duckdb.connect(str(duckdb_path)) as duck_conn:
                # Connect to source SQLite
                sqlite_conn = sqlite3.connect(str(sqlite_path))

                # Determine what tables exist in both databases
                existing_tables = self._get_table_list(duck_conn)
                source_tables = self._get_sqlite_tables(sqlite_conn)

                common_tables = set(existing_tables) & set(source_tables)

                records_added = 0
                records_updated = 0
                records_deleted = 0
                tables_affected = []

                # Process each table
                for table_name in common_tables:
                    if table_name not in STANDARD_FIA_TABLES:
                        continue

                    logger.info(f"Updating table: {table_name}")

                    # Load new data
                    new_data = pl.read_database(
                        f"SELECT * FROM {table_name}",
                        sqlite_conn
                    )

                    if len(new_data) == 0:
                        continue

                    # Filter to current state if STATECD column exists
                    if "STATECD" in new_data.columns:
                        new_data = new_data.filter(pl.col("STATECD") == state_code)

                    if len(new_data) == 0:
                        continue

                    # Perform incremental update logic
                    table_stats = self._update_table_incrementally(
                        duck_conn, table_name, new_data, state_code
                    )

                    records_added += table_stats.get("added", 0)
                    records_updated += table_stats.get("updated", 0)
                    records_deleted += table_stats.get("deleted", 0)
                    tables_affected.append(table_name)

                sqlite_conn.close()

            # Validate update if requested
            validation = None
            if self.config.validate_data:
                validation = self.validator.validate_database(duckdb_path)

            return UpdateResult(
                status=ConversionStatus.COMPLETED,
                update_time=update_start,
                state_code=state_code,
                records_added=records_added,
                records_updated=records_updated,
                records_deleted=records_deleted,
                tables_affected=tables_affected,
                validation=validation
            )

        except Exception as e:
            logger.error(f"Incremental update failed for state {state_code}: {e}")

            return UpdateResult(
                status=ConversionStatus.FAILED,
                update_time=update_start,
                state_code=state_code,
                error_message=str(e)
            )

    def validate_conversion(self, duckdb_path: Path) -> bool:
        """
        Validate a converted DuckDB database.

        Parameters
        ----------
        duckdb_path : Path
            Path to DuckDB database to validate

        Returns
        -------
        bool
            True if validation passes
        """
        logger.info(f"Validating converted database: {duckdb_path}")

        try:
            validation_result = self.validator.validate_database(duckdb_path)

            if validation_result.is_valid:
                logger.info("Database validation passed")
                return True
            else:
                logger.error(f"Database validation failed with {len(validation_result.errors)} errors")
                for error in validation_result.errors[:5]:  # Show first 5 errors
                    logger.error(f"  - {error.message}")
                return False

        except Exception as e:
            logger.error(f"Validation failed with exception: {e}")
            return False

    def _convert_with_progress(
        self,
        pipeline: 'ConversionPipeline',
        state_codes: List[int]
    ) -> ConversionResult:
        """
        Execute conversion with Rich progress tracking.

        Parameters
        ----------
        pipeline : ConversionPipeline
            Conversion pipeline to execute
        state_codes : List[int]
            State codes being processed

        Returns
        -------
        ConversionResult
            Conversion results
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console
        ) as progress:

            # Add main conversion task
            main_task = progress.add_task(
                f"Converting {len(state_codes)} state(s)",
                total=len(pipeline.stages)
            )

            # Execute pipeline with progress updates
            result = pipeline.execute_with_progress(state_codes, progress, main_task)

            return result

    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def _setup_temp_directory(self) -> Path:
        """
        Setup temporary directory for conversion operations.

        Returns
        -------
        Path
            Temporary directory path
        """
        if self.config.temp_dir:
            temp_dir = self.config.temp_dir
            temp_dir.mkdir(parents=True, exist_ok=True)
        else:
            temp_dir = Path(tempfile.mkdtemp(prefix="pyfia_convert_"))

        logger.debug(f"Using temporary directory: {temp_dir}")
        return temp_dir

    def _extract_state_code_from_path(self, path: Path) -> Optional[int]:
        """
        Extract state code from file path.

        Parameters
        ----------
        path : Path
            File path

        Returns
        -------
        Optional[int]
            State code if found
        """
        # Common patterns: OR_FIA.db, california.sqlite, etc.
        filename = path.stem.upper()

        # State abbreviation mapping (partial)
        state_abbrev_to_code = {
            'AL': 1, 'AK': 2, 'AZ': 4, 'AR': 5, 'CA': 6, 'CO': 8, 'CT': 9,
            'DE': 10, 'FL': 12, 'GA': 13, 'HI': 15, 'ID': 16, 'IL': 17,
            'IN': 18, 'IA': 19, 'KS': 20, 'KY': 21, 'LA': 22, 'ME': 23,
            'MD': 24, 'MA': 25, 'MI': 26, 'MN': 27, 'MS': 28, 'MO': 29,
            'MT': 30, 'NE': 31, 'NV': 32, 'NH': 33, 'NJ': 34, 'NM': 35,
            'NY': 36, 'NC': 37, 'ND': 38, 'OH': 39, 'OK': 40, 'OR': 41,
            'PA': 42, 'RI': 44, 'SC': 45, 'SD': 46, 'TN': 47, 'TX': 48,
            'UT': 49, 'VT': 50, 'VA': 51, 'WA': 53, 'WV': 54, 'WI': 55, 'WY': 56
        }

        # Look for state abbreviation
        for abbrev, code in state_abbrev_to_code.items():
            if filename.startswith(abbrev + '_') or filename == abbrev:
                return code

        return None

    def _get_state_code_from_db(self, sqlite_path: Path) -> Optional[int]:
        """
        Get state code by querying the SQLite database.

        Parameters
        ----------
        sqlite_path : Path
            SQLite database path

        Returns
        -------
        Optional[int]
            State code if found
        """
        try:
            conn = sqlite3.connect(str(sqlite_path))
            cursor = conn.cursor()

            # Try to get state code from PLOT table
            cursor.execute("SELECT DISTINCT STATECD FROM PLOT LIMIT 1")
            result = cursor.fetchone()

            conn.close()

            return result[0] if result else None

        except Exception as e:
            logger.debug(f"Could not extract state code from {sqlite_path}: {e}")
            return None

    def _get_table_list(self, conn: duckdb.DuckDBPyConnection) -> List[str]:
        """
        Get list of tables in DuckDB database.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection

        Returns
        -------
        List[str]
            List of table names
        """
        result = conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def _get_sqlite_tables(self, conn: sqlite3.Connection) -> List[str]:
        """
        Get list of tables in SQLite database.

        Parameters
        ----------
        conn : sqlite3.Connection
            SQLite connection

        Returns
        -------
        List[str]
            List of table names
        """
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]

    def _update_table_incrementally(
        self,
        duck_conn: duckdb.DuckDBPyConnection,
        table_name: str,
        new_data: pl.DataFrame,
        state_code: int
    ) -> Dict[str, int]:
        """
        Update a table incrementally with new data.

        Parameters
        ----------
        duck_conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of table to update
        new_data : pl.DataFrame
            New data to merge
        state_code : int
            State code for filtering

        Returns
        -------
        Dict[str, int]
            Statistics about the update (added, updated, deleted counts)
        """
        # This is a simplified incremental update strategy
        # In practice, this would need more sophisticated logic based on
        # table structure and business rules

        try:
            # Delete existing data for this state
            if "STATECD" in new_data.columns:
                duck_conn.execute(f"DELETE FROM {table_name} WHERE STATECD = {state_code}")
                deleted_count = duck_conn.execute("SELECT changes()").fetchone()[0]
            else:
                deleted_count = 0

            # Insert new data
            duck_conn.register("new_data", new_data)
            duck_conn.execute(f"INSERT INTO {table_name} SELECT * FROM new_data")
            added_count = len(new_data)

            return {
                "added": added_count,
                "updated": 0,  # Simple strategy doesn't track updates separately
                "deleted": deleted_count
            }

        except Exception as e:
            logger.error(f"Failed to update table {table_name}: {e}")
            return {"added": 0, "updated": 0, "deleted": 0}

    def __del__(self):
        """Cleanup temporary directory on destruction."""
        if hasattr(self, 'temp_dir') and self.temp_dir.exists():
            try:
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                logger.debug(f"Failed to cleanup temp directory: {e}")

    def _read_table_with_schema(
        self,
        sqlite_conn,
        table_name: str,
        source_path: Path
    ) -> pl.DataFrame:
        """
        Read table using predefined schema with multiple fallback strategies.

        Parameters
        ----------
        sqlite_conn
            SQLite database connection
        table_name : str
            Name of table to read
        source_path : Path
            Path to source database file

        Returns
        -------
        pl.DataFrame
            Loaded table data
        """
        # Check if table is large enough to require streaming
        row_count = self._get_table_row_count(sqlite_conn, table_name)
        use_streaming = (
            self.config.stream_large_tables and
            row_count > self.config.large_table_threshold
        )

        if use_streaming:
            logger.info(f"Table {table_name} has {row_count:,} rows, using streaming approach")
            polars_schema = self.schema_loader.get_table_schema(table_name)
            return self._read_table_streaming(sqlite_conn, table_name, source_path, polars_schema)

        # Strategy 1: Try predefined Polars schema from YAML (for non-streaming)
        polars_schema = self.schema_loader.get_table_schema(table_name)
        if polars_schema:
            logger.debug(f"Found YAML schema for {table_name} with {len(polars_schema)} columns")
            try:
                df = pl.read_database(
                    f"SELECT * FROM {table_name}",
                    sqlite_conn,
                    schema_overrides=polars_schema
                )
                logger.info(f"Successfully read {table_name} using YAML predefined schema")
                return df
            except Exception as e:
                logger.error(f"YAML predefined schema failed for {table_name}: {e}")
                logger.debug(f"Failed schema for {table_name}: {polars_schema}")
                raise RuntimeError(f"Failed to read table {table_name} with YAML schema: {e}")
        else:
            logger.error(f"No YAML schema found for {table_name}")
            raise RuntimeError(f"No YAML schema found for table {table_name}. Schema must be defined.")

    def _get_table_row_count(self, sqlite_conn, table_name: str) -> int:
        """Get row count for a table to determine if streaming is needed."""
        try:
            cursor = sqlite_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            logger.warning(f"Failed to get row count for {table_name}: {e}")
            return 0

    def _read_table_streaming(
        self,
        sqlite_conn,
        table_name: str,
        source_path: Path,
        polars_schema: Optional[Dict] = None
    ) -> pl.DataFrame:
        """
        Read large table using streaming/chunked approach.

        Parameters
        ----------
        sqlite_conn
            SQLite database connection
        table_name : str
            Name of table to read
        source_path : Path
            Path to source database file
        polars_schema : Dict, optional
            Predefined Polars schema to use

        Returns
        -------
        pl.DataFrame
            Loaded table data
        """
        logger.info(f"Using streaming approach for large table: {table_name}")

        # Get total row count for progress tracking
        total_rows = self._get_table_row_count(sqlite_conn, table_name)
        batch_size = self.config.batch_size

        logger.info(f"Streaming {table_name}: {total_rows:,} rows in batches of {batch_size:,}")

        all_chunks = []
        offset = 0

        while True:
            # Read chunk with LIMIT/OFFSET
            chunk_query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"

            try:
                if not polars_schema:
                    raise RuntimeError(f"No YAML schema provided for streaming table {table_name}")

                chunk_df = pl.read_database(
                    chunk_query,
                    sqlite_conn,
                    schema_overrides=polars_schema
                )

                if len(chunk_df) == 0:
                    # No more data
                    break

                all_chunks.append(chunk_df)
                offset += batch_size

                # Log progress
                progress_pct = min(100, (offset / total_rows) * 100) if total_rows > 0 else 0
                logger.debug(f"Streamed {offset:,}/{total_rows:,} rows ({progress_pct:.1f}%) from {table_name}")

                # Safety check to prevent infinite loop
                if len(chunk_df) < batch_size:
                    # Last chunk
                    break

            except Exception as e:
                logger.error(f"Failed to read chunk at offset {offset} for {table_name}: {e}")
                logger.error(f"YAML schema error in streaming for {table_name}: {polars_schema}")
                raise RuntimeError(f"Failed to read chunk at offset {offset} for {table_name} with YAML schema: {e}")

        if not all_chunks:
            logger.warning(f"No data retrieved for {table_name}")
            return pl.DataFrame()

        # Combine all chunks
        logger.info(f"Combining {len(all_chunks)} chunks for {table_name}")
        combined_df = pl.concat(all_chunks, how="vertical")
        logger.info(f"Successfully streamed {table_name}: {len(combined_df):,} total rows")

        return combined_df


class ConversionPipeline:
    """
    Manages the staged conversion workflow with error recovery.
    """

    def __init__(
        self,
        converter: FIAConverter,
        source_paths: List[Path],
        target_path: Path,
        stats: ConversionStats,
        is_merge: bool = False
    ):
        """
        Initialize conversion pipeline.

        Parameters
        ----------
        converter : FIAConverter
            Parent converter instance
        source_paths : List[Path]
            Source database paths
        target_path : Path
            Target database path
        stats : ConversionStats
            Statistics tracker
        is_merge : bool
            Whether this is a multi-state merge
        """
        self.converter = converter
        self.source_paths = source_paths
        self.target_path = target_path
        self.stats = stats
        self.is_merge = is_merge

        # Define pipeline stages
        self.stages = [
            "validate_sources",
            "initialize_target",
            "convert_reference_tables",
            "convert_population_tables",
            "convert_measurement_tables",
            "create_indexes",
            "validate_target",
            "finalize"
        ]

        # Checkpoint storage
        self.checkpoint_path = (
            converter.temp_dir / f"checkpoint_{target_path.stem}.json"
        )

    def execute(self, state_codes: List[int]) -> ConversionResult:
        """Execute the conversion pipeline."""
        return self._execute_stages(state_codes)

    def execute_with_progress(
        self,
        state_codes: List[int],
        progress: Progress,
        main_task
    ) -> ConversionResult:
        """Execute with progress tracking."""
        self.progress = progress
        self.main_task = main_task
        return self._execute_stages(state_codes)

    def _execute_stages(self, state_codes: List[int]) -> ConversionResult:
        """
        Execute all pipeline stages.

        Parameters
        ----------
        state_codes : List[int]
            State codes to process

        Returns
        -------
        ConversionResult
            Conversion results
        """
        try:
            # Execute each stage
            for stage in self.stages:
                logger.info(f"Executing stage: {stage}")

                # Update progress if available
                if hasattr(self, 'progress'):
                    self.progress.update(
                        self.main_task,
                        description=f"[bold blue]{stage.replace('_', ' ').title()}"
                    )

                # Execute stage
                success = self._execute_stage(stage, state_codes)

                if not success:
                    raise RuntimeError(f"Stage {stage} failed")

                # Save checkpoint if enabled
                if self.converter.config.checkpoint_enabled:
                    self._save_checkpoint(stage)

                # Update progress
                if hasattr(self, 'progress'):
                    self.progress.advance(self.main_task)

            # Finalize statistics
            self.stats.end_time = datetime.now()
            self.stats.calculate_derived_metrics()

            # Create successful result
            result = ConversionResult(
                status=ConversionStatus.COMPLETED,
                config=self.converter.config,
                stats=self.stats,
                source_paths=self.source_paths,
                target_path=self.target_path
            )

            # Add validation result if performed
            if self.converter.config.validate_data:
                result.validation = self.converter.validator.validate_database(
                    self.target_path
                )

            return result

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")

            # Finalize statistics for failure
            self.stats.end_time = datetime.now()
            self.stats.errors_encountered += 1
            self.stats.calculate_derived_metrics()

            return ConversionResult(
                status=ConversionStatus.FAILED,
                config=self.converter.config,
                stats=self.stats,
                source_paths=self.source_paths,
                target_path=self.target_path,
                error_message=str(e),
                error_traceback=traceback.format_exc()
            )

    def _execute_stage(self, stage: str, state_codes: List[int]) -> bool:
        """
        Execute a specific pipeline stage.

        Parameters
        ----------
        stage : str
            Stage name
        state_codes : List[int]
            State codes to process

        Returns
        -------
        bool
            True if stage succeeded
        """
        try:
            if stage == "validate_sources":
                return self._validate_sources()
            elif stage == "initialize_target":
                return self._initialize_target()
            elif stage == "convert_reference_tables":
                return self._convert_reference_tables(state_codes)
            elif stage == "convert_population_tables":
                return self._convert_population_tables(state_codes)
            elif stage == "convert_measurement_tables":
                return self._convert_measurement_tables(state_codes)
            elif stage == "create_indexes":
                return self._create_indexes()
            elif stage == "validate_target":
                return self._validate_target()
            elif stage == "finalize":
                return self._finalize(state_codes)
            else:
                logger.error(f"Unknown stage: {stage}")
                return False

        except Exception as e:
            logger.error(f"Stage {stage} failed: {e}")
            return False

    def _validate_sources(self) -> bool:
        """Validate source databases."""
        for path in self.source_paths:
            if not path.exists():
                logger.error(f"Source file not found: {path}")
                return False

            try:
                # Basic SQLite connectivity test
                conn = sqlite3.connect(str(path))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                table_count = cursor.fetchone()[0]
                conn.close()

                if table_count == 0:
                    logger.error(f"No tables found in {path}")
                    return False

                logger.debug(f"Source {path} validated: {table_count} tables")

            except Exception as e:
                logger.error(f"Failed to validate source {path}: {e}")
                return False

        return True

    def _initialize_target(self) -> bool:
        """Initialize target DuckDB database."""
        try:
            # Create target directory if needed
            self.target_path.parent.mkdir(parents=True, exist_ok=True)

            # Initialize DuckDB with optimal settings
            with duckdb.connect(str(self.target_path)) as conn:
                # Configure DuckDB for optimal performance
                self.converter.schema_optimizer.configure_compression(
                    conn, self.converter.config.compression_level
                )

                # Set memory limit
                conn.execute(f"SET memory_limit = '{self.converter.config.memory_limit}'")

                # Create metadata table
                self._create_metadata_table(conn)

            logger.info(f"Target database initialized: {self.target_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize target database: {e}")
            return False

    def _get_existing_states(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> Set[int]:
        """
        Get existing state codes in a DuckDB table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of the table to check

        Returns
        -------
        Set[int]
            Set of existing state codes
        """
        try:
            # Check if table exists
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                [table_name.upper()]
            ).fetchone()

            if not result:
                return set()

            # Check if STATECD column exists in the table
            columns_result = conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ? AND column_name = 'STATECD'",
                [table_name.upper()]
            ).fetchone()

            if not columns_result:
                # Table doesn't have STATECD column (like reference tables)
                return set()

            # Get existing state codes
            states_result = conn.execute(
                f"SELECT DISTINCT STATECD FROM {table_name} WHERE STATECD IS NOT NULL"
            ).fetchall()

            return {row[0] for row in states_result}

        except Exception:
            # Table doesn't exist or doesn't have STATECD column
            return set()

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        """
        Check if a table exists in the DuckDB database.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of the table to check

        Returns
        -------
        bool
            True if table exists, False otherwise
        """
        try:
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                [table_name.upper()]
            ).fetchone()
            return result is not None
        except Exception as e:
            logger.debug(f"Error checking if table {table_name} exists: {e}")
            return False

    def _convert_reference_tables(self, state_codes: List[int]) -> bool:
        """Convert reference tables (species, forest types, etc.)."""
        reference_tables = ["REF_SPECIES", "REF_FOREST_TYPE", "REF_HABITAT_TYPE"]

        with duckdb.connect(str(self.target_path)) as duck_conn:
            for table_name in reference_tables:
                if not self._convert_table_from_sources(duck_conn, table_name, state_codes):
                    logger.warning(f"Failed to convert reference table: {table_name}")
                    # Continue with other tables - reference tables may not exist in all databases

        return True

    def _convert_population_tables(self, state_codes: List[int]) -> bool:
        """Convert population and evaluation tables."""
        pop_tables = [
            "POP_EVAL", "POP_EVAL_TYP", "POP_EVAL_GRP",
            "POP_ESTN_UNIT", "POP_STRATUM"
        ]

        with duckdb.connect(str(self.target_path)) as duck_conn:
            for table_name in pop_tables:
                if not self._convert_table_from_sources(duck_conn, table_name, state_codes):
                    logger.error(f"Failed to convert population table: {table_name}")
                    return False

        return True

    def _convert_measurement_tables(self, state_codes: List[int]) -> bool:
        """Convert measurement tables (PLOT, TREE, COND, etc.)."""
        measurement_tables = [
            "PLOT", "COND", "SUBPLOT", "BOUNDARY", "TREE", "POP_PLOT_STRATUM_ASSGN"
        ]

        with duckdb.connect(str(self.target_path)) as duck_conn:
            for table_name in measurement_tables:
                if not self._convert_table_from_sources(duck_conn, table_name, state_codes):
                    logger.error(f"Failed to convert measurement table: {table_name}")
                    return False

        return True

    def _convert_table_from_sources(
        self,
        duck_conn: duckdb.DuckDBPyConnection,
        table_name: str,
        state_codes: List[int]
    ) -> bool:
        """
        Convert a table from all source databases.

        Parameters
        ----------
        duck_conn : duckdb.DuckDBPyConnection
            DuckDB connection
        table_name : str
            Name of table to convert
        state_codes : List[int]
            State codes being processed

        Returns
        -------
        bool
            True if conversion succeeded
        """
        logger.info(f"Converting table: {table_name}")

        all_data = []
        table_exists_in_sources = False

        # Collect data from all sources
        for i, source_path in enumerate(self.source_paths):
            try:
                sqlite_conn = sqlite3.connect(str(source_path))

                # Check if table exists
                cursor = sqlite_conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (table_name,)
                )

                if cursor.fetchone()[0] == 0:
                    logger.debug(f"Table {table_name} not found in {source_path}")
                    sqlite_conn.close()
                    continue

                table_exists_in_sources = True

                # Read table data with predefined schema first, then fallback strategies
                df = self.converter._read_table_with_schema(sqlite_conn, table_name, source_path)
                sqlite_conn.close()

                if len(df) > 0:
                    # Filter by state if applicable and requested
                    if (len(state_codes) > 0 and
                        "STATECD" in df.columns and
                        not self._is_reference_table(table_name)):

                        # Handle string vs integer STATECD comparison
                        if df.select("STATECD").dtypes[0] == pl.Utf8:
                            # STATECD is string, convert state_codes to strings
                            state_codes_str = [str(code) for code in state_codes]
                            df = df.filter(pl.col("STATECD").is_in(state_codes_str))
                        else:
                            # STATECD is numeric
                            df = df.filter(pl.col("STATECD").is_in(state_codes))

                    if len(df) > 0:
                        all_data.append(df)
                        self.stats.source_records_processed += len(df)

            except Exception as e:
                logger.error(f"Failed to read {table_name} from {source_path}: {e}")
                return False

        if not table_exists_in_sources:
            logger.debug(f"Table {table_name} not found in any sources")
            return True  # Not an error if table doesn't exist

        if not all_data:
            logger.warning(f"No data found for table {table_name}")
            return True

        # Merge data from all sources
        if self.is_merge and len(all_data) > 1:
            # Use state merger for multi-state conflicts
            merged_df = self.converter.state_merger.merge_table_data(table_name, all_data)
        else:
            # Simple concatenation
            merged_df = pl.concat(all_data, how="vertical_relaxed")

        # Check if table exists (needed for both append and fresh mode)
        table_exists = self._table_exists(duck_conn, table_name)

        # Handle table creation and schema based on append mode
        if self.converter.config.append_mode:
            # Get existing states if table exists
            existing_states = self._get_existing_states(duck_conn, table_name) if table_exists else set()
            if not table_exists:
                # New table: optimize schema normally
                schema = self.converter.schema_optimizer.optimize_table_schema(
                    table_name, merged_df, self.converter.config.compression_level
                )
                create_sql = self.converter.schema_optimizer.get_create_table_sql(
                    table_name, schema
                )
                duck_conn.execute(create_sql)
                logger.info(f"Created new table {table_name} in append mode")
            else:
                # Existing table: get existing schema and ensure compatibility
                logger.info(f"Using existing table schema for {table_name} in append mode")

                # Get existing table schema for type compatibility
                try:
                    existing_schema = duck_conn.execute(f"DESCRIBE {table_name}").fetchall()
                    schema_map = {col[0]: col[1] for col in existing_schema}

                    # Apply type casting to ensure schema compatibility
                    cast_exprs = []
                    for col in merged_df.columns:
                        if col in schema_map:
                            target_type = schema_map[col]
                            if "SMALLINT" in target_type.upper():
                                cast_exprs.append(pl.col(col).cast(pl.Int16).alias(col))
                            elif "INTEGER" in target_type.upper():
                                cast_exprs.append(pl.col(col).cast(pl.Int32).alias(col))
                            elif "BIGINT" in target_type.upper():
                                cast_exprs.append(pl.col(col).cast(pl.Int64).alias(col))
                            elif "FLOAT" in target_type.upper() or "DOUBLE" in target_type.upper():
                                cast_exprs.append(pl.col(col).cast(pl.Float64).alias(col))
                            elif "VARCHAR" in target_type.upper() or "TEXT" in target_type.upper():
                                cast_exprs.append(pl.col(col).cast(pl.Utf8).alias(col))
                            else:
                                cast_exprs.append(pl.col(col))
                        else:
                            cast_exprs.append(pl.col(col))

                    if cast_exprs:
                        merged_df = merged_df.select(cast_exprs)
                        logger.info(f"Applied schema compatibility for {table_name}")

                except Exception as e:
                    logger.warning(f"Schema compatibility adjustment failed for {table_name}: {e}")

                schema = None

                # Remove data for states we're about to re-add
                new_states = set()
                if "STATECD" in merged_df.columns:
                    new_states = set(merged_df.select("STATECD").unique().drop_nulls().to_series().to_list())

                for state in new_states:
                    if state in existing_states:
                        duck_conn.execute(f"DELETE FROM {table_name} WHERE STATECD = ?", [state])
                        logger.info(f"Removed existing data for state {state} from {table_name}")
        else:
            # Original behavior: optimize schema and drop/recreate table
            schema = self.converter.schema_optimizer.optimize_table_schema(
                table_name, merged_df, self.converter.config.compression_level
            )
            duck_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            create_sql = self.converter.schema_optimizer.get_create_table_sql(
                table_name, schema
            )
            duck_conn.execute(create_sql)

        # Insert data using appropriate strategy
        total_rows = len(merged_df)

        # Import insertion strategies
        from .insertion_strategies import InsertionStrategyFactory

        try:
            # Select appropriate insertion strategy
            strategy = InsertionStrategyFactory.create_strategy(
                append_mode=self.converter.config.append_mode,
                table_exists=table_exists,
                batch_size=self.converter.config.batch_size
            )

            # Execute insertion
            strategy.insert(duck_conn, table_name, merged_df)

        except Exception as e:
            logger.error(f"Data insertion failed for {table_name}: {e}")
            raise

        self.stats.target_records_written += total_rows
        self.stats.target_tables_created += 1

        logger.info(f"Converted {table_name}: {total_rows:,} records")
        return True

    def _create_indexes(self) -> bool:
        """Create optimized indexes."""
        try:
            with duckdb.connect(str(self.target_path)) as conn:
                tables = self.converter.schema_optimizer.INDEX_CONFIGS.keys()

                for table_name in tables:
                    # Check if table exists
                    try:
                        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                    except:
                        continue  # Table doesn't exist, skip

                    self.converter.schema_optimizer.create_indexes(conn, table_name)
                    self.stats.target_indexes_created += len(
                        self.converter.schema_optimizer.INDEX_CONFIGS.get(table_name, [])
                    )

            return True

        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            return False

    def _validate_target(self) -> bool:
        """Validate target database."""
        if not self.converter.config.validate_data:
            return True

        try:
            validation_result = self.converter.validator.validate_database(
                self.target_path
            )

            if not validation_result.is_valid:
                logger.error(f"Target validation failed with {len(validation_result.errors)} errors")
                return False

            return True

        except Exception as e:
            logger.error(f"Target validation failed: {e}")
            return False

    def _finalize(self, state_codes: List[int]) -> bool:
        """Finalize conversion and save metadata."""
        try:
            # Calculate final statistics
            with duckdb.connect(str(self.target_path)) as conn:
                self.stats.target_size_bytes = self.target_path.stat().st_size

                # Save conversion metadata
                metadata = ConversionMetadata(
                    conversion_time=datetime.now(),
                    converter_version="1.0.0",  # TODO: Get from package
                    pyfia_version="0.2.0",  # TODO: Get from package
                    source_files=[str(p) for p in self.source_paths],
                    source_checksums={},  # TODO: Calculate checksums
                    states_included=state_codes,
                    evalids_included=[],  # TODO: Extract from data
                    tables_converted=[],  # TODO: Track converted tables
                    indexes_created=[],  # TODO: Track created indexes
                    total_records=self.stats.target_records_written,
                    total_size_bytes=self.stats.target_size_bytes or 0
                )

                # Insert metadata
                conn.execute(metadata.to_sql_insert())

            return True

        except Exception as e:
            logger.error(f"Finalization failed: {e}")
            return False

    def _create_metadata_table(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create metadata table for tracking conversion info."""
        metadata_sql = """
        CREATE TABLE IF NOT EXISTS __pyfia_metadata__ (
            conversion_time TIMESTAMP,
            converter_version VARCHAR(50),
            pyfia_version VARCHAR(50),
            source_files TEXT,
            states_included TEXT,
            evalids_included TEXT,
            tables_converted TEXT,
            total_records BIGINT,
            total_size_bytes BIGINT
        )
        """
        conn.execute(metadata_sql)

    def _is_reference_table(self, table_name: str) -> bool:
        """Check if table is a reference table."""
        return table_name.startswith("REF_")

    def _save_checkpoint(self, stage: str) -> None:
        """Save checkpoint for error recovery."""
        try:
            checkpoint_data = {
                "completed_stage": stage,
                "timestamp": datetime.now().isoformat(),
                "stats": self.stats.model_dump() if hasattr(self.stats, 'model_dump') else {}
            }

            with open(self.checkpoint_path, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)

        except Exception as e:
            logger.debug(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self) -> Optional[Dict]:
        """Load checkpoint for error recovery."""
        try:
            if self.checkpoint_path.exists():
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.debug(f"Failed to load checkpoint: {e}")

        return None
