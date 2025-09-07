"""
Pydantic models for FIA converter configuration and results.

This module defines the data models used throughout the converter system,
providing type safety and validation for configuration, results, and metadata.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConversionStatus(str, Enum):
    """Status of conversion operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ValidationLevel(str, Enum):
    """Level of validation to perform."""
    NONE = "none"
    BASIC = "basic"
    STANDARD = "standard"
    COMPREHENSIVE = "comprehensive"


class CompressionLevel(str, Enum):
    """DuckDB compression levels."""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    ADAPTIVE = "adaptive"


class ConverterConfig(BaseSettings):
    """
    Configuration for FIA SQLite to DuckDB converter.

    Supports environment variable overrides with PYFIA_CONVERTER_ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="PYFIA_CONVERTER_",
        env_file=".env",
        extra="forbid"
    )

    # Source and target paths
    source_dir: Path = Field(
        description="Directory containing SQLite source files"
    )
    target_path: Path = Field(
        default=Path("fia.duckdb"),
        description="Target DuckDB database path"
    )
    temp_dir: Optional[Path] = Field(
        default=None,
        description="Temporary directory for conversion (defaults to system temp)"
    )

    # Processing configuration
    batch_size: int = Field(
        default=100_000,
        ge=1000,
        le=1_000_000,
        description="Records per batch for processing"
    )
    parallel_workers: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Number of parallel worker threads"
    )
    memory_limit: str = Field(
        default="4GB",
        description="DuckDB memory limit"
    )

    # Large table handling
    large_table_threshold: int = Field(
        default=500_000,
        ge=100_000,
        le=10_000_000,
        description="Row count threshold for considering a table 'large' requiring streaming"
    )
    stream_large_tables: bool = Field(
        default=True,
        description="Enable streaming processing for large tables"
    )

    # Validation and quality
    validation_level: ValidationLevel = Field(
        default=ValidationLevel.STANDARD,
        description="Level of data validation to perform"
    )
    validate_data: bool = Field(
        default=True,
        description="Enable data validation during conversion"
    )
    check_referential_integrity: bool = Field(
        default=True,
        description="Check referential integrity constraints"
    )

    # Optimization settings
    create_indexes: bool = Field(
        default=True,
        description="Create optimized indexes after conversion"
    )
    optimize_storage: bool = Field(
        default=True,
        description="Apply storage optimizations"
    )
    compression_level: CompressionLevel = Field(
        default=CompressionLevel.MEDIUM,
        description="DuckDB compression level"
    )
    enable_partitioning: bool = Field(
        default=True,
        description="Enable table partitioning by state"
    )

    # Progress and logging
    show_progress: bool = Field(
        default=True,
        description="Show progress bars during conversion"
    )
    log_level: str = Field(
        default="CRITICAL",
        description="Logging level"
    )
    checkpoint_enabled: bool = Field(
        default=True,
        description="Enable checkpointing for error recovery"
    )
    checkpoint_interval: int = Field(
        default=10_000,
        description="Checkpoint every N records"
    )
    append_mode: bool = Field(
        default=False,
        description="Append data to existing tables without removing existing data"
    )
    dedupe_on_append: bool = Field(
        default=False,
        description="Remove duplicate records when appending based on dedupe_keys"
    )
    dedupe_keys: Optional[List[str]] = Field(
        default=None,
        description="Column names to use for identifying duplicates during append (e.g., ['CN'] for unique records)"
    )

    # State filtering
    include_states: Optional[List[int]] = Field(
        default=None,
        description="State codes to include (None for all)"
    )
    exclude_states: Optional[List[int]] = Field(
        default=None,
        description="State codes to exclude"
    )

    # Table filtering
    include_tables: Optional[List[str]] = Field(
        default=None,
        description="Tables to include (None for all standard tables)"
    )
    exclude_tables: Optional[List[str]] = Field(
        default=None,
        description="Tables to exclude from conversion"
    )


class OptimizedSchema(BaseModel):
    """
    Schema optimization information for a table.
    """

    model_config = ConfigDict(extra="forbid")

    table_name: str
    optimized_types: Dict[str, str]
    indexes: List[str]
    partitioning: Optional[str] = None
    compression_config: Dict[str, Any]
    estimated_size_reduction: float = Field(
        description="Estimated storage size reduction ratio"
    )


class ConversionStats(BaseModel):
    """
    Statistics about the conversion process.
    """

    model_config = ConfigDict(extra="forbid")

    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Source statistics
    source_file_count: int
    source_total_size_bytes: int
    source_tables_processed: int
    source_records_processed: int

    # Target statistics
    target_size_bytes: Optional[int] = None
    target_tables_created: int = 0
    target_records_written: int = 0
    target_indexes_created: int = 0

    # Performance metrics
    compression_ratio: Optional[float] = None
    throughput_records_per_second: Optional[float] = None
    memory_peak_usage_bytes: Optional[int] = None

    # Error statistics
    errors_encountered: int = 0
    warnings_encountered: int = 0
    tables_failed: List[str] = Field(default_factory=list)

    def calculate_derived_metrics(self):
        """Calculate derived performance metrics."""
        if self.end_time and self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()

            if self.duration_seconds > 0:
                self.throughput_records_per_second = (
                    self.target_records_written / self.duration_seconds
                )

        if self.source_total_size_bytes > 0 and self.target_size_bytes:
            self.compression_ratio = (
                self.source_total_size_bytes / self.target_size_bytes
            )


class ValidationError(BaseModel):
    """
    Individual validation error details.
    """

    model_config = ConfigDict(extra="forbid")

    error_type: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    message: str
    severity: str = Field(default="error")  # error, warning, info
    record_count: Optional[int] = None


class ValidationResult(BaseModel):
    """
    Results of data validation process.
    """

    model_config = ConfigDict(extra="forbid")

    is_valid: bool
    validation_time: datetime
    validation_duration_seconds: float

    errors: List[ValidationError] = Field(default_factory=list)
    warnings: List[ValidationError] = Field(default_factory=list)

    # Summary statistics
    tables_validated: int = 0
    records_validated: int = 0
    constraints_checked: int = 0
    referential_integrity_checks: int = 0

    def add_error(self, error: ValidationError):
        """Add a validation error."""
        if error.severity == "error":
            self.errors.append(error)
            self.is_valid = False
        else:
            self.warnings.append(error)


class ConversionResult(BaseModel):
    """
    Results of a conversion operation.
    """

    model_config = ConfigDict(extra="forbid")

    status: ConversionStatus
    config: ConverterConfig
    stats: ConversionStats
    validation: Optional[ValidationResult] = None

    # File paths
    source_paths: List[Path]
    target_path: Path
    checkpoint_path: Optional[Path] = None
    log_path: Optional[Path] = None

    # Schema information
    schemas: Dict[str, OptimizedSchema] = Field(default_factory=dict)

    # Error information
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    def is_successful(self) -> bool:
        """Check if conversion was successful."""
        return self.status == ConversionStatus.COMPLETED and (
            self.validation is None or self.validation.is_valid
        )

    def summary(self) -> str:
        """Generate human-readable summary of results."""
        if self.status == ConversionStatus.COMPLETED:
            summary = "✅ Conversion completed successfully\n"
            summary += f"📁 Converted {len(self.source_paths)} source file(s)\n"
            summary += f"📊 Processed {self.stats.source_records_processed:,} records\n"

            if self.stats.duration_seconds:
                summary += f"⏱️  Completed in {self.stats.duration_seconds:.1f} seconds\n"

            if self.stats.compression_ratio:
                summary += f"🗜️  Compression ratio: {self.stats.compression_ratio:.2f}x\n"

            if self.stats.throughput_records_per_second:
                summary += f"⚡ Throughput: {self.stats.throughput_records_per_second:,.0f} records/sec\n"

        else:
            summary = f"❌ Conversion {self.status.value}\n"
            if self.error_message:
                summary += f"Error: {self.error_message}\n"

        return summary


class UpdateResult(BaseModel):
    """
    Results of an incremental update operation.
    """

    model_config = ConfigDict(extra="forbid")

    status: ConversionStatus
    update_time: datetime
    state_code: int

    # Update statistics
    records_added: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    tables_affected: List[str] = Field(default_factory=list)

    # Validation
    validation: Optional[ValidationResult] = None

    # Error information
    error_message: Optional[str] = None


class ConversionMetadata(BaseModel):
    """
    Metadata about a converted database.
    """

    model_config = ConfigDict(extra="forbid")

    # Conversion information
    conversion_time: datetime
    converter_version: str
    pyfia_version: str

    # Source information
    source_files: List[str]
    source_checksums: Dict[str, str]

    # State information
    states_included: List[int]
    evalids_included: List[int]

    # Schema information
    tables_converted: List[str]
    indexes_created: List[str]

    # Statistics
    total_records: int
    total_size_bytes: int

    def to_sql_insert(self) -> str:
        """Generate SQL INSERT statement for metadata table."""
        return f"""
        INSERT INTO __pyfia_metadata__ (
            conversion_time, converter_version, pyfia_version,
            source_files, states_included, evalids_included,
            tables_converted, total_records, total_size_bytes
        ) VALUES (
            '{self.conversion_time.isoformat()}',
            '{self.converter_version}',
            '{self.pyfia_version}',
            '{",".join(self.source_files)}',
            '{",".join(map(str, self.states_included))}',
            '{",".join(map(str, self.evalids_included))}',
            '{",".join(self.tables_converted)}',
            {self.total_records},
            {self.total_size_bytes}
        )
        """


@dataclass
class StateData:
    """
    Container for state-specific data during merging.
    """
    state_code: int
    source_path: Path
    tables: Dict[str, Any]  # Table name -> DataFrame
    metadata: Dict[str, Any]

    def __post_init__(self):
        """Validate state data after initialization."""
        if not self.source_path.exists():
            raise FileNotFoundError(f"Source file not found: {self.source_path}")


class IntegrityError(BaseModel):
    """
    Referential integrity error details.
    """

    model_config = ConfigDict(extra="forbid")

    constraint_type: str  # foreign_key, unique, check, etc.
    parent_table: str
    child_table: str
    parent_column: str
    child_column: str
    violation_count: int
    example_values: List[str] = Field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"{self.constraint_type} violation: "
            f"{self.child_table}.{self.child_column} -> "
            f"{self.parent_table}.{self.parent_column} "
            f"({self.violation_count} violations)"
        )


# Define standard FIA table configurations
STANDARD_FIA_TABLES = {
    "PLOT", "TREE", "COND", "SUBPLOT", "BOUNDARY",
    "POP_EVAL", "POP_EVAL_TYP", "POP_EVAL_GRP",
    "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN", "POP_ESTN_UNIT",
    "REF_SPECIES", "REF_FOREST_TYPE", "REF_HABITAT_TYPE"
}

# Priority order for table conversion (dependencies first)
TABLE_CONVERSION_ORDER = [
    # Reference tables first
    "REF_SPECIES", "REF_FOREST_TYPE", "REF_HABITAT_TYPE",
    # Population tables
    "POP_EVAL", "POP_EVAL_TYP", "POP_EVAL_GRP",
    "POP_ESTN_UNIT", "POP_STRATUM",
    # Core measurement tables
    "PLOT", "COND", "SUBPLOT", "BOUNDARY",
    "TREE",
    # Assignment tables last
    "POP_PLOT_STRATUM_ASSGN"
]
