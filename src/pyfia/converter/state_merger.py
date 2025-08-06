"""
Multi-state database merging utilities for FIA converter.

This module handles merging data from multiple state FIA databases,
including conflict resolution, EVALID management, and cross-state validation.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List

import polars as pl

from .models import IntegrityError, StateData

logger = logging.getLogger(__name__)


class StateMerger:
    """
    Handles merging multiple state databases with conflict resolution.

    Features:
    - Intelligent conflict resolution
    - EVALID management across states
    - Cross-state referential integrity
    - Deduplication strategies
    """

    # Tables that can be safely merged across states
    MERGEABLE_TABLES = {
        "PLOT", "TREE", "COND", "SUBPLOT", "BOUNDARY",
        "POP_PLOT_STRATUM_ASSGN", "POP_EVAL", "POP_STRATUM",
        "POP_ESTN_UNIT", "POP_EVAL_TYP", "POP_EVAL_GRP"
    }

    # Reference tables that should be deduplicated
    REFERENCE_TABLES = {
        "REF_SPECIES", "REF_FOREST_TYPE", "REF_HABITAT_TYPE"
    }

    # Priority order for resolving conflicts (higher number = higher priority)
    CONFLICT_RESOLUTION_PRIORITY = {
        "most_recent_invyr": 1,
        "highest_data_quality": 2,
        "state_precedence": 3,
        "largest_dataset": 4
    }

    def __init__(self):
        """Initialize the state merger."""
        self.merge_statistics = defaultdict(int)
        self.conflicts_detected = []
        self.deduplication_stats = defaultdict(int)

    def merge_table_data(
        self,
        table_name: str,
        dataframes: List[pl.DataFrame]
    ) -> pl.DataFrame:
        """
        Merge data from multiple states for a specific table.

        Parameters
        ----------
        table_name : str
            Name of the table being merged
        dataframes : List[pl.DataFrame]
            List of dataframes from different states

        Returns
        -------
        pl.DataFrame
            Merged dataframe with conflicts resolved
        """
        if not dataframes:
            return pl.DataFrame()

        if len(dataframes) == 1:
            return dataframes[0]

        logger.info(f"Merging {len(dataframes)} dataframes for table {table_name}")

        # Apply table-specific merge strategy
        if table_name in self.REFERENCE_TABLES:
            merged_df = self._merge_reference_table(table_name, dataframes)
        elif table_name in self.MERGEABLE_TABLES:
            merged_df = self._merge_measurement_table(table_name, dataframes)
        else:
            # Default: simple concatenation with basic deduplication
            merged_df = self._merge_with_basic_deduplication(table_name, dataframes)

        logger.info(
            f"Merged {table_name}: {sum(len(df) for df in dataframes)} -> "
            f"{len(merged_df)} records"
        )

        return merged_df

    def merge_states(
        self,
        states: List[StateData]
    ) -> Dict[str, pl.DataFrame]:
        """
        Merge complete state datasets.

        Parameters
        ----------
        states : List[StateData]
            List of state data containers

        Returns
        -------
        Dict[str, pl.DataFrame]
            Merged tables by name
        """
        logger.info(f"Merging {len(states)} complete state datasets")

        # Collect all unique table names
        all_tables = set()
        for state in states:
            all_tables.update(state.tables.keys())

        merged_tables = {}

        # Merge each table
        for table_name in sorted(all_tables):
            # Collect dataframes for this table from all states that have it
            table_dfs = []
            for state in states:
                if table_name in state.tables and state.tables[table_name] is not None:
                    df = state.tables[table_name]
                    if len(df) > 0:
                        # Add state identifier if not present
                        if "STATECD" not in df.columns and hasattr(state, 'state_code'):
                            df = df.with_columns(pl.lit(state.state_code).alias("STATECD"))
                        table_dfs.append(df)

            if table_dfs:
                merged_tables[table_name] = self.merge_table_data(table_name, table_dfs)
            else:
                logger.warning(f"No data found for table {table_name} across all states")

        return merged_tables

    def resolve_conflicts(
        self,
        df: pl.DataFrame,
        conflict_columns: List[str] = None
    ) -> pl.DataFrame:
        """
        Resolve conflicts in merged data using priority rules.

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe with potential conflicts
        conflict_columns : List[str], optional
            Columns to check for conflicts (defaults to key columns)

        Returns
        -------
        pl.DataFrame
            Dataframe with conflicts resolved
        """
        if conflict_columns is None:
            conflict_columns = self._get_key_columns(df)

        # Find duplicates based on key columns
        if not conflict_columns:
            return df

        # Group by key columns and detect conflicts
        grouped = df.group_by(conflict_columns)

        # For each group with conflicts, apply resolution strategy
        resolved_parts = []

        for group_key, group_df in grouped:
            if len(group_df) > 1:
                # Conflict detected
                resolved_record = self._resolve_single_conflict(group_df, conflict_columns)
                resolved_parts.append(resolved_record)

                self.conflicts_detected.append({
                    "key_columns": conflict_columns,
                    "key_values": group_key,
                    "conflict_count": len(group_df),
                    "resolution_method": "priority_based"
                })
            else:
                # No conflict
                resolved_parts.append(group_df)

        if resolved_parts:
            return pl.concat(resolved_parts)
        else:
            return df

    def update_evalids(
        self,
        df: pl.DataFrame,
        state_code: int
    ) -> pl.DataFrame:
        """
        Update EVALID values to ensure uniqueness across states.

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe with EVALID column
        state_code : int
            State FIPS code

        Returns
        -------
        pl.DataFrame
            Dataframe with updated EVALIDs
        """
        if "EVALID" not in df.columns:
            return df

        # FIA EVALID format: SSYYYY where SS=state code, YYYY=sequence
        # Ensure EVALIDs are unique by incorporating state code

        def ensure_unique_evalid(evalid: int, state: int) -> int:
            """Ensure EVALID is unique by incorporating state code."""
            # If EVALID already includes state code, return as-is
            if evalid >= state * 10000 and evalid < (state + 1) * 10000:
                return evalid

            # Otherwise, create unique EVALID
            sequence = evalid % 10000
            return state * 10000 + sequence

        # Update EVALIDs
        updated_df = df.with_columns(
            pl.col("EVALID").map_elements(
                lambda x: ensure_unique_evalid(x, state_code),
                return_dtype=pl.Int32
            )
        )

        return updated_df

    def validate_cross_state_integrity(
        self,
        merged_tables: Dict[str, pl.DataFrame]
    ) -> List[IntegrityError]:
        """
        Validate referential integrity across merged state data.

        Parameters
        ----------
        merged_tables : Dict[str, pl.DataFrame]
            Merged tables to validate

        Returns
        -------
        List[IntegrityError]
            List of integrity violations found
        """
        integrity_errors = []

        # Define key relationships to check
        relationships = [
            ("TREE", "PLT_CN", "PLOT", "CN"),
            ("COND", "PLT_CN", "PLOT", "CN"),
            ("POP_PLOT_STRATUM_ASSGN", "PLT_CN", "PLOT", "CN"),
            ("POP_PLOT_STRATUM_ASSGN", "STRATUM_CN", "POP_STRATUM", "CN"),
            ("POP_STRATUM", "EVALID", "POP_EVAL", "EVALID"),
        ]

        for child_table, child_col, parent_table, parent_col in relationships:
            if child_table in merged_tables and parent_table in merged_tables:
                errors = self._check_foreign_key_integrity(
                    child_table, child_col, merged_tables[child_table],
                    parent_table, parent_col, merged_tables[parent_table]
                )
                integrity_errors.extend(errors)

        # Check for duplicate primary keys within tables
        for table_name, df in merged_tables.items():
            if "CN" in df.columns:
                errors = self._check_primary_key_uniqueness(table_name, df, "CN")
                integrity_errors.extend(errors)

        return integrity_errors

    def get_merge_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the merge operation.

        Returns
        -------
        Dict[str, Any]
            Merge statistics
        """
        return {
            "tables_merged": dict(self.merge_statistics),
            "conflicts_detected": len(self.conflicts_detected),
            "conflicts_by_table": self._group_conflicts_by_table(),
            "deduplication_stats": dict(self.deduplication_stats),
            "integrity_checks_performed": len(self.conflicts_detected)
        }

    def _merge_reference_table(
        self,
        table_name: str,
        dataframes: List[pl.DataFrame]
    ) -> pl.DataFrame:
        """
        Merge reference tables with deduplication.

        Reference tables (REF_SPECIES, etc.) should have identical content
        across states, so we deduplicate based on primary keys.
        """
        # Concatenate all dataframes
        combined = pl.concat(dataframes, how="vertical_relaxed")

        # Determine primary key column(s)
        if table_name == "REF_SPECIES":
            pk_cols = ["SPCD"]
        elif table_name == "REF_FOREST_TYPE":
            pk_cols = ["FORTYPCD"]
        elif table_name == "REF_HABITAT_TYPE":
            pk_cols = ["HABTYPCD"] if "HABTYPCD" in combined.columns else ["CN"]
        else:
            pk_cols = ["CN"] if "CN" in combined.columns else []

        if pk_cols:
            # Remove duplicates based on primary key
            original_count = len(combined)
            deduplicated = combined.unique(subset=pk_cols, keep="first")

            duplicates_removed = original_count - len(deduplicated)
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicates from {table_name}")
                self.deduplication_stats[table_name] = duplicates_removed

            return deduplicated
        else:
            # Fallback: remove exact duplicates
            return combined.unique()

    def _merge_measurement_table(
        self,
        table_name: str,
        dataframes: List[pl.DataFrame]
    ) -> pl.DataFrame:
        """
        Merge measurement tables with conflict resolution.

        Measurement tables may have overlapping plots near state boundaries
        or different measurement years that need intelligent merging.
        """
        # Concatenate all dataframes
        combined = pl.concat(dataframes, how="vertical_relaxed")

        # Apply table-specific merge logic
        if table_name == "PLOT":
            return self._merge_plot_table(combined)
        elif table_name == "TREE":
            return self._merge_tree_table(combined)
        elif table_name == "COND":
            return self._merge_condition_table(combined)
        elif table_name.startswith("POP_"):
            return self._merge_population_table(combined)
        else:
            # Default merge with basic conflict resolution
            return self._merge_with_conflict_resolution(combined)

    def _merge_plot_table(self, df: pl.DataFrame) -> pl.DataFrame:
        """Merge PLOT table with boundary plot handling."""
        if "CN" not in df.columns:
            return df

        # Check for duplicate plots (same location, different states)
        if all(col in df.columns for col in ["LAT", "LON", "STATECD"]):
            # Group by approximate location (round to avoid floating-point issues)
            df_with_rounded = df.with_columns([
                (pl.col("LAT") * 100000).round() / 100000,
                (pl.col("LON") * 100000).round() / 100000
            ])

            # Find potential boundary plots
            location_groups = df_with_rounded.group_by(["LAT", "LON"])

            resolved_plots = []
            for group_key, group_df in location_groups:
                if len(group_df) > 1 and len(group_df.select("STATECD").unique()) > 1:
                    # Boundary plot - choose most recent or highest quality
                    resolved_plot = self._resolve_boundary_plot(group_df)
                    resolved_plots.append(resolved_plot)
                else:
                    resolved_plots.append(group_df)

            if resolved_plots:
                return pl.concat(resolved_plots)

        # Fallback: resolve by CN
        return self.resolve_conflicts(df, ["CN"])

    def _merge_tree_table(self, df: pl.DataFrame) -> pl.DataFrame:
        """Merge TREE table handling cross-state plot references."""
        # Trees should be unique by CN
        if "CN" in df.columns:
            return df.unique(subset=["CN"], keep="first")
        else:
            return df

    def _merge_condition_table(self, df: pl.DataFrame) -> pl.DataFrame:
        """Merge COND table with condition-specific logic."""
        # Conditions should be unique by CN
        if "CN" in df.columns:
            return df.unique(subset=["CN"], keep="first")
        else:
            return df

    def _merge_population_table(self, df: pl.DataFrame) -> pl.DataFrame:
        """Merge population tables with EVALID awareness."""
        # Population tables may have state-specific EVALIDs that need coordination
        if "EVALID" in df.columns and "STATECD" in df.columns:
            # Ensure EVALIDs are unique across states
            unique_states = df.select("STATECD").unique().to_series().to_list()

            updated_parts = []
            for state_code in unique_states:
                state_data = df.filter(pl.col("STATECD") == state_code)
                updated_state_data = self.update_evalids(state_data, state_code)
                updated_parts.append(updated_state_data)

            return pl.concat(updated_parts)

        # Default handling
        if "CN" in df.columns:
            return df.unique(subset=["CN"], keep="first")
        else:
            return df

    def _merge_with_basic_deduplication(
        self,
        table_name: str,
        dataframes: List[pl.DataFrame]
    ) -> pl.DataFrame:
        """Basic merge with simple deduplication."""
        combined = pl.concat(dataframes, how="vertical_relaxed")

        # Try to deduplicate by CN if it exists
        if "CN" in combined.columns:
            original_count = len(combined)
            deduplicated = combined.unique(subset=["CN"], keep="first")

            if len(deduplicated) < original_count:
                logger.info(f"Removed {original_count - len(deduplicated)} duplicates from {table_name}")
                self.deduplication_stats[table_name] = original_count - len(deduplicated)

            return deduplicated
        else:
            # Remove exact row duplicates
            return combined.unique()

    def _merge_with_conflict_resolution(self, df: pl.DataFrame) -> pl.DataFrame:
        """Merge with intelligent conflict resolution."""
        key_columns = self._get_key_columns(df)

        if key_columns:
            return self.resolve_conflicts(df, key_columns)
        else:
            return df.unique()

    def _resolve_single_conflict(
        self,
        conflict_df: pl.DataFrame,
        key_columns: List[str]
    ) -> pl.DataFrame:
        """
        Resolve a single conflict using priority rules.

        Parameters
        ----------
        conflict_df : pl.DataFrame
            Dataframe containing conflicting records
        key_columns : List[str]
            Key columns that define the conflict

        Returns
        -------
        pl.DataFrame
            Single resolved record
        """
        if len(conflict_df) <= 1:
            return conflict_df

        # Priority 1: Most recent inventory year
        if "INVYR" in conflict_df.columns:
            max_year = conflict_df.select("INVYR").max().item()
            recent_records = conflict_df.filter(pl.col("INVYR") == max_year)
            if len(recent_records) == 1:
                return recent_records
            conflict_df = recent_records

        # Priority 2: Highest data quality (least nulls)
        if len(conflict_df) > 1:
            null_counts = []
            for i in range(len(conflict_df)):
                row = conflict_df.slice(i, 1)
                null_count = sum(row.null_count().row(0))
                null_counts.append((null_count, i))

            # Choose record with fewest nulls
            min_nulls, best_idx = min(null_counts)
            best_record = conflict_df.slice(best_idx, 1)

            # Check if there are ties
            tied_records = [idx for null_count, idx in null_counts if null_count == min_nulls]
            if len(tied_records) == 1:
                return best_record

            # If tied, take the first one
            conflict_df = conflict_df.slice(tied_records[0], 1)

        # Priority 3: Take first record (arbitrary but consistent)
        return conflict_df.slice(0, 1)

    def _resolve_boundary_plot(self, plot_df: pl.DataFrame) -> pl.DataFrame:
        """
        Resolve conflicts for plots near state boundaries.

        Parameters
        ----------
        plot_df : pl.DataFrame
            Dataframe with boundary plot records

        Returns
        -------
        pl.DataFrame
            Single resolved plot record
        """
        # For boundary plots, prefer the plot from the state that "owns" it
        # This is typically determined by plot status, ownership, or other criteria

        # Priority 1: Active plots over inactive
        if "PLOT_STATUS_CD" in plot_df.columns:
            active_plots = plot_df.filter(pl.col("PLOT_STATUS_CD") == 1)
            if len(active_plots) > 0:
                plot_df = active_plots

        # Priority 2: Most recent measurement
        if "INVYR" in plot_df.columns and len(plot_df) > 1:
            max_year = plot_df.select("INVYR").max().item()
            recent_plots = plot_df.filter(pl.col("INVYR") == max_year)
            if len(recent_plots) > 0:
                plot_df = recent_plots

        # Priority 3: State with lower FIPS code (arbitrary but consistent)
        if "STATECD" in plot_df.columns and len(plot_df) > 1:
            min_state = plot_df.select("STATECD").min().item()
            preferred_state = plot_df.filter(pl.col("STATECD") == min_state)
            if len(preferred_state) > 0:
                plot_df = preferred_state

        # Return first record if still tied
        return plot_df.slice(0, 1)

    def _get_key_columns(self, df: pl.DataFrame) -> List[str]:
        """
        Determine key columns for conflict detection.

        Parameters
        ----------
        df : pl.DataFrame
            Dataframe to analyze

        Returns
        -------
        List[str]
            Key columns for uniqueness
        """
        # Standard FIA key column priorities
        key_candidates = [
            ["CN"],  # Control Number (primary key)
            ["STATECD", "UNITCD", "COUNTYCD", "PLOT", "INVYR"],  # Plot identifier
            ["PLT_CN", "CONDID"],  # Condition identifier
            ["PLT_CN", "TREE"],  # Tree identifier
            ["EVALID", "PLT_CN"],  # Evaluation-plot combination
        ]

        for candidate in key_candidates:
            if all(col in df.columns for col in candidate):
                return candidate

        # Fallback: any column with 'CD' or 'ID' suffix
        fallback_cols = [col for col in df.columns
                        if col.endswith('CD') or col.endswith('ID') or col == 'CN']

        return fallback_cols[:3]  # Limit to first 3 columns

    def _check_foreign_key_integrity(
        self,
        child_table: str,
        child_col: str,
        child_df: pl.DataFrame,
        parent_table: str,
        parent_col: str,
        parent_df: pl.DataFrame
    ) -> List[IntegrityError]:
        """Check foreign key integrity between tables."""
        errors = []

        try:
            # Get child values that don't exist in parent
            child_values = child_df.select(child_col).unique()
            parent_values = parent_df.select(parent_col).unique()

            # Find orphaned references
            orphaned = child_values.join(
                parent_values,
                left_on=child_col,
                right_on=parent_col,
                how="anti"
            )

            if len(orphaned) > 0:
                # Count violations
                violation_counts = child_df.join(
                    orphaned,
                    on=child_col,
                    how="inner"
                ).group_by(child_col).len()

                for row in violation_counts.iter_rows():
                    value, count = row
                    errors.append(IntegrityError(
                        constraint_type="foreign_key",
                        parent_table=parent_table,
                        child_table=child_table,
                        parent_column=parent_col,
                        child_column=child_col,
                        violation_count=count,
                        example_values=[str(value)]
                    ))

        except Exception as e:
            logger.debug(f"Failed to check FK integrity {child_table}.{child_col} -> {parent_table}.{parent_col}: {e}")

        return errors

    def _check_primary_key_uniqueness(
        self,
        table_name: str,
        df: pl.DataFrame,
        pk_col: str
    ) -> List[IntegrityError]:
        """Check primary key uniqueness within a table."""
        errors = []

        try:
            # Find duplicate primary keys
            pk_counts = df.group_by(pk_col).len()
            duplicates = pk_counts.filter(pl.col("len") > 1)

            if len(duplicates) > 0:
                total_violations = duplicates.select(pl.col("len").sum()).item()
                duplicate_values = duplicates.select(pk_col).to_series().to_list()

                errors.append(IntegrityError(
                    constraint_type="primary_key",
                    parent_table=table_name,
                    child_table=table_name,
                    parent_column=pk_col,
                    child_column=pk_col,
                    violation_count=total_violations,
                    example_values=[str(v) for v in duplicate_values[:10]]
                ))

        except Exception as e:
            logger.debug(f"Failed to check PK uniqueness for {table_name}.{pk_col}: {e}")

        return errors

    def _group_conflicts_by_table(self) -> Dict[str, int]:
        """Group detected conflicts by table for reporting."""
        conflicts_by_table = defaultdict(int)

        for conflict in self.conflicts_detected:
            # Extract table from conflict context (this is simplified)
            conflicts_by_table["unknown"] += 1

        return dict(conflicts_by_table)
