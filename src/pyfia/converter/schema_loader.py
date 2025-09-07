"""
Schema loader for FIA table definitions.

This module loads and parses YAML schema definitions for FIA tables,
providing type mapping between YAML schema and Polars/DuckDB types.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

import polars as pl
import yaml

logger = logging.getLogger(__name__)


class FIASchemaLoader:
    """
    Loads and manages FIA table schema definitions from YAML files.

    Provides mapping between YAML schema definitions and Polars/DuckDB
    data types for consistent schema handling across the conversion pipeline.
    """

    # Mapping from YAML schema types to Polars types
    TYPE_MAPPING = {
        "tinyint": pl.Int8,
        "smallint": pl.Int16,
        "integer": pl.Int32,
        "bigint": pl.Int64,
        "decimal": pl.Float64,  # Will be overridden for specific precisions
        "varchar": pl.Utf8,
        "boolean": pl.Boolean,
        "date": pl.Date,
        "timestamp": pl.Datetime,
    }

    def __init__(self, schema_file: Optional[Union[str, Path]] = None):
        """
        Initialize schema loader.

        Parameters
        ----------
        schema_file : str or Path, optional
            Path to YAML schema file. If None, uses default bundled schema.
        """
        if schema_file is None:
            # Use bundled schema file
            schema_file = Path(__file__).parent / "schemas" / "fia_table_schemas.yaml"

        self.schema_file = Path(schema_file)
        self.schema_dir = self.schema_file.parent
        self.schemas = self._load_schemas()

        # Load separate TREE schema if it exists
        tree_schema_file = self.schema_dir / "tree_table_schema.yaml"
        if tree_schema_file.exists():
            self.tree_schema = self._load_tree_schema(tree_schema_file)
            logger.info(f"Loaded separate TREE schema from {tree_schema_file}")
        else:
            self.tree_schema = None

        # Load separate COND schema if it exists
        cond_schema_file = self.schema_dir / "cond_table_schema.yaml"
        if cond_schema_file.exists():
            self.cond_schema = self._load_cond_schema(cond_schema_file)
            logger.info(f"Loaded separate COND schema from {cond_schema_file}")
        else:
            self.cond_schema = None

        logger.info(f"Loaded FIA schemas from {self.schema_file}")

    def _load_schemas(self) -> Dict[str, Any]:
        """Load schema definitions from YAML file."""
        try:
            with open(self.schema_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load schema file {self.schema_file}: {e}")
            return {}

    def _load_tree_schema(self, tree_schema_file: Path) -> Dict[str, Any]:
        """Load TREE table schema from separate YAML file."""
        try:
            with open(tree_schema_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load TREE schema file {tree_schema_file}: {e}")
            return {}

    def _load_cond_schema(self, cond_schema_file: Path) -> Dict[str, Any]:
        """Load COND table schema from separate YAML file."""
        try:
            with open(cond_schema_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load COND schema file {cond_schema_file}: {e}")
            return {}

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, pl.DataType]]:
        """
        Get Polars schema for a specific table.

        Parameters
        ----------
        table_name : str
            Name of the FIA table

        Returns
        -------
        Dict[str, pl.DataType] or None
            Polars schema mapping column names to data types
        """
        # Check for TREE table in separate schema file first
        if table_name == "TREE" and self.tree_schema is not None:
            if "TREE" in self.tree_schema:
                return self._convert_yaml_schema_to_polars(self.tree_schema["TREE"])

        # Check for COND table in separate schema file first
        if table_name == "COND" and self.cond_schema is not None:
            if "COND" in self.cond_schema:
                return self._convert_yaml_schema_to_polars(self.cond_schema["COND"])

        # Check all table categories in main schema
        for category in ["reference_tables", "population_tables", "measurement_tables"]:
            if category in self.schemas:
                tables = self.schemas[category]
                if table_name in tables:
                    return self._convert_yaml_schema_to_polars(tables[table_name])

        logger.debug(f"No schema definition found for table: {table_name}")
        return None

    def get_table_duckdb_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """
        Get DuckDB schema for a specific table.

        Parameters
        ----------
        table_name : str
            Name of the FIA table

        Returns
        -------
        Dict[str, str] or None
            DuckDB schema mapping column names to SQL types
        """
        # Check for TREE table in separate schema file first
        if table_name == "TREE" and self.tree_schema is not None:
            if "TREE" in self.tree_schema:
                return self._convert_yaml_schema_to_duckdb(self.tree_schema["TREE"])

        # Check for COND table in separate schema file first
        if table_name == "COND" and self.cond_schema is not None:
            if "COND" in self.cond_schema:
                return self._convert_yaml_schema_to_duckdb(self.cond_schema["COND"])

        # Check all table categories in main schema
        for category in ["reference_tables", "population_tables", "measurement_tables"]:
            if category in self.schemas:
                tables = self.schemas[category]
                if table_name in tables:
                    return self._convert_yaml_schema_to_duckdb(tables[table_name])

        logger.debug(f"No schema definition found for table: {table_name}")
        return None

    def _convert_yaml_schema_to_polars(self, table_def: Dict[str, Any]) -> Dict[str, pl.DataType]:
        """Convert YAML table definition to Polars schema."""
        polars_schema = {}

        columns = table_def.get("columns", {})
        for col_name, col_def in columns.items():
            col_type = col_def.get("type", "varchar")
            polars_schema[col_name] = self._yaml_type_to_polars(col_type)

        return polars_schema

    def _convert_yaml_schema_to_duckdb(self, table_def: Dict[str, Any]) -> Dict[str, str]:
        """
        Convert YAML table definition to DuckDB schema.
        
        Prioritizes 'duckdb_type' field if present, otherwise uses 'type' field.
        """
        duckdb_schema = {}

        columns = table_def.get("columns", {})
        for col_name, col_def in columns.items():
            col_type = col_def.get("type", "varchar")
            duckdb_schema[col_name] = self._yaml_type_to_duckdb(col_type)

        return duckdb_schema

    def _yaml_type_to_polars(self, yaml_type: str) -> pl.DataType:
        """Convert YAML type definition to Polars data type."""
        # Handle parameterized types like decimal(10,3) and varchar(100)
        base_type = yaml_type.split("(")[0].lower()

        if base_type in self.TYPE_MAPPING:
            polars_type = self.TYPE_MAPPING[base_type]

            # Special handling for decimal types
            if base_type == "decimal":
                # For Polars, we'll use Float64 for decimals
                # DuckDB will handle the precision/scale
                return pl.Float64

            return polars_type
        else:
            logger.warning(f"Unknown YAML type: {yaml_type}, defaulting to Utf8")
            return pl.Utf8

    def _yaml_type_to_duckdb(self, yaml_type: str) -> str:
        """Convert YAML type definition to DuckDB SQL type."""
        # DuckDB types can be used directly from YAML
        return yaml_type.upper()

    def list_available_tables(self) -> Dict[str, list]:
        """
        List all tables defined in the schema.

        Returns
        -------
        Dict[str, list]
            Dictionary mapping category names to lists of table names
        """
        available_tables = {}

        for category in ["reference_tables", "population_tables", "measurement_tables"]:
            if category in self.schemas:
                available_tables[category] = list(self.schemas[category].keys())

        return available_tables

    def get_table_description(self, table_name: str) -> Optional[str]:
        """Get description for a specific table."""
        # Check for TREE table in separate schema file first
        if table_name == "TREE" and self.tree_schema is not None:
            if "TREE" in self.tree_schema:
                return self.tree_schema["TREE"].get("description")

        # Check for COND table in separate schema file first
        if table_name == "COND" and self.cond_schema is not None:
            if "COND" in self.cond_schema:
                return self.cond_schema["COND"].get("description")

        # Check main schema
        for category in ["reference_tables", "population_tables", "measurement_tables"]:
            if category in self.schemas:
                tables = self.schemas[category]
                if table_name in tables:
                    return tables[table_name].get("description")
        return None

    def get_column_info(self, table_name: str, column_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific column."""
        # Check for TREE table in separate schema file first
        if table_name == "TREE" and self.tree_schema is not None:
            if "TREE" in self.tree_schema:
                columns = self.tree_schema["TREE"].get("columns", {})
                if column_name in columns:
                    return columns[column_name]

        # Check for COND table in separate schema file first
        if table_name == "COND" and self.cond_schema is not None:
            if "COND" in self.cond_schema:
                columns = self.cond_schema["COND"].get("columns", {})
                if column_name in columns:
                    return columns[column_name]

        # Check main schema
        for category in ["reference_tables", "population_tables", "measurement_tables"]:
            if category in self.schemas:
                tables = self.schemas[category]
                if table_name in tables:
                    columns = tables[table_name].get("columns", {})
                    if column_name in columns:
                        return columns[column_name]
        return None

    def is_problematic_column(self, table_name: str, column_name: str) -> bool:
        """Check if a column is known to have data quality issues."""
        metadata = self.schemas.get("metadata", {})
        problematic = metadata.get("problematic_columns", [])

        for prob_col in problematic:
            if prob_col.get("column") == column_name:
                return True
        return False

    def get_schema_version(self) -> str:
        """Get the version of the loaded schema."""
        return self.schemas.get("version", "unknown")

    def validate_table_against_schema(
        self,
        table_name: str,
        df: pl.DataFrame
    ) -> Dict[str, Any]:
        """
        Validate a DataFrame against the defined schema.

        Parameters
        ----------
        table_name : str
            Name of the table
        df : pl.DataFrame
            DataFrame to validate

        Returns
        -------
        Dict[str, Any]
            Validation results including missing columns, extra columns, type mismatches
        """
        schema = self.get_table_schema(table_name)
        if schema is None:
            return {"error": f"No schema definition for table {table_name}"}

        df_columns = set(df.columns)
        schema_columns = set(schema.keys())

        missing_columns = schema_columns - df_columns
        extra_columns = df_columns - schema_columns

        type_mismatches = []
        for col in df_columns.intersection(schema_columns):
            expected_type = schema[col]
            actual_type = df.select(col).dtypes[0]
            if expected_type != actual_type:
                type_mismatches.append({
                    "column": col,
                    "expected": str(expected_type),
                    "actual": str(actual_type)
                })

        return {
            "table_name": table_name,
            "valid": len(missing_columns) == 0 and len(type_mismatches) == 0,
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "type_mismatches": type_mismatches,
            "total_columns": len(schema_columns),
            "matched_columns": len(df_columns.intersection(schema_columns))
        }


# Global schema loader instance
_schema_loader = None

def get_schema_loader() -> FIASchemaLoader:
    """Get global schema loader instance (singleton)."""
    global _schema_loader
    if _schema_loader is None:
        _schema_loader = FIASchemaLoader()
    return _schema_loader
