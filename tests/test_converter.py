"""
Basic tests for FIA SQLite to DuckDB converter functionality.

This module provides fundamental tests for the converter components
to ensure core functionality works correctly.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import polars as pl

from pyfia.converter import (
    ConverterConfig,
    FIAConverter,
    SchemaOptimizer,
    StateMerger,
    DataValidator
)
from pyfia.converter.models import (
    ConversionStatus,
    ValidationLevel,
    CompressionLevel,
    OptimizedSchema
)


class TestConverterConfig:
    """Test converter configuration."""
    
    def test_default_config(self):
        """Test default configuration creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = ConverterConfig(source_dir=Path(temp_dir))
            
            assert config.source_dir == Path(temp_dir)
            assert config.target_path == Path("fia.duckdb")
            assert config.batch_size == 100_000
            assert config.parallel_workers == 4
            assert config.validation_level == ValidationLevel.STANDARD
            assert config.compression_level == CompressionLevel.MEDIUM
            assert config.create_indexes is True
            assert config.show_progress is True

    def test_config_validation(self):
        """Test configuration validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Valid config
            config = ConverterConfig(
                source_dir=Path(temp_dir),
                batch_size=50_000,
                parallel_workers=2
            )
            assert config.batch_size == 50_000
            assert config.parallel_workers == 2
            
            # Invalid batch size (too small)
            with pytest.raises(ValueError):
                ConverterConfig(
                    source_dir=Path(temp_dir),
                    batch_size=500  # Below minimum
                )


class TestSchemaOptimizer:
    """Test schema optimization functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.optimizer = SchemaOptimizer()
    
    def test_optimize_table_schema(self):
        """Test table schema optimization."""
        # Create sample dataframe
        df = pl.DataFrame({
            "CN": [1, 2, 3],
            "STATECD": [37, 37, 37],
            "DIA": [10.5, 12.3, 8.7],
            "SPCD": [131, 110, 833],
            "STATUSCD": [1, 1, 2]
        })
        
        schema = self.optimizer.optimize_table_schema("TREE", df)
        
        assert isinstance(schema, OptimizedSchema)
        assert schema.table_name == "TREE"
        assert "CN" in schema.optimized_types
        assert "STATECD" in schema.optimized_types
        assert schema.optimized_types["CN"] == "BIGINT"
        assert schema.optimized_types["STATECD"] == "TINYINT"
        assert len(schema.indexes) > 0
    
    def test_type_optimization(self):
        """Test data type optimization."""
        # Test integer optimization
        assert "CN" in self.optimizer.OPTIMIZED_TYPES
        assert self.optimizer.OPTIMIZED_TYPES["CN"] == "BIGINT"
        assert self.optimizer.OPTIMIZED_TYPES["STATECD"] == "TINYINT"
        
        # Test decimal optimization
        assert self.optimizer.OPTIMIZED_TYPES["DIA"] == "DECIMAL(6,2)"
        assert self.optimizer.OPTIMIZED_TYPES["LAT"] == "DECIMAL(9,6)"
    
    def test_index_configuration(self):
        """Test index configuration."""
        assert "TREE" in self.optimizer.INDEX_CONFIGS
        tree_indexes = self.optimizer.INDEX_CONFIGS["TREE"]
        assert "CN" in tree_indexes
        assert "PLT_CN" in tree_indexes
        assert "STATUSCD" in tree_indexes


class TestStateMerger:
    """Test multi-state merging functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.merger = StateMerger()
    
    def test_merge_reference_table(self):
        """Test reference table merging with deduplication."""
        # Create duplicate reference data
        df1 = pl.DataFrame({
            "SPCD": [131, 110, 833],
            "COMMON_NAME": ["Loblolly pine", "Virginia pine", "Chestnut oak"],
            "SCIENTIFIC_NAME": ["Pinus taeda", "Pinus virginiana", "Quercus montana"]
        })
        
        df2 = pl.DataFrame({
            "SPCD": [131, 110, 802],  # 131, 110 are duplicates
            "COMMON_NAME": ["Loblolly pine", "Virginia pine", "White oak"],
            "SCIENTIFIC_NAME": ["Pinus taeda", "Pinus virginiana", "Quercus alba"]
        })
        
        merged = self.merger.merge_table_data("REF_SPECIES", [df1, df2])
        
        # Should have 4 unique species (deduplicated 131, 110)
        assert len(merged) == 4
        species_codes = merged.select("SPCD").to_series().sort().to_list()
        assert species_codes == [110, 131, 802, 833]
    
    def test_merge_measurement_table(self):
        """Test measurement table merging."""
        # Create plot data from different states
        df1 = pl.DataFrame({
            "CN": [1001, 1002],
            "STATECD": [37, 37],
            "INVYR": [2020, 2020],
            "LAT": [35.5, 35.6],
            "LON": [-80.5, -80.4]
        })
        
        df2 = pl.DataFrame({
            "CN": [2001, 2002],
            "STATECD": [45, 45],
            "INVYR": [2020, 2020],
            "LAT": [34.5, 34.6],
            "LON": [-81.5, -81.4]
        })
        
        merged = self.merger.merge_table_data("PLOT", [df1, df2])
        
        # Should have all 4 plots
        assert len(merged) == 4
        states = merged.select("STATECD").unique().sort("STATECD").to_series().to_list()
        assert states == [37, 45]
    
    def test_evalid_update(self):
        """Test EVALID uniqueness across states."""
        df = pl.DataFrame({
            "EVALID": [372301, 372302],
            "STATECD": [37, 37],
            "OTHER_COL": ["A", "B"]
        })
        
        updated = self.merger.update_evalids(df, 37)
        
        # EVALIDs should remain the same if already properly formatted
        evalids = updated.select("EVALID").to_series().to_list()
        assert 372301 in evalids
        assert 372302 in evalids


class TestDataValidator:
    """Test data validation functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.validator = DataValidator()
    
    def test_schema_validation(self):
        """Test schema validation."""
        # Create dataframe with expected columns
        df = pl.DataFrame({
            "CN": [1, 2, 3],
            "PLT_CN": [101, 102, 103],
            "STATUSCD": [1, 1, 2],
            "SPCD": [131, 110, 833],
            "DIA": [10.5, 12.3, 8.7],
            "TPA_UNADJ": [6.0, 6.0, 6.0]
        })
        
        expected_schema = {
            "CN": "BIGINT",
            "PLT_CN": "BIGINT", 
            "STATUSCD": "TINYINT",
            "SPCD": "SMALLINT",
            "DIA": "DECIMAL",
            "TPA_UNADJ": "DECIMAL"
        }
        
        result = self.validator.validate_schema(df, expected_schema, "TREE")
        
        assert result.is_valid
        assert len(result.errors) == 0
        assert result.validation_duration_seconds >= 0
    
    def test_schema_validation_missing_columns(self):
        """Test schema validation with missing columns."""
        # Create dataframe missing required columns
        df = pl.DataFrame({
            "CN": [1, 2, 3],
            "STATUSCD": [1, 1, 2]
            # Missing PLT_CN, SPCD, DIA, TPA_UNADJ
        })
        
        expected_schema = {
            "CN": "BIGINT",
            "PLT_CN": "BIGINT",
            "STATUSCD": "TINYINT",
            "SPCD": "SMALLINT",
            "DIA": "DECIMAL",
            "TPA_UNADJ": "DECIMAL"
        }
        
        result = self.validator.validate_schema(df, expected_schema, "TREE")
        
        assert not result.is_valid
        assert len(result.errors) == 4  # Missing 4 columns
        
        # Check that error messages are descriptive
        error_messages = [error.message for error in result.errors]
        assert any("PLT_CN" in msg for msg in error_messages)
        assert any("SPCD" in msg for msg in error_messages)
    
    def test_statistical_validation(self):
        """Test statistical validation between source and target."""
        source_stats = {
            "total_records": 1000,
            "avg_diameter": 12.5,
            "total_trees": 5000
        }
        
        # Target stats within tolerance
        target_stats = {
            "total_records": 1002,  # 0.2% difference
            "avg_diameter": 12.4,   # 0.8% difference
            "total_trees": 5010     # 0.2% difference
        }
        
        is_valid = self.validator.validate_statistics(source_stats, target_stats, tolerance=0.05)
        assert is_valid
        
        # Target stats outside tolerance
        target_stats_bad = {
            "total_records": 900,   # 10% difference
            "avg_diameter": 12.4,
            "total_trees": 5010
        }
        
        is_valid = self.validator.validate_statistics(source_stats, target_stats_bad, tolerance=0.05)
        assert not is_valid


class TestFIAConverter:
    """Test main converter functionality."""
    
    @patch('pyfia.converter.sqlite_to_duckdb.sqlite3')
    @patch('pyfia.converter.sqlite_to_duckdb.duckdb')
    def test_converter_initialization(self, mock_duckdb, mock_sqlite):
        """Test converter initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = ConverterConfig(source_dir=Path(temp_dir))
            converter = FIAConverter(config)
            
            assert converter.config == config
            assert hasattr(converter, 'schema_optimizer')
            assert hasattr(converter, 'state_merger')
            assert hasattr(converter, 'validator')
            assert converter.temp_dir.exists()
    
    def test_state_code_extraction(self):
        """Test state code extraction from file paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = ConverterConfig(source_dir=Path(temp_dir))
            converter = FIAConverter(config)
            
            # Test Oregon
            or_path = Path("OR_FIA.db")
            assert converter._extract_state_code_from_path(or_path) == 41
            
            # Test California  
            ca_path = Path("CA_FIA.db")
            assert converter._extract_state_code_from_path(ca_path) == 6
            
            # Test unknown state
            unknown_path = Path("unknown_file.db")
            assert converter._extract_state_code_from_path(unknown_path) is None


class TestIntegration:
    """Integration tests for converter workflow."""
    
    def test_basic_conversion_workflow(self):
        """Test basic conversion workflow components."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create mock configuration
            config = ConverterConfig(
                source_dir=temp_path,
                target_path=temp_path / "test.duckdb",
                batch_size=1000,
                validation_level=ValidationLevel.BASIC
            )
            
            # Test that converter can be initialized
            converter = FIAConverter(config)
            assert converter is not None
            
            # Test that components are properly initialized
            assert isinstance(converter.schema_optimizer, SchemaOptimizer)
            assert isinstance(converter.state_merger, StateMerger)
            assert isinstance(converter.validator, DataValidator)
    
    def test_schema_optimization_pipeline(self):
        """Test schema optimization pipeline."""
        optimizer = SchemaOptimizer()
        
        # Create test data representing common FIA table
        plot_data = pl.DataFrame({
            "CN": [1001, 1002, 1003],
            "STATECD": [37, 37, 37],
            "UNITCD": [1, 1, 1],
            "COUNTYCD": [183, 183, 183],
            "PLOT": [1, 2, 3],
            "INVYR": [2020, 2020, 2020],
            "LAT": [35.123456, 35.234567, 35.345678],
            "LON": [-80.123456, -80.234567, -80.345678],
            "PLOT_STATUS_CD": [1, 1, 1]
        })
        
        # Optimize schema
        schema = optimizer.optimize_table_schema("PLOT", plot_data)
        
        # Verify optimization results
        assert schema.table_name == "PLOT"
        assert len(schema.optimized_types) == len(plot_data.columns)
        assert len(schema.indexes) > 0
        assert schema.estimated_size_reduction > 1.0  # Should show some improvement
        
        # Verify specific optimizations
        assert schema.optimized_types["CN"] == "BIGINT"
        assert schema.optimized_types["STATECD"] == "TINYINT"
        assert schema.optimized_types["LAT"] == "DECIMAL(9,6)"
        assert schema.optimized_types["LON"] == "DECIMAL(10,6)"


if __name__ == '__main__':
    pytest.main([__file__])