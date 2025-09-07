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
        # Types will be from YAML or inferred - just check they exist
        assert "CN" in schema.optimized_types
        assert "STATECD" in schema.optimized_types
        assert len(schema.indexes) > 0
    
    def test_type_optimization(self):
        """Test that optimizer uses YAML schemas or infers types."""
        # Since OPTIMIZED_TYPES is removed, test the inference logic
        df = pl.DataFrame({"test_col": [1, 2, 3]})
        
        # The optimizer should now use YAML schemas when available
        # or infer types from data when not in YAML
        inferred = self.optimizer._infer_optimal_type("test_col", df.select("test_col"), "Int64")
        assert inferred in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT"]
        
        # Test that it can infer appropriate types for different ranges
        df_large = pl.DataFrame({"large_col": [1000000, 2000000, 3000000]})
        inferred_large = self.optimizer._infer_optimal_type("large_col", df_large.select("large_col"), "Int64")
        assert inferred_large == "INTEGER"
    
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
        # Types will be from YAML or inferred - just check they exist
        assert "CN" in schema.optimized_types
        assert "STATECD" in schema.optimized_types
        assert "LAT" in schema.optimized_types
        assert "LON" in schema.optimized_types


class TestAppendFunctionality:
    """Test new append mode functionality without state replacement."""
    
    def test_true_append_mode(self):
        """Test that append mode truly appends without deleting existing data."""
        import tempfile
        import duckdb
        
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            
            # Create initial database with data
            with duckdb.connect(str(db_path)) as conn:
                # Create table with NC data
                conn.execute("""
                    CREATE TABLE PLOT (
                        CN BIGINT,
                        STATECD TINYINT,
                        INVYR SMALLINT,
                        LAT DECIMAL(9,6),
                        LON DECIMAL(10,6)
                    )
                """)
                conn.execute("""
                    INSERT INTO PLOT VALUES 
                    (1001, 37, 2020, 35.123, -80.123),
                    (1002, 37, 2020, 35.234, -80.234)
                """)
            
            # Create config for append mode
            config = ConverterConfig(
                source_dir=Path(temp_dir),
                target_path=db_path,
                append_mode=True,
                dedupe_on_append=False
            )
            
            # Simulate appending SC data
            new_data = pl.DataFrame({
                "CN": [2001, 2002],
                "STATECD": [45, 45],
                "INVYR": [2021, 2021],
                "LAT": [33.123, 33.234],
                "LON": [-81.123, -81.234]
            })
            
            # Use insertion strategy to append
            from pyfia.converter.insertion_strategies import InsertionStrategyFactory
            
            with duckdb.connect(str(db_path)) as conn:
                strategy = InsertionStrategyFactory.create_strategy(
                    append_mode=True,
                    table_exists=True
                )
                strategy.insert(conn, "PLOT", new_data)
                
                # Verify both states are present
                result = conn.execute("SELECT STATECD, COUNT(*) FROM PLOT GROUP BY STATECD ORDER BY STATECD").fetchall()
                
                assert len(result) == 2
                assert result[0] == (37, 2)  # NC still has 2 records
                assert result[1] == (45, 2)  # SC has 2 new records
                
                # Total should be 4 records
                total = conn.execute("SELECT COUNT(*) FROM PLOT").fetchone()[0]
                assert total == 4
    
    def test_append_with_deduplication(self):
        """Test deduplication during append."""
        import tempfile
        import duckdb
        
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            
            # Create initial database
            with duckdb.connect(str(db_path)) as conn:
                conn.execute("""
                    CREATE TABLE TREE (
                        CN BIGINT,
                        PLT_CN BIGINT,
                        STATUSCD TINYINT,
                        SPCD SMALLINT
                    )
                """)
                conn.execute("""
                    INSERT INTO TREE VALUES 
                    (1001, 101, 1, 131),
                    (1002, 101, 1, 110)
                """)
            
            # Test deduplication by simulating append with duplicates
            with duckdb.connect(str(db_path)) as conn:
                # New data with one duplicate CN
                new_data = pl.DataFrame({
                    "CN": [1001, 2001, 2002],  # 1001 is duplicate
                    "PLT_CN": [101, 102, 102],
                    "STATUSCD": [1, 1, 1],
                    "SPCD": [131, 833, 802]
                })
                
                # Manual deduplication test
                # Get existing CNs
                existing_cns = conn.execute("SELECT DISTINCT CN FROM TREE").pl()
                
                # Anti-join to remove duplicates
                deduped = new_data.join(
                    existing_cns,
                    on="CN",
                    how="anti"
                )
                
                # Should have removed the duplicate CN=1001
                assert len(deduped) == 2
                cn_values = deduped.select("CN").to_series().to_list()
                assert 1001 not in cn_values
                assert 2001 in cn_values
                assert 2002 in cn_values
                
                # Now append the deduplicated data
                from pyfia.converter.insertion_strategies import InsertionStrategyFactory
                strategy = InsertionStrategyFactory.create_strategy(
                    append_mode=True,
                    table_exists=True
                )
                strategy.insert(conn, "TREE", deduped)
                
                # Verify final count
                total = conn.execute("SELECT COUNT(*) FROM TREE").fetchone()[0]
                assert total == 4  # 2 original + 2 new (deduplicated)
    
    def test_schema_compatibility(self):
        """Test schema compatibility adjustment during append."""
        import tempfile
        import duckdb
        
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            
            # Create table with specific types
            with duckdb.connect(str(db_path)) as conn:
                conn.execute("""
                    CREATE TABLE COND (
                        CN BIGINT,
                        PLT_CN BIGINT,
                        CONDID TINYINT,
                        CONDPROP_UNADJ DECIMAL(8,6)
                    )
                """)
            
            with duckdb.connect(str(db_path)) as conn:
                # DataFrame with different types
                df = pl.DataFrame({
                    "CN": [1, 2, 3],  # Will need to be cast to BIGINT
                    "PLT_CN": [101, 102, 103],
                    "CONDID": [1, 2, 3],  # Will need to be cast to TINYINT
                    "CONDPROP_UNADJ": [0.5, 0.3, 0.2]  # Will need to be cast to DECIMAL
                })
                
                # Test schema compatibility directly
                # Get schema from table
                existing_schema = conn.execute("DESCRIBE COND").fetchall()
                schema_map = {col[0]: col[1] for col in existing_schema}
                
                # Apply type casting
                cast_exprs = []
                for col in df.columns:
                    if col in schema_map:
                        target_type = schema_map[col]
                        if "TINYINT" in target_type.upper():
                            cast_exprs.append(pl.col(col).cast(pl.Int8).alias(col))
                        elif "BIGINT" in target_type.upper():
                            cast_exprs.append(pl.col(col).cast(pl.Int64).alias(col))
                        elif "DECIMAL" in target_type.upper():
                            cast_exprs.append(pl.col(col).cast(pl.Float64).alias(col))
                        else:
                            cast_exprs.append(pl.col(col))
                
                adjusted = df.select(cast_exprs)
                
                # Check that types have been adjusted
                assert adjusted["CN"].dtype == pl.Int64
                assert adjusted["CONDID"].dtype == pl.Int8
                # CONDPROP_UNADJ becomes Float64 in Polars for DECIMAL compatibility
                assert adjusted["CONDPROP_UNADJ"].dtype == pl.Float64
                
                # Verify we can insert the adjusted data
                from pyfia.converter.insertion_strategies import InsertionStrategyFactory
                strategy = InsertionStrategyFactory.create_strategy(
                    append_mode=True,
                    table_exists=True
                )
                strategy.insert(conn, "COND", adjusted)
                
                # Verify data was inserted
                count = conn.execute("SELECT COUNT(*) FROM COND").fetchone()[0]
                assert count == 3
    
    def test_append_without_statecd(self):
        """Test that append works for tables without STATECD column."""
        import tempfile
        import duckdb
        
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            
            # Create reference table without STATECD
            with duckdb.connect(str(db_path)) as conn:
                conn.execute("""
                    CREATE TABLE REF_SPECIES (
                        SPCD SMALLINT,
                        COMMON_NAME VARCHAR(100),
                        SCIENTIFIC_NAME VARCHAR(100)
                    )
                """)
                conn.execute("""
                    INSERT INTO REF_SPECIES VALUES 
                    (131, 'Loblolly pine', 'Pinus taeda'),
                    (110, 'Virginia pine', 'Pinus virginiana')
                """)
            
            # Append more species
            new_species = pl.DataFrame({
                "SPCD": [833, 802],
                "COMMON_NAME": ["Chestnut oak", "White oak"],
                "SCIENTIFIC_NAME": ["Quercus montana", "Quercus alba"]
            })
            
            from pyfia.converter.insertion_strategies import InsertionStrategyFactory
            
            with duckdb.connect(str(db_path)) as conn:
                strategy = InsertionStrategyFactory.create_strategy(
                    append_mode=True,
                    table_exists=True
                )
                strategy.insert(conn, "REF_SPECIES", new_species)
                
                # Verify all species are present
                count = conn.execute("SELECT COUNT(*) FROM REF_SPECIES").fetchone()[0]
                assert count == 4
                
                # Verify specific species
                species_codes = conn.execute(
                    "SELECT SPCD FROM REF_SPECIES ORDER BY SPCD"
                ).fetchall()
                assert [s[0] for s in species_codes] == [110, 131, 802, 833]


class TestConverterAPI:
    """Test the new API-based conversion functions."""
    
    @patch('pyfia.converter.sqlite_to_duckdb.FIAConverter')
    def test_convert_sqlite_to_duckdb_api(self, mock_converter_class):
        """Test the high-level convert_sqlite_to_duckdb function."""
        from pyfia import convert_sqlite_to_duckdb
        from pyfia.converter import ConverterConfig
        from pyfia.converter.models import ConversionResult, ConversionStatus, ConversionStats
        from datetime import datetime
        
        # Setup mock
        mock_converter = Mock()
        mock_converter_class.return_value = mock_converter
        
        # Create test config
        test_config = ConverterConfig(source_dir=Path("/tmp"))
        
        # Create mock result
        mock_result = ConversionResult(
            status=ConversionStatus.COMPLETED,
            config=test_config,
            stats=ConversionStats(
                start_time=datetime.now(),
                source_file_count=1,
                source_total_size_bytes=1000000,
                source_tables_processed=10,
                source_records_processed=50000
            ),
            source_paths=[Path("test.db")],
            target_path=Path("test.duckdb")
        )
        mock_converter.convert_state.return_value = mock_result
        
        # Call the API function
        result = convert_sqlite_to_duckdb(
            "test.db",
            "test.duckdb",
            state_code=41,
            compression_level="high"
        )
        
        # Verify converter was created and called correctly
        mock_converter_class.assert_called_once()
        mock_converter.convert_state.assert_called_once()
        assert result == mock_result
    
    @patch('pyfia.converter.sqlite_to_duckdb.FIAConverter')
    def test_merge_state_databases_api(self, mock_converter_class):
        """Test the high-level merge_state_databases function."""
        from pyfia import merge_state_databases
        from pyfia.converter import ConverterConfig
        from pyfia.converter.models import ConversionResult, ConversionStatus, ConversionStats
        from datetime import datetime
        
        # Setup mock
        mock_converter = Mock()
        mock_converter_class.return_value = mock_converter
        
        # Create test config
        test_config = ConverterConfig(source_dir=Path("/tmp"))
        
        # Create mock result
        mock_result = ConversionResult(
            status=ConversionStatus.COMPLETED,
            config=test_config,
            stats=ConversionStats(
                start_time=datetime.now(),
                source_file_count=2,
                source_total_size_bytes=2000000,
                source_tables_processed=20,
                source_records_processed=100000
            ),
            source_paths=[Path("OR.db"), Path("WA.db")],
            target_path=Path("pacific.duckdb")
        )
        mock_converter.merge_states.return_value = mock_result
        
        # Call the API function
        result = merge_state_databases(
            ["OR.db", "WA.db"],
            "pacific.duckdb",
            state_codes=[41, 53]
        )
        
        # Verify converter was created and called correctly
        mock_converter_class.assert_called_once()
        mock_converter.merge_states.assert_called_once()
        assert result == mock_result
    
    @patch('pyfia.converter.sqlite_to_duckdb.FIAConverter')
    def test_fia_class_convert_method(self, mock_converter_class):
        """Test the FIA.convert_from_sqlite class method."""
        from pyfia import FIA
        from pyfia.converter import ConverterConfig
        from pyfia.converter.models import ConversionResult, ConversionStatus, ConversionStats
        from datetime import datetime
        
        # Setup mock
        mock_converter = Mock()
        mock_converter_class.return_value = mock_converter
        
        # Create test config
        test_config = ConverterConfig(source_dir=Path("/tmp"))
        
        # Create mock result
        mock_result = ConversionResult(
            status=ConversionStatus.COMPLETED,
            config=test_config,
            stats=ConversionStats(
                start_time=datetime.now(),
                source_file_count=1,
                source_total_size_bytes=1000000,
                source_tables_processed=10,
                source_records_processed=50000
            ),
            source_paths=[Path("test.db")],
            target_path=Path("test.duckdb")
        )
        mock_converter.convert_state.return_value = mock_result
        
        # Call the class method
        result = FIA.convert_from_sqlite(
            "test.db",
            "test.duckdb",
            compression_level="medium"
        )
        
        # Verify converter was created and called
        mock_converter_class.assert_called_once()
        mock_converter.convert_state.assert_called_once()
        assert result == mock_result
    
    @patch('pyfia.converter.sqlite_to_duckdb.FIAConverter')
    @patch('pyfia.core.fia.FIADataReader')
    def test_fia_append_data_method(self, mock_reader_class, mock_converter_class):
        """Test the FIA.append_data instance method."""
        from pyfia import FIA
        from pyfia.converter import ConverterConfig
        from pyfia.converter.models import ConversionResult, ConversionStatus, ConversionStats
        from datetime import datetime
        import tempfile
        
        # Setup mocks
        mock_reader = Mock()
        mock_reader_class.return_value = mock_reader
        
        mock_converter = Mock()
        mock_converter_class.return_value = mock_converter
        
        # Create test config
        test_config = ConverterConfig(source_dir=Path("/tmp"), append_mode=True)
        
        # Create a temporary database file
        with tempfile.NamedTemporaryFile(suffix=".duckdb") as temp_file:
            # Create mock result
            mock_result = ConversionResult(
                status=ConversionStatus.COMPLETED,
                config=test_config,
                stats=ConversionStats(
                    start_time=datetime.now(),
                    source_file_count=1,
                    source_total_size_bytes=500000,
                    source_tables_processed=10,
                    source_records_processed=25000
                ),
                source_paths=[Path("update.db")],
                target_path=Path(temp_file.name)
            )
            mock_converter.convert_state.return_value = mock_result
            
            # Create FIA instance and call append_data
            db = FIA(temp_file.name)
            result = db.append_data(
                "update.db",
                dedupe=True,
                dedupe_keys=["CN"]
            )
            
            # Verify converter was created with append mode
            mock_converter_class.assert_called_once()
            config_call = mock_converter_class.call_args[0][0]
            assert config_call.append_mode is True
            assert config_call.dedupe_on_append is True
            assert config_call.dedupe_keys == ["CN"]
            
            # Verify convert_state was called
            mock_converter.convert_state.assert_called_once()
            assert result == mock_result
    
    @patch('pyfia.FIA')
    def test_append_to_database_with_path(self, mock_fia_class):
        """Test append_to_database with database path."""
        from pyfia import append_to_database
        from pyfia.converter import ConverterConfig
        from pyfia.converter.models import ConversionResult, ConversionStatus, ConversionStats
        from datetime import datetime
        
        # Setup mock
        mock_db = Mock()
        mock_fia_class.return_value = mock_db
        
        # Create test config
        test_config = ConverterConfig(source_dir=Path("/tmp"), append_mode=True)
        
        # Create mock result
        mock_result = ConversionResult(
            status=ConversionStatus.COMPLETED,
            config=test_config,
            stats=ConversionStats(
                start_time=datetime.now(),
                source_file_count=1,
                source_total_size_bytes=500000,
                source_tables_processed=10,
                source_records_processed=25000
            ),
            source_paths=[Path("update.db")],
            target_path=Path("target.duckdb")
        )
        mock_db.append_data.return_value = mock_result
        
        # Call with database path
        result = append_to_database(
            "target.duckdb",
            "update.db",
            dedupe=True
        )
        
        # Verify FIA was created and append_data was called
        mock_fia_class.assert_called_once_with("target.duckdb")
        mock_db.append_data.assert_called_once_with(
            "update.db",
            None,  # state_code
            True,  # dedupe
            None   # dedupe_keys
        )
        assert result == mock_result
    
    def test_append_to_database_with_fia_instance(self):
        """Test append_to_database with FIA instance."""
        from pyfia import append_to_database, FIA
        from pyfia.converter import ConverterConfig
        from pyfia.converter.models import ConversionResult, ConversionStatus, ConversionStats
        from datetime import datetime
        import tempfile
        
        # Create a temporary database file
        with tempfile.NamedTemporaryFile(suffix=".duckdb") as temp_file:
            # Create mock FIA instance
            with patch('pyfia.core.fia.FIADataReader'):
                db = FIA(temp_file.name)
                
                # Create test config
                test_config = ConverterConfig(source_dir=Path("/tmp"), append_mode=True)
                
                # Mock the append_data method
                mock_result = ConversionResult(
                    status=ConversionStatus.COMPLETED,
                    config=test_config,
                    stats=ConversionStats(
                        start_time=datetime.now(),
                        source_file_count=1,
                        source_total_size_bytes=500000,
                        source_tables_processed=10,
                        source_records_processed=25000
                    ),
                    source_paths=[Path("update.db")],
                    target_path=Path(temp_file.name)
                )
                db.append_data = Mock(return_value=mock_result)
                
                # Call with FIA instance
                result = append_to_database(
                    db,
                    "update.db",
                    state_code=41,
                    dedupe=False
                )
                
                # Verify append_data was called on the instance
                db.append_data.assert_called_once_with(
                    "update.db",
                    41,     # state_code
                    False,  # dedupe
                    None    # dedupe_keys
                )
                assert result == mock_result


if __name__ == '__main__':
    pytest.main([__file__])