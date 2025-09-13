"""
Comprehensive test suite for area() function lazy column loading optimization.

Tests the dynamic column selection, domain parsing, schema discovery,
and performance characteristics of the optimized implementation.
"""

import pytest
import polars as pl
from unittest.mock import Mock, patch, MagicMock
import tracemalloc
from pyfia import FIA, area
from pyfia.estimation.estimators.area import AreaEstimator


class TestColumnSelection:
    """Test dynamic column selection based on configuration."""
    
    def test_minimal_columns_no_grouping(self):
        """Test that basic area estimation loads minimal columns."""
        db = Mock(spec=FIA)
        db.evalid = None
        db.tables = {}
        db._reader = Mock()
        
        config = {"land_type": "forest"}
        estimator = AreaEstimator(db, config)
        
        cond_cols = estimator.get_cond_columns()
        plot_cols = estimator._get_plot_columns()
        
        # Should only load core columns
        assert set(cond_cols) == {
            "PLT_CN", "CONDID", "COND_STATUS_CD", 
            "CONDPROP_UNADJ", "PROP_BASIS"
        }
        assert plot_cols == ["CN"]
    
    def test_columns_with_single_groupby(self):
        """Test column selection with single group-by column."""
        db = Mock(spec=FIA)
        config = {"land_type": "forest", "grp_by": "OWNGRPCD"}
        estimator = AreaEstimator(db, config)
        
        cond_cols = estimator.get_cond_columns()
        
        assert "OWNGRPCD" in cond_cols
        assert len(cond_cols) == 6  # 5 core + 1 grouping
    
    def test_columns_with_multiple_groupby(self):
        """Test column selection with multiple group-by columns."""
        db = Mock(spec=FIA)
        config = {
            "land_type": "forest", 
            "grp_by": ["FORTYPCD", "STDSZCD", "OWNGRPCD"]
        }
        estimator = AreaEstimator(db, config)
        
        cond_cols = estimator.get_cond_columns()
        
        assert "FORTYPCD" in cond_cols
        assert "STDSZCD" in cond_cols
        assert "OWNGRPCD" in cond_cols
        assert len(cond_cols) == 8  # 5 core + 3 grouping
    
    def test_timber_land_type_adds_filter_columns(self):
        """Test that timber land type adds required filter columns."""
        db = Mock(spec=FIA)
        config = {"land_type": "timber"}
        estimator = AreaEstimator(db, config)
        
        cond_cols = estimator.get_cond_columns()
        
        assert "SITECLCD" in cond_cols
        assert "RESERVCD" in cond_cols
        assert len(cond_cols) == 7  # 5 core + 2 filter
    
    def test_plot_columns_for_plot_groupby(self):
        """Test that PLOT table columns are selected when needed."""
        db = Mock(spec=FIA)
        config = {"grp_by": ["STATECD", "INVYR", "FORTYPCD"]}
        estimator = AreaEstimator(db, config)
        
        plot_cols = estimator._get_plot_columns()
        cond_cols = estimator.get_cond_columns()
        
        # STATECD and INVYR from PLOT
        assert "STATECD" in plot_cols
        assert "INVYR" in plot_cols
        assert "CN" in plot_cols
        
        # FORTYPCD from COND
        assert "FORTYPCD" in cond_cols
        # STATECD and INVYR should also be in COND cols for consistency
        assert "STATECD" in cond_cols
        assert "INVYR" in cond_cols


class TestDomainParsing:
    """Test domain expression parsing for column extraction."""
    
    def test_no_domain_parsing_needed(self):
        """Test that we don't parse domain expressions at all."""
        db = Mock(spec=FIA)
        
        # Test 1: Simple domain
        config = {
            "land_type": "forest",
            "area_domain": "STDAGE > 50"
        }
        estimator = AreaEstimator(db, config)
        cond_cols = estimator.get_cond_columns()
        assert len(cond_cols) == 5  # Only core columns
        assert "STDAGE" not in cond_cols  # We don't extract from domain
        
        # Test 2: Complex domain
        config = {
            "land_type": "forest",
            "area_domain": "STDAGE > 50 AND FORTYPCD IN (161, 162) AND OWNGRPCD == 40"
        }
        estimator = AreaEstimator(db, config)
        cond_cols = estimator.get_cond_columns()
        assert len(cond_cols) == 5  # Only core columns
        assert "STDAGE" not in cond_cols
        assert "FORTYPCD" not in cond_cols
        assert "OWNGRPCD" not in cond_cols
        
        # Test 3: Domain with SQL keywords - we don't care, we don't parse
        config = {
            "land_type": "forest",
            "area_domain": "STDAGE > 50 AND FORTYPCD NOT IN (161, 162) OR OWNGRPCD IS NULL"
        }
        estimator = AreaEstimator(db, config)
        cond_cols = estimator.get_cond_columns()
        assert len(cond_cols) == 5  # Only core columns
    


class TestSchemaDiscovery:
    """Test database schema discovery functionality."""
    
    def test_schema_discovery_using_cached_method(self):
        """Test that schema discovery uses the FIA cached method."""
        db = Mock(spec=FIA)
        db.get_table_columns = Mock(return_value=[
            'PLT_CN', 'CONDID', 'COND_STATUS_CD',
            'CONDPROP_UNADJ', 'PROP_BASIS', 'OWNGRPCD'
        ])
        
        estimator = AreaEstimator(db, {})
        columns = estimator._get_available_columns("COND")
        
        # Should have called the FIA method
        db.get_table_columns.assert_called_with("COND")
        assert columns == [
            'PLT_CN', 'CONDID', 'COND_STATUS_CD',
            'CONDPROP_UNADJ', 'PROP_BASIS', 'OWNGRPCD'
        ]
    
    def test_schema_discovery_empty_fallback(self):
        """Test fallback when no columns returned."""
        db = Mock(spec=FIA)
        db.get_table_columns = Mock(return_value=[])
        
        estimator = AreaEstimator(db, {})
        columns = estimator._get_available_columns("COND")
        
        # Should return None for empty list
        assert columns is None


class TestTableReloading:
    """Test table reloading when columns are missing."""
    
    def test_reload_when_columns_missing(self):
        """Test that tables are reloaded when needed columns are missing."""
        db = Mock(spec=FIA)
        db.tables = {}
        db._reader = Mock()
        db._reader.backend = "duckdb"
        db.load_table = Mock()
        db.get_table_columns = Mock(return_value=["PLT_CN", "CONDID", "COND_STATUS_CD", 
                                                    "CONDPROP_UNADJ", "PROP_BASIS"])
        
        # Create a mock LazyFrame with limited columns
        mock_df = Mock(spec=pl.LazyFrame)
        mock_df.collect_schema.return_value.names.return_value = [
            "PLT_CN", "CONDID", "COND_STATUS_CD", 
            "CONDPROP_UNADJ", "PROP_BASIS"
        ]
        db.tables["COND"] = mock_df
        
        # Create mock PLOT table
        mock_plot = Mock(spec=pl.LazyFrame)
        mock_plot.collect_schema.return_value.names.return_value = ["CN"]
        db.tables["PLOT"] = mock_plot
        
        config = {"grp_by": "OWNGRPCD"}  # Request column not in mock
        estimator = AreaEstimator(db, config)
        
        # Try to load data - should trigger reload due to missing OWNGRPCD
        estimator.load_data()
        
        # Should have called load_table to reload with all columns
        db.load_table.assert_called()
    
    def test_no_reload_when_columns_present(self):
        """Test that tables are not reloaded when columns are present."""
        db = Mock(spec=FIA)
        db.tables = {}
        db._reader = Mock()
        db.load_table = Mock()
        
        # Create a mock LazyFrame with all needed columns
        mock_df = Mock(spec=pl.LazyFrame)
        mock_df.collect_schema.return_value.names.return_value = [
            "PLT_CN", "CONDID", "COND_STATUS_CD", 
            "CONDPROP_UNADJ", "PROP_BASIS", "OWNGRPCD"
        ]
        mock_df.select.return_value = mock_df
        mock_df.lazy.return_value = mock_df
        db.tables["COND"] = mock_df
        
        # Mock PLOT table
        mock_plot = Mock(spec=pl.LazyFrame)
        mock_plot.collect_schema.return_value.names.return_value = ["CN"]
        mock_plot.select.return_value = mock_plot
        mock_plot.lazy.return_value = mock_plot
        mock_plot.join.return_value = mock_df  # Return mock for join
        db.tables["PLOT"] = mock_plot
        
        config = {"grp_by": "OWNGRPCD"}
        estimator = AreaEstimator(db, config)
        
        with patch.object(estimator, '_get_available_columns', return_value=None):
            result = estimator.load_data()
        
        # Should NOT have called load_table (tables already loaded)
        db.load_table.assert_not_called()


class TestPerformance:
    """Test performance characteristics and memory usage."""
    
    @pytest.mark.slow
    def test_memory_reduction(self):
        """Test that lazy loading reduces memory usage."""
        # This would require a real database to test properly
        # Marking as slow test that runs only in CI
        pytest.skip("Requires real FIA database for memory testing")
        
        # Theoretical test structure:
        # tracemalloc.start()
        # 
        # # Baseline: Load all columns
        # snapshot1 = tracemalloc.take_snapshot()
        # result1 = area_with_all_columns(db)
        # snapshot2 = tracemalloc.take_snapshot()
        # baseline_memory = calculate_peak(snapshot1, snapshot2)
        # 
        # # Optimized: Load only needed columns
        # snapshot3 = tracemalloc.take_snapshot()
        # result2 = area_with_lazy_loading(db)
        # snapshot4 = tracemalloc.take_snapshot()
        # optimized_memory = calculate_peak(snapshot3, snapshot4)
        # 
        # # Assert 90% reduction
        # reduction = (baseline_memory - optimized_memory) / baseline_memory
        # assert reduction >= 0.9
    
    def test_column_count_reduction(self):
        """Test that fewer columns are loaded with optimization."""
        db = Mock(spec=FIA)
        db.tables = {}
        db._reader = Mock()
        
        # Test minimal query
        config_minimal = {"land_type": "forest"}
        estimator_minimal = AreaEstimator(db, config_minimal)
        cols_minimal = estimator_minimal.get_cond_columns()
        
        # Test complex query (no domain parsing needed)
        config_complex = {
            "land_type": "timber",  # Adds 2 columns (SITECLCD, RESERVCD)
            "grp_by": ["FORTYPCD", "STDSZCD", "OWNGRPCD"],  # Adds 3 columns
            "area_domain": "STDAGE > 50 AND BALIVE > 100"  # Doesn't add columns
        }
        estimator_complex = AreaEstimator(db, config_complex)
        cols_complex = estimator_complex.get_cond_columns()
        
        # Minimal should have 5 columns
        assert len(cols_minimal) == 5
        
        # Complex should have 10 columns (5 core + 2 timber + 3 groupby)
        assert len(cols_complex) == 10
        assert len(cols_complex) > len(cols_minimal)


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    def test_nonexistent_groupby_column(self):
        """Test handling of non-existent group-by columns."""
        db = Mock(spec=FIA)
        db.tables = {}
        db._reader = Mock()
        
        config = {"grp_by": "NONEXISTENT_COLUMN"}
        estimator = AreaEstimator(db, config)
        
        # Should include the column in request even if it doesn't exist
        # Error will occur later in pipeline (current behavior)
        cond_cols = estimator.get_cond_columns()
        assert "NONEXISTENT_COLUMN" in cond_cols
    
    def test_empty_domain_expression(self):
        """Test handling of empty domain expressions."""
        db = Mock(spec=FIA)
        config = {"area_domain": ""}
        estimator = AreaEstimator(db, config)
        
        cond_cols = estimator.get_cond_columns()
        # Should just return core columns
        assert len(cond_cols) == 5
    
    def test_malformed_domain_expression(self):
        """Test handling of malformed domain expressions."""
        db = Mock(spec=FIA)
        config = {"area_domain": "STDAGE >> << !! ##"}
        estimator = AreaEstimator(db, config)
        
        # We don't parse domain expressions anymore
        cond_cols = estimator.get_cond_columns()
        assert len(cond_cols) == 5  # Just core columns


class TestIntegration:
    """Integration tests with real-like scenarios."""
    
    @pytest.mark.integration
    def test_full_estimation_workflow(self):
        """Test complete estimation workflow with lazy loading."""
        pytest.skip("Requires real FIA database")
        
        # This would test:
        # 1. Column selection
        # 2. Data loading
        # 3. Filtering
        # 4. Aggregation
        # 5. Results
    
    def test_multiple_sequential_estimations(self):
        """Test that sequential estimations don't interfere."""
        db = Mock(spec=FIA)
        db.tables = {}
        db._reader = Mock()
        db.evalid = None
        
        # First estimation with minimal columns
        config1 = {"land_type": "forest"}
        estimator1 = AreaEstimator(db, config1)
        cols1 = estimator1.get_cond_columns()
        
        # Second estimation with different columns
        config2 = {"land_type": "timber", "grp_by": "OWNGRPCD"}
        estimator2 = AreaEstimator(db, config2)
        cols2 = estimator2.get_cond_columns()
        
        # Should have different column sets
        assert cols1 != cols2
        assert len(cols1) < len(cols2)


class TestRegression:
    """Test that existing functionality is not broken."""
    
    def test_backward_compatibility(self):
        """Test that old usage patterns still work."""
        # Would test with real database
        pytest.skip("Requires real FIA database")
    
    def test_all_land_types_work(self):
        """Test all land type options work correctly."""
        db = Mock(spec=FIA)
        
        for land_type in ["forest", "timber", "all"]:
            config = {"land_type": land_type}
            estimator = AreaEstimator(db, config)
            cols = estimator.get_cond_columns()
            assert len(cols) >= 5  # At least core columns
    
    def test_all_standard_groupby_columns(self):
        """Test common group-by columns are recognized."""
        db = Mock(spec=FIA)
        
        common_columns = [
            "OWNGRPCD", "FORTYPCD", "STDSZCD", "STDORGCD",
            "STATECD", "INVYR", "SITECLCD", "PHYSCLCD"
        ]
        
        for col in common_columns:
            config = {"grp_by": col}
            estimator = AreaEstimator(db, config)
            cols = estimator.get_cond_columns()
            assert col in cols


if __name__ == "__main__":
    # Run specific test categories
    pytest.main([__file__, "-v", "-k", "not slow and not integration"])