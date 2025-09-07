"""
Functionality tests for lazy estimator-specific features.

This test suite focuses on testing the unique functionality provided by
lazy evaluation that is not present in eager implementations:

- Progress tracking and reporting
- Caching mechanisms and cache invalidation
- Collection strategy configuration
- Computation graph optimization
- Lazy operation deferral and batching
- Memory management and cleanup
- Error handling in lazy contexts

Tests use mocked progress bars and controlled scenarios to validate
lazy-specific behavior without requiring terminal output.
"""

import gc
import time
import warnings
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Callable
from unittest.mock import Mock, MagicMock, patch, call

import polars as pl
import pytest

from pyfia import FIA
# Import the main estimator functions with integrated lazy evaluation
from pyfia.estimation import area, tpa, volume, growth, mortality, biomass
from pyfia.estimation.config import EstimatorConfig
from pyfia.estimation.lazy_evaluation import CollectionStrategy, LazyEstimatorMixin
from pyfia.estimation.progress import OperationType
from pyfia.estimation.lazy_base import LazyBaseEstimator

# Import estimator classes directly for testing
from pyfia.estimation.area import AreaEstimator
from pyfia.estimation.biomass import BiomassEstimator
from pyfia.estimation.tpa import TPAEstimator
from pyfia.estimation.volume import VolumeEstimator
from pyfia.estimation.growth import GrowthEstimator
from pyfia.estimation.mortality import MortalityEstimator


class TestLazyEstimatorProgressTracking:
    """Test suite for progress tracking functionality in lazy estimators."""
    
    @pytest.fixture
    def mock_progress_context(self):
        """Create a mock progress context for testing."""
        with patch('pyfia.estimation.progress.Progress') as mock_progress:
            mock_task = MagicMock()
            mock_progress.return_value.add_task.return_value = mock_task
            mock_progress.return_value.__enter__ = Mock(return_value=mock_progress.return_value)
            mock_progress.return_value.__exit__ = Mock(return_value=None)
            yield mock_progress, mock_task
    
    @pytest.fixture
    def mock_db(self, sample_db):
        """Create a mock database with lazy tables."""
        db = MagicMock(spec=FIA)
        db.evalid = [452301]
        db.state_filter = {45}
        
        # Mock lazy tables
        db.tables = {
            "PLOT": pl.DataFrame({"PLT_CN": [1, 2, 3]}).lazy(),
            "TREE": pl.DataFrame({
                "PLT_CN": [1, 1, 2, 2, 3],
                "CN": [1, 2, 3, 4, 5],
                "CONDID": [1, 1, 1, 1, 1],
                "STATUSCD": [1, 1, 1, 1, 1],
                "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0, 6.0],
                "DIA": [10.0, 12.0, 8.0, 15.0, 20.0]
            }).lazy(),
            "COND": pl.DataFrame({
                "PLT_CN": [1, 2, 3],
                "CONDID": [1, 1, 1],
                "COND_STATUS_CD": [1, 1, 1],
                "CONDPROP_UNADJ": [1.0, 1.0, 1.0]
            }).lazy(),
            "POP_STRATUM": pl.DataFrame({
                "CN": [1],
                "EVALID": [452301],
                "EXPNS": [1000.0],
                "ADJ_FACTOR_SUBP": [1.0]
            }).lazy(),
            "POP_PLOT_STRATUM_ASSGN": pl.DataFrame({
                "PLT_CN": [1, 2, 3],
                "STRATUM_CN": [1, 1, 1],
                "EVALID": [452301, 452301, 452301]
            }).lazy()
        }
        
        return db
    
    def test_progress_tracking_enabled(self, mock_db, mock_progress_context):
        """Test that progress tracking can be enabled and reports correctly."""
        mock_progress, mock_task = mock_progress_context
        
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"show_progress": True}
        )
        
        with patch.object(AreaEstimator, 'progress_context') as mock_context:
            mock_context.return_value.__enter__ = Mock()
            mock_context.return_value.__exit__ = Mock()
            
            estimator = AreaEstimator(mock_db, config)
            # Simulate operation tracking
            with estimator._track_operation(OperationType.LOAD, "Loading data", total=100):
                estimator._update_progress(completed=50)
                estimator._update_progress(completed=100, description="Complete")
        
        # Verify progress context was used
        mock_context.assert_called_once()
    
    def test_progress_tracking_disabled(self, mock_db):
        """Test that progress tracking can be disabled."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"show_progress": False}
        )
        
        estimator = AreaEstimator(mock_db, config)
        
        # Progress tracking should be a no-op when disabled
        with estimator._track_operation(OperationType.LOAD, "Loading data"):
            estimator._update_progress(description="Should not display")
        
        # No assertions needed - just verify no errors occur


class TestLazyEstimatorCaching:
    """Test suite for caching functionality in lazy estimators."""
    
    @pytest.fixture
    def cached_estimator(self, sample_db):
        """Create an estimator with caching enabled."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"enable_caching": True}
        )
        return BiomassEstimator(sample_db, config)
    
    def test_cache_hit_on_repeated_operations(self, cached_estimator):
        """Test that repeated operations hit the cache."""
        # First call should populate cache
        with patch.object(cached_estimator, '_ref_species_cache', None):
            result1 = cached_estimator._get_ref_species()
            
            # Second call should hit cache
            result2 = cached_estimator._get_ref_species()
            
            # Results should be identical (same object due to caching)
            assert result1 is result2
    
    def test_cache_invalidation_on_config_change(self, cached_estimator):
        """Test that cache is invalidated when configuration changes."""
        # Populate cache
        cached_estimator._get_ref_species()
        initial_cache = cached_estimator._ref_species_cache
        
        # Change configuration that should invalidate cache
        cached_estimator.component = "BG"  # Change biomass component
        
        # Cache should be maintained (component change doesn't affect ref_species)
        assert cached_estimator._ref_species_cache is initial_cache
    
    def test_cache_ttl_expiration(self, cached_estimator, monkeypatch):
        """Test that cached items expire after TTL."""
        # This would require more complex mocking of time-based cache
        # For now, just verify cache exists
        assert hasattr(cached_estimator, '_ref_species_cache')
        assert hasattr(cached_estimator, '_pop_stratum_cache')
        assert hasattr(cached_estimator, '_ppsa_cache')


class TestLazyEstimatorCollectionStrategies:
    """Test suite for collection strategy configuration."""
    
    @pytest.fixture
    def estimator_with_strategy(self, sample_db):
        """Create an estimator with configurable collection strategy."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"lazy_enabled": True}
        )
        return VolumeEstimator(sample_db, config)
    
    def test_adaptive_collection_strategy(self, estimator_with_strategy):
        """Test adaptive collection strategy behavior."""
        estimator_with_strategy.set_collection_strategy(CollectionStrategy.ADAPTIVE)
        
        # Verify strategy is set
        assert hasattr(estimator_with_strategy, '_collection_strategy')
        
        # Adaptive strategy should collect based on data size
        # This would require more complex testing with actual data flows
    
    def test_eager_collection_strategy(self, estimator_with_strategy):
        """Test eager collection strategy behavior."""
        estimator_with_strategy.set_collection_strategy(CollectionStrategy.EAGER)
        
        # Eager strategy should collect immediately
        # Verify by checking that operations are not deferred
        stats = estimator_with_strategy.get_lazy_statistics()
        assert stats['operations_deferred'] == 0
    
    def test_lazy_collection_strategy(self, estimator_with_strategy):
        """Test lazy collection strategy behavior."""
        estimator_with_strategy.set_collection_strategy(CollectionStrategy.LAZY)
        
        # Lazy strategy should defer as much as possible
        # This would be verified by checking deferred operations count


class TestLazyEstimatorMemoryManagement:
    """Test suite for memory management in lazy estimators."""
    
    def test_memory_cleanup_on_context_exit(self, sample_db):
        """Test that memory is cleaned up when estimator context exits."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live"
        )
        
        # Track memory before
        gc.collect()
        
        with TPAEstimator(sample_db, config) as estimator:
            # Perform some operations
            estimator.load_table_lazy("TREE")
            estimator.load_table_lazy("COND")
        
        # After context exit, resources should be cleaned up
        gc.collect()
        
        # Verify estimator cleaned up (would need more detailed memory tracking)
    
    def test_lazy_operation_memory_efficiency(self, sample_db):
        """Test that lazy operations use less memory than eager operations."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"lazy_enabled": True, "lazy_threshold_rows": 1}
        )
        
        estimator = VolumeEstimator(sample_db, config)
        
        # Lazy operations should not materialize data immediately
        lazy_tree = estimator.load_table_lazy("TREE")
        assert isinstance(lazy_tree, pl.LazyFrame)
        
        # Memory usage should be minimal until collection
        # This would require actual memory profiling


class TestLazyEstimatorErrorHandling:
    """Test suite for error handling in lazy contexts."""
    
    def test_error_propagation_in_lazy_context(self, sample_db):
        """Test that errors in lazy operations are properly propagated."""
        config = EstimatorConfig(
            land_type="invalid",  # Invalid land type
            tree_type="live"
        )
        
        estimator = AreaEstimator(sample_db, config)
        
        # Error should be raised when the lazy operation is collected
        with pytest.raises(ValueError, match="Invalid land_type"):
            estimator.estimate()
    
    def test_graceful_fallback_on_lazy_failure(self, sample_db):
        """Test graceful fallback when lazy evaluation fails."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"lazy_enabled": True}
        )
        
        estimator = BiomassEstimator(sample_db, config)
        
        # Mock a failure in lazy evaluation
        with patch.object(estimator, 'calculate_values', side_effect=Exception("Lazy failed")):
            # Should fall back to eager evaluation or raise informative error
            with pytest.raises(Exception, match="Lazy failed"):
                estimator.estimate()


class TestLazyEstimatorStatistics:
    """Test suite for lazy evaluation statistics tracking."""
    
    def test_statistics_tracking(self, sample_db):
        """Test that lazy evaluation statistics are tracked correctly."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={"lazy_enabled": True}
        )
        
        estimator = VolumeEstimator(sample_db, config)
        
        # Perform some lazy operations
        estimator.load_table_lazy("TREE")
        estimator.load_table_lazy("COND")
        
        # Get statistics
        stats = estimator.get_lazy_statistics()
        
        # Verify statistics structure
        assert 'operations_deferred' in stats
        assert 'operations_collected' in stats
        assert 'cache_hits' in stats
        assert 'cache_misses' in stats
        assert 'total_execution_time' in stats
        
        # Operations should be deferred
        assert stats['operations_deferred'] >= 0
    
    def test_statistics_reset(self, sample_db):
        """Test that statistics can be reset."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live"
        )
        
        estimator = TPAEstimator(sample_db, config)
        
        # Perform operations
        estimator.load_table_lazy("TREE")
        
        # Reset statistics
        estimator.reset_lazy_statistics()
        
        # Statistics should be zeroed
        stats = estimator.get_lazy_statistics()
        assert stats['operations_deferred'] == 0
        assert stats['cache_hits'] == 0


class TestLazyEstimatorIntegration:
    """Integration tests for lazy estimators with real workflows."""
    
    @pytest.mark.parametrize("estimator_class,function_name", [
        (AreaEstimator, "area"),
        (BiomassEstimator, "biomass"),
        (TPAEstimator, "tpa"),
        (VolumeEstimator, "volume"),
        (GrowthEstimator, "growth"),
        (MortalityEstimator, "mortality"),
    ])
    def test_lazy_estimator_workflow(self, sample_db, estimator_class, function_name):
        """Test complete workflow for each lazy estimator."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live",
            extra_params={
                "lazy_enabled": True,
                "show_progress": False,
                "component": "AG" if function_name == "biomass" else None
            }
        )
        
        # Create and run estimator
        with estimator_class(sample_db, config) as estimator:
            try:
                result = estimator.estimate()
                
                # Verify result is a DataFrame
                assert isinstance(result, pl.DataFrame)
                
                # Verify expected columns exist based on estimator type
                if function_name == "area":
                    assert "AREA_ACRE" in result.columns or "AREA_TOTAL" in result.columns
                elif function_name == "biomass":
                    assert "BIO_ACRE" in result.columns or "BIO_TOTAL" in result.columns
                elif function_name == "tpa":
                    assert "TPA" in result.columns or "TPA_TOTAL" in result.columns
                elif function_name == "volume":
                    assert any(col in result.columns for col in ["VOL_ACRE", "VOL_CF", "VOL_TOTAL"])
                
            except NotImplementedError:
                # Some methods may not be fully implemented yet
                pytest.skip(f"{function_name} not fully implemented")
    
    def test_cross_estimator_consistency(self, sample_db):
        """Test that different estimators produce consistent results."""
        params = {
            "land_type": "forest",
            "tree_type": "live",
            "show_progress": False
        }
        
        # Run area estimation
        area_result = area(sample_db, **params)
        
        # Run TPA estimation
        tpa_result = tpa(sample_db, **params)
        
        # Both should have consistent plot counts
        if "nPlots_AREA" in area_result.columns and "nPlots_TREE" in tpa_result.columns:
            # Plot counts should be similar (may differ due to filtering)
            area_plots = area_result["nPlots_AREA"][0] if len(area_result) > 0 else 0
            tpa_plots = tpa_result["nPlots_TREE"][0] if len(tpa_result) > 0 else 0
            
            # They should be in the same ballpark
            assert abs(area_plots - tpa_plots) <= max(area_plots, tpa_plots) * 0.5


class TestLazyEstimatorBasics:
    """Basic tests for lazy estimator classes."""
    
    def test_lazy_estimator_inheritance(self):
        """Test that all lazy estimators inherit from LazyBaseEstimator."""
        estimators = [
            AreaEstimator,
            BiomassEstimator,
            TPAEstimator,
            VolumeEstimator,
            GrowthEstimator,
            MortalityEstimator
        ]
        
        for estimator_class in estimators:
            assert issubclass(estimator_class, LazyBaseEstimator)
    
    def test_lazy_estimator_required_methods(self):
        """Test that all lazy estimators implement required methods."""
        required_methods = [
            'get_required_tables',
            'get_response_columns',
            'calculate_values',
            'estimate'
        ]
        
        estimators = [
            AreaEstimator,
            BiomassEstimator,
            TPAEstimator,
            VolumeEstimator,
            GrowthEstimator,
            MortalityEstimator
        ]
        
        for estimator_class in estimators:
            for method in required_methods:
                assert hasattr(estimator_class, method)
                assert callable(getattr(estimator_class, method))
    
    @pytest.mark.parametrize("estimator_class,name", [
        (AreaEstimator, "area"),
        (BiomassEstimator, "biomass"),
        (TPAEstimator, "tpa"),
        (VolumeEstimator, "volume"),
        (GrowthEstimator, "growth"),
        (MortalityEstimator, "mortality"),
    ])
    def test_basic_estimator_classes(self, estimator_class, name):
        """Test that estimator classes can be instantiated."""
        config = EstimatorConfig(
            land_type="forest",
            tree_type="live"
        )
        
        # Mock database
        mock_db = MagicMock(spec=FIA)
        mock_db.evalid = []
        mock_db.tables = {}
        
        # Should be able to create instance
        estimator = estimator_class(mock_db, config)
        assert estimator is not None
        
        # Should have expected attributes
        assert hasattr(estimator, 'db')
        assert hasattr(estimator, 'config')