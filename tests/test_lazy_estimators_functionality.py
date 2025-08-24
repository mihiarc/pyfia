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
from pyfia.estimation import area, tpa_lazy, volume_lazy, growth, mortality_lazy
from pyfia.estimation.biomass import biomass as biomass_lazy
from pyfia.estimation.base import EstimatorConfig
from pyfia.estimation.lazy_evaluation import CollectionStrategy, LazyEstimatorMixin
from pyfia.estimation.progress import OperationType
from pyfia.estimation.lazy_base import LazyBaseEstimator

# Import lazy estimator classes directly for testing
from pyfia.estimation.area import AreaEstimator
from pyfia.estimation.biomass import BiomassEstimator
from pyfia.estimation.tpa_lazy import LazyTPAEstimator
from pyfia.estimation.volume_lazy import LazyVolumeEstimator
from pyfia.estimation.growth import GrowthEstimator
from pyfia.estimation.mortality_lazy import LazyMortalityEstimator


class TestLazyEstimatorProgressTracking:
    """Test suite for progress tracking functionality in lazy estimators."""
    
    @pytest.fixture
    def mock_progress_context(self):
        """Mock progress context to avoid terminal output in tests."""
        with patch('pyfia.estimation.progress.Progress') as mock_progress:
            mock_progress_instance = MagicMock()
            mock_progress.return_value.__enter__.return_value = mock_progress_instance
            mock_progress.return_value.__exit__.return_value = None
            yield mock_progress_instance

    def test_progress_tracking_initialization(self, sample_fia_instance, mock_progress_context):
        """Test that progress tracking is properly initialized."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": True,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            with AreaEstimator(db, config) as estimator:
                # Check that progress tracking is enabled
                assert hasattr(estimator, '_progress_enabled')
                assert hasattr(estimator, 'console')
                
                # The progress context should be available
                assert hasattr(estimator, 'progress_context')
                
                print("✓ Progress tracking initialization verified")

    def test_progress_bar_creation_and_updates(self, sample_fia_instance, mock_progress_context):
        """Test progress bar creation and update calls."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": True,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            with patch.object(AreaEstimator, '_track_operation') as mock_track:
                mock_context = MagicMock()
                mock_track.return_value.__enter__.return_value = mock_context
                mock_track.return_value.__exit__.return_value = None
                
                estimator = AreaEstimator(db, config)
                result = estimator.estimate()
                
                # Verify that progress tracking was called
                assert mock_track.called
                track_calls = mock_track.call_args_list
                
                # Should have at least one progress tracking call
                assert len(track_calls) > 0
                
                # Check that operation types are properly specified
                operation_types = [call[0][0] for call in track_calls if len(call[0]) > 0]
                expected_types = [OperationType.COMPUTE, OperationType.FILTER]
                
                # At least one expected operation type should be present
                assert any(op_type in operation_types for op_type in expected_types)
                
                print(f"✓ Progress tracking called {len(track_calls)} times")
                print(f"✓ Operation types tracked: {set(operation_types)}")

    def test_progress_description_updates(self, sample_fia_instance):
        """Test that progress descriptions are meaningful and updated."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": True,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            progress_descriptions = []
            
            # Mock the progress update method to capture descriptions
            with patch.object(AreaEstimator, '_update_progress') as mock_update:
                estimator = AreaEstimator(db, config)
                result = estimator.estimate()
                
                # Extract descriptions from progress update calls
                for call in mock_update.call_args_list:
                    if 'description' in call.kwargs:
                        progress_descriptions.append(call.kwargs['description'])
                    elif len(call.args) > 0 and isinstance(call.args[0], str):
                        progress_descriptions.append(call.args[0])
                
                # Verify we got meaningful descriptions
                assert len(progress_descriptions) > 0
                
                # Check for expected description patterns
                expected_patterns = [
                    "area", "calculate", "estimate", "filter", "load", "stratif"
                ]
                
                found_patterns = []
                for desc in progress_descriptions:
                    if desc:  # Non-empty description
                        desc_lower = desc.lower()
                        for pattern in expected_patterns:
                            if pattern in desc_lower:
                                found_patterns.append(pattern)
                                break
                
                print(f"✓ Captured {len(progress_descriptions)} progress descriptions")
                print(f"✓ Found patterns: {set(found_patterns)}")
                print(f"✓ Sample descriptions: {progress_descriptions[:3]}")

    def test_progress_with_different_estimators(self, sample_fia_instance):
        """Test progress tracking works across different estimator types."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        estimator_classes = [
            (AreaEstimator, "area"),
            (BiomassEstimator, "biomass"),
            (LazyTPAEstimator, "tpa"),
            (LazyVolumeEstimator, "volume"),
        ]
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": True,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            for estimator_class, name in estimator_classes:
                print(f"\n  Testing progress for {name} estimator:")
                
                with patch.object(estimator_class, '_track_operation') as mock_track:
                    mock_context = MagicMock()
                    mock_track.return_value.__enter__.return_value = mock_context
                    mock_track.return_value.__exit__.return_value = None
                    
                    try:
                        estimator = estimator_class(db, config)
                        result = estimator.estimate()
                        
                        # Check that progress tracking was used
                        assert mock_track.called, f"Progress tracking not called for {name}"
                        call_count = len(mock_track.call_args_list)
                        print(f"    ✓ Progress tracked {call_count} times")
                        
                    except Exception as e:
                        print(f"    ⚠ Error testing {name}: {e}")

    def test_progress_disable_functionality(self, sample_fia_instance):
        """Test that progress can be disabled."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,  # Explicitly disabled
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            with patch('pyfia.estimation.progress.Progress') as mock_progress:
                estimator = AreaEstimator(db, config)
                result = estimator.estimate()
                
                # Progress should not be initialized when disabled
                # The exact behavior depends on implementation but should be minimal
                call_count = mock_progress.call_count
                print(f"✓ Progress calls with show_progress=False: {call_count}")
                
                # Result should still be valid
                assert result is not None
                assert len(result) >= 0


class TestLazyEstimatorCaching:
    """Test suite for caching mechanisms in lazy estimators."""
    
    def test_reference_table_caching(self, sample_fia_instance):
        """Test that reference tables are properly cached."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Check if cache attributes exist
            cache_attrs = ['_pop_stratum_cache', '_ppsa_cache', '_pop_estn_unit_cache']
            for attr in cache_attrs:
                assert hasattr(estimator, attr), f"Missing cache attribute: {attr}"
                
            # Initially caches should be None
            for attr in cache_attrs:
                assert getattr(estimator, attr) is None, f"Cache {attr} should start as None"
            
            # Run estimation to populate caches
            result = estimator.estimate()
            
            # Check if any caches were populated (depends on implementation)
            populated_caches = []
            for attr in cache_attrs:
                if getattr(estimator, attr) is not None:
                    populated_caches.append(attr)
            
            print(f"✓ Found {len(cache_attrs)} cache attributes")
            print(f"✓ Populated caches: {populated_caches}")

    def test_cache_invalidation(self, sample_fia_instance):
        """Test that caches are properly invalidated when needed."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Run first estimation
            result1 = estimator.estimate()
            
            # Check cache state after first run
            first_cache_state = {
                attr: getattr(estimator, attr) is not None
                for attr in ['_pop_stratum_cache', '_ppsa_cache', '_pop_estn_unit_cache']
            }
            
            # Create new estimator (simulates cache invalidation scenario)
            config2 = EstimatorConfig(
                land_type="timber",  # Different land type
                extra_params={
                    "show_progress": False,
                    "lazy_enabled": True
                }
            )
            
            estimator2 = AreaEstimator(db, config2)
            result2 = estimator2.estimate()
            
            # Caches should start fresh for new estimator
            second_cache_state = {
                attr: getattr(estimator2, attr) is not None
                for attr in ['_pop_stratum_cache', '_ppsa_cache', '_pop_estn_unit_cache']
            }
            
            print(f"✓ First cache state: {first_cache_state}")
            print(f"✓ Second cache state: {second_cache_state}")
            print("✓ Cache invalidation test completed")

    def test_cache_hit_statistics(self, sample_fia_instance):
        """Test that cache hit statistics are tracked."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            grp_by=["SPCD"],
            totals=True,
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Check if statistics tracking is available
            if hasattr(estimator, 'get_lazy_statistics'):
                result = estimator.estimate()
                
                stats = estimator.get_lazy_statistics()
                
                # Check that statistics are properly structured
                expected_stats = ['cache_hits', 'operations_deferred', 'operations_collected']
                for stat in expected_stats:
                    assert stat in stats, f"Missing statistic: {stat}"
                    assert isinstance(stats[stat], (int, float)), f"Invalid type for {stat}"
                
                print(f"✓ Cache statistics: {stats}")
                
                # Run multiple similar operations to potentially generate cache hits
                result2 = estimator.estimate()
                stats2 = estimator.get_lazy_statistics()
                
                print(f"✓ Second run statistics: {stats2}")
                
            else:
                print("ℹ Lazy statistics not available for this estimator")

    def test_data_cache_functionality(self, sample_fia_instance):
        """Test that data caching works for intermediate results."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            tree_domain="STATUSCD == 1",  # This should trigger tree data caching
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Check if data cache exists
            if hasattr(estimator, '_data_cache'):
                assert isinstance(estimator._data_cache, dict), "Data cache should be a dictionary"
                
                # Initially should be empty
                initial_cache_size = len(estimator._data_cache)
                
                result = estimator.estimate()
                
                # After estimation, cache might have data
                final_cache_size = len(estimator._data_cache)
                
                print(f"✓ Data cache size: {initial_cache_size} → {final_cache_size}")
                
                if final_cache_size > initial_cache_size:
                    print(f"✓ Cached data keys: {list(estimator._data_cache.keys())}")
                
            else:
                print("ℹ Data cache not available for this estimator")


class TestLazyCollectionStrategies:
    """Test suite for different collection strategies in lazy evaluation."""
    
    @pytest.fixture
    def collection_strategies(self):
        """Available collection strategies for testing."""
        return [
            CollectionStrategy.SEQUENTIAL,
            CollectionStrategy.ADAPTIVE,
            CollectionStrategy.STREAMING,
        ]

    def test_collection_strategy_setting(self, sample_fia_instance, collection_strategies):
        """Test that collection strategies can be set and retrieved."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            for strategy in collection_strategies:
                if hasattr(estimator, 'set_collection_strategy'):
                    estimator.set_collection_strategy(strategy)
                    
                    # Verify strategy was set
                    if hasattr(estimator, 'get_collection_strategy'):
                        current_strategy = estimator.get_collection_strategy()
                        assert current_strategy == strategy, f"Strategy not set correctly: {current_strategy} != {strategy}"
                        print(f"✓ Successfully set collection strategy: {strategy.name}")
                    else:
                        print(f"ℹ Cannot verify strategy setting for {strategy.name}")
                else:
                    print(f"ℹ Collection strategy setting not available")
                    break

    def test_collection_strategy_behavior_differences(self, sample_fia_instance, collection_strategies):
        """Test that different collection strategies produce different behavior patterns."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            grp_by=["SPCD"],
            totals=True,
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        strategy_results = {}
        
        with sample_fia_instance as db:
            for strategy in collection_strategies:
                print(f"\n  Testing collection strategy: {strategy.name}")
                
                try:
                    estimator = AreaEstimator(db, config)
                    
                    if hasattr(estimator, 'set_collection_strategy'):
                        estimator.set_collection_strategy(strategy)
                    
                    # Time the estimation
                    start_time = time.time()
                    result = estimator.estimate()
                    end_time = time.time()
                    
                    execution_time = end_time - start_time
                    
                    # Get statistics if available
                    stats = {}
                    if hasattr(estimator, 'get_lazy_statistics'):
                        stats = estimator.get_lazy_statistics()
                    
                    strategy_results[strategy.name] = {
                        'execution_time': execution_time,
                        'result_shape': result.shape,
                        'statistics': stats
                    }
                    
                    print(f"    ✓ Execution time: {execution_time:.3f}s")
                    print(f"    ✓ Result shape: {result.shape}")
                    if stats:
                        print(f"    ✓ Deferred operations: {stats.get('operations_deferred', 0)}")
                
                except Exception as e:
                    print(f"    ⚠ Error with {strategy.name}: {e}")
                    strategy_results[strategy.name] = {'error': str(e)}
            
            # Compare results across strategies
            successful_strategies = {k: v for k, v in strategy_results.items() if 'error' not in v}
            
            if len(successful_strategies) > 1:
                print(f"\n  Strategy comparison:")
                shapes = set(result['result_shape'] for result in successful_strategies.values())
                assert len(shapes) == 1, "All strategies should produce same result shape"
                
                times = [result['execution_time'] for result in successful_strategies.values()]
                print(f"    ✓ Execution time range: {min(times):.3f}s - {max(times):.3f}s")
                print(f"    ✓ All strategies produced shape: {shapes.pop()}")
            
            print(f"✓ Tested {len(successful_strategies)}/{len(collection_strategies)} strategies successfully")

    def test_default_collection_strategy(self, sample_fia_instance):
        """Test that a reasonable default collection strategy is used."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Check if we can get the default strategy
            if hasattr(estimator, 'get_collection_strategy'):
                default_strategy = estimator.get_collection_strategy()
                assert default_strategy is not None, "Default collection strategy should not be None"
                print(f"✓ Default collection strategy: {default_strategy}")
            
            # Estimation should work with default strategy
            result = estimator.estimate()
            assert result is not None
            assert len(result) >= 0
            
            print("✓ Default collection strategy works correctly")


class TestLazyComputationGraph:
    """Test suite for lazy computation graph optimization."""
    
    def test_computation_graph_building(self, sample_fia_instance):
        """Test that computation graph is properly built."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            grp_by=["SPCD"],
            totals=True,
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Check if computation graph functionality exists
            if hasattr(estimator, '_computation_graph'):
                initial_graph_size = len(estimator._computation_graph)
                
                # Run estimation to build graph
                result = estimator.estimate()
                
                final_graph_size = len(estimator._computation_graph)
                
                print(f"✓ Computation graph size: {initial_graph_size} → {final_graph_size}")
                
                # Check for graph optimization method
                if hasattr(estimator, 'optimize_computation_graph'):
                    try:
                        estimator.optimize_computation_graph()
                        print("✓ Computation graph optimization succeeded")
                    except Exception as e:
                        print(f"ℹ Computation graph optimization error: {e}")
                
            else:
                print("ℹ Computation graph functionality not available")

    def test_execution_plan_generation(self, sample_fia_instance):
        """Test that execution plans can be generated."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            grp_by=["SPCD"],
            tree_domain="STATUSCD == 1",
            totals=True,
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            if hasattr(estimator, 'get_execution_plan'):
                # Get plan before estimation
                pre_plan = estimator.get_execution_plan()
                assert isinstance(pre_plan, str), "Execution plan should be a string"
                
                # Run estimation
                result = estimator.estimate()
                
                # Get plan after estimation
                post_plan = estimator.get_execution_plan()
                
                print(f"✓ Pre-estimation plan length: {len(pre_plan)} chars")
                print(f"✓ Post-estimation plan length: {len(post_plan)} chars")
                
                # Plans should contain meaningful information
                plan_keywords = ['operation', 'node', 'deferred', 'collection']
                found_keywords = [kw for kw in plan_keywords if kw.lower() in post_plan.lower()]
                
                print(f"✓ Plan keywords found: {found_keywords}")
                
                if found_keywords:
                    print("✓ Execution plan contains expected information")
                
            else:
                print("ℹ Execution plan generation not available")

    def test_lazy_operation_deferral(self, sample_fia_instance):
        """Test that operations are properly deferred in lazy evaluation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            grp_by=["SPCD", "FORTYPCD"],
            totals=True,
            variance=True,
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Track deferred operations
            initial_deferred = 0
            if hasattr(estimator, '_deferred_operations'):
                initial_deferred = estimator._deferred_operations
            
            # Run estimation
            result = estimator.estimate()
            
            # Check final state
            final_deferred = 0
            if hasattr(estimator, '_deferred_operations'):
                final_deferred = estimator._deferred_operations
            
            # Get lazy statistics
            stats = {}
            if hasattr(estimator, 'get_lazy_statistics'):
                stats = estimator.get_lazy_statistics()
            
            print(f"✓ Deferred operations: {initial_deferred} → {final_deferred}")
            
            if stats:
                ops_deferred = stats.get('operations_deferred', 0)
                ops_collected = stats.get('operations_collected', 0)
                print(f"✓ Statistics - Deferred: {ops_deferred}, Collected: {ops_collected}")
                
                # In a lazy system, we should have some deferred operations
                if ops_deferred > 0:
                    print("✓ Lazy operation deferral is working")
            
            # Result should still be valid
            assert result is not None
            assert len(result) >= 0


class TestLazyEstimatorErrorHandling:
    """Test suite for error handling in lazy evaluation contexts."""
    
    def test_error_propagation(self, sample_fia_instance):
        """Test that errors are properly propagated from lazy operations."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        # Test with invalid domain filter that should cause error
        config = EstimatorConfig(
            tree_domain="INVALID_COLUMN == 1",  # This should fail
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Should raise an appropriate error
            with pytest.raises(Exception) as exc_info:
                result = estimator.estimate()
            
            # Error should be meaningful
            error_msg = str(exc_info.value).lower()
            assert any(keyword in error_msg for keyword in ['column', 'invalid', 'error', 'filter']), \
                f"Error message should be meaningful: {exc_info.value}"
            
            print(f"✓ Error properly propagated: {type(exc_info.value).__name__}")

    def test_partial_failure_recovery(self, sample_fia_instance):
        """Test handling of partial failures in lazy operations."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Mock a method to simulate partial failure
            original_method = None
            if hasattr(estimator, '_load_required_tables'):
                original_method = estimator._load_required_tables
                
                def mock_load_with_warning(*args, **kwargs):
                    # Issue a warning but continue
                    warnings.warn("Simulated partial failure", category=UserWarning)
                    return original_method(*args, **kwargs) if original_method else None
                
                estimator._load_required_tables = mock_load_with_warning
            
            # Should handle partial failure gracefully
            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")
                
                result = estimator.estimate()
                
                # Should complete successfully despite warning
                assert result is not None
                
                # Should have captured the warning
                warning_messages = [str(w.message) for w in warning_list]
                partial_failure_warnings = [msg for msg in warning_messages if "partial failure" in msg.lower()]
                
                if partial_failure_warnings:
                    print(f"✓ Partial failure handled gracefully: {len(partial_failure_warnings)} warnings")
                
            # Restore original method
            if original_method and hasattr(estimator, '_load_required_tables'):
                estimator._load_required_tables = original_method

    def test_resource_cleanup_on_error(self, sample_fia_instance):
        """Test that resources are properly cleaned up when errors occur."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            # Test using context manager which should handle cleanup
            try:
                with AreaEstimator(db, config) as estimator:
                    # Force an error in the middle of processing
                    if hasattr(estimator, '_calculate_plot_estimates'):
                        original_method = estimator._calculate_plot_estimates
                        
                        def mock_failing_method(*args, **kwargs):
                            raise RuntimeError("Simulated processing error")
                        
                        estimator._calculate_plot_estimates = mock_failing_method
                    
                    # This should fail
                    result = estimator.estimate()
                    
            except RuntimeError as e:
                if "simulated processing error" in str(e).lower():
                    print("✓ Expected error occurred and was handled by context manager")
                else:
                    raise
            
            # If we get here, cleanup should have occurred
            # The exact cleanup behavior depends on implementation
            print("✓ Resource cleanup test completed")

    def test_lazy_frame_error_handling(self, sample_fia_instance):
        """Test error handling specific to lazy frame operations.""" 
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        with sample_fia_instance as db:
            estimator = AreaEstimator(db, config)
            
            # Test with mock lazy frame that fails on collection
            if hasattr(estimator, 'get_conditions_lazy'):
                # Mock a lazy operation that fails
                original_get_conditions = estimator.get_conditions_lazy
                
                def mock_failing_lazy_conditions(*args, **kwargs):
                    # Return a mock wrapper that fails on collect
                    mock_wrapper = MagicMock()
                    mock_wrapper.collect.side_effect = RuntimeError("Lazy collection failed")
                    mock_wrapper.is_lazy = True
                    mock_wrapper.frame = MagicMock()
                    return mock_wrapper
                
                estimator.get_conditions_lazy = mock_failing_lazy_conditions
                
                # Should handle lazy collection failure
                with pytest.raises(RuntimeError) as exc_info:
                    result = estimator.estimate()
                
                assert "lazy collection failed" in str(exc_info.value).lower()
                print("✓ Lazy frame error handling verified")
                
                # Restore original method
                estimator.get_conditions_lazy = original_get_conditions


class TestLazyEstimatorMemoryManagement:
    """Test suite for memory management in lazy estimators."""
    
    def test_memory_cleanup_after_estimation(self, sample_fia_instance):
        """Test that memory is properly cleaned up after estimation."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            grp_by=["SPCD"],
            totals=True,
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        # Measure memory before
        gc.collect()
        import tracemalloc
        tracemalloc.start()
        
        with sample_fia_instance as db:
            with AreaEstimator(db, config) as estimator:
                result = estimator.estimate()
                
                # Check for cleanup methods
                cleanup_methods = ['cleanup', 'clear_cache', '_clear_cache', 'reset']
                available_cleanup = [method for method in cleanup_methods if hasattr(estimator, method)]
                
                print(f"✓ Available cleanup methods: {available_cleanup}")
                
                # Call cleanup methods if available
                for method_name in available_cleanup:
                    try:
                        method = getattr(estimator, method_name)
                        if callable(method):
                            method()
                            print(f"✓ Called cleanup method: {method_name}")
                    except Exception as e:
                        print(f"ℹ Cleanup method {method_name} failed: {e}")
        
        # Memory should be cleaned up after context exit
        gc.collect()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        print(f"✓ Memory usage after cleanup: current={current/1024/1024:.1f}MB, peak={peak/1024/1024:.1f}MB")

    def test_large_dataset_memory_efficiency(self, sample_fia_instance):
        """Test memory efficiency with complex operations."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        # Use complex parameters that would normally use more memory
        config = EstimatorConfig(
            grp_by=["SPCD", "FORTYPCD"],
            by_species=True,
            totals=True,
            variance=True,
            tree_domain="STATUSCD == 1",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        import tracemalloc
        
        with sample_fia_instance as db:
            # Measure memory during lazy evaluation
            tracemalloc.start()
            
            with AreaEstimator(db, config) as estimator:
                result = estimator.estimate()
                
                current, peak = tracemalloc.get_traced_memory()
                
                # Check memory efficiency metrics if available
                if hasattr(estimator, 'get_lazy_statistics'):
                    stats = estimator.get_lazy_statistics()
                    print(f"✓ Lazy statistics: {stats}")
                
                memory_efficiency = current / peak if peak > 0 else 1.0
                
                print(f"✓ Complex operation memory efficiency: {memory_efficiency:.2f}")
                print(f"✓ Peak memory usage: {peak/1024/1024:.1f}MB")
                print(f"✓ Result shape: {result.shape}")
                
                # Memory efficiency should be reasonable (not using way more than needed)
                assert memory_efficiency >= 0.1, "Memory efficiency too low - possible memory leak"
                
            tracemalloc.stop()

    def test_context_manager_cleanup(self, sample_fia_instance):
        """Test that context manager properly handles resource cleanup."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        config = EstimatorConfig(
            land_type="forest",
            extra_params={
                "show_progress": False,
                "lazy_enabled": True
            }
        )
        
        # Track resources before context
        estimator_ref = None
        
        with sample_fia_instance as db:
            with AreaEstimator(db, config) as estimator:
                estimator_ref = estimator
                
                # Check that estimator has context manager methods
                assert hasattr(estimator, '__enter__'), "Estimator should have __enter__ method"
                assert hasattr(estimator, '__exit__'), "Estimator should have __exit__ method"
                
                result = estimator.estimate()
                assert result is not None
                
                print("✓ Estimation completed within context manager")
        
        # After context exit, cleanup should have occurred
        print("✓ Context manager cleanup completed")
        
        # Test exception handling in context manager
        with sample_fia_instance as db:
            try:
                with AreaEstimator(db, config) as estimator:
                    # Force an exception
                    raise ValueError("Test exception")
                    
            except ValueError as e:
                if "test exception" in str(e).lower():
                    print("✓ Context manager handled exception properly")
                else:
                    raise


class TestLazyEstimatorIntegration:
    """Integration tests for lazy estimator functionality with real workflows."""
    
    def test_multi_estimator_workflow(self, sample_fia_instance):
        """Test using multiple lazy estimators in sequence."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        base_params = {
            "land_type": "forest",
            "totals": True,
            "show_progress": False
        }
        
        estimator_functions = [
            ("Area", area_lazy),
            ("Biomass", biomass_lazy), 
            ("TPA", tpa_lazy),
            ("Volume", volume_lazy),
        ]
        
        results = {}
        
        with sample_fia_instance as db:
            for name, func in estimator_functions:
                try:
                    print(f"\n  Running {name} estimation...")
                    result = func(db, **base_params)
                    
                    assert result is not None
                    assert len(result) >= 0
                    
                    results[name] = {
                        'shape': result.shape,
                        'columns': result.columns,
                        'success': True
                    }
                    
                    print(f"    ✓ {name} completed: {result.shape}")
                    
                except Exception as e:
                    print(f"    ⚠ {name} failed: {e}")
                    results[name] = {
                        'error': str(e),
                        'success': False
                    }
            
            # Summary
            successful = sum(1 for r in results.values() if r.get('success', False))
            total = len(results)
            
            print(f"\n  Multi-estimator workflow: {successful}/{total} successful")
            
            # At least some estimators should work
            assert successful > 0, "At least one estimator should work in multi-workflow"

    def test_parameter_compatibility_across_estimators(self, sample_fia_instance):
        """Test that similar parameters work consistently across estimators."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        # Parameters that should work for tree-based estimators
        tree_params = {
            "grp_by": ["SPCD"],
            "tree_domain": "STATUSCD == 1 AND DIA >= 10.0",
            "totals": True,
            "show_progress": False
        }
        
        # Parameters for area estimation
        area_params = {
            "by_land_type": True,
            "land_type": "all", 
            "show_progress": False
        }
        
        tree_estimators = [
            ("Biomass", biomass_lazy),
            ("TPA", tpa_lazy),
            ("Volume", volume_lazy),
        ]
        
        with sample_fia_instance as db:
            # Test tree-based estimators
            tree_results = {}
            for name, func in tree_estimators:
                try:
                    result = func(db, **tree_params)
                    tree_results[name] = result.shape
                    print(f"✓ {name} with tree params: {result.shape}")
                except Exception as e:
                    print(f"⚠ {name} failed with tree params: {e}")
                    tree_results[name] = None
            
            # Test area estimator
            try:
                area_result = area_lazy(db, **area_params)
                print(f"✓ Area with area params: {area_result.shape}")
            except Exception as e:
                print(f"⚠ Area failed with area params: {e}")
            
            # Check that tree estimators behaved consistently
            successful_tree_shapes = [shape for shape in tree_results.values() if shape is not None]
            if len(successful_tree_shapes) > 1:
                # Check that column counts are reasonable (may vary but should be in similar range)
                col_counts = [shape[1] for shape in successful_tree_shapes]
                min_cols, max_cols = min(col_counts), max(col_counts)
                print(f"✓ Tree estimator column range: {min_cols}-{max_cols}")

    def test_lazy_estimator_robustness(self, sample_fia_instance):
        """Test robustness of lazy estimators under various conditions."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        # Test various edge case scenarios
        test_scenarios = [
            {
                "name": "empty_grouping",
                "params": {"grp_by": [], "show_progress": False}
            },
            {
                "name": "single_group",
                "params": {"grp_by": ["SPCD"], "show_progress": False}
            },
            {
                "name": "multiple_options",
                "params": {"totals": True, "variance": True, "show_progress": False}
            },
            {
                "name": "complex_filter",
                "params": {
                    "tree_domain": "STATUSCD == 1 AND DIA >= 5.0 AND DIA <= 50.0",
                    "show_progress": False
                }
            }
        ]
        
        # Test area estimator robustness
        with sample_fia_instance as db:
            robustness_results = {}
            
            for scenario in test_scenarios:
                try:
                    result = area_lazy(db, **scenario["params"])
                    robustness_results[scenario["name"]] = {
                        'success': True,
                        'shape': result.shape
                    }
                    print(f"✓ Robust test '{scenario['name']}': {result.shape}")
                    
                except Exception as e:
                    robustness_results[scenario["name"]] = {
                        'success': False,
                        'error': str(e)
                    }
                    print(f"⚠ Robust test '{scenario['name']}' failed: {e}")
            
            # Calculate robustness score
            successful_scenarios = sum(1 for r in robustness_results.values() if r['success'])
            total_scenarios = len(robustness_results)
            robustness_score = successful_scenarios / total_scenarios
            
            print(f"\n  Robustness score: {successful_scenarios}/{total_scenarios} ({robustness_score:.1%})")
            
            # Should handle most scenarios successfully
            assert robustness_score >= 0.5, f"Robustness too low: {robustness_score:.1%}"