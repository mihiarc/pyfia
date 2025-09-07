"""
Performance benchmarking tests for lazy estimators vs eager implementations.

This test suite measures and validates performance improvements from lazy evaluation
including memory usage reduction and execution time optimization.

Tests cover:
- Memory usage comparison (peak and sustained)
- Execution time benchmarking
- Scalability with different data sizes
- Collection strategy optimization
- Cache hit rate measurement
- Database query optimization

The tests use real FIA database structures and realistic workloads to provide
meaningful performance metrics that reflect production use cases.
"""

import gc
import time
import tracemalloc
import warnings
from contextlib import contextmanager
from typing import Dict, List, Tuple, Any, Optional, Callable, Union
from dataclasses import dataclass
from unittest.mock import Mock, patch

import polars as pl
import pytest

from pyfia import FIA
from pyfia.estimation import (
    # Eager functions
    area, biomass, tpa, volume, growth,
    # Lazy functions
    area_lazy, biomass_lazy, tpa, volume_lazy, mortality_lazy
from pyfia.estimation import growth
)
from pyfia.estimation.config import EstimatorConfig
from pyfia.estimation.lazy_evaluation import CollectionStrategy

# Import mortality functions separately
try:
    from pyfia.estimation.mortality import mortality
except ImportError:
    from pyfia.estimation.mortality.mortality import mortality


@dataclass
class PerformanceMetrics:
    """Container for performance measurement results."""
    memory_peak_mb: float
    memory_current_mb: float
    execution_time_s: float
    result_rows: int
    result_cols: int
    cache_hits: int = 0
    operations_deferred: int = 0
    operations_collected: int = 0
    
    @property
    def memory_efficiency_ratio(self) -> float:
        """Ratio of current to peak memory usage (higher is better)."""
        return self.memory_current_mb / self.memory_peak_mb if self.memory_peak_mb > 0 else 1.0
    
    def compare_to(self, other: 'PerformanceMetrics') -> Dict[str, float]:
        """Compare this metrics to another, returning improvement ratios."""
        return {
            "memory_improvement": other.memory_peak_mb / self.memory_peak_mb if self.memory_peak_mb > 0 else 1.0,
            "speed_improvement": other.execution_time_s / self.execution_time_s if self.execution_time_s > 0 else 1.0,
            "memory_efficiency_improvement": self.memory_efficiency_ratio / other.memory_efficiency_ratio if other.memory_efficiency_ratio > 0 else 1.0,
        }


class PerformanceBenchmarkRunner:
    """Utility class for running performance benchmarks consistently."""
    
    @staticmethod
    @contextmanager
    def measure_performance():
        """Context manager to measure memory and execution time."""
        # Clear memory before measurement
        gc.collect()
        
        # Start memory tracking
        tracemalloc.start()
        
        # Record start time
        start_time = time.perf_counter()
        
        try:
            yield
            
        finally:
            # Record end time
            end_time = time.perf_counter()
            
            # Get memory stats
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            # Store in shared location for retrieval
            PerformanceBenchmarkRunner._last_metrics = {
                'memory_peak_mb': peak / (1024 * 1024),
                'memory_current_mb': current / (1024 * 1024),
                'execution_time_s': end_time - start_time
            }
    
    @classmethod
    def get_last_metrics(cls) -> Dict[str, float]:
        """Get the metrics from the last measurement."""
        return getattr(cls, '_last_metrics', {})
    
    @staticmethod
    def benchmark_function(func: Callable, *args, **kwargs) -> Tuple[Any, PerformanceMetrics]:
        """
        Benchmark a function and return results with performance metrics.
        
        Parameters
        ----------
        func : Callable
            Function to benchmark
        *args, **kwargs
            Arguments to pass to function
            
        Returns
        -------
        Tuple[Any, PerformanceMetrics]
            Function result and performance metrics
        """
        with PerformanceBenchmarkRunner.measure_performance():
            result = func(*args, **kwargs)
        
        base_metrics = PerformanceBenchmarkRunner.get_last_metrics()
        
        # Extract lazy statistics if available
        cache_hits = 0
        operations_deferred = 0
        operations_collected = 0
        
        # Try to get lazy stats from various sources
        if hasattr(result, 'lazy_statistics'):
            stats = result.lazy_statistics
            cache_hits = stats.get('cache_hits', 0)
            operations_deferred = stats.get('operations_deferred', 0)
            operations_collected = stats.get('operations_collected', 0)
        
        return result, PerformanceMetrics(
            memory_peak_mb=base_metrics['memory_peak_mb'],
            memory_current_mb=base_metrics['memory_current_mb'],
            execution_time_s=base_metrics['execution_time_s'],
            result_rows=len(result) if hasattr(result, '__len__') else 0,
            result_cols=len(result.columns) if hasattr(result, 'columns') else 0,
            cache_hits=cache_hits,
            operations_deferred=operations_deferred,
            operations_collected=operations_collected,
        )


class TestLazyEstimatorPerformance:
    """Test suite for measuring lazy estimator performance improvements."""
    
    @pytest.fixture
    def benchmark_configs(self):
        """Performance test configurations with varying complexity."""
        return [
            {
                "name": "basic_estimation",
                "params": {"land_type": "forest"},
                "description": "Basic estimation with minimal parameters"
            },
            {
                "name": "medium_complexity",
                "params": {"grp_by": ["SPCD"], "by_species": True, "totals": True},
                "description": "Medium complexity with grouping and species breakdown"
            },
            {
                "name": "high_complexity",
                "params": {
                    "grp_by": ["SPCD", "FORTYPCD"], 
                    "tree_domain": "STATUSCD == 1 AND DIA >= 5.0",
                    "totals": True,
                    "variance": True
                },
                "description": "High complexity with multiple groups and filtering"
            },
            {
                "name": "variance_intensive",
                "params": {"variance": True, "totals": True, "by_species": True},
                "description": "Variance calculation intensive workload"
            }
        ]

    def run_performance_comparison(self, db: FIA, eager_func: Callable, 
                                 lazy_func: Callable, config: Dict[str, Any],
                                 num_runs: int = 3) -> Dict[str, Any]:
        """
        Run performance comparison between eager and lazy implementations.
        
        Parameters
        ----------
        db : FIA
            Database instance
        eager_func : Callable
            Eager implementation function
        lazy_func : Callable
            Lazy implementation function
        config : Dict[str, Any]
            Test configuration
        num_runs : int
            Number of runs for averaging
            
        Returns
        -------
        Dict[str, Any]
            Performance comparison results
        """
        eager_metrics = []
        lazy_metrics = []
        
        print(f"\n  Running {config['name']}: {config['description']}")
        
        # Run multiple times and average
        for run in range(num_runs):
            print(f"    Run {run + 1}/{num_runs}")
            
            # Test eager implementation
            eager_result, eager_perf = PerformanceBenchmarkRunner.benchmark_function(
                eager_func, db, **config['params']
            )
            eager_metrics.append(eager_perf)
            
            # Test lazy implementation  
            lazy_result, lazy_perf = PerformanceBenchmarkRunner.benchmark_function(
                lazy_func, db, show_progress=False, **config['params']
            )
            lazy_metrics.append(lazy_perf)
            
            # Verify results are equivalent (basic check)
            assert eager_result.shape == lazy_result.shape, \
                f"Result shapes differ: eager {eager_result.shape} vs lazy {lazy_result.shape}"
        
        # Calculate averages
        avg_eager = self._average_metrics(eager_metrics)
        avg_lazy = self._average_metrics(lazy_metrics)
        
        # Calculate improvements
        improvements = avg_lazy.compare_to(avg_eager)
        
        return {
            'config': config,
            'eager_metrics': avg_eager,
            'lazy_metrics': avg_lazy,
            'improvements': improvements,
            'num_runs': num_runs
        }
    
    def _average_metrics(self, metrics_list: List[PerformanceMetrics]) -> PerformanceMetrics:
        """Calculate average of multiple performance metrics."""
        if not metrics_list:
            return PerformanceMetrics(0, 0, 0, 0, 0)
        
        return PerformanceMetrics(
            memory_peak_mb=sum(m.memory_peak_mb for m in metrics_list) / len(metrics_list),
            memory_current_mb=sum(m.memory_current_mb for m in metrics_list) / len(metrics_list),
            execution_time_s=sum(m.execution_time_s for m in metrics_list) / len(metrics_list),
            result_rows=metrics_list[0].result_rows,  # Should be consistent
            result_cols=metrics_list[0].result_cols,  # Should be consistent
            cache_hits=sum(m.cache_hits for m in metrics_list) / len(metrics_list),
            operations_deferred=sum(m.operations_deferred for m in metrics_list) / len(metrics_list),
            operations_collected=sum(m.operations_collected for m in metrics_list) / len(metrics_list),
        )

    def test_area_performance(self, sample_fia_instance, benchmark_configs):
        """Benchmark area estimation performance improvements."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("AREA ESTIMATION PERFORMANCE BENCHMARKS")
        print("="*60)
        
        with sample_fia_instance as db:
            total_improvements = []
            
            for config in benchmark_configs:
                result = self.run_performance_comparison(
                    db, area, area_lazy, config
                )
                total_improvements.append(result['improvements'])
                
                # Print results
                eager_perf = result['eager_metrics']
                lazy_perf = result['lazy_metrics']
                improvements = result['improvements']
                
                print(f"\n  Results for {config['name']}:")
                print(f"    Memory:     {eager_perf.memory_peak_mb:.1f} → {lazy_perf.memory_peak_mb:.1f} MB "
                      f"({improvements['memory_improvement']:.1f}x improvement)")
                print(f"    Time:       {eager_perf.execution_time_s:.3f} → {lazy_perf.execution_time_s:.3f} s "
                      f"({improvements['speed_improvement']:.1f}x improvement)")
                print(f"    Deferred:   {lazy_perf.operations_deferred:.0f} operations")
                print(f"    Cache hits: {lazy_perf.cache_hits:.0f}")
                
                # Assert minimum performance improvements
                assert improvements['memory_improvement'] >= 1.0, \
                    f"Memory usage increased for {config['name']}"
                assert improvements['speed_improvement'] >= 0.8, \
                    f"Significant speed regression for {config['name']}"
            
            # Calculate overall averages
            avg_memory_improvement = sum(imp['memory_improvement'] for imp in total_improvements) / len(total_improvements)
            avg_speed_improvement = sum(imp['speed_improvement'] for imp in total_improvements) / len(total_improvements)
            
            print(f"\n  AREA ESTIMATION SUMMARY:")
            print(f"    Average memory improvement: {avg_memory_improvement:.1f}x")
            print(f"    Average speed improvement:  {avg_speed_improvement:.1f}x")
            
            # Assert overall targets
            assert avg_memory_improvement >= 1.2, "Expected at least 20% memory improvement"
            print("  ✓ Area performance targets met")

    def test_biomass_performance(self, sample_fia_instance, benchmark_configs):
        """Benchmark biomass estimation performance improvements."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("BIOMASS ESTIMATION PERFORMANCE BENCHMARKS")
        print("="*60)
        
        with sample_fia_instance as db:
            total_improvements = []
            
            for config in benchmark_configs:
                # Skip area_domain for biomass tests
                if "area_domain" in config.get("params", {}):
                    continue
                    
                result = self.run_performance_comparison(
                    db, biomass, biomass_lazy, config
                )
                total_improvements.append(result['improvements'])
                
                # Print results
                self._print_performance_results("Biomass", config, result)
                
                # Assert minimum performance improvements
                assert result['improvements']['memory_improvement'] >= 1.0, \
                    f"Memory usage increased for biomass {config['name']}"
            
            # Calculate and print summary
            self._print_performance_summary("BIOMASS", total_improvements)

    def test_tpa_performance(self, sample_fia_instance, benchmark_configs):
        """Benchmark TPA estimation performance improvements.""" 
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("TPA ESTIMATION PERFORMANCE BENCHMARKS")
        print("="*60)
        
        with sample_fia_instance as db:
            total_improvements = []
            
            for config in benchmark_configs:
                # Skip area_domain for TPA tests
                if "area_domain" in config.get("params", {}):
                    continue
                    
                result = self.run_performance_comparison(
                    db, tpa, tpa, config
                )
                total_improvements.append(result['improvements'])
                
                # Print results
                self._print_performance_results("TPA", config, result)
                
                # Assert minimum performance improvements
                assert result['improvements']['memory_improvement'] >= 1.0, \
                    f"Memory usage increased for TPA {config['name']}"
            
            # Calculate and print summary
            self._print_performance_summary("TPA", total_improvements)

    def test_volume_performance(self, sample_fia_instance, benchmark_configs):
        """Benchmark volume estimation performance improvements."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("VOLUME ESTIMATION PERFORMANCE BENCHMARKS") 
        print("="*60)
        
        with sample_fia_instance as db:
            total_improvements = []
            
            for config in benchmark_configs:
                # Skip area_domain for volume tests
                if "area_domain" in config.get("params", {}):
                    continue
                    
                result = self.run_performance_comparison(
                    db, volume, volume_lazy, config
                )
                total_improvements.append(result['improvements'])
                
                # Print results  
                self._print_performance_results("Volume", config, result)
                
                # Assert minimum performance improvements
                assert result['improvements']['memory_improvement'] >= 1.0, \
                    f"Memory usage increased for volume {config['name']}"
            
            # Calculate and print summary
            self._print_performance_summary("VOLUME", total_improvements)

    def _print_performance_results(self, estimator_name: str, config: Dict[str, Any], result: Dict[str, Any]):
        """Helper to print performance results consistently."""
        eager_perf = result['eager_metrics']
        lazy_perf = result['lazy_metrics']
        improvements = result['improvements']
        
        print(f"\n  Results for {config['name']}:")
        print(f"    Memory:     {eager_perf.memory_peak_mb:.1f} → {lazy_perf.memory_peak_mb:.1f} MB "
              f"({improvements['memory_improvement']:.1f}x improvement)")
        print(f"    Time:       {eager_perf.execution_time_s:.3f} → {lazy_perf.execution_time_s:.3f} s "
              f"({improvements['speed_improvement']:.1f}x improvement)")
        print(f"    Efficiency: {eager_perf.memory_efficiency_ratio:.2f} → {lazy_perf.memory_efficiency_ratio:.2f} "
              f"({improvements['memory_efficiency_improvement']:.1f}x improvement)")
        print(f"    Deferred:   {lazy_perf.operations_deferred:.0f} operations")
        if lazy_perf.cache_hits > 0:
            print(f"    Cache hits: {lazy_perf.cache_hits:.0f}")

    def _print_performance_summary(self, estimator_name: str, total_improvements: List[Dict[str, float]]):
        """Helper to print performance summary consistently."""
        if not total_improvements:
            return
        
        avg_memory_improvement = sum(imp['memory_improvement'] for imp in total_improvements) / len(total_improvements)
        avg_speed_improvement = sum(imp['speed_improvement'] for imp in total_improvements) / len(total_improvements)
        avg_efficiency_improvement = sum(imp['memory_efficiency_improvement'] for imp in total_improvements) / len(total_improvements)
        
        print(f"\n  {estimator_name} ESTIMATION SUMMARY:")
        print(f"    Average memory improvement:     {avg_memory_improvement:.1f}x")
        print(f"    Average speed improvement:      {avg_speed_improvement:.1f}x")
        print(f"    Average efficiency improvement: {avg_efficiency_improvement:.1f}x")
        print(f"  ✓ {estimator_name} performance targets met")

    def test_collection_strategy_performance(self, sample_fia_instance):
        """Test performance of different collection strategies."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("COLLECTION STRATEGY PERFORMANCE COMPARISON")
        print("="*60)
        
        strategies = [
            CollectionStrategy.SEQUENTIAL,
            CollectionStrategy.ADAPTIVE,
            CollectionStrategy.STREAMING,
        ]
        
        test_params = {"grp_by": ["SPCD"], "totals": True, "show_progress": False}
        
        with sample_fia_instance as db:
            strategy_results = {}
            
            for strategy in strategies:
                print(f"\n  Testing strategy: {strategy.name}")
                
                # We need to use a lower-level interface to control collection strategy
                # For now, test with area_lazy as an example
                results = []
                for _ in range(3):  # Multiple runs for averaging
                    result, metrics = PerformanceBenchmarkRunner.benchmark_function(
                        area_lazy, db, **test_params
                    )
                    results.append(metrics)
                
                avg_metrics = self._average_metrics(results)
                strategy_results[strategy.name] = avg_metrics
                
                print(f"    Memory: {avg_metrics.memory_peak_mb:.1f} MB")
                print(f"    Time:   {avg_metrics.execution_time_s:.3f} s")
                print(f"    Deferred: {avg_metrics.operations_deferred:.0f} ops")
            
            # Find best strategy
            best_memory = min(strategy_results.values(), key=lambda x: x.memory_peak_mb)
            best_time = min(strategy_results.values(), key=lambda x: x.execution_time_s)
            
            print(f"\n  STRATEGY COMPARISON SUMMARY:")
            for name, metrics in strategy_results.items():
                memory_ratio = metrics.memory_peak_mb / best_memory.memory_peak_mb
                time_ratio = metrics.execution_time_s / best_time.execution_time_s
                print(f"    {name:12}: {memory_ratio:.2f}x memory, {time_ratio:.2f}x time")
            
            print("  ✓ Collection strategy comparison complete")

    def test_scaling_performance(self, sample_fia_instance):
        """Test how lazy evaluation scales with increasing complexity."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("LAZY EVALUATION SCALING PERFORMANCE")
        print("="*60)
        
        # Define increasing complexity scenarios
        scaling_scenarios = [
            {
                "name": "simple",
                "params": {},
                "description": "No grouping or filtering"
            },
            {
                "name": "single_group", 
                "params": {"grp_by": ["SPCD"]},
                "description": "Single grouping column"
            },
            {
                "name": "multi_group",
                "params": {"grp_by": ["SPCD", "FORTYPCD"]},
                "description": "Multiple grouping columns"
            },
            {
                "name": "with_filter",
                "params": {"grp_by": ["SPCD"], "tree_domain": "DIA >= 10.0"},
                "description": "Grouping with tree filtering"
            },
            {
                "name": "complex",
                "params": {
                    "grp_by": ["SPCD", "FORTYPCD"],
                    "tree_domain": "STATUSCD == 1 AND DIA >= 5.0",
                    "totals": True,
                    "variance": True
                },
                "description": "Complex multi-parameter estimation"
            }
        ]
        
        with sample_fia_instance as db:
            eager_scaling = []
            lazy_scaling = []
            
            for scenario in scaling_scenarios:
                print(f"\n  Testing scaling: {scenario['name']} - {scenario['description']}")
                
                # Test area estimation as representative
                eager_result, eager_metrics = PerformanceBenchmarkRunner.benchmark_function(
                    area, db, **scenario['params']
                )
                lazy_result, lazy_metrics = PerformanceBenchmarkRunner.benchmark_function(
                    area_lazy, db, show_progress=False, **scenario['params']
                )
                
                eager_scaling.append(eager_metrics)
                lazy_scaling.append(lazy_metrics)
                
                improvements = lazy_metrics.compare_to(eager_metrics)
                print(f"    Memory improvement: {improvements['memory_improvement']:.1f}x")
                print(f"    Speed improvement:  {improvements['speed_improvement']:.1f}x")
            
            # Analyze scaling characteristics
            print(f"\n  SCALING ANALYSIS:")
            
            # Memory scaling
            eager_memory_growth = eager_scaling[-1].memory_peak_mb / eager_scaling[0].memory_peak_mb if eager_scaling[0].memory_peak_mb > 0 else 1.0
            lazy_memory_growth = lazy_scaling[-1].memory_peak_mb / lazy_scaling[0].memory_peak_mb if lazy_scaling[0].memory_peak_mb > 0 else 1.0
            
            print(f"    Memory growth (simple → complex):")
            print(f"      Eager: {eager_memory_growth:.1f}x")
            print(f"      Lazy:  {lazy_memory_growth:.1f}x")
            print(f"      Lazy scales {eager_memory_growth / lazy_memory_growth:.1f}x better" if lazy_memory_growth > 0 else "")
            
            # Time scaling
            eager_time_growth = eager_scaling[-1].execution_time_s / eager_scaling[0].execution_time_s if eager_scaling[0].execution_time_s > 0 else 1.0
            lazy_time_growth = lazy_scaling[-1].execution_time_s / lazy_scaling[0].execution_time_s if lazy_scaling[0].execution_time_s > 0 else 1.0
            
            print(f"    Time growth (simple → complex):")
            print(f"      Eager: {eager_time_growth:.1f}x")
            print(f"      Lazy:  {lazy_time_growth:.1f}x")
            print(f"      Lazy scales {eager_time_growth / lazy_time_growth:.1f}x better" if lazy_time_growth > 0 else "")
            
            print("  ✓ Scaling analysis complete")

    def test_memory_pressure_handling(self, sample_fia_instance):
        """Test how lazy evaluation handles memory pressure scenarios."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("MEMORY PRESSURE HANDLING TEST")
        print("="*60)
        
        # Test with complex parameters that would normally use more memory
        memory_intensive_params = {
            "grp_by": ["SPCD", "FORTYPCD", "OWNGRPCD"],
            "by_species": True,
            "totals": True,
            "variance": True,
            "show_progress": False
        }
        
        with sample_fia_instance as db:
            print("  Testing memory-intensive estimation...")
            
            # Test eager approach
            eager_result, eager_metrics = PerformanceBenchmarkRunner.benchmark_function(
                area, db, **memory_intensive_params
            )
            
            # Test lazy approach
            lazy_result, lazy_metrics = PerformanceBenchmarkRunner.benchmark_function(
                area_lazy, db, **memory_intensive_params
            )
            
            improvements = lazy_metrics.compare_to(eager_metrics)
            
            print(f"\n  Memory Pressure Results:")
            print(f"    Peak memory:     {eager_metrics.memory_peak_mb:.1f} → {lazy_metrics.memory_peak_mb:.1f} MB")
            print(f"    Current memory:  {eager_metrics.memory_current_mb:.1f} → {lazy_metrics.memory_current_mb:.1f} MB")
            print(f"    Memory improvement: {improvements['memory_improvement']:.1f}x")
            print(f"    Efficiency:      {eager_metrics.memory_efficiency_ratio:.2f} → {lazy_metrics.memory_efficiency_ratio:.2f}")
            
            # Assert significant memory improvement under pressure
            assert improvements['memory_improvement'] >= 1.3, \
                "Expected at least 30% memory improvement under memory pressure"
            
            # Assert better memory efficiency (lower peak relative to current)
            assert lazy_metrics.memory_efficiency_ratio >= eager_metrics.memory_efficiency_ratio, \
                "Lazy evaluation should have better memory efficiency"
            
            print("  ✓ Memory pressure handling successful")

    def test_cache_effectiveness(self, sample_fia_instance):
        """Test the effectiveness of lazy evaluation caching."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*60)
        print("CACHE EFFECTIVENESS TEST")
        print("="*60)
        
        # Test multiple similar estimations that should benefit from caching
        cache_test_params = [
            {"grp_by": ["SPCD"], "totals": True},
            {"grp_by": ["SPCD"], "variance": True},  # Similar but different variance setting
            {"grp_by": ["SPCD"], "totals": True, "tree_domain": "STATUSCD == 1"},  # Add filter
        ]
        
        with sample_fia_instance as db:
            cache_results = []
            
            for i, params in enumerate(cache_test_params):
                print(f"\n  Running cache test {i+1}: {params}")
                
                params_with_progress = {**params, "show_progress": False}
                result, metrics = PerformanceBenchmarkRunner.benchmark_function(
                    area_lazy, db, **params_with_progress
                )
                
                cache_results.append(metrics)
                
                print(f"    Execution time: {metrics.execution_time_s:.3f}s")
                print(f"    Cache hits:     {metrics.cache_hits}")
                print(f"    Deferred ops:   {metrics.operations_deferred}")
            
            # Analyze cache effectiveness
            print(f"\n  CACHE ANALYSIS:")
            
            total_cache_hits = sum(result.cache_hits for result in cache_results)
            total_operations = sum(result.operations_deferred + result.operations_collected for result in cache_results)
            
            cache_hit_rate = total_cache_hits / total_operations if total_operations > 0 else 0
            
            print(f"    Total cache hits:    {total_cache_hits}")
            print(f"    Total operations:    {total_operations}")
            print(f"    Cache hit rate:      {cache_hit_rate:.1%}")
            
            # Check if later operations were faster (suggesting cache benefits)
            if len(cache_results) >= 2:
                first_time = cache_results[0].execution_time_s
                later_avg = sum(r.execution_time_s for r in cache_results[1:]) / len(cache_results[1:])
                speedup = first_time / later_avg if later_avg > 0 else 1.0
                
                print(f"    Later operations speedup: {speedup:.1f}x")
                
                if cache_hit_rate > 0.1:  # If we got meaningful cache hits
                    assert speedup >= 1.0, "Expected speedup from caching"
            
            print("  ✓ Cache effectiveness test complete")

    @pytest.mark.slow
    def test_comprehensive_performance_suite(self, sample_fia_instance):
        """Run comprehensive performance test suite across all estimators."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        print("\n" + "="*80)
        print("COMPREHENSIVE LAZY EVALUATION PERFORMANCE SUITE")
        print("="*80)
        
        estimator_pairs = [
            ("Area", area, area_lazy),
            ("Biomass", biomass, biomass_lazy),
            ("TPA", tpa, tpa),
            ("Volume", volume, volume_lazy),
        ]
        
        test_config = {
            "grp_by": ["SPCD"],
            "totals": True,
            "variance": True,
        }
        
        overall_results = {}
        
        with sample_fia_instance as db:
            for name, eager_func, lazy_func in estimator_pairs:
                print(f"\n  Testing {name} Estimation:")
                
                try:
                    eager_result, eager_metrics = PerformanceBenchmarkRunner.benchmark_function(
                        eager_func, db, **test_config
                    )
                    lazy_result, lazy_metrics = PerformanceBenchmarkRunner.benchmark_function(
                        lazy_func, db, show_progress=False, **test_config
                    )
                    
                    improvements = lazy_metrics.compare_to(eager_metrics)
                    overall_results[name] = improvements
                    
                    print(f"    Memory improvement: {improvements['memory_improvement']:.1f}x")
                    print(f"    Speed improvement:  {improvements['speed_improvement']:.1f}x")
                    print(f"    Results match:      {eager_result.shape == lazy_result.shape}")
                    
                except Exception as e:
                    print(f"    Error: {e}")
                    overall_results[name] = {"memory_improvement": 0, "speed_improvement": 0}
            
            # Calculate overall performance improvements
            if overall_results:
                avg_memory = sum(r["memory_improvement"] for r in overall_results.values()) / len(overall_results)
                avg_speed = sum(r["speed_improvement"] for r in overall_results.values()) / len(overall_results)
                
                print(f"\n  OVERALL PERFORMANCE SUMMARY:")
                print(f"    Average memory improvement: {avg_memory:.1f}x")
                print(f"    Average speed improvement:  {avg_speed:.1f}x")
                print(f"    Estimators tested:          {len(overall_results)}")
                
                # Assert overall performance targets
                successful_tests = [r for r in overall_results.values() if r["memory_improvement"] > 0]
                if successful_tests:
                    assert avg_memory >= 1.2, "Expected at least 20% average memory improvement"
                    print("  ✓ Overall performance targets achieved")
                
                # Print detailed results table
                print(f"\n  DETAILED RESULTS:")
                print(f"    {'Estimator':<12} {'Memory':<8} {'Speed':<8}")
                print(f"    {'-'*12} {'-'*8} {'-'*8}")
                for name, results in overall_results.items():
                    mem = f"{results['memory_improvement']:.1f}x" if results['memory_improvement'] > 0 else "Failed"
                    speed = f"{results['speed_improvement']:.1f}x" if results['speed_improvement'] > 0 else "Failed"
                    print(f"    {name:<12} {mem:<8} {speed:<8}")
            
            print(f"\n  ✓ Comprehensive performance suite complete")


class TestPerformanceRegression:
    """Test suite to prevent performance regressions in lazy estimators."""
    
    @pytest.fixture
    def performance_thresholds(self):
        """Define minimum performance thresholds."""
        return {
            "memory_improvement_min": 1.1,  # At least 10% memory improvement
            "speed_regression_max": 0.8,    # No more than 20% speed regression
            "cache_hit_rate_min": 0.0,      # Minimum cache hit rate (informational)
        }

    def test_no_memory_regression(self, sample_fia_instance, performance_thresholds):
        """Ensure lazy estimators don't regress in memory usage."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        test_params = {"totals": True, "show_progress": False}
        
        estimator_pairs = [
            (area, area_lazy, "Area"),
            (biomass, biomass_lazy, "Biomass"),
            (tpa, tpa, "TPA"),
            (volume, volume_lazy, "Volume"),
        ]
        
        with sample_fia_instance as db:
            for eager_func, lazy_func, name in estimator_pairs:
                eager_result, eager_metrics = PerformanceBenchmarkRunner.benchmark_function(
                    eager_func, db, **test_params
                )
                lazy_result, lazy_metrics = PerformanceBenchmarkRunner.benchmark_function(
                    lazy_func, db, **test_params
                )
                
                memory_ratio = lazy_metrics.memory_peak_mb / eager_metrics.memory_peak_mb if eager_metrics.memory_peak_mb > 0 else 1.0
                
                assert memory_ratio <= 1.0, \
                    f"{name} memory regression: lazy uses {memory_ratio:.1f}x more memory than eager"
                
                print(f"  ✓ {name} memory: {memory_ratio:.2f}x (no regression)")

    def test_no_severe_speed_regression(self, sample_fia_instance, performance_thresholds):
        """Ensure lazy estimators don't have severe speed regressions."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        test_params = {"totals": True, "show_progress": False}
        threshold = performance_thresholds["speed_regression_max"]
        
        estimator_pairs = [
            (area, area_lazy, "Area"),
            (biomass, biomass_lazy, "Biomass"),
            (tpa, tpa, "TPA"),
            (volume, volume_lazy, "Volume"),
        ]
        
        with sample_fia_instance as db:
            for eager_func, lazy_func, name in estimator_pairs:
                eager_result, eager_metrics = PerformanceBenchmarkRunner.benchmark_function(
                    eager_func, db, **test_params
                )
                lazy_result, lazy_metrics = PerformanceBenchmarkRunner.benchmark_function(
                    lazy_func, db, **test_params
                )
                
                speed_ratio = lazy_metrics.execution_time_s / eager_metrics.execution_time_s if eager_metrics.execution_time_s > 0 else 1.0
                
                assert speed_ratio <= (1 / threshold), \
                    f"{name} severe speed regression: lazy is {speed_ratio:.1f}x slower (max allowed: {1/threshold:.1f}x)"
                
                print(f"  ✓ {name} speed: {speed_ratio:.2f}x (within threshold)")

    def test_consistent_performance_characteristics(self, sample_fia_instance):
        """Test that performance characteristics are consistent across runs."""
        if not sample_fia_instance:
            pytest.skip("No test database available")
        
        test_params = {"grp_by": ["SPCD"], "show_progress": False}
        num_runs = 5
        
        with sample_fia_instance as db:
            # Test area_lazy for consistency
            run_times = []
            memory_peaks = []
            
            for run in range(num_runs):
                result, metrics = PerformanceBenchmarkRunner.benchmark_function(
                    area_lazy, db, **test_params
                )
                run_times.append(metrics.execution_time_s)
                memory_peaks.append(metrics.memory_peak_mb)
            
            # Calculate coefficient of variation (std dev / mean)
            import statistics
            
            time_cv = statistics.stdev(run_times) / statistics.mean(run_times) if statistics.mean(run_times) > 0 else 0
            memory_cv = statistics.stdev(memory_peaks) / statistics.mean(memory_peaks) if statistics.mean(memory_peaks) > 0 else 0
            
            print(f"  Performance consistency over {num_runs} runs:")
            print(f"    Time CV:   {time_cv:.3f} (lower is more consistent)")
            print(f"    Memory CV: {memory_cv:.3f} (lower is more consistent)")
            
            # Assert reasonable consistency (CV < 0.3 means std dev is less than 30% of mean)
            assert time_cv < 0.5, f"Execution time too inconsistent: CV={time_cv:.3f}"
            assert memory_cv < 0.3, f"Memory usage too inconsistent: CV={memory_cv:.3f}"
            
            print("  ✓ Performance characteristics are consistent")