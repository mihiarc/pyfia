# Phase 3 Performance Benchmark Report

## Executive Summary

This report presents actual performance measurements for pyFIA Phase 3 optimizations.
Benchmarks were run on 2025-08-24 16:47:37 using:
- Database type: Synthetic test data
- Python version: 3.12.10 (main, Apr  9 2025, 03:49:38) [Clang 20.1.0 ]
- Total benchmarks: 8

## Key Findings

- **optimized_join_vs_direct**: 1.1x faster (11.6% improvement)

## Detailed Results

### direct_tree_query

- **Execution Time**: 0.0035s ± 0.0009s
- **Memory Usage**: 0.0 MB (peak: 0.1 MB)
- **Success Rate**: 100.0%
- **Iterations**: 5

### direct_tree_plot_join

- **Execution Time**: 0.0059s ± 0.0001s
- **Memory Usage**: 0.0 MB (peak: 0.1 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

### optimized_join

- **Execution Time**: 0.0053s ± 0.0003s
- **Memory Usage**: 0.0 MB (peak: 0.1 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

### post_join_filter

- **Execution Time**: 0.0056s ± 0.0003s
- **Memory Usage**: 0.0 MB (peak: 0.1 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

### pre_join_filter

- **Execution Time**: 0.0052s ± 0.0007s
- **Memory Usage**: 0.0 MB (peak: 0.1 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

### naive_multi_join

- **Execution Time**: 0.0069s ± 0.0007s
- **Memory Usage**: 0.0 MB (peak: 0.1 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

### optimized_multi_join

- **Execution Time**: 0.0070s ± 0.0005s
- **Memory Usage**: 0.0 MB (peak: 0.2 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

### volume_estimation_workflow

- **Execution Time**: 0.0062s ± 0.0008s
- **Memory Usage**: 0.0 MB (peak: 0.2 MB)
- **Success Rate**: 100.0%
- **Iterations**: 3

## Performance Comparisons

### optimized_join_vs_direct

- **Time Speedup**: 1.12x
- **Time Improvement**: 11.6%
- **Memory Change**: 5.4%
- **Baseline Time**: 0.0059s
- **Optimized Time**: 0.0053s

### filter_pushdown_improvement

- **Time Speedup**: 1.07x
- **Time Improvement**: 6.8%
- **Memory Change**: -4.7%
- **Baseline Time**: 0.0056s
- **Optimized Time**: 0.0052s

## Methodology

Each benchmark was run with:
- Multiple iterations with warmup runs
- Memory profiling using psutil and tracemalloc
- Garbage collection between tests
- Statistical analysis of timing variations

## Limitations

- Synthetic data may not reflect real-world query patterns
- Single-machine testing may not show distributed benefits
- Cache effects may influence sequential measurements

Report generated on 2025-08-24 16:47:37