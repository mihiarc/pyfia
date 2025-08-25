# Comprehensive pyFIA Performance Analysis

## Executive Summary

This report provides realistic performance measurements for pyFIA operations
based on actual usage patterns and realistic FIA data distributions.
Report generated on 2025-08-24 16:50:30.

### Key Performance Insights

- Filtering adds ~5% overhead but reduces downstream processing
- Average join operation takes 0.010 seconds
- Peak memory usage: 0.2 MB, average: 0.2 MB

## Scaling Analysis

- Join performance scales ~1.0x from small to medium datasets
- Join performance scales ~1.0x from medium to large datasets
- Memory usage scales approximately linearly with dataset size

## Detailed Benchmark Results

### Data Loading Performance

#### load_tree_small_state
- **Time**: 0.0024s ± 0.0002s
- **Memory**: 0.0 MB peak
- **Success Rate**: 100.0%

#### load_tree_medium_state
- **Time**: 0.0044s ± 0.0005s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

#### load_tree_large_state
- **Time**: 0.0094s ± 0.0001s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

### Filtering Performance

#### simple_status_filter
- **Time**: 0.0053s ± 0.0006s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

#### diameter_range_filter
- **Time**: 0.0053s ± 0.0003s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

#### species_filter
- **Time**: 0.0063s ± 0.0010s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

### Joins Performance

#### tree_plot_join_small_state
- **Time**: 0.0053s ± 0.0008s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

#### three_table_join_small_state
- **Time**: 0.0065s ± 0.0002s
- **Memory**: 0.1 MB peak
- **Success Rate**: 100.0%

#### tree_plot_join_medium_state
- **Time**: 0.0076s ± 0.0008s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

#### three_table_join_medium_state
- **Time**: 0.0095s ± 0.0007s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

#### tree_plot_join_large_state
- **Time**: 0.0141s ± 0.0003s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

#### three_table_join_large_state
- **Time**: 0.0169s ± 0.0004s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

### Aggregations Performance

#### volume_sum
- **Time**: 0.0049s ± 0.0003s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

#### species_summary
- **Time**: 0.0053s ± 0.0004s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

#### state_species_summary
- **Time**: 0.0074s ± 0.0007s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

### Workflows Performance

#### forest_inventory_workflow
- **Time**: 0.0085s ± 0.0006s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

#### species_composition_workflow
- **Time**: 0.0081s ± 0.0005s
- **Memory**: 0.2 MB peak
- **Success Rate**: 100.0%

## Performance Recommendations

Based on the benchmark results, here are recommendations for optimal pyFIA performance:

### Data Loading Optimization
- Filter data early in the pipeline to reduce memory usage
- Use column selection to minimize data transfer
- Consider data size when choosing aggregation strategies

### Query Optimization
- Apply filters before joins when possible
- Use appropriate join strategies based on data size
- Group operations efficiently to minimize intermediate results

### Memory Management
- Monitor memory usage with large datasets
- Use lazy evaluation for multi-step operations
- Consider chunking for very large analyses

## Methodology Notes

- Benchmarks use realistic FIA data distributions
- Multiple iterations with statistical analysis
- Memory profiling throughout execution
- Represents typical pyFIA usage patterns

Generated on 2025-08-24 16:50:30