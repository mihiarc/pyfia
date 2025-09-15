"""
Performance tests for area estimation variance calculation (PR #35).

This module tests the performance characteristics of the new variance calculation
implementation to ensure it scales appropriately and doesn't introduce
performance regressions.

Performance areas tested:
- Memory usage with plot-condition data storage
- Computation time scaling with dataset size
- Efficiency of domain indicator approach vs. filtering
- Variance calculation complexity with multiple groups/strata
- Real-world performance with large FIA databases
"""

import time
import pytest
import polars as pl
import numpy as np
from pathlib import Path
from unittest.mock import Mock

from pyfia.estimation.estimators.area import AreaEstimator
from pyfia.estimation import area
from pyfia.core import FIA


class TestMemoryUsage:
    """Test memory usage characteristics of new variance calculation."""

    def test_plot_condition_data_storage_scaling(self, mock_fia_database):
        """Test memory usage scaling with plot-condition data storage."""
        # Test different dataset sizes
        dataset_sizes = [100, 1000, 5000]
        memory_usage = []

        for n_plots in dataset_sizes:
            # Create synthetic dataset
            test_data = pl.DataFrame({
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "CONDID": [1] * n_plots,
                "AREA_VALUE": np.random.uniform(0, 1, n_plots),
                "ADJ_FACTOR_AREA": [1.0] * n_plots,
                "EXPNS": [1000.0] * n_plots,
                "ESTN_UNIT": np.random.randint(1, 10, n_plots),
                "STRATUM_CN": [f"S{i % 20}" for i in range(n_plots)]
            })

            config = {}
            estimator = AreaEstimator(mock_fia_database, config)

            # Mock stratification data
            with patch('pyfia.estimation.estimators.area.AreaEstimator._get_stratification_data') as mock_strat:
                mock_strat.return_value = test_data.lazy()

                # Measure memory usage (approximate via data size)
                start_time = time.time()
                result = estimator.aggregate_results(test_data.lazy())
                end_time = time.time()

                # Check that plot-condition data was stored
                assert estimator.plot_condition_data is not None
                stored_size = len(estimator.plot_condition_data)
                memory_usage.append((n_plots, stored_size, end_time - start_time))

        # Memory usage should scale linearly with dataset size
        for i in range(1, len(memory_usage)):
            size_ratio = memory_usage[i][0] / memory_usage[i-1][0]
            stored_ratio = memory_usage[i][1] / memory_usage[i-1][1]
            # Stored data should scale proportionally
            assert 0.8 <= stored_ratio / size_ratio <= 1.2

    def test_memory_efficient_column_selection(self, mock_fia_database):
        """Test that column selection reduces memory usage."""
        # Create dataset with many columns
        n_plots = 1000
        base_data = {
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "CONDID": [1] * n_plots,
            "AREA_VALUE": np.random.uniform(0, 1, n_plots),
            "ADJ_FACTOR_AREA": [1.0] * n_plots,
            "EXPNS": [1000.0] * n_plots,
        }

        # Add many extra columns that shouldn't be needed
        extra_columns = {f"EXTRA_COL_{i}": np.random.randn(n_plots) for i in range(50)}
        all_data = {**base_data, **extra_columns}

        test_data = pl.DataFrame(all_data)

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        with patch('pyfia.estimation.estimators.area.AreaEstimator._get_stratification_data') as mock_strat:
            mock_strat.return_value = test_data.lazy()

            result = estimator.aggregate_results(test_data.lazy())

            # Stored data should only contain essential columns
            stored_cols = set(estimator.plot_condition_data.columns)
            essential_cols = {"PLT_CN", "AREA_VALUE", "ADJ_FACTOR_AREA", "EXPNS"}

            # Should contain essential columns
            assert essential_cols.issubset(stored_cols)

            # Should not contain many extra columns
            extra_cols_stored = [col for col in stored_cols if col.startswith("EXTRA_COL_")]
            assert len(extra_cols_stored) == 0  # No extra columns should be stored

    def test_lazy_evaluation_memory_efficiency(self, mock_fia_database):
        """Test that lazy evaluation is used efficiently."""
        # Create large dataset
        n_plots = 10000
        test_data = pl.DataFrame({
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "CONDID": [1] * n_plots,
            "COND_STATUS_CD": np.random.choice([1, 2, 3], n_plots),
            "CONDPROP_UNADJ": np.random.uniform(0.1, 1.0, n_plots),
            "PROP_BASIS": np.random.choice(["SUBP", "MACR"], n_plots)
        })

        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        # apply_filters should use lazy evaluation internally where possible
        start_time = time.time()
        result = estimator.apply_filters(test_data.lazy())
        end_time = time.time()

        # Should complete reasonably quickly
        processing_time = end_time - start_time
        assert processing_time < 5.0  # Should not take more than 5 seconds

        # Result should be a LazyFrame initially
        assert isinstance(result, pl.LazyFrame)


class TestComputationScaling:
    """Test computation time scaling with dataset characteristics."""

    def test_variance_calculation_scaling_with_plots(self, mock_fia_database):
        """Test variance calculation scaling with number of plots."""
        plot_counts = [100, 500, 1000, 2000]
        computation_times = []

        for n_plots in plot_counts:
            plot_data = pl.DataFrame({
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "ESTN_UNIT": np.random.randint(1, 5, n_plots),
                "STRATUM": np.random.randint(1, 10, n_plots),
                "y_i": np.random.uniform(0, 1, n_plots),
                "EXPNS": [1000.0] * n_plots
            })

            config = {}
            estimator = AreaEstimator(mock_fia_database, config)

            start_time = time.time()
            var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])
            end_time = time.time()

            computation_times.append((n_plots, end_time - start_time))

        # Computation time should scale reasonably (not exponentially)
        for i in range(1, len(computation_times)):
            size_ratio = computation_times[i][0] / computation_times[i-1][0]
            time_ratio = computation_times[i][1] / computation_times[i-1][1]
            # Time should not increase faster than O(n log n)
            assert time_ratio <= size_ratio * np.log2(size_ratio) + 1

    def test_variance_calculation_scaling_with_strata(self, mock_fia_database):
        """Test variance calculation scaling with number of strata."""
        strata_counts = [1, 5, 10, 20, 50]
        computation_times = []

        n_plots = 1000
        for n_strata in strata_counts:
            plot_data = pl.DataFrame({
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "ESTN_UNIT": np.random.randint(1, max(1, n_strata // 5), n_plots),
                "STRATUM": np.random.randint(1, n_strata + 1, n_plots),
                "y_i": np.random.uniform(0, 1, n_plots),
                "EXPNS": [1000.0] * n_plots
            })

            config = {}
            estimator = AreaEstimator(mock_fia_database, config)

            start_time = time.time()
            var_stats = estimator._calculate_variance_for_group(plot_data, ["ESTN_UNIT", "STRATUM"])
            end_time = time.time()

            computation_times.append((n_strata, end_time - start_time))

        # Should scale reasonably with number of strata
        for n_strata, comp_time in computation_times:
            # Even with many strata, should complete quickly
            assert comp_time < 2.0  # Should not take more than 2 seconds

    def test_grouping_complexity_scaling(self, mock_fia_database):
        """Test scaling with complex grouping scenarios."""
        # Test different grouping complexities
        grouping_scenarios = [
            [],  # No grouping
            ["FORTYPCD"],  # Single variable
            ["FORTYPCD", "OWNGRPCD"],  # Two variables
            ["FORTYPCD", "OWNGRPCD", "STDSZCD"],  # Three variables
        ]

        n_plots = 1000
        for group_vars in grouping_scenarios:
            # Create data with grouping variables
            plot_data = {
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "AREA_VALUE": np.random.uniform(0, 1, n_plots),
                "ADJ_FACTOR_AREA": [1.0] * n_plots,
                "EXPNS": [1000.0] * n_plots,
                "ESTN_UNIT": np.random.randint(1, 5, n_plots),
                "STRATUM": np.random.randint(1, 10, n_plots)
            }

            # Add grouping columns
            if "FORTYPCD" in group_vars:
                plot_data["FORTYPCD"] = np.random.choice([161, 406, 703], n_plots)
            if "OWNGRPCD" in group_vars:
                plot_data["OWNGRPCD"] = np.random.choice([10, 20, 30, 40], n_plots)
            if "STDSZCD" in group_vars:
                plot_data["STDSZCD"] = np.random.choice([1, 2, 3, 4, 5], n_plots)

            config = {"grp_by": group_vars} if group_vars else {}
            estimator = AreaEstimator(mock_fia_database, config)

            estimator.plot_condition_data = pl.DataFrame(plot_data)
            estimator.group_cols = group_vars

            # Create results with appropriate grouping
            if group_vars:
                unique_groups = estimator.plot_condition_data.select(group_vars).unique()
                results = unique_groups.with_columns([
                    pl.lit(1000.0).alias("AREA_TOTAL"),
                    pl.lit(10).alias("N_PLOTS")
                ])
            else:
                results = pl.DataFrame({
                    "AREA_TOTAL": [10000.0],
                    "N_PLOTS": [n_plots]
                })

            start_time = time.time()
            variance_results = estimator.calculate_variance(results)
            end_time = time.time()

            computation_time = end_time - start_time
            n_groups = len(results)

            # Even complex grouping should complete quickly
            assert computation_time < 10.0  # Should not take more than 10 seconds
            assert len(variance_results) == n_groups


class TestDomainIndicatorEfficiency:
    """Test efficiency of domain indicator approach vs. traditional filtering."""

    def test_domain_indicator_vs_filtering_performance(self, mock_fia_database):
        """Compare performance of domain indicator vs. filtering approaches."""
        n_plots = 5000
        test_data = pl.DataFrame({
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "CONDID": [1] * n_plots,
            "COND_STATUS_CD": np.random.choice([1, 2, 3], n_plots, p=[0.7, 0.2, 0.1]),
            "CONDPROP_UNADJ": np.random.uniform(0.1, 1.0, n_plots),
            "PROP_BASIS": ["SUBP"] * n_plots
        })

        config = {"land_type": "forest"}

        # Test domain indicator approach (current implementation)
        estimator_domain = AreaEstimator(mock_fia_database, config)

        start_time = time.time()
        result_domain = estimator_domain.apply_filters(test_data.lazy())
        domain_data = estimator_domain.calculate_values(result_domain).collect()
        domain_time = time.time() - start_time

        # Test traditional filtering approach (for comparison)
        start_time = time.time()
        filtered_data = test_data.filter(pl.col("COND_STATUS_CD") == 1)
        filter_time = time.time() - start_time

        # Domain indicator approach should retain all plots
        assert len(domain_data) == n_plots

        # Traditional filtering reduces dataset
        assert len(filtered_data) < n_plots

        # Domain indicator may be slightly slower but should be reasonable
        # (acceptable trade-off for statistical correctness)
        assert domain_time < filter_time * 5  # Should not be more than 5x slower

    def test_memory_usage_domain_vs_filtering(self, mock_fia_database):
        """Compare memory usage of domain indicator vs. filtering."""
        n_plots = 10000
        test_data = pl.DataFrame({
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "CONDID": [1] * n_plots,
            "COND_STATUS_CD": np.random.choice([1, 2], n_plots, p=[0.6, 0.4]),  # 60% forest
            "CONDPROP_UNADJ": np.random.uniform(0.1, 1.0, n_plots),
            "PROP_BASIS": ["SUBP"] * n_plots
        })

        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        # Domain indicator approach
        result_domain = estimator.apply_filters(test_data.lazy())
        domain_final = estimator.calculate_values(result_domain).collect()

        # Memory usage is proportional to number of rows retained
        domain_rows = len(domain_final)
        expected_filtered_rows = len(test_data.filter(pl.col("COND_STATUS_CD") == 1))

        # Domain indicator retains all rows
        assert domain_rows == n_plots

        # But adds domain indicator column for statistical correctness
        assert "DOMAIN_IND" in domain_final.columns


class TestRealWorldPerformance:
    """Test performance with real FIA databases."""

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_real_database_performance_single_state(self):
        """Test performance with real FIA data for single state."""
        with FIA("fia.duckdb") as db:
            db.clip_by_state(45, most_recent=True)  # South Carolina

            # Test basic area estimation performance
            start_time = time.time()
            result = area(db, land_type="forest", totals=True)
            end_time = time.time()

            computation_time = end_time - start_time
            n_plots = result["N_PLOTS"][0]

            print(f"Single state performance: {computation_time:.2f}s for {n_plots:,} plots")

            # Should complete within reasonable time
            assert computation_time < 30.0  # Should not take more than 30 seconds
            assert n_plots > 0

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_real_database_performance_complex_grouping(self):
        """Test performance with complex grouping on real data."""
        with FIA("fia.duckdb") as db:
            db.clip_by_state(13, most_recent=True)  # Georgia

            # Test complex grouping performance
            start_time = time.time()
            result = area(
                db,
                grp_by=["FORTYPCD", "OWNGRPCD"],
                land_type="timber",
                totals=True
            )
            end_time = time.time()

            computation_time = end_time - start_time
            n_groups = len(result)
            total_plots = result["N_PLOTS"].sum()

            print(f"Complex grouping performance: {computation_time:.2f}s for {n_groups} groups, {total_plots:,} total plots")

            # Should complete within reasonable time even with grouping
            assert computation_time < 60.0  # Should not take more than 1 minute
            assert n_groups > 1
            assert total_plots > 0

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_real_database_variance_calculation_performance(self):
        """Test variance calculation performance on real data."""
        with FIA("fia.duckdb") as db:
            db.clip_by_evalid([132301])  # Georgia specific EVALID

            # Measure just the variance calculation portion
            # First get the area estimate
            result = area(db, land_type="timber", totals=True)

            # Check that variance was calculated
            assert "AREA_SE" in result.columns
            assert result["AREA_SE"][0] > 0

            # Variance calculation should produce reasonable sampling error
            area_total = result["AREA_TOTAL"][0]
            area_se = result["AREA_SE"][0]
            se_percent = (area_se / area_total) * 100

            # Sampling error should be reasonable (0.1% to 5%)
            assert 0.1 <= se_percent <= 5.0

    @pytest.mark.skipif(
        not Path("fia.duckdb").exists(),
        reason="Real FIA database not available"
    )
    def test_memory_usage_real_data(self):
        """Test memory usage with real data."""
        with FIA("fia.duckdb") as db:
            db.clip_by_state([13, 45], most_recent=True)  # Georgia + South Carolina

            # Test with moderately large dataset
            start_time = time.time()
            result = area(
                db,
                grp_by="FORTYPCD",
                land_type="forest",
                totals=True
            )
            end_time = time.time()

            computation_time = end_time - start_time
            total_plots = result["N_PLOTS"].sum()
            n_groups = len(result)

            print(f"Multi-state performance: {computation_time:.2f}s for {total_plots:,} plots, {n_groups} groups")

            # Should handle multi-state data efficiently
            assert computation_time < 120.0  # Should not take more than 2 minutes
            assert total_plots > 1000  # Should have substantial data
            assert n_groups > 5  # Should have multiple forest types


class TestPerformanceRegression:
    """Test for performance regressions compared to baseline."""

    def test_variance_overhead_vs_no_variance(self, mock_fia_database):
        """Test overhead of variance calculation vs. no variance calculation."""
        n_plots = 2000
        test_data = pl.DataFrame({
            "PLT_CN": [f"P{i}" for i in range(n_plots)],
            "CONDID": [1] * n_plots,
            "COND_STATUS_CD": [1] * n_plots,
            "CONDPROP_UNADJ": np.random.uniform(0.1, 1.0, n_plots),
            "PROP_BASIS": ["SUBP"] * n_plots
        })

        # Mock successful execution path
        config = {"land_type": "forest"}
        estimator = AreaEstimator(mock_fia_database, config)

        # Test time for basic processing (without variance)
        start_time = time.time()
        result = estimator.apply_filters(test_data.lazy())
        calculated = estimator.calculate_values(result).collect()
        basic_time = time.time() - start_time

        # Simulate variance calculation overhead
        estimator.plot_condition_data = calculated.with_columns([
            pl.lit(1.0).alias("ADJ_FACTOR_AREA"),
            pl.lit(1000.0).alias("EXPNS"),
            pl.lit(1).alias("ESTN_UNIT"),
            pl.lit(1).alias("STRATUM")
        ])

        mock_results = pl.DataFrame({
            "AREA_TOTAL": [1000.0],
            "N_PLOTS": [n_plots]
        })

        start_time = time.time()
        variance_result = estimator.calculate_variance(mock_results)
        variance_time = time.time() - start_time

        total_time = basic_time + variance_time

        print(f"Performance breakdown: Basic={basic_time:.3f}s, Variance={variance_time:.3f}s, Total={total_time:.3f}s")

        # Variance calculation should not dominate total time
        assert variance_time <= basic_time * 3  # Variance overhead should be reasonable
        assert total_time < 5.0  # Total should complete quickly

    def test_scaling_efficiency_benchmarks(self, mock_fia_database):
        """Benchmark scaling efficiency for different dataset characteristics."""
        benchmark_results = {}

        # Test different scenarios
        scenarios = [
            ("small_simple", 500, 1, []),
            ("medium_simple", 2000, 5, []),
            ("large_simple", 5000, 10, []),
            ("medium_grouped", 2000, 5, ["FORTYPCD"]),
            ("medium_complex", 2000, 20, ["FORTYPCD", "OWNGRPCD"])
        ]

        for scenario_name, n_plots, n_strata, group_vars in scenarios:
            # Create scenario data
            plot_data = {
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "AREA_VALUE": np.random.uniform(0, 1, n_plots),
                "ADJ_FACTOR_AREA": [1.0] * n_plots,
                "EXPNS": [1000.0] * n_plots,
                "ESTN_UNIT": np.random.randint(1, max(1, n_strata // 2), n_plots),
                "STRATUM": np.random.randint(1, n_strata + 1, n_plots)
            }

            # Add grouping variables
            for group_var in group_vars:
                if group_var == "FORTYPCD":
                    plot_data[group_var] = np.random.choice([161, 406, 703], n_plots)
                elif group_var == "OWNGRPCD":
                    plot_data[group_var] = np.random.choice([10, 20, 30, 40], n_plots)

            config = {"grp_by": group_vars} if group_vars else {}
            estimator = AreaEstimator(mock_fia_database, config)
            estimator.plot_condition_data = pl.DataFrame(plot_data)
            estimator.group_cols = group_vars

            # Create appropriate results
            if group_vars:
                unique_groups = estimator.plot_condition_data.select(group_vars).unique()
                results = unique_groups.with_columns([
                    pl.lit(1000.0).alias("AREA_TOTAL"),
                    pl.lit(10).alias("N_PLOTS")
                ])
            else:
                results = pl.DataFrame({
                    "AREA_TOTAL": [10000.0],
                    "N_PLOTS": [n_plots]
                })

            # Benchmark the scenario
            start_time = time.time()
            variance_results = estimator.calculate_variance(results)
            end_time = time.time()

            benchmark_results[scenario_name] = {
                "time": end_time - start_time,
                "plots": n_plots,
                "strata": n_strata,
                "groups": len(results),
                "time_per_plot": (end_time - start_time) / n_plots
            }

        # Print benchmark results
        print("\nVariance Calculation Performance Benchmarks:")
        print("Scenario           Time(s)  Plots  Strata Groups  Time/Plot(ms)")
        print("-" * 65)
        for name, stats in benchmark_results.items():
            print(f"{name:15} {stats['time']:7.3f} {stats['plots']:6} "
                  f"{stats['strata']:6} {stats['groups']:6} "
                  f"{stats['time_per_plot']*1000:11.3f}")

        # Performance assertions
        for name, stats in benchmark_results.items():
            # All scenarios should complete within reasonable time
            assert stats["time"] < 10.0, f"{name} took too long: {stats['time']:.3f}s"

            # Time per plot should be reasonable
            assert stats["time_per_plot"] < 0.01, f"{name} too slow per plot: {stats['time_per_plot']*1000:.3f}ms"