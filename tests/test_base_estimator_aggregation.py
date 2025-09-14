"""
Tests for the shared two-stage aggregation method in BaseEstimator.

This module directly tests the _apply_two_stage_aggregation method
to ensure correct behavior across all scenarios.
"""

import pytest
import polars as pl
from pyfia.estimation.base import BaseEstimator
from pyfia.core import FIA


class MockFIA:
    """Mock FIA database for testing."""
    def __init__(self):
        self.evalid = None
        self.tables = {}


class ConcreteEstimator(BaseEstimator):
    """Concrete implementation for testing the abstract BaseEstimator."""

    def __init__(self, config):
        """Initialize without requiring a real database."""
        self.db = MockFIA()
        self.config = config
        self._owns_db = False
        self._ref_species_cache = None
        self._stratification_cache = None

    def get_required_tables(self):
        return ["TREE", "COND", "PLOT"]

    def get_tree_columns(self):
        return ["PLT_CN", "CONDID", "TPA_UNADJ", "DIA"]

    def get_cond_columns(self):
        return ["PLT_CN", "CONDID", "CONDPROP_UNADJ"]

    def calculate_values(self, data):
        return data

    def aggregate_results(self, data):
        return pl.DataFrame()

    def calculate_variance(self, results):
        return results

    def format_output(self, results):
        return results


class TestTwoStageAggregation:
    """Test the shared _apply_two_stage_aggregation method."""

    @pytest.fixture
    def estimator(self):
        """Create a concrete estimator for testing."""
        return ConcreteEstimator(config={})

    @pytest.fixture
    def sample_data(self):
        """Create sample data for testing two-stage aggregation."""
        return pl.DataFrame({
            "PLT_CN": [1, 1, 1, 2, 2, 2, 3, 3],
            "CONDID": [1, 1, 2, 1, 1, 2, 1, 1],
            "STRATUM_CN": [100, 100, 100, 200, 200, 200, 100, 100],
            "EXPNS": [1000.0, 1000.0, 1000.0, 2000.0, 2000.0, 2000.0, 1000.0, 1000.0],
            "CONDPROP_UNADJ": [0.6, 0.6, 0.4, 0.7, 0.7, 0.3, 1.0, 1.0],
            "METRIC_ADJ": [10.0, 15.0, 20.0, 5.0, 8.0, 12.0, 30.0, 25.0],
            "SPCD": [131, 131, 110, 131, 833, 110, 131, 131]  # Species codes
        }).lazy()

    def test_basic_aggregation(self, estimator, sample_data):
        """Test normal two-stage aggregation with multiple conditions."""
        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=sample_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Verify results structure
        assert "METRIC_ACRE" in results.columns
        assert "METRIC_TOTAL" in results.columns
        assert "N_PLOTS" in results.columns
        assert "N_TREES" in results.columns

        # Verify aggregation math
        # Stage 1: Condition totals
        # Plot 1, Cond 1: 10 + 15 = 25
        # Plot 1, Cond 2: 20
        # Plot 2, Cond 1: 5 + 8 = 13
        # Plot 2, Cond 2: 12
        # Plot 3, Cond 1: 30 + 25 = 55

        # Stage 2: Expansion
        # Numerator: 25*1000 + 20*1000 + 13*2000 + 12*2000 + 55*1000 = 150,000
        # Denominator: 0.6*1000 + 0.4*1000 + 0.7*2000 + 0.3*2000 + 1.0*1000 = 4,000
        # Per acre: 150,000 / 4,000 = 37.5

        assert results["METRIC_ACRE"][0] == pytest.approx(37.5, rel=1e-6)
        assert results["METRIC_TOTAL"][0] == pytest.approx(150000.0, rel=1e-6)
        assert results["N_PLOTS"][0] == 3
        assert results["N_TREES"][0] == 8

    def test_grouping_by_species(self, estimator, sample_data):
        """Test aggregation with grouping by species."""
        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=sample_data,
            metric_mappings=metric_mappings,
            group_cols=["SPCD"],
            use_grm_adjustment=False
        )

        # Should have 3 species groups
        assert len(results) == 3
        assert "SPCD" in results.columns

        # Verify each species has correct aggregation
        species_131 = results.filter(pl.col("SPCD") == 131)
        assert len(species_131) == 1
        assert species_131["N_TREES"][0] == 5  # 5 trees with SPCD=131

    def test_empty_data_handling(self, estimator):
        """Test handling of empty LazyFrame."""
        empty_data = pl.DataFrame({
            "PLT_CN": [],
            "CONDID": [],
            "STRATUM_CN": [],
            "EXPNS": [],
            "CONDPROP_UNADJ": [],
            "METRIC_ADJ": []
        }).lazy()

        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=empty_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Should return empty result with proper structure
        assert len(results) == 0
        assert "METRIC_ACRE" in results.columns
        assert "METRIC_TOTAL" in results.columns

    def test_single_condition_edge_case(self, estimator):
        """Test with only one condition across all plots."""
        single_condition_data = pl.DataFrame({
            "PLT_CN": [1, 2, 3],
            "CONDID": [1, 1, 1],
            "STRATUM_CN": [100, 100, 100],
            "EXPNS": [1000.0, 1000.0, 1000.0],
            "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
            "METRIC_ADJ": [10.0, 20.0, 30.0]
        }).lazy()

        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=single_condition_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Per acre: (10*1000 + 20*1000 + 30*1000) / (1*1000 + 1*1000 + 1*1000) = 20.0
        assert results["METRIC_ACRE"][0] == pytest.approx(20.0, rel=1e-6)
        assert results["N_PLOTS"][0] == 3

    def test_division_by_zero_protection(self, estimator):
        """Test protection against division by zero."""
        zero_area_data = pl.DataFrame({
            "PLT_CN": [1],
            "CONDID": [1],
            "STRATUM_CN": [100],
            "EXPNS": [1000.0],
            "CONDPROP_UNADJ": [0.0],  # Zero area
            "METRIC_ADJ": [10.0]
        }).lazy()

        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=zero_area_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Should return 0.0 instead of NaN/Inf
        assert results["METRIC_ACRE"][0] == 0.0

    def test_multiple_metrics(self, estimator, sample_data):
        """Test aggregation with multiple metrics simultaneously."""
        # Add second metric to data
        data_with_two_metrics = sample_data.with_columns([
            (pl.col("METRIC_ADJ") * 2.0).alias("METRIC2_ADJ")
        ])

        metric_mappings = {
            "METRIC_ADJ": "CONDITION_METRIC",
            "METRIC2_ADJ": "CONDITION_METRIC2"
        }

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=data_with_two_metrics,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Should have both metrics in results
        assert "METRIC_ACRE" in results.columns
        assert "METRIC2_ACRE" in results.columns
        assert "METRIC_TOTAL" in results.columns
        assert "METRIC2_TOTAL" in results.columns

        # Second metric should be 2x the first
        assert results["METRIC2_ACRE"][0] == pytest.approx(results["METRIC_ACRE"][0] * 2.0, rel=1e-6)

    def test_missing_required_columns(self, estimator):
        """Test error handling for missing required columns."""
        incomplete_data = pl.DataFrame({
            "PLT_CN": [1, 2],
            "CONDID": [1, 1],
            # Missing STRATUM_CN, EXPNS, CONDPROP_UNADJ
            "METRIC_ADJ": [10.0, 20.0]
        }).lazy()

        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        # Should handle missing columns gracefully
        with pytest.raises(Exception):  # Will raise when trying to access missing columns
            results = estimator._apply_two_stage_aggregation(
                data_with_strat=incomplete_data,
                metric_mappings=metric_mappings,
                group_cols=[],
                use_grm_adjustment=False
            )

    def test_grm_vs_standard_parameter(self, estimator, sample_data):
        """Test that use_grm_adjustment parameter is handled."""
        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        # Test with use_grm_adjustment=True
        results_grm = estimator._apply_two_stage_aggregation(
            data_with_strat=sample_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=True
        )

        # Test with use_grm_adjustment=False
        results_standard = estimator._apply_two_stage_aggregation(
            data_with_strat=sample_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Currently the parameter doesn't affect the calculation
        # but both should produce valid results
        assert "METRIC_ACRE" in results_grm.columns
        assert "METRIC_ACRE" in results_standard.columns

    def test_numerical_stability(self, estimator):
        """Test numerical stability with large expansion factors."""
        large_expns_data = pl.DataFrame({
            "PLT_CN": [1, 2],
            "CONDID": [1, 1],
            "STRATUM_CN": [100, 200],
            "EXPNS": [1e6, 1e6],  # Very large expansion factors
            "CONDPROP_UNADJ": [0.5, 0.5],
            "METRIC_ADJ": [1.0, 2.0]
        }).lazy()

        metric_mappings = {"METRIC_ADJ": "CONDITION_METRIC"}

        results = estimator._apply_two_stage_aggregation(
            data_with_strat=large_expns_data,
            metric_mappings=metric_mappings,
            group_cols=[],
            use_grm_adjustment=False
        )

        # Should handle large numbers without overflow
        assert results["METRIC_TOTAL"][0] == pytest.approx(3e6, rel=1e-6)
        assert results["METRIC_ACRE"][0] == pytest.approx(1.5, rel=1e-6)


class TestTwoStageAggregationEquivalence:
    """Test that refactored estimators produce identical results to original."""

    @pytest.mark.skip(reason="Requires baseline results from pre-refactoring code")
    def test_tpa_equivalence(self):
        """Verify TPA estimator produces identical results after refactoring."""
        # This test would compare against saved baseline results
        pass

    @pytest.mark.skip(reason="Requires baseline results from pre-refactoring code")
    def test_volume_equivalence(self):
        """Verify Volume estimator produces identical results after refactoring."""
        pass

    @pytest.mark.skip(reason="Requires baseline results from pre-refactoring code")
    def test_biomass_equivalence(self):
        """Verify Biomass estimator produces identical results after refactoring."""
        pass