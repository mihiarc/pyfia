"""
Comprehensive tests for variance formula verification following Bechtold & Patterson (2005).

This is a CRITICAL test module for statistical validity. All variance calculations
in pyFIA must adhere to the FIA design-based estimation methodology.

Reference: Bechtold, W.A., and Patterson, P.L., eds. 2005. The enhanced Forest
Inventory and Analysis program - national sampling design and estimation procedures.
Gen. Tech. Rep. SRS-80. Asheville, NC: U.S. Department of Agriculture, Forest Service,
Southern Research Station. 85 p.

The tests verify:
1. Stratified variance formula: V(Y) = sum_h(N_h^2 * s_h^2 / n_h)
2. Single stratum variance with known hand-calculated values
3. Multi-stratum variance aggregation
4. Edge cases (single plot per stratum)
5. Property tests for non-negativity
6. Standard error percentage calculation
"""

from unittest.mock import Mock

import numpy as np
import polars as pl
import pytest

from pyfia.estimation.base import AggregationResult
from pyfia.estimation.estimators.area import AreaEstimator
from pyfia.estimation.variance import (
    calculate_confidence_interval,
    calculate_cv,
    safe_divide,
    safe_sqrt,
)

# =============================================================================
# Test Fixtures for Variance Testing
# =============================================================================


@pytest.fixture
def mock_fia_database():
    """Create a mock FIA database object for testing."""
    db = Mock()
    db.evalid = [372301]
    db.statecd = [37]
    db.tables = {}
    db.load_table = Mock()
    db._reader = Mock()
    db._reader.conn = None
    return db


@pytest.fixture
def single_stratum_plot_data():
    """
    Create test data for single stratum variance calculation.

    Known values for hand calculation verification:
    - 4 plots with values [0.8, 1.0, 0.6, 0.9]
    - EXPNS = 1000 (expansion factor, represents acres per plot)
    - All plots in same stratum

    Expected calculations:
    - n_h = 4 (number of plots)
    - ybar_h = mean([0.8, 1.0, 0.6, 0.9]) = 0.825
    - s2_yh = var([0.8, 1.0, 0.6, 0.9], ddof=1) = 0.029166...
    - w_h = 1000 (EXPNS)

    For domain total estimation:
    V(Y_D) = w_h^2 * s2_yh * n_h
           = 1000^2 * 0.029166... * 4
           = 116,666.67
    """
    return pl.DataFrame(
        {
            "PLT_CN": ["P1", "P2", "P3", "P4"],
            "ESTN_UNIT": [1, 1, 1, 1],
            "STRATUM": [1, 1, 1, 1],
            "y_i": [0.8, 1.0, 0.6, 0.9],
            "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
        }
    )


@pytest.fixture
def multi_stratum_plot_data():
    """
    Create test data for multi-stratum variance calculation.

    Two strata with different characteristics:

    Stratum A (ESTN_UNIT=1, STRATUM=1):
    - 3 plots: y_i = [0.8, 1.0, 0.6], EXPNS = 1000
    - n_h = 3, ybar = 0.8, s2 = 0.04
    - V_h = 1000^2 * 0.04 * 3 = 120,000

    Stratum B (ESTN_UNIT=1, STRATUM=2):
    - 2 plots: y_i = [0.5, 0.9], EXPNS = 1500
    - n_h = 2, ybar = 0.7, s2 = 0.08
    - V_h = 1500^2 * 0.08 * 2 = 360,000

    Total variance = V_A + V_B = 120,000 + 360,000 = 480,000
    """
    return pl.DataFrame(
        {
            "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
            "ESTN_UNIT": [1, 1, 1, 1, 1],
            "STRATUM": [1, 1, 1, 2, 2],
            "y_i": [0.8, 1.0, 0.6, 0.5, 0.9],
            "EXPNS": [1000.0, 1000.0, 1000.0, 1500.0, 1500.0],
        }
    )


# =============================================================================
# TestStratifiedVarianceFormula
# =============================================================================


class TestStratifiedVarianceFormula:
    """
    Verify the stratified variance formula: V(Y) = sum_h(N_h^2 * s_h^2 / n_h)

    For domain total estimation in FIA, the formula is modified to:
    V(Y_D) = sum_h(w_h^2 * s2_yh * n_h)

    where:
    - w_h = EXPNS (expansion factor / weight per plot)
    - s2_yh = sample variance of response within stratum
    - n_h = number of sampled plots in stratum
    """

    def test_formula_components_exist(self, mock_fia_database):
        """Verify that all required components for variance calculation are available."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Verify the method exists
        assert hasattr(estimator, "_calculate_variance_for_group")
        assert callable(estimator._calculate_variance_for_group)

    def test_single_stratum_formula_exact(
        self, mock_fia_database, single_stratum_plot_data
    ):
        """
        Test exact formula calculation with known values.

        Formula: V(Y_D) = w_h^2 * s2_yh * n_h

        With values [0.8, 1.0, 0.6, 0.9], EXPNS=1000, n=4:
        - s2 = var([0.8, 1.0, 0.6, 0.9], ddof=1)
        - V = 1000^2 * s2 * 4
        """
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        strat_cols = ["ESTN_UNIT", "STRATUM"]
        var_stats = estimator._calculate_variance_for_group(
            single_stratum_plot_data, strat_cols
        )

        # Calculate expected variance manually
        y_values = [0.8, 1.0, 0.6, 0.9]
        n_h = 4
        w_h = 1000.0
        s2_yh = np.var(y_values, ddof=1)  # Sample variance with Bessel's correction
        expected_variance = (w_h**2) * s2_yh * n_h

        # Verify the result
        assert var_stats is not None
        assert "variance" in var_stats
        assert abs(var_stats["variance"] - expected_variance) < 1e-6, (
            f"Expected variance {expected_variance}, got {var_stats['variance']}"
        )

    def test_formula_with_different_expns(self, mock_fia_database):
        """Test that different EXPNS values affect variance correctly."""
        # Create data with doubled EXPNS
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
                "y_i": [0.8, 1.0, 0.6, 0.9],
                "EXPNS": [2000.0, 2000.0, 2000.0, 2000.0],  # Doubled
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Calculate expected: doubling EXPNS should quadruple variance
        y_values = [0.8, 1.0, 0.6, 0.9]
        s2_yh = np.var(y_values, ddof=1)
        expected_variance = (2000.0**2) * s2_yh * 4  # w^2 effect

        assert abs(var_stats["variance"] - expected_variance) < 1e-6

    def test_formula_scaling_with_sample_size(self, mock_fia_database):
        """
        Verify that variance scales correctly with sample size.

        Adding more plots with similar values should affect the variance
        through both the n_h multiplier and the s2_yh calculation.
        """
        # Small sample
        small_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2"],
                "ESTN_UNIT": [1, 1],
                "STRATUM": [1, 1],
                "y_i": [0.8, 1.0],
                "EXPNS": [1000.0, 1000.0],
            }
        )

        # Larger sample with same mean and similar variance
        large_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6"],
                "ESTN_UNIT": [1, 1, 1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1, 1, 1],
                "y_i": [0.8, 1.0, 0.8, 1.0, 0.8, 1.0],  # Same values repeated
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        small_var = estimator._calculate_variance_for_group(small_data, strat_cols)
        large_var = estimator._calculate_variance_for_group(large_data, strat_cols)

        # With same values repeated, variance increases proportionally with n
        # V = w^2 * s2 * n
        # Small: 1000^2 * 0.02 * 2 = 40,000
        # Large: 1000^2 * 0.02 * 6 = 120,000 (approx 3x)
        assert large_var["variance"] > small_var["variance"]


# =============================================================================
# TestSingleStratumVariance
# =============================================================================


class TestSingleStratumVariance:
    """
    Test each estimator with single stratum using known input values
    with hand-calculated expected variance.
    """

    def test_exact_values_hand_calculation(
        self, mock_fia_database, single_stratum_plot_data
    ):
        """
        Use exact values: 4 plots with values [0.8, 1.0, 0.6, 0.9], EXPNS=1000.

        Hand calculation:
        - n = 4
        - mean = (0.8 + 1.0 + 0.6 + 0.9) / 4 = 0.825
        - deviations: [-0.025, 0.175, -0.225, 0.075]
        - squared deviations: [0.000625, 0.030625, 0.050625, 0.005625]
        - sum of squared deviations = 0.0875
        - sample variance s2 = 0.0875 / 3 = 0.029166...
        - V = 1000^2 * 0.029166... * 4 = 116,666.67
        - SE = sqrt(V) = 341.565...
        """
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(
            single_stratum_plot_data, strat_cols
        )

        # Hand-calculated expected values
        expected_variance = 116666.66666666667  # 1000^2 * (0.0875/3) * 4
        expected_se = np.sqrt(expected_variance)  # ~341.565

        assert abs(var_stats["variance"] - expected_variance) < 0.01
        assert abs(var_stats["se_total"] - expected_se) < 0.01

    def test_homogeneous_values_zero_variance(self, mock_fia_database):
        """
        Test with identical values - variance should be zero.

        When all y_i values are identical:
        - s2_yh = 0
        - V = w^2 * 0 * n = 0
        """
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
                "y_i": [0.8, 0.8, 0.8, 0.8],  # All identical
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        assert var_stats["variance"] == 0.0
        assert var_stats["se_total"] == 0.0

    def test_high_variability_data(self, mock_fia_database):
        """Test with high variability data."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
                "y_i": [0.1, 0.9, 0.2, 0.8],  # High variability
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Calculate expected
        y_values = [0.1, 0.9, 0.2, 0.8]
        s2 = np.var(y_values, ddof=1)
        expected_variance = (1000.0**2) * s2 * 4

        assert abs(var_stats["variance"] - expected_variance) < 1e-6
        assert var_stats["variance"] > 0  # Non-zero for varied data

    def test_area_estimator_variance_integration(self, mock_fia_database):
        """Test variance calculation through full AreaEstimator workflow."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Create mock plot-condition data
        plot_condition_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "CONDID": [1, 1, 1, 1],
                "AREA_VALUE": [0.8, 1.0, 0.6, 0.9],
                "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0, 1.0],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
            }
        )

        # Create mock results
        results = pl.DataFrame(
            {
                "AREA_TOTAL": [3300.0],  # Total area estimate
                "N_PLOTS": [4],
            }
        )

        # Create AggregationResult with explicit data passing
        agg_result = AggregationResult(
            results=results,
            plot_tree_data=plot_condition_data,
            group_cols=[],
        )

        # Calculate variance
        variance_results = estimator.calculate_variance(agg_result)

        # Should have variance columns
        assert "AREA_SE" in variance_results.columns
        assert "AREA_VARIANCE" in variance_results.columns

        # Variance should be positive
        assert variance_results["AREA_VARIANCE"][0] > 0


# =============================================================================
# TestMultiStratumVariance
# =============================================================================


class TestMultiStratumVariance:
    """
    Test variance calculation with multiple strata.
    Verify that variances sum correctly across strata.
    """

    def test_two_strata_sum_correctly(self, mock_fia_database, multi_stratum_plot_data):
        """
        Test with two strata with known values.

        Stratum 1: 3 plots, y=[0.8, 1.0, 0.6], EXPNS=1000
        Stratum 2: 2 plots, y=[0.5, 0.9], EXPNS=1500

        Total variance = sum of stratum variances
        """
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(
            multi_stratum_plot_data, strat_cols
        )

        # Calculate expected variances for each stratum
        y1 = [0.8, 1.0, 0.6]
        s2_1 = np.var(y1, ddof=1)
        v1 = (1000.0**2) * s2_1 * 3

        y2 = [0.5, 0.9]
        s2_2 = np.var(y2, ddof=1)
        v2 = (1500.0**2) * s2_2 * 2

        expected_total_variance = v1 + v2

        assert abs(var_stats["variance"] - expected_total_variance) < 1e-6

    def test_three_strata_sum(self, mock_fia_database):
        """Test variance summation across three strata."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6", "P7"],
                "ESTN_UNIT": [1, 1, 1, 1, 1, 1, 1],
                "STRATUM": [1, 1, 2, 2, 2, 3, 3],
                "y_i": [0.8, 0.6, 0.9, 0.7, 0.8, 0.5, 0.7],
                "EXPNS": [1000.0, 1000.0, 1200.0, 1200.0, 1200.0, 800.0, 800.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Calculate expected variance for each stratum
        # Stratum 1: n=2, y=[0.8, 0.6], EXPNS=1000
        y1 = [0.8, 0.6]
        v1 = (1000.0**2) * np.var(y1, ddof=1) * 2

        # Stratum 2: n=3, y=[0.9, 0.7, 0.8], EXPNS=1200
        y2 = [0.9, 0.7, 0.8]
        v2 = (1200.0**2) * np.var(y2, ddof=1) * 3

        # Stratum 3: n=2, y=[0.5, 0.7], EXPNS=800
        y3 = [0.5, 0.7]
        v3 = (800.0**2) * np.var(y3, ddof=1) * 2

        expected_total = v1 + v2 + v3

        assert abs(var_stats["variance"] - expected_total) < 1e-6

    def test_unequal_strata_sizes(self, mock_fia_database):
        """Test with very unequal strata sizes."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"],
                "ESTN_UNIT": [1, 1, 1, 1, 1, 1, 1, 1, 2, 2],
                "STRATUM": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                "y_i": [0.8, 0.7, 0.9, 0.6, 0.8, 0.7, 0.9, 0.8, 0.5, 0.6],
                "EXPNS": [1000.0] * 8 + [2000.0] * 2,
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Variance should be positive
        assert var_stats["variance"] > 0
        assert var_stats["se_total"] > 0


# =============================================================================
# TestSinglePlotPerStratum
# =============================================================================


class TestSinglePlotPerStratum:
    """
    Test edge case when n_h = 1 (single plot per stratum).

    With only one plot, sample variance is undefined (division by n-1 = 0).
    The implementation should handle this gracefully.
    """

    def test_single_plot_variance_zero(self, mock_fia_database):
        """
        Test that variance is zero (or handled gracefully) with single plot.

        When n_h = 1, s2_yh is undefined (NaN with ddof=1).
        The implementation converts this to 0.
        """
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1"],
                "ESTN_UNIT": [1],
                "STRATUM": [1],
                "y_i": [0.8],
                "EXPNS": [1000.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Implementation should handle this gracefully
        assert var_stats["variance"] == 0.0
        assert var_stats["se_total"] == 0.0

    def test_multiple_single_plot_strata(self, mock_fia_database):
        """Test with multiple strata each having only one plot."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3"],
                "ESTN_UNIT": [1, 2, 3],  # Each unit has its own stratum
                "STRATUM": [1, 1, 1],
                "y_i": [0.8, 0.9, 0.7],
                "EXPNS": [1000.0, 1200.0, 800.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # All strata have single plots, total variance should be 0
        assert var_stats["variance"] == 0.0

    def test_mixed_single_and_multiple_plots(self, mock_fia_database):
        """Test with mix of single-plot and multi-plot strata."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "ESTN_UNIT": [1, 1, 1, 2],  # Unit 1 has 3 plots, Unit 2 has 1
                "STRATUM": [1, 1, 1, 1],
                "y_i": [0.8, 1.0, 0.6, 0.9],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1500.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Only Unit 1 contributes to variance (Unit 2 has single plot = 0 variance)
        y1 = [0.8, 1.0, 0.6]
        expected_variance = (1000.0**2) * np.var(y1, ddof=1) * 3

        assert abs(var_stats["variance"] - expected_variance) < 1e-6


# =============================================================================
# TestVarianceNonNegativity
# =============================================================================


class TestVarianceNonNegativity:
    """
    Property test: Variance should always be >= 0.

    Uses pytest parametrize to test various random inputs.
    """

    @pytest.mark.parametrize("seed", range(10))  # Run with 10 different seeds
    def test_variance_non_negative_random_data(self, mock_fia_database, seed):
        """Test that variance is always non-negative with random data."""
        np.random.seed(seed)
        n_plots = np.random.randint(2, 20)

        plot_data = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "ESTN_UNIT": [1] * n_plots,
                "STRATUM": [1] * n_plots,
                "y_i": np.random.uniform(0, 1, n_plots).tolist(),
                "EXPNS": [np.random.uniform(500, 2000)] * n_plots,
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        assert var_stats["variance"] >= 0, (
            f"Variance was negative: {var_stats['variance']}"
        )
        assert var_stats["se_total"] >= 0, f"SE was negative: {var_stats['se_total']}"

    @pytest.mark.parametrize("n_strata", [1, 2, 5, 10])
    def test_variance_non_negative_multi_strata(self, mock_fia_database, n_strata):
        """Test variance non-negativity with multiple strata."""
        np.random.seed(42)

        data_rows = []
        for s in range(n_strata):
            n_plots = np.random.randint(2, 8)
            for p in range(n_plots):
                data_rows.append(
                    {
                        "PLT_CN": f"P{s}_{p}",
                        "ESTN_UNIT": 1,
                        "STRATUM": s + 1,
                        "y_i": np.random.uniform(0, 1),
                        "EXPNS": np.random.uniform(500, 2000),
                    }
                )

        plot_data = pl.DataFrame(data_rows)

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        assert var_stats["variance"] >= 0

    def test_variance_non_negative_extreme_values(self, mock_fia_database):
        """Test variance non-negativity with extreme values."""
        test_cases = [
            # Very small values
            {"y_i": [0.001, 0.002, 0.001, 0.002], "EXPNS": 100.0},
            # Very large values
            {"y_i": [999.0, 1000.0, 998.0, 1001.0], "EXPNS": 10000.0},
            # Mix of zeros and ones
            {"y_i": [0.0, 1.0, 0.0, 1.0], "EXPNS": 1000.0},
            # Near-identical values
            {"y_i": [0.5000001, 0.5000002, 0.5000001, 0.5000003], "EXPNS": 1000.0},
        ]

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        for case in test_cases:
            plot_data = pl.DataFrame(
                {
                    "PLT_CN": ["P1", "P2", "P3", "P4"],
                    "ESTN_UNIT": [1, 1, 1, 1],
                    "STRATUM": [1, 1, 1, 1],
                    "y_i": case["y_i"],
                    "EXPNS": [case["EXPNS"]] * 4,
                }
            )

            var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)
            assert var_stats["variance"] >= 0, f"Failed for case: {case}"


# =============================================================================
# TestSEPercentCalculation
# =============================================================================


class TestSEPercentCalculation:
    """
    Verify SE% calculation: SE% = (SE / estimate) * 100
    """

    def test_se_percent_basic(self, mock_fia_database):
        """Test basic SE% calculation."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Create mock plot-condition data
        plot_condition_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "CONDID": [1, 1, 1, 1],
                "AREA_VALUE": [0.8, 1.0, 0.6, 0.9],
                "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0, 1.0],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
            }
        )

        # Create results with known total
        area_total = 3300.0
        results = pl.DataFrame(
            {
                "AREA_TOTAL": [area_total],
                "N_PLOTS": [4],
            }
        )

        # Create AggregationResult with explicit data passing
        agg_result = AggregationResult(
            results=results,
            plot_tree_data=plot_condition_data,
            group_cols=[],
        )

        variance_results = estimator.calculate_variance(agg_result)

        # Verify SE% calculation: SE% = (SE / estimate) * 100
        se = variance_results["AREA_SE"][0]
        expected_se_percent = (se / area_total) * 100

        assert "AREA_SE_PERCENT" in variance_results.columns
        assert abs(variance_results["AREA_SE_PERCENT"][0] - expected_se_percent) < 0.01

    def test_se_percent_zero_estimate(self, mock_fia_database):
        """Test SE% when estimate is zero (should handle division by zero)."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Create mock plot-condition data with zero values
        plot_condition_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "CONDID": [1, 1, 1, 1],
                "AREA_VALUE": [0.0, 0.0, 0.0, 0.0],  # All zeros
                "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0, 1.0],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
            }
        )

        results = pl.DataFrame(
            {
                "AREA_TOTAL": [0.0],
                "N_PLOTS": [4],
            }
        )

        # Create AggregationResult with explicit data passing
        agg_result = AggregationResult(
            results=results,
            plot_tree_data=plot_condition_data,
            group_cols=[],
        )

        variance_results = estimator.calculate_variance(agg_result)

        # SE% should be 0 when estimate is 0 (to avoid division by zero)
        assert variance_results["AREA_SE_PERCENT"][0] == 0

    def test_cv_calculation_function(self):
        """Test the standalone CV calculation function."""
        # Normal case
        cv = calculate_cv(estimate=100.0, se=10.0)
        assert cv == 10.0  # (10/100) * 100 = 10%

        # Zero estimate
        cv_zero = calculate_cv(estimate=0.0, se=10.0)
        assert cv_zero == 0.0

        # Small CV
        cv_small = calculate_cv(estimate=1000.0, se=1.0)
        assert cv_small == 0.1  # (1/1000) * 100 = 0.1%


# =============================================================================
# TestConfidenceIntervals
# =============================================================================


class TestConfidenceIntervals:
    """Test confidence interval calculations."""

    def test_ci_95_percent(self):
        """Test 95% confidence interval calculation."""
        estimate = 1000.0
        se = 50.0

        lower, upper = calculate_confidence_interval(estimate, se, confidence=0.95)

        # 95% CI: estimate +/- 1.96 * SE
        expected_lower = 1000.0 - 1.96 * 50.0  # 902
        expected_upper = 1000.0 + 1.96 * 50.0  # 1098

        assert abs(lower - expected_lower) < 0.01
        assert abs(upper - expected_upper) < 0.01

    def test_ci_90_percent(self):
        """Test 90% confidence interval calculation."""
        estimate = 1000.0
        se = 50.0

        lower, upper = calculate_confidence_interval(estimate, se, confidence=0.90)

        # 90% CI: estimate +/- 1.645 * SE
        expected_lower = 1000.0 - 1.645 * 50.0
        expected_upper = 1000.0 + 1.645 * 50.0

        assert abs(lower - expected_lower) < 0.01
        assert abs(upper - expected_upper) < 0.01

    def test_ci_99_percent(self):
        """Test 99% confidence interval calculation."""
        estimate = 1000.0
        se = 50.0

        lower, upper = calculate_confidence_interval(estimate, se, confidence=0.99)

        # 99% CI: estimate +/- 2.576 * SE
        expected_lower = 1000.0 - 2.576 * 50.0
        expected_upper = 1000.0 + 2.576 * 50.0

        assert abs(lower - expected_lower) < 0.01
        assert abs(upper - expected_upper) < 0.01


# =============================================================================
# TestSafetyFunctions
# =============================================================================


class TestSafetyFunctions:
    """Test safe division and square root functions."""

    def test_safe_divide_normal(self):
        """Test safe divide with normal values."""
        df = pl.DataFrame({"num": [10.0, 20.0, 30.0], "denom": [2.0, 4.0, 5.0]})
        result = df.select(safe_divide(pl.col("num"), pl.col("denom")))
        expected = [5.0, 5.0, 6.0]
        assert result.to_series().to_list() == expected

    def test_safe_divide_zero_denom(self):
        """Test safe divide with zero denominator."""
        df = pl.DataFrame({"num": [10.0, 20.0], "denom": [2.0, 0.0]})
        result = df.select(safe_divide(pl.col("num"), pl.col("denom"), default=0.0))
        assert result.to_series().to_list() == [5.0, 0.0]

    def test_safe_sqrt_positive(self):
        """Test safe square root with positive values."""
        df = pl.DataFrame({"val": [4.0, 9.0, 16.0]})
        result = df.select(safe_sqrt(pl.col("val")))
        expected = [2.0, 3.0, 4.0]
        assert result.to_series().to_list() == expected

    def test_safe_sqrt_negative(self):
        """Test safe square root with negative values."""
        df = pl.DataFrame({"val": [4.0, -1.0, 9.0]})
        result = df.select(safe_sqrt(pl.col("val"), default=-999.0))
        assert result.to_series().to_list() == [2.0, -999.0, 3.0]


# =============================================================================
# TestVarianceWithGrouping
# =============================================================================


class TestVarianceWithGrouping:
    """Test variance calculation with grouping variables."""

    def test_variance_by_group(self, mock_fia_database):
        """Test that variance is calculated separately for each group."""
        config = {"grp_by": "FORTYPCD"}
        estimator = AreaEstimator(mock_fia_database, config)

        # Create mock plot-condition data with two groups
        plot_condition_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6"],
                "CONDID": [1, 1, 1, 1, 1, 1],
                "AREA_VALUE": [0.8, 1.0, 0.6, 0.5, 0.9, 0.7],
                "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1500.0, 1500.0, 1500.0],
                "ESTN_UNIT": [1, 1, 1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1, 1, 1],
                "FORTYPCD": [161, 161, 161, 406, 406, 406],  # Two forest types
            }
        )

        # Create results for each group
        results = pl.DataFrame(
            {
                "FORTYPCD": [161, 406],
                "AREA_TOTAL": [2400.0, 3150.0],
                "N_PLOTS": [3, 3],
            }
        )

        # Create AggregationResult with explicit data passing
        agg_result = AggregationResult(
            results=results,
            plot_tree_data=plot_condition_data,
            group_cols=["FORTYPCD"],
        )

        variance_results = estimator.calculate_variance(agg_result)

        # Should have two rows (one per group)
        assert len(variance_results) == 2

        # Each group should have variance columns
        assert "AREA_SE" in variance_results.columns
        assert "AREA_VARIANCE" in variance_results.columns

        # Variances should be calculated independently
        # They might be different due to different EXPNS values
        variances = variance_results["AREA_VARIANCE"].to_list()
        assert all(v >= 0 for v in variances)

    def test_variance_multiple_grouping_vars(self, mock_fia_database):
        """Test variance with multiple grouping variables."""
        config = {"grp_by": ["FORTYPCD", "OWNGRPCD"]}
        estimator = AreaEstimator(mock_fia_database, config)

        # Create mock plot-condition data with multiple groups
        plot_condition_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "CONDID": [1, 1, 1, 1],
                "AREA_VALUE": [0.8, 1.0, 0.6, 0.9],
                "ADJ_FACTOR_AREA": [1.0, 1.0, 1.0, 1.0],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
                "FORTYPCD": [161, 161, 406, 406],
                "OWNGRPCD": [10, 20, 10, 20],
            }
        )

        # Create results for each group combination
        results = pl.DataFrame(
            {
                "FORTYPCD": [161, 161, 406, 406],
                "OWNGRPCD": [10, 20, 10, 20],
                "AREA_TOTAL": [800.0, 1000.0, 600.0, 900.0],
                "N_PLOTS": [1, 1, 1, 1],
            }
        )

        # Create AggregationResult with explicit data passing
        agg_result = AggregationResult(
            results=results,
            plot_tree_data=plot_condition_data,
            group_cols=["FORTYPCD", "OWNGRPCD"],
        )

        variance_results = estimator.calculate_variance(agg_result)

        # Should have 4 rows (one per group combination)
        assert len(variance_results) == 4


# =============================================================================
# TestEdgeCases
# =============================================================================


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_data(self, mock_fia_database):
        """Test handling of empty data."""
        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        # Empty DataFrame
        empty_data = pl.DataFrame(
            {
                "PLT_CN": [],
                "ESTN_UNIT": [],
                "STRATUM": [],
                "y_i": [],
                "EXPNS": [],
            }
        ).cast(
            {
                "ESTN_UNIT": pl.Int64,
                "STRATUM": pl.Int64,
                "y_i": pl.Float64,
                "EXPNS": pl.Float64,
            }
        )

        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(empty_data, strat_cols)

        # Should handle gracefully - variance of empty set is 0
        assert var_stats["variance"] == 0.0 or var_stats["variance"] is None

    def test_all_zero_values(self, mock_fia_database):
        """Test with all zero y_i values."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
                "y_i": [0.0, 0.0, 0.0, 0.0],  # All zeros
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Variance of constant zero values is zero
        assert var_stats["variance"] == 0.0

    def test_very_large_expns(self, mock_fia_database):
        """Test numerical stability with very large EXPNS values."""
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "ESTN_UNIT": [1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 1],
                "y_i": [0.8, 1.0, 0.6, 0.9],
                "EXPNS": [1e9, 1e9, 1e9, 1e9],  # Very large
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        strat_cols = ["ESTN_UNIT", "STRATUM"]

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Should not overflow or produce NaN
        assert not np.isnan(var_stats["variance"])
        assert not np.isinf(var_stats["variance"])
        assert var_stats["variance"] > 0

    def test_no_stratification_columns(self, mock_fia_database):
        """
        Test when no stratification columns are available in the data.

        Note: The _calculate_variance_for_group method expects at least one
        stratification column. When no stratification columns are present in
        the original data, the calculate_variance method creates a default
        "STRATUM" column before calling _calculate_variance_for_group.

        This test verifies that behavior by simulating what calculate_variance does.
        """
        # Original data without stratification
        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3"],
                "y_i": [0.8, 1.0, 0.6],
                "EXPNS": [1000.0, 1000.0, 1000.0],
                # No ESTN_UNIT or STRATUM columns
            }
        )

        # Add default stratum column (as calculate_variance does internally)
        plot_data = plot_data.with_columns([pl.lit(1).alias("STRATUM")])
        strat_cols = ["STRATUM"]

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)

        var_stats = estimator._calculate_variance_for_group(plot_data, strat_cols)

        # Should calculate variance correctly using the default single stratum
        y_values = [0.8, 1.0, 0.6]
        s2 = np.var(y_values, ddof=1)
        expected_variance = (1000.0**2) * s2 * 3

        assert abs(var_stats["variance"] - expected_variance) < 1e-6


# =============================================================================
# TestFormulaDocumentation
# =============================================================================


class TestFormulaDocumentation:
    """
    Tests to verify that the implementation matches the documented formulas.

    These tests serve as executable documentation for the variance formulas
    used in FIA estimation following Bechtold & Patterson (2005).
    """

    def test_domain_total_formula(self, mock_fia_database):
        """
        Verify the domain total estimation variance formula:

        V(Y_D) = sum_h [w_h^2 * s2_yDh * n_h]

        Where:
        - w_h = EXPNS (expansion factor, acres per plot)
        - s2_yDh = variance of domain indicator within stratum h
        - n_h = number of sampled plots in stratum h

        This formula is for domain TOTALS, not means. The n_h appears as a
        multiplier (not divisor) because we're estimating a sum, not an average.
        """
        # Simple test case for documentation
        y_values = [0.8, 1.0, 0.6, 0.9]
        n = 4
        w = 1000.0
        s2 = np.var(y_values, ddof=1)

        # Formula: V = w^2 * s2 * n
        expected_variance = (w**2) * s2 * n

        plot_data = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(n)],
                "ESTN_UNIT": [1] * n,
                "STRATUM": [1] * n,
                "y_i": y_values,
                "EXPNS": [w] * n,
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(
            plot_data, ["ESTN_UNIT", "STRATUM"]
        )

        assert abs(var_stats["variance"] - expected_variance) < 1e-6, (
            f"Formula mismatch: expected {expected_variance}, got {var_stats['variance']}"
        )

    def test_stratified_sampling_additivity(self, mock_fia_database):
        """
        Verify that variances from different strata are additive:

        V_total = V_1 + V_2 + ... + V_H

        This is a fundamental property of stratified sampling variance.
        """
        # Create two strata with known variances
        y1 = [0.8, 1.0, 0.6]  # Stratum 1
        y2 = [0.5, 0.9, 0.7]  # Stratum 2
        w1 = 1000.0
        w2 = 1500.0

        v1 = (w1**2) * np.var(y1, ddof=1) * len(y1)
        v2 = (w2**2) * np.var(y2, ddof=1) * len(y2)
        expected_total = v1 + v2

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6"],
                "ESTN_UNIT": [1, 1, 1, 1, 1, 1],
                "STRATUM": [1, 1, 1, 2, 2, 2],
                "y_i": y1 + y2,
                "EXPNS": [w1, w1, w1, w2, w2, w2],
            }
        )

        config = {}
        estimator = AreaEstimator(mock_fia_database, config)
        var_stats = estimator._calculate_variance_for_group(
            plot_data, ["ESTN_UNIT", "STRATUM"]
        )

        assert abs(var_stats["variance"] - expected_total) < 1e-6, (
            f"Additivity violation: V1={v1}, V2={v2}, expected total={expected_total}, "
            f"got {var_stats['variance']}"
        )
