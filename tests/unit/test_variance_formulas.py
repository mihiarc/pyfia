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


# =============================================================================
# TestExactBPVarianceFormula
# =============================================================================


class TestExactBPVarianceFormula:
    """
    Test the exact Bechtold & Patterson (2005) post-stratified variance formula.

    The exact formula is:
        V_EU = (A²/n) × Σ_h W_h × s²_yh + (A²/n²) × Σ_h (1-W_h) × s²_yh
             = V1 + V2

    Where V2 is the post-stratification correction term that captures
    uncertainty from estimating stratum weights from the sample.
    Note: s²_yh is used directly (NOT s²_yh/n_h). The sample size is
    accounted for in the A²/n and A²/n² terms.

    These tests directly call calculate_domain_total_variance with all B&P
    columns present to exercise the exact formula path.
    """

    def test_single_eu_single_stratum_hand_calculated(self):
        """
        Hand-calculated exact B&P variance for a single EU with one stratum.

        Setup:
        - 1 EU with AREA_USED = 10000 acres
        - 1 stratum: W_h = 1.0 (all P1 points in this stratum)
        - n_h = 4 plots
        - y values = [10, 20, 30, 40]
        - s² = var([10,20,30,40], ddof=1) = 166.6667
        - n = 4 (total plots in EU)

        B&P formula uses s² directly (NOT s²/n_h):
        V1 = (A²/n) × W_h × s²
           = (10000²/4) × 1.0 × 166.6667
           = 25000000 × 166.6667
           = 4,166,666,750

        V2 = (A²/n²) × (1 - W_h) × s²
           = 0  (since W_h = 1.0)

        V_total = V1 + V2 = 4,166,666,750
        """
        from pyfia.estimation.variance import calculate_domain_total_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": [1, 1, 1, 1],
                "EXPNS": [2500.0, 2500.0, 2500.0, 2500.0],
                "ESTN_UNIT_CN": [100, 100, 100, 100],
                "STRATUM_WGT": [1.0, 1.0, 1.0, 1.0],
                "AREA_USED": [10000.0, 10000.0, 10000.0, 10000.0],
                "P2POINTCNT": [4.0, 4.0, 4.0, 4.0],
                "y_i": [10.0, 20.0, 30.0, 40.0],
            }
        )

        result = calculate_domain_total_variance(plot_data, "y_i")

        # Hand calculation: B&P uses s² directly in V1/V2
        s2 = np.var([10, 20, 30, 40], ddof=1)  # 166.6667
        A = 10000.0
        n = 4
        W_h = 1.0

        v1 = (A**2 / n) * W_h * s2
        v2 = (A**2 / n**2) * (1 - W_h) * s2
        expected_variance = v1 + v2

        assert abs(result["variance_total"] - expected_variance) < 1.0, (
            f"Expected {expected_variance}, got {result['variance_total']}"
        )
        assert abs(result["se_total"] - expected_variance**0.5) < 0.01

    def test_two_strata_proportional_allocation(self):
        """
        With proportional allocation (W_h = n_h/n), V2 should be zero,
        and the result should match the simplified formula.

        Setup:
        - 1 EU, AREA_USED = 10000
        - Stratum A: W_h = 0.6, n_h = 3, y = [10, 20, 30]
        - Stratum B: W_h = 0.4, n_h = 2, y = [50, 60]
        - n = 5, proportional: W_A = 3/5 = 0.6, W_B = 2/5 = 0.4

        When W_h = n_h/n exactly, V2 terms cancel out:
        (1 - W_h) = (1 - n_h/n) = (n - n_h)/n
        """
        from pyfia.estimation.variance import calculate_domain_total_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
                "STRATUM_CN": [1, 1, 1, 2, 2],
                "EXPNS": [2000.0, 2000.0, 2000.0, 2000.0, 2000.0],
                "ESTN_UNIT_CN": [100, 100, 100, 100, 100],
                "STRATUM_WGT": [0.6, 0.6, 0.6, 0.4, 0.4],
                "AREA_USED": [10000.0] * 5,
                "P2POINTCNT": [3.0, 3.0, 3.0, 2.0, 2.0],
                "y_i": [10.0, 20.0, 30.0, 50.0, 60.0],
            }
        )

        result = calculate_domain_total_variance(plot_data, "y_i")

        # Hand calculation: B&P uses s² directly (NOT s²/n_h)
        A = 10000.0
        n = 5

        s2_a = np.var([10, 20, 30], ddof=1)  # 100
        s2_b = np.var([50, 60], ddof=1)  # 50

        v1 = (A**2 / n) * (0.6 * s2_a + 0.4 * s2_b)
        v2 = (A**2 / n**2) * ((1 - 0.6) * s2_a + (1 - 0.4) * s2_b)
        expected = v1 + v2

        assert abs(result["variance_total"] - expected) < 1.0, (
            f"Expected {expected}, got {result['variance_total']}"
        )

    def test_non_proportional_allocation_v2_positive(self):
        """
        With non-proportional allocation, V2 should be > 0.

        Setup:
        - 1 EU, AREA_USED = 10000
        - Stratum A: W_h = 0.9 (many P1 points), n_h = 2 (few P2 plots)
        - Stratum B: W_h = 0.1 (few P1 points), n_h = 8 (many P2 plots)
        - n = 10

        Non-proportional because:
          W_A = 0.9 but n_A/n = 2/10 = 0.2
          W_B = 0.1 but n_B/n = 8/10 = 0.8
        """
        from pyfia.estimation.variance import calculate_domain_total_variance

        np.random.seed(42)
        y_a = np.random.uniform(10, 30, 2).tolist()
        y_b = np.random.uniform(40, 60, 8).tolist()

        plot_data = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(10)],
                "STRATUM_CN": [1, 1] + [2] * 8,
                "EXPNS": [5000.0] * 2 + [125.0] * 8,
                "ESTN_UNIT_CN": [100] * 10,
                "STRATUM_WGT": [0.9] * 2 + [0.1] * 8,
                "AREA_USED": [10000.0] * 10,
                "P2POINTCNT": [2.0] * 2 + [8.0] * 8,
                "y_i": y_a + y_b,
            }
        )

        result = calculate_domain_total_variance(plot_data, "y_i")

        # Calculate V2 manually to verify it's positive
        # B&P uses s² directly (NOT s²/n_h)
        A = 10000.0
        n = 10
        s2_a = np.var(y_a, ddof=1)
        s2_b = np.var(y_b, ddof=1)

        v2 = (A**2 / n**2) * ((1 - 0.9) * s2_a + (1 - 0.1) * s2_b)
        assert v2 > 0, "V2 should be positive for non-proportional allocation"

        v1 = (A**2 / n) * (0.9 * s2_a + 0.1 * s2_b)
        expected = v1 + v2

        assert abs(result["variance_total"] - expected) < 1.0, (
            f"Expected {expected}, got {result['variance_total']}"
        )
        assert result["variance_total"] > 0

    def test_multiple_estimation_units(self):
        """
        Test variance aggregation across multiple estimation units.

        Setup:
        - EU 1: AREA_USED = 8000, 1 stratum, W_h = 1.0, n_h = 3
        - EU 2: AREA_USED = 12000, 1 stratum, W_h = 1.0, n_h = 2
        - V_total = V_EU1 + V_EU2
        """
        from pyfia.estimation.variance import calculate_domain_total_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
                "STRATUM_CN": [1, 1, 1, 2, 2],
                "EXPNS": [2666.67, 2666.67, 2666.67, 6000.0, 6000.0],
                "ESTN_UNIT_CN": [100, 100, 100, 200, 200],
                "STRATUM_WGT": [1.0, 1.0, 1.0, 1.0, 1.0],
                "AREA_USED": [8000.0, 8000.0, 8000.0, 12000.0, 12000.0],
                "P2POINTCNT": [3.0, 3.0, 3.0, 2.0, 2.0],
                "y_i": [10.0, 20.0, 30.0, 50.0, 70.0],
            }
        )

        result = calculate_domain_total_variance(plot_data, "y_i")

        # EU 1: A=8000, n=3, W_h=1.0 — B&P uses s² directly
        s2_1 = np.var([10, 20, 30], ddof=1)  # 100
        v_eu1 = (8000**2 / 3) * 1.0 * s2_1  # V1 only (V2=0 since W=1)

        # EU 2: A=12000, n=2, W_h=1.0
        s2_2 = np.var([50, 70], ddof=1)  # 200
        v_eu2 = (12000**2 / 2) * 1.0 * s2_2  # V1 only

        expected = v_eu1 + v_eu2

        assert abs(result["variance_total"] - expected) < 1.0, (
            f"Expected {expected}, got {result['variance_total']}"
        )

    def test_single_plot_stratum_excluded(self):
        """
        Strata with n_h = 1 should contribute 0 variance (s² undefined).
        """
        from pyfia.estimation.variance import calculate_domain_total_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": [1, 1, 1, 2],  # Stratum 2 has only 1 plot
                "EXPNS": [2500.0, 2500.0, 2500.0, 2500.0],
                "ESTN_UNIT_CN": [100, 100, 100, 100],
                "STRATUM_WGT": [0.75, 0.75, 0.75, 0.25],
                "AREA_USED": [10000.0] * 4,
                "P2POINTCNT": [3.0, 3.0, 3.0, 1.0],
                "y_i": [10.0, 20.0, 30.0, 100.0],
            }
        )

        result = calculate_domain_total_variance(plot_data, "y_i")

        # Only stratum 1 contributes (stratum 2 has n_h=1)
        # B&P uses s² directly (NOT s²/n_h)
        A = 10000.0
        n = 4
        s2_1 = np.var([10, 20, 30], ddof=1)  # 100
        # Stratum 2 s2 = 0 (excluded, n_h=1)

        v1 = (A**2 / n) * (0.75 * s2_1 + 0.25 * 0)
        v2 = (A**2 / n**2) * ((1 - 0.75) * s2_1 + (1 - 0.25) * 0)
        expected = v1 + v2

        assert abs(result["variance_total"] - expected) < 1.0, (
            f"Expected {expected}, got {result['variance_total']}"
        )

    def test_fallback_without_bp_columns(self):
        """
        When B&P columns are absent, should fall back to simplified formula.
        """
        from pyfia.estimation.variance import calculate_domain_total_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": [1, 1, 1, 1],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
                "y_i": [0.8, 1.0, 0.6, 0.9],
            }
        )

        result = calculate_domain_total_variance(plot_data, "y_i")

        # Simplified formula: V = w² × s² × n
        s2 = np.var([0.8, 1.0, 0.6, 0.9], ddof=1)
        expected = (1000.0**2) * s2 * 4

        assert abs(result["variance_total"] - expected) < 1e-6

    def test_grouped_exact_bp_variance(self):
        """
        Test grouped variance with exact B&P formula.

        Two species groups in one EU, each with one stratum.
        """
        from pyfia.estimation.variance import (
            calculate_grouped_domain_total_variance,
        )

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6"],
                "STRATUM_CN": [1, 1, 1, 1, 1, 1],
                "EXPNS": [2000.0] * 6,
                "ESTN_UNIT_CN": [100] * 6,
                "STRATUM_WGT": [1.0] * 6,
                "AREA_USED": [12000.0] * 6,
                "P2POINTCNT": [6.0] * 6,
                "y_i": [10.0, 20.0, 30.0, 5.0, 15.0, 25.0],
                "SPCD": [100, 100, 100, 200, 200, 200],
                "x_i": [1.0] * 6,
            }
        )

        result = calculate_grouped_domain_total_variance(
            plot_data,
            group_cols=["SPCD"],
            y_col="y_i",
            x_col="x_i",
        )

        assert len(result) == 2
        assert "variance_total" in result.columns
        assert "se_total" in result.columns

        # Each group should have positive variance
        for row in result.iter_rows(named=True):
            assert row["variance_total"] > 0
            assert row["se_total"] > 0

    @pytest.mark.parametrize("seed", range(5))
    def test_exact_bp_variance_non_negative(self, seed):
        """Property test: exact B&P variance must always be non-negative."""
        from pyfia.estimation.variance import calculate_domain_total_variance

        np.random.seed(seed)
        n_plots = np.random.randint(2, 15)
        n_strata = np.random.randint(1, min(4, n_plots))

        # Assign plots to strata
        strata = np.sort(np.random.choice(range(1, n_strata + 1), size=n_plots))
        stratum_weights = np.random.dirichlet(np.ones(n_strata))
        area_used = np.random.uniform(5000, 50000)

        rows = []
        for i in range(n_plots):
            s = strata[i]
            n_h = np.sum(strata == s)
            rows.append(
                {
                    "PLT_CN": f"P{i}",
                    "STRATUM_CN": int(s),
                    "EXPNS": area_used / n_plots,
                    "ESTN_UNIT_CN": 100,
                    "STRATUM_WGT": float(stratum_weights[s - 1]),
                    "AREA_USED": area_used,
                    "P2POINTCNT": float(n_h),
                    "y_i": float(np.random.uniform(0, 100)),
                }
            )

        plot_data = pl.DataFrame(rows)
        result = calculate_domain_total_variance(plot_data, "y_i")

        assert result["variance_total"] >= 0, (
            f"Variance was negative: {result['variance_total']}"
        )
        assert result["se_total"] >= 0


# =============================================================================
# TestRatioOfMeansVariance
# =============================================================================


class TestRatioOfMeansVariance:
    """
    Test the ratio-of-means variance formula from Bechtold & Patterson (2005):

        V(R) = (1/X^2) * [V(Y) + R^2 * V(X) - 2*R*Cov(Y,X)]

    Where R = Y/X is the per-acre estimate.
    """

    def test_ratio_variance_hand_calculated(self):
        """
        Hand-calculated ratio variance for a single EU, single stratum.

        Setup:
        - 4 plots with y = [10, 20, 30, 40], x = [0.8, 0.9, 1.0, 0.7]
        - Single stratum, W_h = 1.0, A = 10000, n = 4, P2POINTCNT = 4

        B&P post-stratified variance uses s² directly (NOT s²/n_h):
        V(Y) = (A²/n) × W_h × s²_y = (1e8/4) × 166.6667 = 4,166,666,750
        V(X) = (A²/n) × W_h × s²_x = (1e8/4) × 0.016667 = 416,666.67
        Cov  = (A²/n) × W_h × cov_yx = (1e8/4) × 1.0 = 25,000,000
        """
        from pyfia.estimation.variance import calculate_ratio_of_means_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": [1, 1, 1, 1],
                "EXPNS": [2500.0, 2500.0, 2500.0, 2500.0],
                "ESTN_UNIT_CN": [100, 100, 100, 100],
                "STRATUM_WGT": [1.0, 1.0, 1.0, 1.0],
                "AREA_USED": [10000.0, 10000.0, 10000.0, 10000.0],
                "P2POINTCNT": [4.0, 4.0, 4.0, 4.0],
                "y_i": [10.0, 20.0, 30.0, 40.0],
                "x_i": [0.8, 0.9, 1.0, 0.7],
            }
        )

        result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")

        # Verify all expected keys are present
        assert "variance_total" in result
        assert "se_total" in result
        assert "variance_ratio" in result
        assert "se_ratio" in result
        assert "total_y" in result
        assert "total_x" in result
        assert "ratio" in result

        # Verify totals
        y_vals = np.array([10, 20, 30, 40])
        x_vals = np.array([0.8, 0.9, 1.0, 0.7])
        expns = 2500.0

        expected_total_y = expns * y_vals.sum()
        expected_total_x = expns * x_vals.sum()
        assert abs(result["total_y"] - expected_total_y) < 1.0
        assert abs(result["total_x"] - expected_total_x) < 1.0

        # Verify ratio
        expected_ratio = expected_total_y / expected_total_x
        assert abs(result["ratio"] - expected_ratio) < 0.01

        # Verify variance is positive and ratio variance is computed
        assert result["variance_total"] > 0
        assert result["variance_ratio"] > 0
        assert result["se_ratio"] > 0

        # Hand-calculate the ratio variance
        # B&P uses s² directly (NOT s²/n_h)
        s2_y = np.var(y_vals, ddof=1)
        s2_x = np.var(x_vals, ddof=1)
        cov_yx = np.cov(y_vals, x_vals, ddof=1)[0, 1]
        area = 10000.0
        n = 4

        var_y = (area**2 / n) * s2_y
        var_x = (area**2 / n) * s2_x
        cov_total = (area**2 / n) * cov_yx

        r_hat = expected_ratio
        expected_var_ratio = (1.0 / expected_total_x**2) * (
            var_y + r_hat**2 * var_x - 2 * r_hat * cov_total
        )

        assert abs(result["variance_ratio"] - expected_var_ratio) < 0.1, (
            f"Expected V(R)={expected_var_ratio}, got {result['variance_ratio']}"
        )

    def test_constant_x_equals_simple_formula(self):
        """
        When all x_i = constant c, V(X)=0 and Cov(Y,X)=0,
        so V(R) = V(Y) / X^2, which is the old simplified formula.
        """
        from pyfia.estimation.variance import (
            calculate_domain_total_variance,
            calculate_ratio_of_means_variance,
        )

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": [1, 1, 1, 1],
                "EXPNS": [2500.0, 2500.0, 2500.0, 2500.0],
                "ESTN_UNIT_CN": [100, 100, 100, 100],
                "STRATUM_WGT": [1.0, 1.0, 1.0, 1.0],
                "AREA_USED": [10000.0, 10000.0, 10000.0, 10000.0],
                "P2POINTCNT": [4.0, 4.0, 4.0, 4.0],
                "y_i": [10.0, 20.0, 30.0, 40.0],
                "x_i": [1.0, 1.0, 1.0, 1.0],  # Constant x
            }
        )

        ratio_result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")
        domain_result = calculate_domain_total_variance(plot_data, "y_i")

        # With constant x, V(X)=0 and Cov=0
        # So V(R) = V(Y) / X^2
        total_x = ratio_result["total_x"]
        expected_var_ratio = domain_result["variance_total"] / total_x**2
        expected_se_ratio = domain_result["se_total"] / total_x

        assert abs(ratio_result["variance_ratio"] - expected_var_ratio) < 1e-6, (
            f"Expected {expected_var_ratio}, got {ratio_result['variance_ratio']}"
        )
        assert abs(ratio_result["se_ratio"] - expected_se_ratio) < 1e-6, (
            f"Expected {expected_se_ratio}, got {ratio_result['se_ratio']}"
        )

    def test_with_bp_columns_multi_eu(self):
        """
        Full B&P ratio variance with 2 EUs and hand-calculated values.
        """
        from pyfia.estimation.variance import calculate_ratio_of_means_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
                "STRATUM_CN": [1, 1, 1, 2, 2],
                "EXPNS": [2666.67, 2666.67, 2666.67, 6000.0, 6000.0],
                "ESTN_UNIT_CN": [100, 100, 100, 200, 200],
                "STRATUM_WGT": [1.0, 1.0, 1.0, 1.0, 1.0],
                "AREA_USED": [8000.0, 8000.0, 8000.0, 12000.0, 12000.0],
                "P2POINTCNT": [3.0, 3.0, 3.0, 2.0, 2.0],
                "y_i": [10.0, 20.0, 30.0, 50.0, 70.0],
                "x_i": [0.8, 1.0, 0.9, 0.7, 0.95],
            }
        )

        result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")

        # Basic assertions
        assert result["variance_total"] > 0
        assert result["variance_ratio"] >= 0
        assert result["se_ratio"] >= 0
        assert result["total_x"] > 0
        assert result["ratio"] > 0

    def test_positive_covariance_reduces_variance(self):
        """
        When Y and X are positively correlated, the ratio variance
        should be less than V(Y)/X^2 (the old simplified formula).

        This is because V(R) = (1/X^2) * [V(Y) + R^2*V(X) - 2*R*Cov]
        and the -2*R*Cov term reduces variance when Cov > 0.
        """
        from pyfia.estimation.variance import calculate_ratio_of_means_variance

        # Create data where y and x are positively correlated
        # High x -> high y (more forest area -> more volume)
        plot_data = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(6)],
                "STRATUM_CN": [1] * 6,
                "EXPNS": [2000.0] * 6,
                "ESTN_UNIT_CN": [100] * 6,
                "STRATUM_WGT": [1.0] * 6,
                "AREA_USED": [12000.0] * 6,
                "P2POINTCNT": [6.0] * 6,
                "y_i": [5.0, 15.0, 25.0, 35.0, 45.0, 55.0],
                "x_i": [0.3, 0.5, 0.7, 0.8, 0.9, 1.0],
            }
        )

        result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")

        # Calculate the old simplified formula: V(Y) / X^2
        old_se_acre = result["se_total"] / result["total_x"]
        old_var_acre = old_se_acre**2

        # Ratio variance should be smaller due to positive covariance
        assert result["variance_ratio"] < old_var_acre, (
            f"Ratio variance {result['variance_ratio']} should be < "
            f"simplified {old_var_acre} for positively correlated data"
        )

    def test_grouped_ratio_variance(self):
        """
        Test grouped ratio variance with two species groups.
        """
        from pyfia.estimation.variance import (
            calculate_grouped_domain_total_variance,
        )

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4", "P5", "P6"],
                "STRATUM_CN": [1, 1, 1, 1, 1, 1],
                "EXPNS": [2000.0] * 6,
                "ESTN_UNIT_CN": [100] * 6,
                "STRATUM_WGT": [1.0] * 6,
                "AREA_USED": [12000.0] * 6,
                "P2POINTCNT": [6.0] * 6,
                "y_i": [10.0, 20.0, 30.0, 5.0, 15.0, 25.0],
                "x_i": [0.8, 0.9, 1.0, 0.6, 0.7, 0.85],
                "SPCD": [100, 100, 100, 200, 200, 200],
            }
        )

        result = calculate_grouped_domain_total_variance(
            plot_data,
            group_cols=["SPCD"],
            y_col="y_i",
            x_col="x_i",
        )

        assert len(result) == 2
        assert "variance_acre" in result.columns
        assert "se_acre" in result.columns
        assert "variance_total" in result.columns
        assert "se_total" in result.columns

        # Each group should have positive variance
        for row in result.iter_rows(named=True):
            assert row["variance_total"] > 0
            assert row["se_total"] > 0
            assert row["se_acre"] > 0
            assert row["variance_acre"] > 0

    def test_fallback_without_bp_columns(self):
        """
        Ratio variance should work with simplified formula when B&P columns absent.
        """
        from pyfia.estimation.variance import calculate_ratio_of_means_variance

        plot_data = pl.DataFrame(
            {
                "PLT_CN": ["P1", "P2", "P3", "P4"],
                "STRATUM_CN": [1, 1, 1, 1],
                "EXPNS": [1000.0, 1000.0, 1000.0, 1000.0],
                "y_i": [10.0, 20.0, 30.0, 40.0],
                "x_i": [0.8, 0.9, 1.0, 0.7],
            }
        )

        result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")

        # Should still compute valid results
        assert result["variance_total"] > 0
        assert result["se_total"] > 0
        assert result["variance_ratio"] >= 0
        assert result["se_ratio"] >= 0
        assert result["total_y"] > 0
        assert result["total_x"] > 0

    @pytest.mark.parametrize("seed", range(10))
    def test_ratio_variance_non_negative(self, seed):
        """Property test: ratio variance should never be negative."""
        from pyfia.estimation.variance import calculate_ratio_of_means_variance

        np.random.seed(seed)
        n_plots = np.random.randint(3, 15)

        plot_data = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(n_plots)],
                "STRATUM_CN": [1] * n_plots,
                "EXPNS": [np.random.uniform(500, 3000)] * n_plots,
                "ESTN_UNIT_CN": [100] * n_plots,
                "STRATUM_WGT": [1.0] * n_plots,
                "AREA_USED": [np.random.uniform(5000, 50000)] * n_plots,
                "P2POINTCNT": [float(n_plots)] * n_plots,
                "y_i": np.random.uniform(0, 100, n_plots).tolist(),
                "x_i": np.random.uniform(0.1, 1.0, n_plots).tolist(),
            }
        )

        result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")

        assert result["variance_ratio"] >= 0, (
            f"Ratio variance was negative: {result['variance_ratio']}"
        )
        assert result["se_ratio"] >= 0

    def test_ungrouped_fallback_uses_ratio_variance(self):
        """
        Test that calculate_grouped_domain_total_variance with no group_cols
        uses ratio variance instead of simple se_total/total_x.
        """
        from pyfia.estimation.variance import (
            calculate_grouped_domain_total_variance,
            calculate_ratio_of_means_variance,
        )

        # Data with positive correlation between y and x
        plot_data = pl.DataFrame(
            {
                "PLT_CN": [f"P{i}" for i in range(5)],
                "STRATUM_CN": [1] * 5,
                "EXPNS": [2000.0] * 5,
                "ESTN_UNIT_CN": [100] * 5,
                "STRATUM_WGT": [1.0] * 5,
                "AREA_USED": [10000.0] * 5,
                "P2POINTCNT": [5.0] * 5,
                "y_i": [5.0, 15.0, 25.0, 35.0, 45.0],
                "x_i": [0.3, 0.5, 0.7, 0.8, 1.0],
            }
        )

        # No valid group cols -> falls back to scalar path
        grouped_result = calculate_grouped_domain_total_variance(
            plot_data,
            group_cols=["NONEXISTENT_COL"],
            y_col="y_i",
            x_col="x_i",
        )

        # Should match scalar ratio variance
        scalar_result = calculate_ratio_of_means_variance(plot_data, "y_i", "x_i")

        assert abs(grouped_result["se_acre"][0] - scalar_result["se_ratio"]) < 1e-10
        assert abs(grouped_result["se_total"][0] - scalar_result["se_total"]) < 1e-10
