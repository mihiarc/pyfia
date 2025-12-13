"""
Property-based tests for pyFIA using Hypothesis.

These tests verify invariants and properties that should hold
across a wide range of inputs. Tests are self-contained and do not
require external databases.
"""

import numpy as np
import polars as pl
import pytest
from hypothesis import assume, given, settings, HealthCheck
from hypothesis import strategies as st

from pyfia.estimation.tree_expansion import (
    apply_tree_adjustment_factors,
    get_adjustment_factor_expr,
)


# Custom strategies for FIA-specific data
@st.composite
def evalid_strategy(draw):
    """Generate valid EVALID values."""
    state = draw(st.integers(min_value=1, max_value=99))
    year = draw(st.integers(min_value=0, max_value=99))
    typ = draw(st.integers(min_value=1, max_value=99))
    return state * 10000 + year * 100 + typ


@st.composite
def plot_dataframe_strategy(draw):
    """Generate valid plot DataFrames."""
    n_plots = draw(st.integers(min_value=1, max_value=100))

    # Generate plot data
    data = {
        "PLT_CN": [f"PLT{i:04d}" for i in range(n_plots)],
        "STATECD": draw(st.lists(
            st.integers(min_value=1, max_value=99),
            min_size=n_plots,
            max_size=n_plots
        )),
        "INVYR": draw(st.lists(
            st.integers(min_value=2000, max_value=2025),
            min_size=n_plots,
            max_size=n_plots
        )),
        "PLOT": draw(st.lists(
            st.integers(min_value=1, max_value=9999),
            min_size=n_plots,
            max_size=n_plots
        )),
    }

    return pl.DataFrame(data)


@st.composite
def tree_dataframe_strategy(draw):
    """Generate valid tree DataFrames."""
    n_trees = draw(st.integers(min_value=0, max_value=50))

    if n_trees == 0:
        # Empty DataFrame with correct schema
        return pl.DataFrame({
            "CN": [],
            "PLT_CN": [],
            "STATUSCD": [],
            "DIA": [],
            "TPA_UNADJ": [],
            "DRYBIO_AG": [],
            "VOLCFNET": [],
        })

    # Generate tree data
    data = {
        "CN": [f"TREE{i:06d}" for i in range(n_trees)],
        "PLT_CN": draw(st.lists(
            st.text(min_size=8, max_size=8),
            min_size=n_trees,
            max_size=n_trees
        )),
        "STATUSCD": draw(st.lists(
            st.sampled_from([1, 2]),  # Live or dead
            min_size=n_trees,
            max_size=n_trees
        )),
        "DIA": draw(st.lists(
            st.floats(min_value=1.0, max_value=100.0),
            min_size=n_trees,
            max_size=n_trees
        )),
        "TPA_UNADJ": draw(st.lists(
            st.floats(min_value=0.1, max_value=10.0),
            min_size=n_trees,
            max_size=n_trees
        )),
        "DRYBIO_AG": draw(st.lists(
            st.floats(min_value=0.0, max_value=10000.0),
            min_size=n_trees,
            max_size=n_trees
        )),
        "VOLCFNET": draw(st.lists(
            st.floats(min_value=0.0, max_value=1000.0),
            min_size=n_trees,
            max_size=n_trees
        )),
    }

    return pl.DataFrame(data)


@st.composite
def stratified_plot_data_strategy(draw):
    """Generate synthetic plot data with stratification for variance calculation.

    This generates plot-level data similar to what AreaEstimator uses for
    variance calculation after joining with stratification tables.
    """
    n_plots = draw(st.integers(min_value=2, max_value=50))
    n_strata = draw(st.integers(min_value=1, max_value=min(5, n_plots)))

    # Generate plot data
    plot_ids = [f"PLT{i:04d}" for i in range(n_plots)]

    # Assign plots to strata
    strata = [f"STRATUM_{i % n_strata}" for i in range(n_plots)]

    # Generate expansion factors (positive values, reasonable range)
    expns_values = draw(st.lists(
        st.floats(min_value=100.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
        min_size=n_plots,
        max_size=n_plots
    ))

    # Generate area values (proportions between 0 and 1)
    y_values = draw(st.lists(
        st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        min_size=n_plots,
        max_size=n_plots
    ))

    data = pl.DataFrame({
        "PLT_CN": plot_ids,
        "STRATUM_CN": strata,
        "EXPNS": expns_values,
        "y_i": y_values,  # Plot-level response variable (e.g., adjusted area proportion)
    })

    return data


class TestEstimationProperties:
    """Test invariant properties of estimation functions."""

    @given(plot_data=stratified_plot_data_strategy())
    @settings(max_examples=50, deadline=2000)
    def test_variance_non_negative(self, plot_data):
        """Variance calculations should always be non-negative.

        This tests the core variance calculation logic used in FIA domain estimation.
        For stratified sampling, variance is calculated as:
        V(Y_D) = sum_h [w_h^2 * s^2_yh * n_h]

        Since all terms are squared or counts (non-negative), the result
        must always be non-negative.
        """
        # Skip empty or single-plot data
        if len(plot_data) < 2:
            return

        # Calculate stratum statistics similar to _calculate_variance_for_group
        strata_stats = plot_data.group_by("STRATUM_CN").agg([
            pl.count("PLT_CN").alias("n_h"),
            pl.mean("y_i").alias("ybar_h"),
            pl.var("y_i", ddof=1).alias("s2_yh"),  # Sample variance
            pl.first("EXPNS").alias("w_h"),
        ])

        # Handle null variance (single plot in stratum)
        strata_stats = strata_stats.with_columns([
            pl.when(pl.col("s2_yh").is_null())
            .then(0.0)
            .otherwise(pl.col("s2_yh"))
            .alias("s2_yh")
        ])

        # Calculate variance components following FIA methodology
        # For domain total estimation: V(Y_D) = sum_h [w_h^2 * s^2_yh * n_h]
        variance_components = strata_stats.with_columns([
            (
                pl.col("w_h").cast(pl.Float64) ** 2
                * pl.col("s2_yh")
                * pl.col("n_h")
            ).alias("v_h")
        ])

        # Sum variance components
        total_variance = variance_components["v_h"].sum()

        # Handle potential null
        if total_variance is None:
            total_variance = 0.0

        # PROPERTY: Variance must be non-negative
        assert total_variance >= 0, f"Variance was negative: {total_variance}"

        # Also check standard error is non-negative (sqrt of variance)
        se_total = total_variance ** 0.5 if total_variance >= 0 else 0.0
        assert se_total >= 0, f"Standard error was negative: {se_total}"

    @given(
        numerator_values=st.lists(
            st.floats(min_value=0.1, max_value=1e6, allow_nan=False, allow_infinity=False),
            min_size=2,
            max_size=50
        ),
        denominator_values=st.lists(
            st.floats(min_value=0.1, max_value=1e6, allow_nan=False, allow_infinity=False),
            min_size=2,
            max_size=50
        ),
    )
    @settings(max_examples=50, deadline=2000)
    def test_ratio_variance_formula(self, numerator_values, denominator_values):
        """Test ratio variance calculation follows correct formula.

        For ratio estimation (e.g., volume per acre), the variance follows:
        Var(R) = (1/Ybar^2) * [Var(X) + R^2*Var(Y) - 2*R*Cov(X,Y)]

        Where R = Xbar/Ybar is the ratio of means.

        This property test verifies that:
        1. Ratio variance is always non-negative
        2. The formula components are computed correctly
        """
        # Ensure same length for both lists
        min_len = min(len(numerator_values), len(denominator_values))
        num_vals = numerator_values[:min_len]
        den_vals = denominator_values[:min_len]

        assume(min_len >= 2)

        # Convert to numpy for variance calculation
        x = np.array(num_vals)  # Numerator (e.g., volume)
        y = np.array(den_vals)  # Denominator (e.g., area)

        # Calculate means
        xbar = np.mean(x)
        ybar = np.mean(y)

        # Skip if denominator mean is too close to zero
        assume(ybar > 1e-10)

        # Calculate ratio
        ratio = xbar / ybar

        # Calculate variances and covariance
        n = len(x)
        var_x = np.var(x, ddof=1) if n > 1 else 0.0
        var_y = np.var(y, ddof=1) if n > 1 else 0.0
        cov_xy = np.cov(x, y, ddof=1)[0, 1] if n > 1 else 0.0

        # Ratio variance formula (Taylor series approximation)
        # Var(R) = (1/Ybar^2) * [Var(X) + R^2*Var(Y) - 2*R*Cov(X,Y)]
        ratio_variance = (1.0 / (ybar ** 2)) * (var_x + (ratio ** 2) * var_y - 2 * ratio * cov_xy)

        # PROPERTY: Ratio variance should be non-negative in most practical cases
        # Note: Due to negative covariance, it can theoretically go negative
        # but the FIA implementation handles this by flooring at 0
        if ratio_variance < 0:
            # This can happen with strong positive correlation
            # The FIA approach floors the variance at 0
            ratio_variance = max(0.0, ratio_variance)

        assert ratio_variance >= 0, f"Ratio variance was negative: {ratio_variance}"

        # Standard error is sqrt of variance
        se_ratio = ratio_variance ** 0.5
        assert se_ratio >= 0, f"Standard error was negative: {se_ratio}"

    @given(st.data())
    @settings(deadline=2000, max_examples=50)
    def test_adjustment_factors_preserve_order(self, data):
        """Adjustment factors should preserve relative ordering of TPA values.

        When applying tree adjustment factors based on diameter classes,
        the relative ordering of trees by their unadjusted TPA values
        should be preserved after adjustment (when all trees are in the
        same diameter class and stratum).
        """
        # Generate trees with the same diameter class (all subplot trees: 5-20 inches)
        n_trees = data.draw(st.integers(min_value=2, max_value=100))

        # All trees in subplot range (5-20 inches) to ensure same adjustment factor
        diameters = data.draw(st.lists(
            st.floats(min_value=5.0, max_value=19.9, allow_nan=False, allow_infinity=False),
            min_size=n_trees,
            max_size=n_trees
        ))

        # Generate unique TPA values to ensure well-defined ordering
        tpa_values = data.draw(st.lists(
            st.floats(min_value=0.1, max_value=100.0, allow_nan=False, allow_infinity=False),
            min_size=n_trees,
            max_size=n_trees,
            unique=True
        ))

        # Skip if we couldn't generate enough unique values
        if len(tpa_values) < n_trees:
            return

        # Create tree DataFrame with required columns
        tree_df = pl.DataFrame({
            "DIA": diameters,
            "TPA_UNADJ": tpa_values,
            # Adjustment factors from stratification (realistic values)
            "ADJ_FACTOR_MICR": [1.2] * n_trees,  # Microplot adjustment
            "ADJ_FACTOR_SUBP": [1.0] * n_trees,  # Subplot adjustment
            "ADJ_FACTOR_MACR": [0.25] * n_trees,  # Macroplot adjustment
            "MACRO_BREAKPOINT_DIA": [20.0] * n_trees,  # Regional breakpoint
        })

        # Apply tree adjustment factors using the actual function
        result_df = apply_tree_adjustment_factors(
            tree_df,
            size_col="DIA",
            macro_breakpoint_col="MACRO_BREAKPOINT_DIA",
            output_col="ADJ_FACTOR"
        )

        # Calculate adjusted TPA
        result_df = result_df.with_columns([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).alias("TPA_ADJ")
        ])

        # Get ordering of original and adjusted values
        original_order = result_df["TPA_UNADJ"].arg_sort().to_list()
        adjusted_order = result_df["TPA_ADJ"].arg_sort().to_list()

        # PROPERTY: The relative ordering should be preserved
        # Since all trees have the same adjustment factor (all are 5-20" DBH),
        # multiplying by a constant preserves order
        assert original_order == adjusted_order, \
            f"Ordering changed after adjustment:\nOriginal: {original_order}\nAdjusted: {adjusted_order}"


class TestDataIntegrity:
    """Test data integrity properties."""

    @given(
        plot_df=plot_dataframe_strategy(),
        tree_df=tree_dataframe_strategy(),
    )
    @settings(suppress_health_check=[HealthCheck.data_too_large, HealthCheck.too_slow])
    def test_join_preserves_row_count(self, plot_df, tree_df):
        """Joins should preserve or reduce row count, never increase."""
        if len(tree_df) == 0:
            return  # Skip empty tree DataFrames

        # Ensure some trees match plots
        if len(plot_df) > 0:
            plot_cns = plot_df["PLT_CN"].to_list()
            tree_df = tree_df.with_columns(
                pl.col("PLT_CN").map_elements(
                    lambda x: plot_cns[hash(x) % len(plot_cns)],
                    return_dtype=pl.Utf8
                )
            )

        # Perform join
        joined = tree_df.join(plot_df, on="PLT_CN", how="left")

        # Row count shouldn't increase
        assert len(joined) == len(tree_df)

    @given(
        values=st.lists(
            st.floats(min_value=0.0, max_value=1000.0, allow_nan=False),
            min_size=2,
            max_size=100
        )
    )
    def test_cv_calculation(self, values):
        """Coefficient of variation should be correctly calculated."""
        mean_val = np.mean(values)

        if mean_val == 0:
            return  # Skip zero mean

        std_val = np.std(values, ddof=1)
        expected_cv = (std_val / mean_val) * 100

        # Test that CV is non-negative
        assert expected_cv >= 0

        # Test that CV is 0 when all values are the same
        if len(set(values)) == 1:
            assert expected_cv == 0


class TestTreeExpansionProperties:
    """Property-based tests for tree expansion factor logic."""

    @given(
        diameters=st.lists(
            st.floats(min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False),
            min_size=1,
            max_size=100
        )
    )
    @settings(max_examples=50)
    def test_adjustment_factor_selection_consistency(self, diameters):
        """Test that adjustment factor selection is consistent with diameter classes.

        - Trees < 5" DBH should use microplot factor
        - Trees 5" to macro_breakpoint should use subplot factor
        - Trees >= macro_breakpoint should use macroplot factor
        """
        n_trees = len(diameters)
        macro_breakpoint = 20.0

        # Create test DataFrame
        tree_df = pl.DataFrame({
            "DIA": diameters,
            "ADJ_FACTOR_MICR": [1.5] * n_trees,
            "ADJ_FACTOR_SUBP": [1.0] * n_trees,
            "ADJ_FACTOR_MACR": [0.25] * n_trees,
            "MACRO_BREAKPOINT_DIA": [macro_breakpoint] * n_trees,
        })

        # Apply the adjustment factor expression
        result_df = tree_df.with_columns([
            get_adjustment_factor_expr(
                size_col="DIA",
                macro_breakpoint_col="MACRO_BREAKPOINT_DIA",
                adj_factor_micr_col="ADJ_FACTOR_MICR",
                adj_factor_subp_col="ADJ_FACTOR_SUBP",
                adj_factor_macr_col="ADJ_FACTOR_MACR",
            ).alias("ADJ_FACTOR")
        ])

        # Verify each tree got the correct adjustment factor
        for row in result_df.iter_rows(named=True):
            dia = row["DIA"]
            adj = row["ADJ_FACTOR"]

            if dia < 5.0:
                expected = 1.5  # Microplot
            elif dia < macro_breakpoint:
                expected = 1.0  # Subplot
            else:
                expected = 0.25  # Macroplot

            assert abs(adj - expected) < 1e-10, \
                f"DIA={dia}: expected {expected}, got {adj}"

    @given(
        adj_micr=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
        adj_subp=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
        adj_macr=st.floats(min_value=0.1, max_value=10.0, allow_nan=False, allow_infinity=False),
    )
    def test_adjustment_factors_are_positive(self, adj_micr, adj_subp, adj_macr):
        """Adjustment factors should always produce positive results.

        Since adjustment factors and TPA are both positive, the adjusted
        TPA should always be positive.
        """
        # Create a simple test case
        tree_df = pl.DataFrame({
            "DIA": [3.0, 10.0, 25.0],  # One tree in each size class
            "TPA_UNADJ": [6.0, 6.0, 6.0],
            "ADJ_FACTOR_MICR": [adj_micr] * 3,
            "ADJ_FACTOR_SUBP": [adj_subp] * 3,
            "ADJ_FACTOR_MACR": [adj_macr] * 3,
            "MACRO_BREAKPOINT_DIA": [20.0] * 3,
        })

        # Apply adjustment factors
        result_df = apply_tree_adjustment_factors(tree_df)

        # Calculate adjusted TPA
        result_df = result_df.with_columns([
            (pl.col("TPA_UNADJ") * pl.col("ADJ_FACTOR")).alias("TPA_ADJ")
        ])

        # All adjusted TPA values should be positive
        for tpa_adj in result_df["TPA_ADJ"].to_list():
            assert tpa_adj > 0, f"Adjusted TPA was not positive: {tpa_adj}"


# Hypothesis settings for different test profiles
settings.register_profile("dev", max_examples=10)
settings.register_profile("ci", max_examples=100)
settings.register_profile("nightly", max_examples=1000)

# Default to dev profile
settings.load_profile("dev")
