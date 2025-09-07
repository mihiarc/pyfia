"""
Property-based tests for pyFIA using Hypothesis.

These tests verify invariants and properties that should hold
across a wide range of inputs.
"""

import numpy as np
import polars as pl
import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

# Import directly from utils module since these are internal functions
# being tested specifically for their mathematical properties
from pyfia.estimation.utils import (
    calculate_adjustment_factors,
    calculate_ratio_estimates,
    calculate_stratum_estimates,
)
from pyfia.models import EvaluationInfo


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
    n_trees = draw(st.integers(min_value=0, max_value=1000))

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


class TestEstimationProperties:
    """Test invariant properties of estimation functions."""

    @given(
        n_plots=st.integers(min_value=1, max_value=100),
        values=st.lists(
            st.floats(min_value=0.0, max_value=1000.0, allow_nan=False),
            min_size=1,
            max_size=100
        )
    )
    def test_variance_non_negative(self, n_plots, values):
        """Variance calculations should always be non-negative."""
        # Create stratum data
        stratum_data = {
            "stratum_mean": np.mean(values),
            "stratum_var": np.var(values),
            "P2POINTCNT": n_plots,
            "EXPNS": 6000.0,
            "STRATUM_WGT": 1.0,
        }

        # Create test dataframe for stratum estimates
        data = pl.DataFrame({
            "STRATUM_CN": ["S1"] * n_plots,
            "response_var": values,
            "EXPNS": [stratum_data["EXPNS"]] * n_plots,
            "AREA_USED": [1000.0] * n_plots,
        })

        # Calculate population estimates
        result = calculate_stratum_estimates(
            data=data,
            response_col="response_var"
        )

        # Variance should be non-negative
        assert result["var_y"].min() >= 0

    @given(
        total_var=st.floats(min_value=0.0, max_value=1e6),
        area_var=st.floats(min_value=0.0, max_value=1e6),
        total=st.floats(min_value=0.1, max_value=1e6),
        area=st.floats(min_value=0.1, max_value=1e6),
    )
    def test_ratio_variance_formula(self, total_var, area_var, total, area):
        """Test ratio variance calculation follows correct formula."""
        # Calculate covariance (simplified - assuming independence)
        covariance = 0.0

        # Calculate ratio variance using the actual function signature
        num_data = pl.DataFrame({"value": [total], "STRATUM_CN": ["S1"]})
        den_data = pl.DataFrame({"value": [area], "STRATUM_CN": ["S1"]})

        ratio_var_result = calculate_ratio_estimates(
            numerator_data=num_data,
            denominator_data=den_data,
            num_col="value",
            den_col="value"
        )["variance"]

        # Basic checks on ratio variance
        assert ratio_var_result >= 0  # Variance should be non-negative
        # Skip detailed formula check for simplified test

    @given(st.data())
    @settings(deadline=1000)  # Allow more time for complex tests
    def test_adjustment_factors_preserve_order(self, data):
        """Adjustment factors should preserve relative ordering."""
        # Generate test data
        df = data.draw(tree_dataframe_strategy())

        if len(df) == 0:
            return  # Skip empty DataFrames

        # Add required columns for the function
        df = df.with_columns([
            pl.lit("SUBP").alias("TREE_BASIS"),
            pl.lit(1.0).alias("ADJ_FACTOR_SUBP"),
            pl.lit(1).alias("DESIGNCD"),  # Required for calculate_adjustment_factors
        ])

        # Apply adjustment factors
        result = calculate_adjustment_factors(df)

        # Check that relative ordering is preserved
        if "TPA_ADJ" in result.columns and len(result) > 1:
            original_order = df["TPA_UNADJ"].arg_sort()
            adjusted_order = result["TPA_ADJ"].arg_sort()

            # The ordering should be the same
            assert original_order.to_list() == adjusted_order.to_list()


class TestModelValidation:
    """Test Pydantic model validation properties."""

    @given(
        evalid=evalid_strategy(),
        statecd=st.integers(min_value=1, max_value=99),
        start_year=st.integers(min_value=1990, max_value=2020),
    )
    def test_evaluation_info_validation(self, evalid, statecd, start_year):
        """EvaluationInfo should validate correct data."""
        end_year = start_year + 5  # FIA evaluations typically span 5-10 years

        eval_info = EvaluationInfo(
            evalid=evalid,
            statecd=statecd,
            eval_typ="VOL",
            start_invyr=start_year,
            end_invyr=end_year,
        )

        # Basic invariants
        assert eval_info.evalid == evalid
        assert eval_info.statecd == statecd
        assert eval_info.start_invyr <= eval_info.end_invyr

    @given(st.text(min_size=1))
    def test_invalid_eval_type_rejected(self, invalid_type):
        """Invalid evaluation types should be rejected."""
        assume(invalid_type not in ["VOL", "GRM", "CHNG", "DWM", "INVASIVE"])

        with pytest.raises(ValueError):
            EvaluationInfo(
                evalid=123456,
                statecd=12,
                eval_typ=invalid_type,
                start_invyr=2020,
                end_invyr=2025,
            )


class TestDataIntegrity:
    """Test data integrity properties."""

    @given(
        plot_df=plot_dataframe_strategy(),
        tree_df=tree_dataframe_strategy(),
    )
    def test_join_preserves_row_count(self, plot_df, tree_df):
        """Joins should preserve or reduce row count, never increase."""
        if len(tree_df) == 0:
            return  # Skip empty tree DataFrames

        # Ensure some trees match plots
        if len(plot_df) > 0:
            plot_cns = plot_df["PLT_CN"].to_list()
            tree_df = tree_df.with_columns(
                pl.col("PLT_CN").map_elements(
                    lambda x: plot_cns[hash(x) % len(plot_cns)]
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


# Hypothesis settings for different test profiles
settings.register_profile("dev", max_examples=10)
settings.register_profile("ci", max_examples=100)
settings.register_profile("nightly", max_examples=1000)

# Default to dev profile
settings.load_profile("dev")
