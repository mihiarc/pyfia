"""
Property-based tests for area estimation module.
"""

import polars as pl
import pytest
from hypothesis import assume, given, settings, strategies as st

from pyfia.area import _calculate_area_estimates


@st.composite
def area_condition_data(draw):
    """Generate condition data for area testing."""
    n_conditions = draw(st.integers(min_value=1, max_value=50))
    
    # Generate conditions
    data = {
        "PLT_CN": draw(st.lists(
            st.text(min_size=8, max_size=8),
            min_size=n_conditions,
            max_size=n_conditions
        )),
        "CONDID": draw(st.lists(
            st.integers(min_value=1, max_value=9),
            min_size=n_conditions,
            max_size=n_conditions
        )),
        "COND_STATUS_CD": draw(st.lists(
            st.sampled_from([1, 2, 3]),  # Forest, Non-forest, Water
            min_size=n_conditions,
            max_size=n_conditions
        )),
        "CONDPROP_UNADJ": draw(st.lists(
            st.floats(min_value=0.0, max_value=1.0),
            min_size=n_conditions,
            max_size=n_conditions
        )),
        "EXPNS": draw(st.lists(
            st.floats(min_value=1000.0, max_value=10000.0),
            min_size=n_conditions,
            max_size=n_conditions
        )),
        "STRATUM_CN": draw(st.lists(
            st.text(min_size=8, max_size=8),
            min_size=n_conditions,
            max_size=n_conditions
        )),
    }
    
    return pl.DataFrame(data)


class TestAreaEstimationProperties:
    """Test properties of area estimation."""
    
    @given(cond_df=area_condition_data())
    def test_area_always_positive(self, cond_df):
        """Estimated area should always be non-negative."""
        if len(cond_df) == 0:
            return
        
        # Add required columns
        cond_df = cond_df.with_columns([
            pl.lit(1).alias("EVALID"),
            pl.lit("Test").alias("GROUP_BY_COL"),
        ])
        
        # Calculate estimates
        result = _calculate_area_estimates(
            cond_df, 
            ["GROUP_BY_COL"], 
            method="TI"
        )
        
        # All area estimates should be non-negative
        assert (result["ESTIMATE"] >= 0).all()
    
    @given(cond_df=area_condition_data())
    def test_condition_proportions_sum(self, cond_df):
        """Condition proportions should sum to <= 1 per plot."""
        if len(cond_df) == 0:
            return
        
        # Sum proportions by plot
        plot_sums = (
            cond_df
            .group_by("PLT_CN")
            .agg(pl.sum("CONDPROP_UNADJ").alias("total_prop"))
        )
        
        # Each plot's proportions should sum to <= 1
        # (allowing small floating point errors)
        assert (plot_sums["total_prop"] <= 1.001).all()
    
    @given(
        n_plots=st.integers(min_value=10, max_value=100),
        forest_proportion=st.floats(min_value=0.0, max_value=1.0)
    )
    def test_forest_area_bounded(self, n_plots, forest_proportion):
        """Forest area should be bounded by total area."""
        # Create synthetic data
        data = {
            "PLT_CN": [f"P{i:04d}" for i in range(n_plots)],
            "CONDID": [1] * n_plots,
            "COND_STATUS_CD": [1 if i < int(n_plots * forest_proportion) else 2 
                               for i in range(n_plots)],
            "CONDPROP_UNADJ": [1.0] * n_plots,
            "EXPNS": [6000.0] * n_plots,
            "STRATUM_CN": ["S001"] * n_plots,
            "EVALID": [1] * n_plots,
        }
        
        cond_df = pl.DataFrame(data)
        
        # Calculate total area
        total_result = _calculate_area_estimates(
            cond_df,
            [],
            method="TI"
        )
        
        # Calculate forest area
        forest_df = cond_df.filter(pl.col("COND_STATUS_CD") == 1)
        if len(forest_df) > 0:
            forest_result = _calculate_area_estimates(
                forest_df,
                [],
                method="TI"
            )
            
            # Forest area should not exceed total area
            assert forest_result["ESTIMATE"][0] <= total_result["ESTIMATE"][0]
        
    @given(cond_df=area_condition_data())
    @settings(deadline=2000)
    def test_cv_calculation_consistency(self, cond_df):
        """CV calculation should be consistent with SE and estimate."""
        if len(cond_df) < 2:  # Need at least 2 conditions for variance
            return
        
        # Add required columns
        cond_df = cond_df.with_columns([
            pl.lit(1).alias("EVALID"),
        ])
        
        # Calculate estimates
        result = _calculate_area_estimates(
            cond_df,
            [],
            method="TI"
        )
        
        if len(result) > 0 and result["ESTIMATE"][0] > 0:
            # CV = (SE / Estimate) * 100
            expected_cv = (result["SE"][0] / result["ESTIMATE"][0]) * 100
            
            # Check consistency (allowing for floating point errors)
            assert abs(result["SE_PERCENT"][0] - expected_cv) < 0.01


class TestAreaDomainFiltering:
    """Test domain filtering in area estimation."""
    
    @given(
        cond_df=area_condition_data(),
        filter_value=st.integers(min_value=1, max_value=3)
    )
    def test_domain_filter_reduces_area(self, cond_df, filter_value):
        """Applying domain filter should reduce or maintain area."""
        if len(cond_df) == 0:
            return
        
        # Add required columns
        cond_df = cond_df.with_columns([
            pl.lit(1).alias("EVALID"),
        ])
        
        # Calculate total area
        total_result = _calculate_area_estimates(
            cond_df,
            [],
            method="TI"
        )
        
        # Calculate filtered area
        filtered_df = cond_df.filter(pl.col("COND_STATUS_CD") == filter_value)
        if len(filtered_df) > 0:
            filtered_result = _calculate_area_estimates(
                filtered_df,
                [],
                method="TI"
            )
            
            # Filtered area should not exceed total
            assert filtered_result["ESTIMATE"][0] <= total_result["ESTIMATE"][0]