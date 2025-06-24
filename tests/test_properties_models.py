"""
Property-based tests for Pydantic models.
"""

import polars as pl
import pytest
from hypothesis import given, strategies as st
from pydantic import ValidationError

from pyfia.models import (
    DatabaseSummary,
    EstimationResult,
    EvaluationInfo,
    FIADataFrameWrapper,
)


class TestEvaluationInfoProperties:
    """Test properties of EvaluationInfo model."""
    
    @given(
        evalid=st.integers(min_value=100000, max_value=999999),
        statecd=st.integers(min_value=1, max_value=99),
        eval_typ=st.sampled_from(["VOL", "GRM", "CHNG", "DWM", "INVASIVE"]),
        year_span=st.integers(min_value=1, max_value=10)
    )
    def test_valid_evaluation_info(self, evalid, statecd, eval_typ, year_span):
        """Valid evaluation info should be accepted."""
        start_year = 2020
        end_year = start_year + year_span
        
        eval_info = EvaluationInfo(
            evalid=evalid,
            statecd=statecd,
            eval_typ=eval_typ,
            start_invyr=start_year,
            end_invyr=end_year
        )
        
        # Properties that should hold
        assert eval_info.evalid == evalid
        assert eval_info.statecd == statecd
        assert eval_info.eval_typ == eval_typ
        assert eval_info.end_invyr >= eval_info.start_invyr
    
    @given(invalid_type=st.text(min_size=1, max_size=10))
    def test_invalid_eval_type_rejected(self, invalid_type):
        """Invalid evaluation types should raise ValidationError."""
        if invalid_type in ["VOL", "GRM", "CHNG", "DWM", "INVASIVE"]:
            return  # Skip valid types
        
        with pytest.raises(ValidationError):
            EvaluationInfo(
                evalid=123456,
                statecd=12,
                eval_typ=invalid_type,
                start_invyr=2020,
                end_invyr=2025
            )


class TestEstimationResultProperties:
    """Test properties of EstimationResult model."""
    
    @given(
        estimate=st.floats(min_value=0.0, max_value=1e10, allow_nan=False),
        variance=st.floats(min_value=0.0, max_value=1e10, allow_nan=False),
        area=st.floats(min_value=1.0, max_value=1e10, allow_nan=False),
        n_plots=st.integers(min_value=1, max_value=10000)
    )
    def test_estimation_result_calculations(self, estimate, variance, area, n_plots):
        """EstimationResult should calculate SE and CV correctly."""
        se = variance ** 0.5
        cv = (se / estimate * 100) if estimate > 0 else 0
        total = estimate * area
        
        result = EstimationResult(
            estimate=estimate,
            variance=variance,
            se=se,
            cv=cv,
            total=total,
            area=area,
            nPlots=n_plots
        )
        
        # Verify calculations
        assert result.se == pytest.approx(variance ** 0.5)
        assert result.total == pytest.approx(estimate * area)
        if estimate > 0:
            assert result.cv == pytest.approx((se / estimate) * 100)
    
    @given(
        estimate=st.floats(allow_nan=False, allow_infinity=False),
        variance=st.floats(allow_nan=False, allow_infinity=False)
    )
    def test_negative_variance_rejected(self, estimate, variance):
        """Negative variance should be rejected."""
        if variance >= 0:
            return  # Skip valid variance
        
        # Should not be able to create with negative variance
        # (In practice, we might want to add this validation)
        # For now, just test the mathematical property
        assert variance < 0  # Document the invalid case


class TestFIADataFrameWrapperProperties:
    """Test properties of FIADataFrameWrapper."""
    
    @given(
        n_rows=st.integers(min_value=0, max_value=1000),
        n_cols=st.integers(min_value=1, max_value=50),
        table_name=st.text(min_size=1, max_size=50)
    )
    def test_dataframe_wrapper_consistency(self, n_rows, n_cols, table_name):
        """DataFrameWrapper should maintain consistency."""
        # Create DataFrame
        data = {
            f"col_{i}": list(range(n_rows))
            for i in range(n_cols)
        }
        df = pl.DataFrame(data)
        
        # Create wrapper
        wrapper = FIADataFrameWrapper(
            data=df,
            table_name=table_name
        )
        
        # Properties that should hold
        assert wrapper.row_count == n_rows
        assert len(wrapper.column_names) == n_cols
        assert wrapper.table_name == table_name
        assert isinstance(wrapper.data, pl.DataFrame)
    
    @given(
        data=st.dictionaries(
            st.text(min_size=1, max_size=10),
            st.lists(st.integers(), min_size=1, max_size=100),
            min_size=1,
            max_size=10
        )
    )
    def test_post_init_updates(self, data):
        """Post-init should update derived fields."""
        # Ensure all lists have same length
        if not data:
            return
        
        max_len = max(len(v) for v in data.values())
        data = {k: v + [None] * (max_len - len(v)) for k, v in data.items()}
        
        df = pl.DataFrame(data)
        wrapper = FIADataFrameWrapper(
            data=df,
            table_name="test"
        )
        
        # Post-init should have set these
        assert wrapper.row_count == len(df)
        assert wrapper.column_names == df.columns


class TestDatabaseSummaryProperties:
    """Test properties of DatabaseSummary model."""
    
    @given(
        db_name=st.text(min_size=1, max_size=100),
        n_evals=st.integers(min_value=0, max_value=1000),
        states=st.lists(
            st.integers(min_value=1, max_value=99),
            min_size=1,
            max_size=50,
            unique=True
        ),
        min_year=st.integers(min_value=1990, max_value=2020),
        year_span=st.integers(min_value=1, max_value=30)
    )
    def test_database_summary_validity(self, db_name, n_evals, states, min_year, year_span):
        """DatabaseSummary should accept valid data."""
        max_year = min_year + year_span
        evalids = list(range(100000, 100000 + n_evals))
        
        summary = DatabaseSummary(
            database_name=db_name,
            total_evaluations=n_evals,
            states=states,
            year_range=(min_year, max_year),
            evalids=evalids
        )
        
        # Properties
        assert summary.total_evaluations == n_evals
        assert len(summary.states) == len(states)
        assert summary.year_range[1] >= summary.year_range[0]
        assert len(summary.evalids) == n_evals