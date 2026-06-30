"""Unit tests for variance-join dtype alignment (issue #105).

biomass(grp_by=DSTRBCD1) crashed on states where the grouping column is null
across every group: per-group variance rows assembled into a DataFrame infer a
polars Null dtype, which cannot join against the Int64 key on the results
frame. align_join_key_dtypes() casts the Null-typed key up to the results dtype
before the join. These tests are synthetic and CI-safe (no database).
"""

import polars as pl

from pyfia.estimation.variance import align_join_key_dtypes


class TestAlignJoinKeyDtypes:
    def test_casts_null_key_to_left_dtype(self):
        left = pl.DataFrame(
            {"FORTYPCD": [161], "DSTRBCD1": [None], "EST": [1.0]},
            schema={"FORTYPCD": pl.Int64, "DSTRBCD1": pl.Int64, "EST": pl.Float64},
        )
        # var_df built from all-None group values -> Null dtype key.
        right = pl.DataFrame({"FORTYPCD": [161], "DSTRBCD1": [None], "SE": [0.5]})
        assert right.schema["DSTRBCD1"] == pl.Null

        aligned = align_join_key_dtypes(left, right, ["FORTYPCD", "DSTRBCD1"])
        assert aligned.schema["DSTRBCD1"] == pl.Int64

    def test_join_succeeds_after_alignment(self):
        # Reproduces the #105 shape: an all-null Int64 key on the left and a
        # Null-dtype key on the right. The raw join raises SchemaError.
        left = pl.DataFrame(
            {"FORTYPCD": [161, 162], "DSTRBCD1": [None, None], "EST": [1.0, 2.0]},
            schema={"FORTYPCD": pl.Int64, "DSTRBCD1": pl.Int64, "EST": pl.Float64},
        )
        right = pl.DataFrame(
            {"FORTYPCD": [161, 162], "DSTRBCD1": [None, None], "SE": [0.1, 0.2]}
        )

        # Without alignment polars refuses the join.
        try:
            left.join(right, on=["FORTYPCD", "DSTRBCD1"], how="left")
            raised = False
        except pl.exceptions.SchemaError:
            raised = True
        assert raised, "expected a SchemaError without dtype alignment"

        # With alignment the join works (no exception).
        right2 = align_join_key_dtypes(left, right, ["FORTYPCD", "DSTRBCD1"])
        joined = left.join(right2, on=["FORTYPCD", "DSTRBCD1"], how="left")
        assert joined.shape[0] == 2

    def test_does_not_change_matching_dtypes(self):
        # When the right key is already typed, the frame is returned unchanged.
        left = pl.DataFrame(
            {"DSTRBCD1": [0, 1], "EST": [1.0, 2.0]},
            schema={"DSTRBCD1": pl.Int64, "EST": pl.Float64},
        )
        right = pl.DataFrame(
            {"DSTRBCD1": [0, 1], "SE": [0.1, 0.2]},
            schema={"DSTRBCD1": pl.Int64, "SE": pl.Float64},
        )
        aligned = align_join_key_dtypes(left, right, ["DSTRBCD1"])
        assert aligned.schema["DSTRBCD1"] == pl.Int64
        # Non-null keyed groups still join to their values unchanged.
        joined = left.join(aligned, on=["DSTRBCD1"], how="left")
        assert joined["SE"].to_list() == [0.1, 0.2]

    def test_ignores_keys_absent_from_a_frame(self):
        left = pl.DataFrame({"A": [1]}, schema={"A": pl.Int64})
        right = pl.DataFrame({"A": [None]})
        # "MISSING" is not in either frame; must be skipped without error.
        aligned = align_join_key_dtypes(left, right, ["A", "MISSING"])
        assert aligned.schema["A"] == pl.Int64
