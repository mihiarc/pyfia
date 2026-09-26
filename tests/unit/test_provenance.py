"""FIA.provenance() and the EVALID stamp on the unit-level builders."""

from __future__ import annotations

import hashlib

import polars as pl

import pyfia
from pyfia import (
    FIA,
    condition_intervals,
    condition_stand_metrics,
    tree_intervals,
)


class TestProvenance:
    def test_unclipped(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            stamp = db.provenance()
        assert stamp["pyfia_version"] == pyfia.__version__
        assert stamp["fiadb_version"] == "FIADB_1.9.4.00"
        assert stamp["evalids"] is None
        assert stamp["database"] == str(fiadb_fixture_path)
        assert stamp["database_bytes"] == fiadb_fixture_path.stat().st_size
        assert stamp["database_modified"].endswith("+00:00")
        assert stamp["database_sha256"] is None

    def test_clipped_with_checksum(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(12403)
            stamp = db.provenance(checksum=True)
        assert stamp["evalids"] == [12403]
        expected = hashlib.sha256(fiadb_fixture_path.read_bytes()).hexdigest()
        assert stamp["database_sha256"] == expected


class TestBuilderEvalid:
    def test_stamped_when_clipped_to_one_evaluation(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(12403)
            frames = [
                condition_intervals(db),
                tree_intervals(db),
                condition_stand_metrics(db),
            ]
        for frame in frames:
            assert frame.schema["EVALID"] == pl.Int64
            assert (frame["EVALID"] == 12403).all()

    def test_null_when_unclipped(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            frames = [
                condition_intervals(db),
                condition_stand_metrics(db, metrics=("ba",)),
            ]
        for frame in frames:
            assert frame["EVALID"].null_count() == frame.height

    def test_null_when_plots_are_named(self, fiadb_fixture_path):
        with FIA(str(fiadb_fixture_path)) as db:
            db.clip_by_evalid(12401)
            some_plot = db.query("SELECT CN FROM PLOT LIMIT 1")["CN"][0]
            frame = condition_stand_metrics(db, plot_cns=[some_plot])
        assert frame["EVALID"].null_count() == frame.height
