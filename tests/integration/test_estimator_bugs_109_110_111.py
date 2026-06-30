"""Regression tests for the 1.4.2 estimator bug fixes (#109, #110, #111).

These run against the real Georgia FIA database and skip automatically when no
local database is available (e.g. in CI). The CI-safe guards live in
tests/unit/test_validation.py and tests/unit/test_variance_columns.py.
"""

import pytest

from pyfia import FIA, biomass, growth, mortality, removals, tpa, volume

pytestmark = [pytest.mark.integration, pytest.mark.db]

GA_FIPS = 13


@pytest.fixture
def ga_path(georgia_db_path):
    return georgia_db_path if isinstance(georgia_db_path, str) else str(georgia_db_path)


def _clip(path, token):
    db = FIA(path)
    db.clip_by_state(GA_FIPS)
    db.clip_most_recent(eval_type=token)
    return db


class TestBiomassComponents:
    """#110: every advertised component works; 'root' is rejected cleanly."""

    @pytest.mark.parametrize(
        "component", ["AG", "BG", "TOTAL", "BOLE", "BRANCH", "FOLIAGE"]
    )
    def test_valid_component_runs(self, ga_path, component):
        db = _clip(ga_path, "VOL")
        result = biomass(db, component=component, land_type="forest")
        assert "BIO_ACRE" in result.columns
        assert len(result) > 0

    def test_root_component_rejected(self, ga_path):
        db = _clip(ga_path, "VOL")
        with pytest.raises(ValueError, match="Invalid biomass component"):
            biomass(db, component="ROOT")


class TestVarianceContract:
    """#109: SE always present; *_VARIANCE added iff variance=True; variance==SE**2."""

    @pytest.mark.parametrize(
        "fn,token,kwargs",
        [
            (volume, "VOL", {}),
            (biomass, "VOL", {"component": "AG"}),
            (tpa, "VOL", {}),
            (mortality, "MORT", {"measure": "volume"}),
        ],
        ids=["volume", "biomass", "tpa", "mortality"],
    )
    def test_variance_flag_is_additive(self, ga_path, fn, token, kwargs):
        db = _clip(ga_path, token)
        off = fn(db, totals=True, variance=False, **kwargs)
        on = fn(db, totals=True, variance=True, **kwargs)

        se_cols = [c for c in off.columns if c.endswith("_SE")]
        assert se_cols, "estimator must always report standard errors"

        # Default: no variance columns.
        assert not [c for c in off.columns if "_VARIANCE" in c]

        # variance=True: SE columns retained AND matching variance columns added.
        for se in se_cols:
            assert se in on.columns
            var = se.replace("_SE", "_VARIANCE")
            assert var in on.columns
            if on[se][0] is not None:
                assert on[var][0] == pytest.approx(on[se][0] ** 2, rel=1e-9)


class TestGRMTreeType:
    """#111: GRM estimators accept gs/al/sl/live/sawtimber with the right aliasing."""

    @pytest.mark.parametrize(
        "fn,token", [(mortality, "MORT"), (growth, "GROW"), (removals, "REMV")]
    )
    def test_grm_tree_type_vocabulary(self, ga_path, fn, token):
        db = _clip(ga_path, token)

        def total(tree_type):
            r = fn(db, measure="volume", land_type="forest", tree_type=tree_type)
            tcol = next(c for c in r.columns if c.endswith("_TOTAL"))
            return r[tcol][0]

        gs, al, sl, live, saw = (
            total("gs"),
            total("al"),
            total("sl"),
            total("live"),
            total("sawtimber"),
        )

        # All produce real estimates.
        for v in (gs, al, sl):
            assert v is not None and v > 0

        # Aliases: live == al, sawtimber == sl.
        assert live == pytest.approx(al, rel=1e-9)
        assert saw == pytest.approx(sl, rel=1e-9)

        # The three populations are distinct (all-live > growing stock > sawtimber).
        assert al > gs > sl

    @pytest.mark.parametrize("fn", [mortality, growth, removals])
    def test_grm_rejects_non_grm_tree_types(self, ga_path, fn):
        db = _clip(ga_path, "MORT")
        for bad in ("all", "dead"):
            with pytest.raises(ValueError, match="Invalid tree_type"):
                fn(db, measure="volume", tree_type=bad)
