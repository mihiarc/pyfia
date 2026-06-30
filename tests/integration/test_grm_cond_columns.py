"""Integration tests for the GRM/COND column cluster (#102, #103, #104).

These exercise the real estimators against the Georgia FIA database, which has
the GRM tables, the EXPGROW/EXPMORT/EXPREMV family, and the COND columns
DSTRBCD1/TRTCD1/FORTYPCD. They skip automatically when no local database is
available (e.g. in CI); the CI-safe guards for this cluster live in
tests/unit/test_eval_type_resolution.py, test_variance_join_dtypes.py, and
test_grm_numeric_cast.py.
"""

import pytest

from pyfia import FIA, biomass, growth, mortality, removals

pytestmark = [pytest.mark.integration, pytest.mark.db]

GA_FIPS = 13

# (function, eval_type token, estimator-specific kwargs)
GRM_SPECS = [
    (growth, "GROW", dict(measure="biomass", tree_type="al")),
    (mortality, "MORT", dict(measure="biomass", tree_type="al")),
    (removals, "REMV", dict(measure="biomass", tree_type="al")),
    (biomass, "VOL", dict(component="AG", tree_type="live")),
]


@pytest.fixture
def ga_path(georgia_db_path):
    """String path/connection for building fresh FIA instances per estimator."""
    return georgia_db_path if isinstance(georgia_db_path, str) else str(georgia_db_path)


def _clip(path, token):
    db = FIA(path)
    db.clip_by_state(GA_FIPS)
    db.clip_most_recent(eval_type=token)
    return db


class TestGRMAlias:
    """#102: clip_most_recent(eval_type='GRM') resolves the shared family EVALID."""

    def test_grm_alias_sets_evalid(self, ga_path):
        db = FIA(ga_path)
        db.clip_by_state(GA_FIPS)
        # Must not raise NoEVALIDError, and must select a single shared EVALID.
        db.clip_most_recent(eval_type="GRM")
        assert db.evalid is not None
        assert len(db.evalid) == 1

    def test_grm_alias_matches_component_tokens(self, ga_path):
        db = FIA(ga_path)
        db.clip_by_state(GA_FIPS)
        grm = db.find_evalid(most_recent=True, eval_type="GRM", state=[GA_FIPS])
        grow = db.find_evalid(most_recent=True, eval_type="GROW", state=[GA_FIPS])
        mort = db.find_evalid(most_recent=True, eval_type="MORT", state=[GA_FIPS])
        remv = db.find_evalid(most_recent=True, eval_type="REMV", state=[GA_FIPS])
        # The family share one EVALID, so GRM equals each component's EVALID.
        assert grm == grow == mort == remv
        assert len(grm) == 1


class TestAreaDomainOnCondColumn:
    """#103: area_domain can filter on a COND column that grp_by accepts."""

    @pytest.mark.parametrize(
        "fn,token,kwargs", GRM_SPECS, ids=[s[0].__name__ for s in GRM_SPECS]
    )
    def test_area_domain_dstrbcd1(self, ga_path, fn, token, kwargs):
        db = _clip(ga_path, token)
        # Previously raised ColumnNotFoundError for the GRM estimators.
        result = fn(db, land_type="forest", area_domain="DSTRBCD1 > 0", **kwargs)
        assert result is not None

    @pytest.mark.parametrize(
        "fn,token,kwargs", GRM_SPECS, ids=[s[0].__name__ for s in GRM_SPECS]
    )
    def test_area_domain_trtcd1(self, ga_path, fn, token, kwargs):
        db = _clip(ga_path, token)
        result = fn(db, land_type="forest", area_domain="TRTCD1 == 10", **kwargs)
        assert result is not None


class TestGrpByExtraCondColumn:
    """#104: a 3-column grp_by including TRTCD1 works for all four estimators."""

    GRP = ["FORTYPCD", "DSTRBCD1", "TRTCD1"]

    @pytest.mark.parametrize(
        "fn,token,kwargs", GRM_SPECS, ids=[s[0].__name__ for s in GRM_SPECS]
    )
    def test_grp_by_retains_trtcd1(self, ga_path, fn, token, kwargs):
        db = _clip(ga_path, token)
        result = fn(db, land_type="forest", grp_by=self.GRP, totals=True, **kwargs)
        # The extra COND column must survive into the grouped output.
        assert "TRTCD1" in result.columns
        assert "DSTRBCD1" in result.columns
        assert "FORTYPCD" in result.columns
