"""Unit tests for the LiveTreeEstimator class and live_tree() public function.

Two layers of coverage:

1. **Isolated unit tests** (the bulk of this file) — exercise config wiring,
   column selection, pool dispatch, and input validation against a MockDB
   with no real FIA database. These run in CI unconditionally.

2. **Database smoke test** (``test_live_tree_smoke_ag_pool``) — exercises
   the full template-method pipeline against the Georgia test database
   if available (via the ``georgia_db`` fixture from ``tests/conftest.py``).
   Skipped automatically when no database is present.

The equivalence between the vectorized NSVB pipeline and the scalar oracle
is tested in ``tests/unit/test_nsvb_vectorized.py`` — this file does not
re-check the math, only the estimator plumbing.
"""

from __future__ import annotations

import polars as pl
import pytest

from pyfia.carbon._estimator_base import _uncovered_nonwoodland_spcds
from pyfia.carbon.live_tree import LiveTreeEstimator, live_tree
from pyfia.carbon.nsvb.coefficients import get_vectorized_lookup_tables


class MockDB:
    """Mock database for testing estimator methods in isolation.

    Matches the pattern used in ``test_carbon_pools_estimator.py``. The
    attributes below are the ones ``BaseEstimator.__init__`` touches, plus
    ``_reader`` as a stub so :meth:`LiveTreeEstimator._load_ref_species`
    can be monkey-patched in tests that need it.
    """

    def __init__(self):
        self.db_path = "/fake/path"
        self.tables = {}
        self.evalid = None
        self.evalids = None
        self._state_filter = None
        self._reader = None


# ---------------------------------------------------------------------------
# Isolated estimator config tests
# ---------------------------------------------------------------------------


class TestGetRequiredTables:
    def test_returns_five_core_tables(self):
        """REF_SPECIES is loaded separately in calculate_values, not via the loader."""
        estimator = LiveTreeEstimator(MockDB(), {"pool": "ag", "land_type": "forest"})
        tables = estimator.get_required_tables()
        assert "TREE" in tables
        assert "COND" in tables
        assert "PLOT" in tables
        assert "POP_PLOT_STRATUM_ASSGN" in tables
        assert "POP_STRATUM" in tables
        assert len(tables) == 5

    def test_tables_consistent_across_pools(self):
        for pool in ("ag", "bg", "total"):
            estimator = LiveTreeEstimator(MockDB(), {"pool": pool})
            tables = estimator.get_required_tables()
            assert set(tables) == {
                "TREE",
                "COND",
                "PLOT",
                "POP_PLOT_STRATUM_ASSGN",
                "POP_STRATUM",
            }


class TestGetTreeColumns:
    def test_nsvb_and_bg_bridge_columns(self):
        """All tree columns the NSVB pipeline + BG bridge need are requested."""
        estimator = LiveTreeEstimator(MockDB(), {"pool": "total"})
        cols = estimator.get_tree_columns()
        assert "SPCD" in cols
        assert "DIA" in cols
        assert "HT" in cols
        assert "CULL" in cols
        assert "CARBON_BG" in cols
        assert "TPA_UNADJ" in cols
        # Standard framework columns added by _get_tree_columns()
        assert "CN" in cols
        assert "PLT_CN" in cols
        assert "CONDID" in cols
        assert "STATUSCD" in cols  # live filter
        # ACTUALHT is not loaded in Phase 1 — intact-top assumption
        assert "ACTUALHT" not in cols

    def test_tree_columns_include_grouping(self):
        """grp_by columns are added to the tree column list."""
        estimator = LiveTreeEstimator(MockDB(), {"pool": "ag", "grp_by": "SPGRPCD"})
        cols = estimator.get_tree_columns()
        assert "SPGRPCD" in cols

    def test_tree_columns_include_carbon_ag_for_woodland_substitution(self):
        """CARBON_AG is loaded so woodland species can be routed to the
        FIADB-stored value instead of recomputing to 0 (issue #6)."""
        estimator = LiveTreeEstimator(MockDB(), {"pool": "ag"})
        assert "CARBON_AG" in estimator.get_tree_columns()


class TestNsvbCoverageGuard:
    """The fail-loud guard for species NSVB cannot compute (issue #6).

    Uses the real vendored coefficient bundle so the covered/uncovered
    determination matches production, with synthetic tree frames.
    """

    @pytest.fixture(scope="class")
    def lookup(self):
        return get_vectorized_lookup_tables()

    def test_uncovered_nonwoodland_species_flagged(self, lookup):
        """A species matching neither an NSVB SPCD row nor a Jenkins group
        (here a bogus SPCD with a null Jenkins group) is flagged."""
        frame = pl.LazyFrame(
            {
                "SPCD": [131, 99999],  # loblolly (Jenkins 8) vs. bogus
                "JENKINS_SPGRPCD": [8, None],
                "WOODLAND": ["N", "N"],
            }
        )
        assert _uncovered_nonwoodland_spcds(frame, lookup) == [99999]

    def test_woodland_species_not_flagged(self, lookup):
        """Woodland species are out of NSVB scope but handled via the FIADB
        substitution, so the guard must not flag them."""
        frame = pl.LazyFrame(
            {
                "SPCD": [65, 133],  # Utah juniper, singleleaf pinyon
                "JENKINS_SPGRPCD": [10, 10],  # woodland group, absent from tables
                "WOODLAND": ["Y", "Y"],
            }
        )
        assert _uncovered_nonwoodland_spcds(frame, lookup) == []

    def test_jenkins_fallback_species_not_flagged(self, lookup):
        """Non-woodland species missing from the SPCD table but carrying a
        valid Jenkins group (1-9) resolve via the fallback — not an error."""
        frame = pl.LazyFrame(
            {
                "SPCD": [113, 101],  # limber/whitebark pine: softwood Jenkins
                "JENKINS_SPGRPCD": [4, 4],
                "WOODLAND": ["N", "N"],
            }
        )
        assert _uncovered_nonwoodland_spcds(frame, lookup) == []

    def test_all_covered_returns_empty(self, lookup):
        frame = pl.LazyFrame(
            {
                "SPCD": [131, 65],
                "JENKINS_SPGRPCD": [8, 10],
                "WOODLAND": ["N", "Y"],
            }
        )
        assert _uncovered_nonwoodland_spcds(frame, lookup) == []

    def test_guard_raises_naming_offending_spcds(self, lookup):
        """``_guard_nsvb_coverage`` raises (not silently zeroes) and names the
        uncovered non-woodland SPCD."""
        estimator = LiveTreeEstimator(MockDB(), {"pool": "ag"})
        frame = pl.LazyFrame(
            {
                "SPCD": [131, 99999],
                "JENKINS_SPGRPCD": [8, None],
                "WOODLAND": ["N", "N"],
            }
        )
        with pytest.raises(ValueError, match="99999"):
            estimator._guard_nsvb_coverage(frame, lookup)

    def test_guard_passes_for_covered_and_woodland(self, lookup):
        """The guard does not raise when every species is covered or woodland."""
        estimator = LiveTreeEstimator(MockDB(), {"pool": "ag"})
        frame = pl.LazyFrame(
            {
                "SPCD": [131, 65],
                "JENKINS_SPGRPCD": [8, 10],
                "WOODLAND": ["N", "Y"],
            }
        )
        estimator._guard_nsvb_coverage(frame, lookup)  # must not raise


class TestWoodlandCarbonSubstitution:
    """Base-class ``_substitute_woodland_carbon_ag`` — shared by the live-tree
    and standing-dead estimators (issue #6).

    Woodland species recompute to 0 under NSVB; the substitution swaps in
    FIADB-stored ``CARBON_AG`` for them while leaving non-woodland species on
    the NSVB-recomputed value.
    """

    def test_woodland_rows_take_fiadb_carbon_ag(self):
        estimator = LiveTreeEstimator(MockDB(), {"pool": "ag"})
        frame = pl.LazyFrame(
            {
                "WOODLAND": ["Y", "N", "Y"],
                "CARBON_AG": [123.0, 999.0, None],  # FIADB stored (lb)
                "_CARBON_AG_LB": [0.0, 50.0, 0.0],  # NSVB recompute (woodland=0)
            }
        )
        out = estimator._substitute_woodland_carbon_ag(frame).collect()
        # Woodland row -> FIADB CARBON_AG; non-woodland keeps NSVB value;
        # woodland with null CARBON_AG -> fill_null(0.0).
        assert out["_CARBON_AG_LB"].to_list() == [123.0, 50.0, 0.0]


class TestLiveTreeFunctionValidation:
    """Input validation for the public ``live_tree()`` function."""

    def test_invalid_pool_raises(self):
        with pytest.raises(ValueError, match="Invalid pool"):
            live_tree(MockDB(), pool="xyz")

    def test_pool_case_insensitive(self, monkeypatch):
        """pool='AG', 'Ag', 'ag' are all accepted.

        Uses monkeypatch to prevent the estimator from touching a real DB —
        we only want to verify that validation accepts the input.
        """
        # Short-circuit ensure_fia_instance + ensure_evalid_set so we don't
        # actually run an estimation. We just need the validation path to
        # pass without raising for the case variants.
        # importlib is used because ``pyfia.carbon.__init__`` re-exports the
        # ``live_tree`` function, shadowing the submodule name under normal
        # attribute access.
        import importlib

        # ensure_fia_instance / ensure_evalid_set live in the shared
        # _estimator_base runner (run_carbon_estimator) since the #127 hoist.
        estimator_base_module = importlib.import_module("pyfia.carbon._estimator_base")

        def fake_ensure_fia_instance(db):
            return (MockDB(), False)

        def fake_ensure_evalid_set(*args, **kwargs):
            pass

        class ShortCircuitError(Exception):
            pass

        def fake_estimate(self):
            raise ShortCircuitError()

        monkeypatch.setattr(
            estimator_base_module, "ensure_fia_instance", fake_ensure_fia_instance
        )
        monkeypatch.setattr(
            estimator_base_module, "ensure_evalid_set", fake_ensure_evalid_set
        )
        monkeypatch.setattr(LiveTreeEstimator, "estimate", fake_estimate)

        for variant in ("ag", "AG", "Ag", "bg", "BG", "total", "TOTAL"):
            with pytest.raises(ShortCircuitError):
                live_tree(MockDB(), pool=variant)

    def test_total_is_valid_pool(self, monkeypatch):
        """'total' is a valid pool value (AG + BG bridge)."""
        # Validation path should accept 'total' without raising.
        import importlib

        # ensure_fia_instance / ensure_evalid_set live in the shared
        # _estimator_base runner (run_carbon_estimator) since the #127 hoist.
        estimator_base_module = importlib.import_module("pyfia.carbon._estimator_base")

        def fake_ensure_fia_instance(db):
            return (MockDB(), False)

        def fake_ensure_evalid_set(*args, **kwargs):
            pass

        class ShortCircuitError(Exception):
            pass

        def fake_estimate(self):
            raise ShortCircuitError()

        monkeypatch.setattr(
            estimator_base_module, "ensure_fia_instance", fake_ensure_fia_instance
        )
        monkeypatch.setattr(
            estimator_base_module, "ensure_evalid_set", fake_ensure_evalid_set
        )
        monkeypatch.setattr(LiveTreeEstimator, "estimate", fake_estimate)

        with pytest.raises(ShortCircuitError):
            live_tree(MockDB(), pool="total")


# ---------------------------------------------------------------------------
# Full end-to-end smoke test (skipped if no FIA database present)
# ---------------------------------------------------------------------------


class TestLiveTreeEndToEnd:
    """End-to-end smoke test using the Georgia test database fixture.

    Skipped automatically when no database is available — the
    ``georgia_db_path`` fixture in ``tests/conftest.py`` calls
    ``pytest.skip(...)`` if it can't find a DuckDB file or a MotherDuck token.
    """

    def test_live_tree_ag_pool_runs_to_completion(self, georgia_db):
        """``live_tree(db, pool='ag', most_recent=True)`` returns a non-empty,
        positively-valued DataFrame with the expected column layout."""
        result = live_tree(georgia_db, pool="ag", most_recent=True)

        assert result.height > 0, "live_tree returned an empty DataFrame"

        # Canonical column layout
        assert "YEAR" in result.columns
        assert "POOL" in result.columns
        assert "CARBON_ACRE" in result.columns
        assert "CARBON_TOTAL" in result.columns
        assert "N_PLOTS" in result.columns
        assert "N_TREES" in result.columns

        # POOL column tags the estimate
        assert result["POOL"][0] == "AG"

        # Per-acre carbon should be positive and within a plausible range
        # (forestland typically runs 10-50 short tons/acre of live tree AG carbon)
        carbon_acre = result["CARBON_ACRE"][0]
        assert carbon_acre > 0, f"Non-positive CARBON_ACRE: {carbon_acre}"
        assert carbon_acre < 500, f"Implausibly large CARBON_ACRE: {carbon_acre}"

    def test_live_tree_bg_pool_uses_fiadb_bridge(self, georgia_db):
        """``pool='bg'`` returns belowground carbon via the FIADB bridge."""
        result = live_tree(georgia_db, pool="bg", most_recent=True)

        assert result.height > 0
        assert result["POOL"][0] == "BG"
        carbon_acre = result["CARBON_ACRE"][0]
        assert carbon_acre > 0
        # BG is typically 15-25% of AG, so should be smaller in absolute terms.
        assert carbon_acre < 100

    def test_live_tree_total_is_sum_of_components(self, georgia_db):
        """``pool='total'`` ≈ ``pool='ag'`` + ``pool='bg'`` at the per-acre level.

        Not strictly additive to floating-point precision because of
        ratio-of-means aggregation, but should agree to within a fraction
        of a percent.
        """
        ag = live_tree(georgia_db, pool="ag", most_recent=True)
        bg = live_tree(georgia_db, pool="bg", most_recent=True)
        total = live_tree(georgia_db, pool="total", most_recent=True)

        ag_acre = ag["CARBON_ACRE"][0]
        bg_acre = bg["CARBON_ACRE"][0]
        total_acre = total["CARBON_ACRE"][0]

        # Relative difference should be <1%
        assert abs(total_acre - (ag_acre + bg_acre)) / total_acre < 0.01, (
            f"total={total_acre}, ag+bg={ag_acre + bg_acre}"
        )

    def test_live_tree_by_species_groups(self, georgia_db):
        """``by_species=True`` produces multiple rows, one per species."""
        result = live_tree(georgia_db, pool="ag", by_species=True, most_recent=True)
        assert result.height > 1
        assert "SPCD" in result.columns


# ---------------------------------------------------------------------------
# Regression tests for the #125 / #126 / #127 cleanup
# ---------------------------------------------------------------------------


class TestSetupGroupingDedup:
    """``BaseEstimator._setup_grouping`` deduplicates group columns (issue #125).

    ``grp_by="SPCD"`` combined with ``by_species=True`` previously produced
    ``["SPCD", "SPCD"]``, which polars' ``group_by`` rejects with a
    ``DuplicateError``. The fix lives in the shared base method, so it is
    exercised here through a carbon estimator instance.
    """

    def test_grp_by_spcd_plus_by_species_dedups(self):
        est = LiveTreeEstimator(
            MockDB(), {"pool": "ag", "grp_by": ["SPCD"], "by_species": True}
        )
        assert est._setup_grouping() == ["SPCD"]

    def test_grp_by_list_with_spcd_and_by_species_dedups(self):
        est = LiveTreeEstimator(
            MockDB(),
            {"pool": "ag", "grp_by": ["OWNGRPCD", "SPCD"], "by_species": True},
        )
        # SPCD kept once; first-seen order preserved.
        assert est._setup_grouping() == ["OWNGRPCD", "SPCD"]

    def test_no_duplicates_preserves_order(self):
        est = LiveTreeEstimator(
            MockDB(),
            {"pool": "ag", "grp_by": ["OWNGRPCD", "FORTYPCD"], "by_species": True},
        )
        assert est._setup_grouping() == ["OWNGRPCD", "FORTYPCD", "SPCD"]

    def test_duplicate_within_grp_by_dedups(self):
        est = LiveTreeEstimator(
            MockDB(), {"pool": "ag", "grp_by": ["OWNGRPCD", "OWNGRPCD"]}
        )
        assert est._setup_grouping() == ["OWNGRPCD"]


class TestTopLevelExports:
    """Carbon estimators are re-exported from the top-level package (issue #127)."""

    def test_live_tree_and_standing_dead_exported(self):
        import pyfia
        from pyfia.carbon.standing_dead import standing_dead as _standing_dead

        assert pyfia.live_tree is live_tree
        assert pyfia.standing_dead is _standing_dead
        assert "live_tree" in pyfia.__all__
        assert "standing_dead" in pyfia.__all__


class _FakeBackend:
    def __init__(self, exists: bool):
        self._exists = exists

    def table_exists(self, table_name: str) -> bool:
        return self._exists


class _FakeReader:
    def __init__(self, backend, read_error=None, frame=None):
        self._backend = backend
        self._read_error = read_error
        self._frame = frame

    def read_table(self, table_name, columns=None):
        if self._read_error is not None:
            raise self._read_error
        return self._frame


class _MockDBWithReader(MockDB):
    def __init__(self, reader):
        super().__init__()
        self._reader = reader


class TestLoadPlotgeomErrorHandling:
    """``_load_plotgeom`` gates on table existence, not a broad except (issue #126)."""

    def test_missing_table_returns_none(self):
        reader = _FakeReader(_FakeBackend(exists=False))
        est = LiveTreeEstimator(_MockDBWithReader(reader), {"pool": "ag"})
        assert est._load_plotgeom() is None

    def test_read_error_on_existing_table_propagates(self):
        # Table exists, but the read fails: the error must NOT be swallowed into
        # the silent DIVISION-disabled fallback (that hides a ~3% biomass bias).
        reader = _FakeReader(
            _FakeBackend(exists=True), read_error=RuntimeError("backend boom")
        )
        est = LiveTreeEstimator(_MockDBWithReader(reader), {"pool": "ag"})
        with pytest.raises(RuntimeError, match="backend boom"):
            est._load_plotgeom()

    def test_present_table_loads(self):
        frame = pl.DataFrame({"CN": ["p1", "p2"], "ECOSUBCD": ["M261A ", "222J "]})
        reader = _FakeReader(_FakeBackend(exists=True), frame=frame)
        est = LiveTreeEstimator(_MockDBWithReader(reader), {"pool": "ag"})
        result = est._load_plotgeom()
        assert result is not None
        assert set(result.columns) == {"PLT_CN", "ECOSUBCD"}
        assert result.height == 2


class TestCleanupRegressionEndToEnd:
    """End-to-end regressions for #125 / #127 on the Georgia test database.

    Skipped automatically when no database is available (``georgia_db``
    fixture calls ``pytest.skip``).
    """

    def test_by_size_class_returns_size_class_column(self, georgia_db):
        """``by_size_class=True`` yields a SIZE_CLASS column with multiple
        standard FIA diameter classes, and the per-class totals sum to the
        ungrouped total (issue #125)."""
        result = live_tree(georgia_db, pool="ag", by_size_class=True, most_recent=True)
        assert "SIZE_CLASS" in result.columns
        classes = set(result["SIZE_CLASS"].to_list())
        assert len(classes) > 1
        assert classes <= {"1.0-4.9", "5.0-9.9", "10.0-19.9", "20.0-29.9", "30.0+"}
        # Partition check: size-class totals sum to the ungrouped total.
        grouped_total = result["CARBON_TOTAL"].sum()
        ungrouped = live_tree(georgia_db, pool="ag", most_recent=True)["CARBON_TOTAL"][
            0
        ]
        assert abs(grouped_total - ungrouped) / ungrouped < 1e-6

    def test_grp_by_spcd_with_by_species_does_not_raise(self, georgia_db):
        """``grp_by='SPCD'`` + ``by_species=True`` no longer raises a
        DuplicateError and yields a single SPCD grouping column (issue #125)."""
        result = live_tree(
            georgia_db,
            pool="ag",
            grp_by="SPCD",
            by_species=True,
            most_recent=True,
        )
        assert result.columns.count("SPCD") == 1
        # Equivalent to by_species alone after dedup.
        by_species = live_tree(georgia_db, pool="ag", by_species=True, most_recent=True)
        assert result.height == by_species.height

    def test_totals_false_variance_true_no_orphan_total_se(self, georgia_db):
        """``totals=False`` must not emit a CARBON_TOTAL_SE with no
        CARBON_TOTAL (issue #127)."""
        result = live_tree(
            georgia_db, pool="ag", totals=False, variance=True, most_recent=True
        )
        assert "CARBON_TOTAL" not in result.columns
        assert "CARBON_TOTAL_SE" not in result.columns
        # The per-acre SE is still present.
        assert "CARBON_ACRE_SE" in result.columns
