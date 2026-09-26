"""growth() applies tree_domain like removals() and mortality() (#171).

It used to filter only when the expression contained the literal text
"DIA_MIDPT >= 5.0" and silently ignore anything else. Tests run on the
committed Alabama fixture (GRM evaluation 12403).
"""

from __future__ import annotations

import polars as pl
import pytest

from pyfia import FIA, growth
from pyfia.core.exceptions import InvalidDomainError

EVALID_GRM = 12403


def growth_total(db_path, **kwargs) -> float:
    with FIA(str(db_path)) as db:
        db.clip_by_evalid(EVALID_GRM)
        return growth(db, tree_type="gs", measure="volume", **kwargs)["GROWTH_TOTAL"][0]


@pytest.fixture(scope="module")
def by_species(fiadb_fixture_path) -> pl.DataFrame:
    with FIA(str(fiadb_fixture_path)) as db:
        db.clip_by_evalid(EVALID_GRM)
        return growth(db, tree_type="gs", measure="volume", grp_by="SPCD")


@pytest.mark.parametrize("spcd", [131, 611])
def test_species_domain_equals_its_group(fiadb_fixture_path, by_species, spcd):
    """A species domain gives that species' row of the grouped estimate."""
    expected = by_species.filter(pl.col("SPCD") == spcd)["GROWTH_TOTAL"][0]
    got = growth_total(fiadb_fixture_path, tree_domain=f"SPCD == {spcd}")
    assert got == pytest.approx(expected, rel=1e-9)
    assert got < growth_total(fiadb_fixture_path)


def test_species_groups_partition_the_total(fiadb_fixture_path, by_species):
    assert by_species["GROWTH_TOTAL"].sum() == pytest.approx(
        growth_total(fiadb_fixture_path), rel=1e-9
    )


def test_domain_on_a_tree_only_column(fiadb_fixture_path):
    """TREECLCD isn't in the GRM tables; it comes from the tree's TREE record."""
    domained = growth_total(fiadb_fixture_path, tree_domain="TREECLCD == 2")
    assert domained != pytest.approx(growth_total(fiadb_fixture_path), rel=1e-6)


def test_diameter_domains_agree_however_written(fiadb_fixture_path):
    """The literal the old code special-cased still works, and so do others."""
    literal = growth_total(fiadb_fixture_path, tree_domain="DIA_MIDPT >= 5.0")
    other = growth_total(fiadb_fixture_path, tree_domain="DIA_MIDPT >= 5")
    assert other == pytest.approx(literal, rel=1e-12)
    assert literal < growth_total(fiadb_fixture_path)


def test_unknown_column_raises(fiadb_fixture_path):
    with pytest.raises(InvalidDomainError, match="NOT_A_COLUMN"):
        growth_total(fiadb_fixture_path, tree_domain="NOT_A_COLUMN > 1")
