"""
Check FIA code constants against the vendored FIADB User Guide and REF tables.

The code tables come from ``reference/fia_handbook/``, the FIADB User Guide
sections vendored in this repository. The species checks read ``REF_SPECIES``
from the committed Alabama fixture.
"""

import re
from pathlib import Path

import duckdb
import pytest

from pyfia.constants.species import SOUTHERN_PINE_E_SPGRPCD, SOUTHERN_PINE_SPCD
from pyfia.constants.status_codes import LandStatus, TreeComponent

HANDBOOK = Path(__file__).parents[2] / "reference" / "fia_handbook"


def user_guide_codes(section_file: str, column: str) -> dict[str, str]:
    """Parse the ``## Codes: <column>`` table(s) of a User Guide section.

    Returns code -> description. A codes table can be split across several
    markdown tables; parsing stops at the next ``## `` heading.
    """
    lines = (HANDBOOK / section_file).read_text().splitlines()
    escaped = column.replace("_", r"\_")
    heading = f"## Codes: {escaped}"
    start = lines.index(heading) + 1
    codes: dict[str, str] = {}
    for line in lines[start:]:
        if line.startswith("## "):
            break
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 2 or cells[0] in ("Code", "") or set(cells[0]) <= {"-"}:
            continue
        codes[cells[0]] = cells[1]
    return codes


def public_members(cls: type) -> dict[str, object]:
    """Class attributes that aren't private."""
    return {name: getattr(cls, name) for name in dir(cls) if not name.startswith("_")}


class TestLandStatus:
    """LandStatus matches COND_STATUS_CD (User Guide 2.5.9)."""

    @pytest.fixture(scope="class")
    def codes(self):
        return user_guide_codes("fia_section_2_5_cond.md", "COND_STATUS_CD")

    def test_code_set(self, codes):
        assert set(codes) == {"1", "2", "3", "4", "5"}

    def test_every_member_is_a_user_guide_code(self, codes):
        for name, value in public_members(LandStatus).items():
            assert str(value) in codes, f"LandStatus.{name} = {value}"

    @pytest.mark.parametrize(
        "member, meaning",
        [
            ("FOREST", "Accessible forest land"),
            ("NONFOREST", "Nonforest land"),
            ("WATER", "Noncensus water"),
            ("CENSUS_WATER", "Census water"),
            ("NONSAMPLED", "Nonsampled, possibility of forest land"),
        ],
    )
    def test_member_meaning(self, codes, member, meaning):
        assert codes[str(getattr(LandStatus, member))].startswith(meaning)

    @pytest.mark.parametrize("member", ["DENIED_ACCESS", "HAZARDOUS", "INACCESSIBLE"])
    def test_removed_members(self, member):
        """Removed in 1.5.0: FIADB has no such codes (deprecated in 1.4.4)."""
        assert not hasattr(LandStatus, member)


class TestTreeComponent:
    """TreeComponent matches the GRM component codes (User Guide 3.3)."""

    @pytest.fixture(scope="class")
    def codes(self):
        table = user_guide_codes(
            "fia_section_3_3_tree_grm_component.md", "MICR_COMPONENT_AL_FOREST"
        )
        # The PDF extraction splits some codes ("CUT 1"); FIADB stores "CUT1".
        return {
            code
            if code.startswith("N/A") or code == "NOT USED"
            else re.sub(r"\s+", "", code)
            for code in table
        }

    def test_exact_values_are_user_guide_codes(self, codes):
        prefixes = {"CUT", "MORTALITY", "DIVERSION", "REVERSION"}
        for name, value in public_members(TreeComponent).items():
            if name in prefixes:
                continue
            assert value in codes, f"TreeComponent.{name} = {value!r}"

    @pytest.mark.parametrize("prefix", ["CUT", "MORTALITY", "DIVERSION", "REVERSION"])
    def test_prefixes_match_numbered_codes(self, codes, prefix):
        value = getattr(TreeComponent, prefix)
        assert {f"{value}1", f"{value}2"} <= codes

    def test_harvest_removed(self):
        """Removed in 1.5.0: no GRM component is named HARVEST."""
        assert not hasattr(TreeComponent, "HARVEST")


class TestSpeciesGroupsAgainstRef:
    """Species sets equal their REF_SPECIES definitions."""

    def test_southern_pines(self, fiadb_fixture_path):
        with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
            rows = con.execute(
                "SELECT SPCD FROM REF_SPECIES WHERE E_SPGRPCD IN "
                f"({', '.join(map(str, SOUTHERN_PINE_E_SPGRPCD))})"
            ).fetchall()
        assert sorted(r[0] for r in rows) == sorted(SOUTHERN_PINE_SPCD)
