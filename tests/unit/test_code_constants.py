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

from pyfia.constants import codes
from pyfia.constants.species import SOUTHERN_PINE_E_SPGRPCD, SOUTHERN_PINE_SPCD
from pyfia.constants.status_codes import LandStatus, TreeComponent
from pyfia.reference import label_codes

HANDBOOK = Path(__file__).parents[2] / "reference" / "fia_handbook"

# PDF page headers that the extraction turned into headings, such as
# "## Condition Table" in the middle of the DSTRBCD1 codes.
PAGE_HEADER = re.compile(r"^## [A-Z][A-Za-z ,]* Table$")


def user_guide_codes(section_file: str, column: str) -> dict[str, str]:
    """Parse the ``## Codes: <column>`` table(s) of a User Guide section.

    Returns code -> description. A codes table can be split across several
    markdown tables and page headers; parsing stops at the next other ``## ``
    heading.
    """
    lines = (HANDBOOK / section_file).read_text().splitlines()
    escaped = column.replace("_", r"\_")
    heading = f"## Codes: {escaped}"
    start = lines.index(heading) + 1
    codes: dict[str, str] = {}
    for line in lines[start:]:
        if line.startswith("## "):
            if PAGE_HEADER.match(line):
                continue
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


COND = "fia_section_2_5_cond.md"
TREE = "fia_section_3_1_tree.md"
GRM = "fia_section_3_3_tree_grm_component.md"


def meaning(description: str) -> str:
    """The rule pyfia.constants.codes documents: the leading term of the
    description (before " - "), without "(core optional)" or a final period."""
    description = re.sub(r"\s*\(\s*core optional\s*\)", "", description)
    term = description.split(" - ", 1)[0]
    return re.sub(r"\s+", " ", term).strip().rstrip(".")


class TestCodeTables:
    """pyfia.constants.codes equals the vendored User Guide "Codes" tables."""

    @pytest.mark.parametrize(
        "name, section, column",
        [
            ("COND_STATUS_CD", COND, "COND_STATUS_CD"),
            ("RESERVCD", COND, "RESERVCD"),
            ("OWNCD", COND, "OWNCD"),
            ("SITECLCD", COND, "SITECLCD"),
            ("STDORGCD", COND, "STDORGCD"),
            ("DSTRBCD", COND, "DSTRBCD1"),
            ("TRTCD", COND, "TRTCD1"),
            ("HARVEST_TYPE_SRS", COND, "HARVEST_TYPE1_SRS"),
            ("STATUSCD", TREE, "STATUSCD"),
            ("TREECLCD", TREE, "TREECLCD"),
            ("AGENTCD", TREE, "AGENTCD"),
        ],
    )
    def test_table_equals_user_guide(self, name, section, column):
        guide = user_guide_codes(section, column)
        expected = {int(code): meaning(desc) for code, desc in guide.items()}
        if name == "STATUSCD":
            # Code 3's description opens "Retired code - ... Removed - Cut and
            # removed ..."; its meaning is the second term.
            assert "Removed - " in guide["3"]
            expected[3] = "Removed"
        assert getattr(codes, name) == expected

    def test_dstrbcd_spans_the_page_break(self):
        # The table continues after a "## Condition Table" page header.
        assert codes.DSTRBCD[54] == "Drought"
        assert codes.DSTRBCD[95] == "Earth movement / avalanches"

    def test_grm_components_equal_user_guide(self):
        guide = user_guide_codes(GRM, "MICR_COMPONENT_AL_FOREST")
        expected = {}
        for code, desc in guide.items():
            # PDF artifacts: "CUT 1" for CUT1 and "TREE. STATUSCD" for TREE.STATUSCD
            spaced = code.startswith("N/A") or code == "NOT USED"
            key = code if spaced else re.sub(r"\s+", "", code)
            text = re.sub(r"TREE\. (?=[A-Z])", "TREE.", re.sub(r"\s+", " ", desc))
            expected[key] = text.strip().rstrip(".")
        assert codes.GRM_COMPONENT == expected
        assert "CUT1" in codes.GRM_COMPONENT

    def test_grm_components_cover_tree_component(self):
        prefixes = {"CUT", "MORTALITY", "DIVERSION", "REVERSION"}
        for name, value in public_members(TreeComponent).items():
            if name not in prefixes:
                assert value in codes.GRM_COMPONENT, f"TreeComponent.{name}"

    def test_user_guide_revision(self):
        # Chapter 2 (COND) and chapter 3 (TREE, GRM) page footers carry it.
        stamps = {
            match
            for path in HANDBOOK.glob("fia_section_[23]_*.md")
            for match in re.findall(
                r"Chapter [23] \(revision: ([\d.]+)\)", path.read_text()
            )
        }
        assert stamps == {codes.USER_GUIDE_REVISION}

    def test_code_tables_columns(self):
        assert codes.CODE_TABLES["TRTCD3"] is codes.TRTCD
        assert codes.CODE_TABLES["DSTRBCD2"] is codes.DSTRBCD
        assert codes.CODE_TABLES["HARVEST_TYPE2_SRS"] is codes.HARVEST_TYPE_SRS
        assert codes.CODE_TABLES["SUBP_COMPONENT_GS_TIMBER"] is codes.GRM_COMPONENT

    @pytest.mark.parametrize(
        "table, column",
        [
            ("COND", "COND_STATUS_CD"),
            ("COND", "RESERVCD"),
            ("COND", "OWNCD"),
            ("COND", "SITECLCD"),
            ("COND", "STDORGCD"),
            ("COND", "DSTRBCD1"),
            ("COND", "DSTRBCD2"),
            ("COND", "DSTRBCD3"),
            ("COND", "TRTCD1"),
            ("COND", "TRTCD2"),
            ("COND", "TRTCD3"),
            ("COND", "HARVEST_TYPE1_SRS"),
            ("COND", "HARVEST_TYPE2_SRS"),
            ("COND", "HARVEST_TYPE3_SRS"),
            ("TREE", "STATUSCD"),
            ("TREE", "TREECLCD"),
            ("TREE", "AGENTCD"),
            ("TREE_GRM_COMPONENT", "SUBP_COMPONENT_AL_FOREST"),
            ("TREE_GRM_COMPONENT", "MICR_COMPONENT_AL_TIMBER"),
        ],
    )
    def test_every_fixture_code_is_labeled(self, fiadb_fixture_path, table, column):
        with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
            values = con.sql(f"SELECT DISTINCT {column} FROM {table}").pl()
        labeled = label_codes(values, column).drop_nulls(column)
        assert labeled.height > 0
        assert labeled[f"{column}_NAME"].null_count() == 0


class TestSpeciesGroupsAgainstRef:
    """Species sets equal their REF_SPECIES definitions."""

    def test_southern_pines(self, fiadb_fixture_path):
        with duckdb.connect(str(fiadb_fixture_path), read_only=True) as con:
            rows = con.execute(
                "SELECT SPCD FROM REF_SPECIES WHERE E_SPGRPCD IN "
                f"({', '.join(map(str, SOUTHERN_PINE_E_SPGRPCD))})"
            ).fetchall()
        assert sorted(r[0] for r in rows) == sorted(SOUTHERN_PINE_SPCD)
