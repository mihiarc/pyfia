"""
FIADB User Guide code tables: one ``code -> meaning`` mapping per column.

Meanings come from the "Codes" tables of the FIADB User Guide vendored in
``reference/fia_handbook/`` (chapters 2 and 3, revision 12.2024). A meaning is
the leading term of the code's description, the text before " - " (or the
whole description when it has none), without its final period or
"(core optional)" notes. The one exception is STATUSCD 3, whose description
opens with "Retired code" and names the code "Removed". GRM component meanings
are the full descriptions.

``tests/unit/test_code_constants.py`` re-derives every mapping from the
vendored guide, so a change here or in the guide shows up as a failing test.

Use :func:`pyfia.reference.label_codes` to add these meanings to a frame, and
:func:`pyfia.reference.join_reference` for codes defined by REF tables
(forest types, species, owner groups, survey units, counties).
"""

from __future__ import annotations

#: Revision of the vendored FIADB User Guide chapters these tables come from.
USER_GUIDE_REVISION = "12.2024"

#: COND.COND_STATUS_CD (User Guide 2.5.9).
COND_STATUS_CD: dict[int, str] = {
    1: "Accessible forest land",
    2: "Nonforest land",
    3: "Noncensus water",
    4: "Census water",
    5: "Nonsampled, possibility of forest land",
}

#: COND.RESERVCD (User Guide 2.5.11).
RESERVCD: dict[int, str] = {
    0: "Not reserved",
    1: "Reserved",
}

#: COND.OWNCD (User Guide 2.5.12). FIADB publishes these codes; the detailed
#: private-owner codes 41-45 are withheld under FIA's data confidentiality
#: policy and appear as 46.
OWNCD: dict[int, str] = {
    11: "National Forest",
    12: "National Grassland and/or Prairie",
    13: "Other Forest Service land",
    21: "National Park Service",
    22: "Bureau of Land Management",
    23: "Fish and Wildlife Service",
    24: "Departments of Defense/Energy",
    25: "Other Federal",
    31: "State including State public universities",
    32: "Local (County, Municipality, etc.) including water authorities",
    33: "Other non-Federal public",
    46: "Undifferentiated private and Native American",
}

#: COND.SITECLCD, site productivity class (User Guide 2.5.21).
SITECLCD: dict[int, str] = {
    1: "225+ cubic feet/acre/year",
    2: "165-224 cubic feet/acre/year",
    3: "120-164 cubic feet/acre/year",
    4: "85-119 cubic feet/acre/year",
    5: "50-84 cubic feet/acre/year",
    6: "20-49 cubic feet/acre/year",
    7: "0-19 cubic feet/acre/year",
}

#: COND.STDORGCD, stand origin (User Guide 2.5.25).
STDORGCD: dict[int, str] = {
    0: "Natural stands",
    1: "Clear evidence of artificial regeneration",
}

#: COND.DSTRBCD1, DSTRBCD2 and DSTRBCD3 (User Guide 2.5.37).
DSTRBCD: dict[int, str] = {
    0: "No visible disturbance",
    10: "Insect damage",
    11: "Insect damage to understory vegetation",
    12: "Insect damage to trees, including seedlings and saplings",
    20: "Disease damage",
    21: "Disease damage to understory vegetation",
    22: "Disease damage to trees, including seedlings and saplings",
    30: "Fire damage (from crown and ground fire, either prescribed or natural)",
    31: "Ground fire damage",
    32: "Crown fire damage",
    40: "Animal damage",
    41: "Beaver (includes flooding caused by beaver)",
    42: "Porcupine",
    43: "Deer/ungulate",
    44: "Bear",
    45: "Rabbit",
    46: "Domestic animal/livestock (includes grazing)",
    50: "Weather damage",
    51: "Ice",
    52: "Wind (includes hurricane, tornado)",
    53: "Flooding (weather induced)",
    54: "Drought",
    60: "Vegetation (suppression, competition, vines)",
    70: "Unknown / not sure / other",
    80: "Human-induced damage",
    90: "Geologic disturbances",
    91: "Landslide",
    92: "Avalanche track",
    93: "Volcanic blast zone",
    94: "Other geologic event",
    95: "Earth movement / avalanches",
}

#: COND.TRTCD1, TRTCD2 and TRTCD3 (User Guide 2.5.43).
TRTCD: dict[int, str] = {
    0: "No observable treatment",
    10: "Cutting",
    20: "Site preparation",
    30: "Artificial regeneration",
    40: "Natural regeneration",
    50: "Other silvicultural treatment",
}

#: COND.HARVEST_TYPE1_SRS through HARVEST_TYPE3_SRS (User Guide 2.5.87),
#: recorded only by the Southern Research Station when TRTCD = 10.
HARVEST_TYPE_SRS: dict[int, str] = {
    11: "Clearcut harvest",
    12: "Partial harvest",
    13: "Seed-tree/shelterwood harvest",
    14: "Commercial thinning",
    15: "Timber stand improvement (cut trees only)",
    16: "Salvage cutting",
}

#: TREE.STATUSCD (User Guide 3.1.15).
STATUSCD: dict[int, str] = {
    0: "No status",
    1: "Live tree",
    2: "Dead tree",
    3: "Removed",
}

#: TREE.TREECLCD (User Guide 3.1.23).
TREECLCD: dict[int, str] = {
    2: "Growing stock",
    3: "Rough cull",
    4: "Rotten cull",
}

#: TREE.AGENTCD, cause of death (User Guide 3.1.27). The guide lists ranges:
#: state programs record specific codes within them (11-19 are insects, for
#: example), so a code's meaning is that of its tens range. See RANGE_CODED.
AGENTCD: dict[int, str] = {
    0: "No agent recorded (only allowed on live trees in data prior to 1999)",
    10: "Insect",
    20: "Disease",
    30: "Fire",
    40: "Animal",
    50: "Weather",
    60: "Vegetation (e.g., suppression, competition, vines/kudzu)",
    70: "Unknown / not sure / other",
    80: "Silvicultural or landclearing activity (death caused by harvesting or other silvicultural activity, including girdling, chaining, etc., or other landclearing activity)",
}

#: TREE_GRM_COMPONENT component codes, e.g. SUBP_COMPONENT_GS_FOREST
#: (User Guide 3.3.13). The guide's PDF splits "CUT1" as "CUT 1"; FIADB
#: stores "CUT1".
GRM_COMPONENT: dict[str, str] = {
    "CUT0": (
        "Tree was killed due to harvesting activity by T2 ((TREE.STATUSCD = "
        "3) or (TREE.STATUSCD = 2 and TREE.AGENTCD = 80)). Applicable only "
        "in periodic-to-periodic, periodic-to-annual, and modeled GRM "
        "estimates"
    ),
    "CUT1": (
        "Tree was previously in estimate at T1 and was killed due to "
        "harvesting activity by T2. The tree must be in the same land basis "
        "(forest land or timberland) at time T1 and T2"
    ),
    "CUT2": (
        "Tree grew across minimum threshold diameter for the estimate since "
        "T1 and was killed due to harvesting activity by T2. The tree must "
        "be in the same land basis (forest land or timberland) at time T1 "
        "and T2"
    ),
    "INGROWTH": (
        "Tree grew across minimum threshold diameter for the estimate since "
        "T1. For example, a sapling grows across the 5-inch diameter "
        "threshold becoming ingrowth on the subplot"
    ),
    "MORTALITY0": (
        "Tree died of natural causes by T2 (TREE.AGENTCD <> 80). Applicable "
        "only in periodic-to-periodic, periodic-to-annual, and modeled GRM "
        "estimates"
    ),
    "MORTALITY1": (
        "Tree was previously in estimate at T1 and died of natural causes "
        "by T2 (TREE.AGENTCD <> 80)"
    ),
    "MORTALITY2": (
        "Tree grew across minimum threshold diameter for the estimate since "
        "T1 and died of natural causes by T2 (TREE.AGENTCD <> 80)"
    ),
    "NOT USED": ("Tree was either live or dead at T1 and has no status at T2"),
    "SURVIVOR": ("Tree has remained live and in the estimate from T1 through T2"),
    "UNKNOWN": (
        "Tree lacks information required to classify component usually due "
        "to procedural changes"
    ),
    "REVERSION1": (
        "Tree grew across minimum threshold diameter for the estimate by "
        "the midpoint of the measurement interval and the condition "
        "reverted to the land basis by T2"
    ),
    "REVERSION2": (
        "Tree grew across minimum threshold diameter for the estimate after "
        "the midpoint of the measurement interval and the condition "
        "reverted to the land basis by T2"
    ),
    "DIVERSION0": (
        "Tree was removed from the estimate by something other than "
        "harvesting activity by T2 (not (TREE.STATUSCD = 3) and not "
        "(TREE.STATUSCD = 2 and TREE.AGENTCD = 80)). Applicable only in "
        "periodic-to-periodic, periodic-to-annual, and modeled GRM "
        "estimates"
    ),
    "DIVERSION1": (
        "Tree was previously in estimate at T1 and the condition diverted "
        "from the land basis by T2. This component assignment is not "
        "dependent upon tree status (TREE.STATUSCD). For example, the tree "
        "can be live, dead and still present, or dead and removed"
    ),
    "DIVERSION2": (
        "Tree grew across minimum threshold diameter for the estimate since "
        "T1 and the condition diverted from the land basis by T2. This "
        "component assignment is not dependent upon tree status "
        "(TREE.STATUSCD). For example, the tree can be live, dead and still "
        "present, or dead and removed"
    ),
    "CULLINCR": ("Not used at this time"),
    "CULLDECR": ("Not used at this time"),
    "N/A - A2A": (
        "Component of change is not defined or does not exist. Applicable "
        "only in annual-to-annual GRM estimates"
    ),
    "N/A - A2A SOON": (
        "Component of change is not defined or does not exist. Applicable "
        "only in annual-to-annual GRM estimates"
    ),
    "N/A - MODELED": (
        "Component of change is not defined or does not exist. Applicable "
        "only in annual-to-annual GRM estimates"
    ),
    "N/A - P2A": (
        "Component of change is not defined or does not exist. Applicable "
        "only in periodic-to-annual GRM estimates"
    ),
    "N/A - P2P": (
        "Component of change is not defined or does not exist. Applicable "
        "only in periodic-to-periodic GRM estimates"
    ),
    "N/A - PERIODIC": (
        "Component of change is not defined or does not exist. Applicable "
        "only in periodic-to-periodic GRM estimates"
    ),
}


_GRM_COLUMNS = [
    f"{prefix}_COMPONENT_{tree}_{land}"
    for prefix, tree in (("MICR", "AL"), ("SUBP", "AL"), ("SUBP", "GS"), ("SUBP", "SL"))
    for land in ("FOREST", "TIMBER")
]

#: Column name -> code table, for every FIADB column the tables above describe.
CODE_TABLES: dict[str, dict] = {
    "COND_STATUS_CD": COND_STATUS_CD,
    "RESERVCD": RESERVCD,
    "OWNCD": OWNCD,
    "SITECLCD": SITECLCD,
    "STDORGCD": STDORGCD,
    **{f"DSTRBCD{i}": DSTRBCD for i in (1, 2, 3)},
    **{f"TRTCD{i}": TRTCD for i in (1, 2, 3)},
    **{f"HARVEST_TYPE{i}_SRS": HARVEST_TYPE_SRS for i in (1, 2, 3)},
    "STATUSCD": STATUSCD,
    "TREECLCD": TREECLCD,
    "AGENTCD": AGENTCD,
    "COMPONENT": GRM_COMPONENT,
    **{col: GRM_COMPONENT for col in _GRM_COLUMNS},
}

#: Columns whose table lists code ranges by their first code (10 for 10-19,
#: ...): a code takes the meaning of ``code // 10 * 10``.
RANGE_CODED: frozenset[str] = frozenset({"AGENTCD"})
