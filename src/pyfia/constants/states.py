"""
State FIPS codes and mappings.

Contains mappings between state abbreviations, names, and FIPS codes
used in FIA data.
"""

from typing import Dict


class StateCodes:
    """State FIPS codes and mappings."""

    # State abbreviations to FIPS codes
    ABBR_TO_CODE: Dict[str, int] = {
        "AL": 1,
        "AK": 2,
        "AZ": 4,
        "AR": 5,
        "CA": 6,
        "CO": 8,
        "CT": 9,
        "DE": 10,
        "FL": 12,
        "GA": 13,
        "HI": 15,
        "ID": 16,
        "IL": 17,
        "IN": 18,
        "IA": 19,
        "KS": 20,
        "KY": 21,
        "LA": 22,
        "ME": 23,
        "MD": 24,
        "MA": 25,
        "MI": 26,
        "MN": 27,
        "MS": 28,
        "MO": 29,
        "MT": 30,
        "NE": 31,
        "NV": 32,
        "NH": 33,
        "NJ": 34,
        "NM": 35,
        "NY": 36,
        "NC": 37,
        "ND": 38,
        "OH": 39,
        "OK": 40,
        "OR": 41,
        "PA": 42,
        "RI": 44,
        "SC": 45,
        "SD": 46,
        "TN": 47,
        "TX": 48,
        "UT": 49,
        "VT": 50,
        "VA": 51,
        "WA": 53,
        "WV": 54,
        "WI": 55,
        "WY": 56,
    }

    # State names to FIPS codes
    NAME_TO_CODE: Dict[str, int] = {
        "alabama": 1,
        "alaska": 2,
        "arizona": 4,
        "arkansas": 5,
        "california": 6,
        "colorado": 8,
        "connecticut": 9,
        "delaware": 10,
        "florida": 12,
        "georgia": 13,
        "hawaii": 15,
        "idaho": 16,
        "illinois": 17,
        "indiana": 18,
        "iowa": 19,
        "kansas": 20,
        "kentucky": 21,
        "louisiana": 22,
        "maine": 23,
        "maryland": 24,
        "massachusetts": 25,
        "michigan": 26,
        "minnesota": 27,
        "mississippi": 28,
        "missouri": 29,
        "montana": 30,
        "nebraska": 31,
        "nevada": 32,
        "new hampshire": 33,
        "new jersey": 34,
        "new mexico": 35,
        "new york": 36,
        "north carolina": 37,
        "north dakota": 38,
        "ohio": 39,
        "oklahoma": 40,
        "oregon": 41,
        "pennsylvania": 42,
        "rhode island": 44,
        "south carolina": 45,
        "south dakota": 46,
        "tennessee": 47,
        "texas": 48,
        "utah": 49,
        "vermont": 50,
        "virginia": 51,
        "washington": 53,
        "west virginia": 54,
        "wisconsin": 55,
        "wyoming": 56,
    }

    # Derived mappings
    CODE_TO_NAME: Dict[int, str] = {v: k.title() for k, v in NAME_TO_CODE.items()}
    CODE_TO_ABBR: Dict[int, str] = {v: k for k, v in ABBR_TO_CODE.items()}
