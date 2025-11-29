"""
EVALID parser for robust handling of FIA evaluation identifiers.

EVALIDs encode state, year, and evaluation type in a 6-digit format: SSYYTT
This module provides utilities for correctly parsing and comparing EVALIDs.
"""

import warnings
from dataclasses import dataclass
from typing import List, Optional, Union


@dataclass
class ParsedEvalid:
    """Parsed representation of an EVALID with proper year interpretation."""

    evalid: int
    state_code: int
    year_2digit: int
    year_4digit: int
    eval_type: int

    def __str__(self) -> str:
        return f"EVALID({self.evalid}: State={self.state_code}, Year={self.year_4digit}, Type={self.eval_type:02d})"

    def __lt__(self, other: "ParsedEvalid") -> bool:
        """Compare EVALIDs by year (descending), then by type."""
        if self.year_4digit != other.year_4digit:
            return self.year_4digit > other.year_4digit  # More recent first
        return self.eval_type < other.eval_type

    def __eq__(self, other: "ParsedEvalid") -> bool:
        """Check equality based on all components."""
        return (
            self.state_code == other.state_code
            and self.year_4digit == other.year_4digit
            and self.eval_type == other.eval_type
        )


def parse_evalid(evalid: Union[int, str]) -> ParsedEvalid:
    """
    Parse an EVALID into its components with correct year interpretation.

    EVALID format: SSYYTT where:
    - SS: State FIPS code (01-99)
    - YY: Year (last 2 digits)
    - TT: Evaluation type code (00-99)

    Year interpretation (standard Y2K windowing):
    - 00-30: Interpreted as 2000-2030
    - 31-99: Interpreted as 1931-1999

    Parameters
    ----------
    evalid : int or str
        The EVALID to parse (e.g., 132301 for Georgia 2023 type 01)

    Returns
    -------
    ParsedEvalid
        Parsed components with 4-digit year

    Raises
    ------
    ValueError
        If EVALID is not valid 6-digit format

    Examples
    --------
    >>> parse_evalid(132301)
    ParsedEvalid(evalid=132301, state_code=13, year_2digit=23, year_4digit=2023, eval_type=1)

    >>> parse_evalid(139901)
    ParsedEvalid(evalid=139901, state_code=13, year_2digit=99, year_4digit=1999, eval_type=1)
    """
    # Convert to string for parsing, preserving leading zeros
    if isinstance(evalid, str):
        evalid_str = evalid
        # Pad with leading zeros if needed
        if len(evalid_str) < 6:
            evalid_str = evalid_str.zfill(6)
        evalid_int = int(evalid_str)
    else:
        evalid_int = int(evalid)
        evalid_str = f"{evalid_int:06d}"  # Format with leading zeros

    # Validate format
    if len(evalid_str) != 6:
        raise ValueError(
            f"EVALID must be 6 digits (SSYYTT format), got {evalid_str} "
            f"with {len(evalid_str)} digits"
        )

    # Parse components
    state_code = int(evalid_str[0:2])
    year_2digit = int(evalid_str[2:4])
    eval_type = int(evalid_str[4:6])

    # Validate state code
    if state_code < 1 or state_code > 99:
        warnings.warn(f"Unusual state code {state_code} in EVALID {evalid}")

    # Convert 2-digit year to 4-digit with Y2K windowing
    # Years 00-30 are 2000-2030, years 31-99 are 1931-1999
    # This handles FIA data from 1931 to 2030
    if year_2digit <= 30:
        year_4digit = 2000 + year_2digit
    else:
        year_4digit = 1900 + year_2digit

    return ParsedEvalid(
        evalid=evalid_int,
        state_code=state_code,
        year_2digit=year_2digit,
        year_4digit=year_4digit,
        eval_type=eval_type,
    )


def sort_evalids_by_year(
    evalids: List[Union[int, str]], descending: bool = True
) -> List[int]:
    """
    Sort EVALIDs by actual year (not numeric value).

    Parameters
    ----------
    evalids : list of int or str
        List of EVALIDs to sort
    descending : bool, default True
        If True, sort most recent first

    Returns
    -------
    list of int
        Sorted EVALID values

    Examples
    --------
    >>> evalids = [139901, 132301, 131501]  # 1999, 2023, 2015
    >>> sort_evalids_by_year(evalids)
    [132301, 131501, 139901]  # 2023, 2015, 1999
    """
    parsed = [parse_evalid(e) for e in evalids]

    # Sort by year (already handles descending in __lt__)
    if descending:
        parsed.sort()  # Uses our custom __lt__ which puts recent first
    else:
        parsed.sort(reverse=True)

    return [p.evalid for p in parsed]


def get_most_recent_evalid(
    evalids: List[Union[int, str]],
    state_code: Optional[int] = None,
    eval_type: Optional[int] = None,
) -> Optional[int]:
    """
    Get the most recent EVALID from a list, optionally filtered.

    Parameters
    ----------
    evalids : list of int or str
        List of EVALIDs to search
    state_code : int, optional
        If provided, only consider EVALIDs for this state
    eval_type : int, optional
        If provided, only consider EVALIDs of this type

    Returns
    -------
    int or None
        Most recent EVALID matching criteria, or None if no matches

    Examples
    --------
    >>> evalids = [139901, 132301, 481901]  # GA 1999, GA 2023, TX 2019
    >>> get_most_recent_evalid(evalids, state_code=13)
    132301  # Georgia 2023
    """
    if not evalids:
        return None

    # Parse all EVALIDs
    parsed = [parse_evalid(e) for e in evalids]

    # Apply filters
    if state_code is not None:
        parsed = [p for p in parsed if p.state_code == state_code]

    if eval_type is not None:
        parsed = [p for p in parsed if p.eval_type == eval_type]

    if not parsed:
        return None

    # Sort by year (most recent first) and return
    parsed.sort()  # Uses our custom __lt__
    return parsed[0].evalid


def compare_evalids(evalid1: Union[int, str], evalid2: Union[int, str]) -> int:
    """
    Compare two EVALIDs chronologically.

    Parameters
    ----------
    evalid1 : int or str
        First EVALID
    evalid2 : int or str
        Second EVALID

    Returns
    -------
    int
        -1 if evalid1 is older than evalid2
         0 if they're from the same year
         1 if evalid1 is more recent than evalid2

    Examples
    --------
    >>> compare_evalids(132301, 139901)  # 2023 vs 1999
    1  # 2023 is more recent

    >>> compare_evalids(139901, 132301)  # 1999 vs 2023
    -1  # 1999 is older
    """
    p1 = parse_evalid(evalid1)
    p2 = parse_evalid(evalid2)

    if p1.year_4digit < p2.year_4digit:
        return -1
    elif p1.year_4digit > p2.year_4digit:
        return 1
    else:
        return 0


def format_evalid_description(evalid: Union[int, str]) -> str:
    """
    Format a human-readable description of an EVALID.

    Parameters
    ----------
    evalid : int or str
        The EVALID to describe

    Returns
    -------
    str
        Human-readable description

    Examples
    --------
    >>> format_evalid_description(132301)
    'Georgia 2023 (Type 01)'
    """
    parsed = parse_evalid(evalid)

    # Import constants if available, otherwise use fallback
    try:
        from ..constants import StateCodes

        state_name = StateCodes.CODE_TO_NAME.get(
            parsed.state_code, f"State {parsed.state_code}"
        )
    except ImportError:
        # Fallback if constants not available
        state_names = {
            1: "Alabama",
            13: "Georgia",
            48: "Texas",
            # Add more as needed
        }
        state_name = state_names.get(parsed.state_code, f"State {parsed.state_code}")

    # Format evaluation type
    eval_type_names = {
        0: "All Area",
        1: "Volume",
        3: "Change",
        7: "DWM",
        9: "Growth",
    }

    type_desc = eval_type_names.get(parsed.eval_type, f"Type {parsed.eval_type:02d}")

    return f"{state_name} {parsed.year_4digit} ({type_desc})"


# Polars extension functions for DataFrame operations
def add_parsed_evalid_columns(df):
    """
    Add parsed EVALID columns to a Polars DataFrame.

    Adds columns: EVALID_YEAR, EVALID_STATE, EVALID_TYPE

    Parameters
    ----------
    df : polars.DataFrame or polars.LazyFrame
        DataFrame with an EVALID column

    Returns
    -------
    polars.DataFrame or polars.LazyFrame
        DataFrame with additional parsed columns
    """
    import polars as pl

    return df.with_columns(
        [
            # Extract year with proper interpretation
            pl.when(pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32) <= 30)
            .then(2000 + pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32))
            .otherwise(
                1900 + pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32)
            )
            .alias("EVALID_YEAR"),
            # Extract state code
            pl.col("EVALID")
            .cast(pl.Utf8)
            .str.slice(0, 2)
            .cast(pl.Int32)
            .alias("EVALID_STATE"),
            # Extract evaluation type
            pl.col("EVALID")
            .cast(pl.Utf8)
            .str.slice(4, 2)
            .cast(pl.Int32)
            .alias("EVALID_TYPE"),
        ]
    )


def filter_most_recent_by_group(df, group_cols=None):
    """
    Filter a DataFrame to keep only the most recent EVALID per group.

    Parameters
    ----------
    df : polars.DataFrame or polars.LazyFrame
        DataFrame with an EVALID column
    group_cols : list of str, optional
        Columns to group by (e.g., ['STATECD', 'EVAL_TYP'])
        If None, returns single most recent across all data

    Returns
    -------
    polars.DataFrame or polars.LazyFrame
        Filtered DataFrame with most recent EVALIDs
    """

    # Add parsed year column
    df_with_year = add_parsed_evalid_columns(df)

    if group_cols:
        # Sort by group and year (descending), then take first per group
        return (
            df_with_year.sort(
                group_cols + ["EVALID_YEAR", "EVALID"],
                descending=[False] * len(group_cols) + [True, False],
            )
            .group_by(group_cols)
            .first()
            .drop(["EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
        )
    else:
        # Just get the single most recent
        return (
            df_with_year.sort(["EVALID_YEAR", "EVALID"], descending=[True, False])
            .head(1)
            .drop(["EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE"])
        )
