"""
Tests for the EVALID parser module.

Tests the robust EVALID parsing functionality that handles Y2K windowing
and ensures correct chronological sorting of evaluation IDs.
"""

import pytest
import polars as pl
from pyfia.core.evalid_parser import (
    ParsedEvalid,
    parse_evalid,
    sort_evalids_by_year,
    get_most_recent_evalid,
    compare_evalids,
    add_parsed_evalid_columns,
)


class TestEvalidParser:
    """Test suite for EVALID parser functionality."""

    def test_parse_evalid_basic(self):
        """Test basic EVALID parsing with different year ranges."""
        # Test 2023 (recent year)
        parsed = parse_evalid(132301)
        assert parsed.state_code == 13
        assert parsed.year_4digit == 2023
        assert parsed.eval_type == 1

        # Test 1999 (Y2K boundary)
        parsed = parse_evalid(139901)
        assert parsed.state_code == 13
        assert parsed.year_4digit == 1999
        assert parsed.eval_type == 1

        # Test 2000 (Y2K boundary)
        parsed = parse_evalid(130001)
        assert parsed.state_code == 13
        assert parsed.year_4digit == 2000
        assert parsed.eval_type == 1

    def test_y2k_windowing(self):
        """Test Y2K windowing logic (00-30 = 2000-2030, 31-99 = 1931-1999)."""
        # Years 00-30 should be 2000-2030
        assert parse_evalid("130001").year_4digit == 2000
        assert parse_evalid("131501").year_4digit == 2015
        assert parse_evalid("133001").year_4digit == 2030

        # Years 31-99 should be 1931-1999
        assert parse_evalid("133101").year_4digit == 1931
        assert parse_evalid("135001").year_4digit == 1950
        assert parse_evalid("139901").year_4digit == 1999

    def test_string_input_with_leading_zeros(self):
        """Test handling of string EVALIDs with leading zeros."""
        # State codes 01-09 need special handling
        parsed = parse_evalid("012301")  # Alabama 2023
        assert parsed.state_code == 1
        assert parsed.year_4digit == 2023
        assert parsed.eval_type == 1

        # Should also handle without leading zero
        parsed = parse_evalid("12301")  # Gets padded to 012301
        assert parsed.state_code == 1
        assert parsed.year_4digit == 2023
        assert parsed.eval_type == 1

    def test_sort_evalids_by_year(self):
        """Test chronological sorting of EVALIDs."""
        evalids = [139901, 132301, 131501, 130801, 139801]  # Mixed years

        # Sort descending (most recent first)
        sorted_desc = sort_evalids_by_year(evalids, descending=True)
        years = [parse_evalid(e).year_4digit for e in sorted_desc]
        assert years == [2023, 2015, 2008, 1999, 1998]

        # Sort ascending (oldest first)
        sorted_asc = sort_evalids_by_year(evalids, descending=False)
        years = [parse_evalid(e).year_4digit for e in sorted_asc]
        assert years == [1998, 1999, 2008, 2015, 2023]

    def test_critical_bug_fix(self):
        """Test the specific bug where 139901 (1999) sorted higher than 132301 (2023)."""
        evalids = [139901, 132301]

        # Numeric sort (WRONG) would put 139901 first
        numeric_sorted = sorted(evalids, reverse=True)
        assert numeric_sorted == [139901, 132301]  # This is wrong chronologically!

        # Our sort (CORRECT) puts 2023 first
        correct_sorted = sort_evalids_by_year(evalids, descending=True)
        assert correct_sorted == [132301, 139901]  # 2023 before 1999

    def test_get_most_recent_evalid(self):
        """Test finding the most recent EVALID with filters."""
        evalids = [139901, 132301, 481901, 482301, "011501"]

        # Most recent overall
        most_recent = get_most_recent_evalid(evalids)
        assert parse_evalid(most_recent).year_4digit == 2023

        # Most recent for Georgia (state 13)
        most_recent_ga = get_most_recent_evalid(evalids, state_code=13)
        assert most_recent_ga == 132301

        # Most recent for Texas (state 48)
        most_recent_tx = get_most_recent_evalid(evalids, state_code=48)
        assert most_recent_tx == 482301

    def test_compare_evalids(self):
        """Test EVALID comparison function."""
        # 2023 vs 1999
        assert compare_evalids(132301, 139901) == 1  # 2023 is more recent
        assert compare_evalids(139901, 132301) == -1  # 1999 is older
        assert compare_evalids(132301, 132201) == 1  # 2023 vs 2022

        # Same year
        assert compare_evalids(132301, 132300) == 0  # Both 2023

    def test_dataframe_operations(self):
        """Test Polars DataFrame integration."""
        df = pl.DataFrame({
            "EVALID": [139901, 132301, 131501, 130801],
            "STATECD": [13, 13, 13, 13],
            "PLOT_COUNT": [6361, 6686, 6543, 6234]
        })

        # Add parsed columns
        df_parsed = add_parsed_evalid_columns(df)

        # Check columns were added
        assert "EVALID_YEAR" in df_parsed.columns
        assert "EVALID_STATE" in df_parsed.columns
        assert "EVALID_TYPE" in df_parsed.columns

        # Check values
        years = df_parsed["EVALID_YEAR"].to_list()
        assert years == [1999, 2023, 2015, 2008]

        # Sort by parsed year
        df_sorted = df_parsed.sort("EVALID_YEAR", descending=True)
        sorted_evalids = df_sorted["EVALID"].to_list()
        assert sorted_evalids == [132301, 131501, 130801, 139901]

    def test_edge_cases(self):
        """Test edge cases and error handling."""
        # Short EVALIDs get padded
        parsed = parse_evalid(12345)  # Gets padded to 012345
        assert parsed.evalid == 12345
        assert parsed.state_code == 1
        assert parsed.year_4digit == 2023

        # Too long EVALID should fail
        with pytest.raises(ValueError, match="EVALID must be 6 digits"):
            parse_evalid(1234567)  # 7 digits

        # Boundary years
        assert parse_evalid("133001").year_4digit == 2030  # Last year in 2000s window
        assert parse_evalid("133101").year_4digit == 1931  # First year in 1900s window

    def test_parsed_evalid_comparison(self):
        """Test ParsedEvalid comparison operators."""
        e2023 = parse_evalid(132301)
        e1999 = parse_evalid(139901)
        e2023_type00 = parse_evalid(132300)

        # Year comparison
        assert e2023 < e1999  # In our system, more recent is "less than" for sorting
        assert not e1999 < e2023

        # Same year, different type
        assert e2023_type00 < e2023  # Type 00 before Type 01

        # Equality
        assert e2023 == parse_evalid(132301)
        assert e2023 != e1999

    def test_real_world_georgia_evalids(self):
        """Test with actual Georgia EVALIDs from the database."""
        ga_evalids = [
            132300,  # Georgia 2023 EXPALL
            132301,  # Georgia 2023 EXPVOL
            139900,  # Georgia 1999 EXPALL
            139901,  # Georgia 1999 EXPVOL
        ]

        # Sort by year
        sorted_evalids = sort_evalids_by_year(ga_evalids)
        years = [parse_evalid(e).year_4digit for e in sorted_evalids]

        # Should be sorted 2023, 2023, 1999, 1999
        assert years[:2] == [2023, 2023]
        assert years[2:] == [1999, 1999]

        # Most recent should be from 2023
        most_recent = get_most_recent_evalid(ga_evalids)
        assert parse_evalid(most_recent).year_4digit == 2023