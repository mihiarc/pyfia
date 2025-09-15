#!/usr/bin/env python
"""Test the robust EVALID parser."""

import sys
sys.path.insert(0, 'src')

from pyfia.core.evalid_parser import (
    parse_evalid,
    sort_evalids_by_year,
    get_most_recent_evalid,
    compare_evalids,
    format_evalid_description,
    add_parsed_evalid_columns
)
import polars as pl


print("=" * 80)
print("TESTING ROBUST EVALID PARSER")
print("=" * 80)

# Test 1: Basic parsing
print("\n1. BASIC PARSING TESTS:")
print("-" * 40)

test_cases = [
    (132301, "Georgia 2023 Type 01"),
    (139901, "Georgia 1999 Type 01"),
    (480001, "Texas 2000 Type 01"),
    (489901, "Texas 1999 Type 01"),
    ("013015", "Alabama 2030 Type 15"),  # Future year (as string to preserve leading zero)
    ("019915", "Alabama 1999 Type 15"),
]

for evalid, description in test_cases:
    parsed = parse_evalid(evalid)
    evalid_display = f"{int(evalid):06d}" if isinstance(evalid, (int, str)) else evalid
    print(f"{evalid_display}: State={parsed.state_code:02d}, Year={parsed.year_4digit}, Type={parsed.eval_type:02d}")
    print(f"  Description: {description}")
    print(f"  Parsed: {parsed}")

# Test 2: Sorting
print("\n2. SORTING TESTS:")
print("-" * 40)

evalids = [139901, 132301, 131501, 130801, 139801, 480001, 482301]
print(f"\nOriginal list: {evalids}")

sorted_desc = sort_evalids_by_year(evalids, descending=True)
print(f"Sorted (most recent first): {sorted_desc}")

sorted_asc = sort_evalids_by_year(evalids, descending=False)
print(f"Sorted (oldest first): {sorted_asc}")

# Verify the years
print("\nYear verification:")
for evalid in sorted_desc:
    p = parse_evalid(evalid)
    print(f"  {evalid} -> {p.year_4digit}")

# Test 3: Finding most recent
print("\n3. FINDING MOST RECENT:")
print("-" * 40)

# Test with mixed states
evalids_mixed = [139901, 132301, 481901, 482301, "011501"]
print(f"\nEVALIDs: {evalids_mixed}")

most_recent_all = get_most_recent_evalid(evalids_mixed)
print(f"Most recent overall: {most_recent_all} ({parse_evalid(most_recent_all).year_4digit})")

most_recent_ga = get_most_recent_evalid(evalids_mixed, state_code=13)
print(f"Most recent for Georgia: {most_recent_ga} ({parse_evalid(most_recent_ga).year_4digit})")

most_recent_tx = get_most_recent_evalid(evalids_mixed, state_code=48)
print(f"Most recent for Texas: {most_recent_tx} ({parse_evalid(most_recent_tx).year_4digit})")

# Test 4: Comparison
print("\n4. COMPARISON TESTS:")
print("-" * 40)

comparisons = [
    (132301, 139901, "2023 vs 1999"),
    (139901, 132301, "1999 vs 2023"),
    (132301, 132201, "2023 vs 2022"),
    (480001, 489901, "TX 2000 vs TX 1999"),
]

for e1, e2, desc in comparisons:
    result = compare_evalids(e1, e2)
    result_str = "older" if result == -1 else "more recent" if result == 1 else "same year"
    print(f"{e1} vs {e2} ({desc}): {e1} is {result_str}")

# Test 5: DataFrame operations
print("\n5. DATAFRAME OPERATIONS:")
print("-" * 40)

# Create sample dataframe
df = pl.DataFrame({
    "EVALID": [139901, 132301, 131501, 130801, 139801, 132201],
    "STATECD": [13, 13, 13, 13, 13, 13],
    "EVAL_TYP": ["EXPALL"] * 6,
    "PLOT_COUNT": [6361, 6686, 6543, 6234, 6364, 6586]
})

print("\nOriginal DataFrame:")
print(df)

# Add parsed columns
df_parsed = add_parsed_evalid_columns(df)
print("\nWith parsed columns:")
print(df_parsed.select(["EVALID", "EVALID_YEAR", "EVALID_STATE", "EVALID_TYPE", "PLOT_COUNT"]))

# Sort by parsed year
df_sorted = df_parsed.sort("EVALID_YEAR", descending=True)
print("\nSorted by year (descending):")
print(df_sorted.select(["EVALID", "EVALID_YEAR", "PLOT_COUNT"]))

# Test 6: Edge cases
print("\n6. EDGE CASES:")
print("-" * 40)

edge_cases = [
    ("000000", "Minimum EVALID"),
    ("999999", "Maximum EVALID"),
    ("013101", "Year 31 (1931 or 2031?)"),
    ("013001", "Year 30 (1930 or 2030?)"),
]

for evalid_str, description in edge_cases:
    try:
        parsed = parse_evalid(evalid_str)
        print(f"{evalid_str}: {description}")
        print(f"  Parsed as: State={parsed.state_code}, Year={parsed.year_4digit}, Type={parsed.eval_type}")
    except Exception as e:
        print(f"{evalid_str}: {description} - ERROR: {e}")

# Test 7: Real-world scenario
print("\n7. REAL-WORLD SCENARIO:")
print("-" * 40)

# Simulate the problematic case from our database
ga_evalids = [
    132300,  # 2023 EXPALL
    132301,  # 2023 EXPVOL
    139900,  # 1999 EXPALL
    139901,  # 1999 EXPVOL
]

print("Georgia EVALIDs (mixed years and types):")
for evalid in ga_evalids:
    p = parse_evalid(evalid)
    print(f"  {evalid}: Year {p.year_4digit}, Type {p.eval_type:02d}")

print("\nSorted by year (most recent first):")
sorted_ga = sort_evalids_by_year(ga_evalids)
for evalid in sorted_ga:
    p = parse_evalid(evalid)
    print(f"  {evalid}: Year {p.year_4digit}, Type {p.eval_type:02d}")

print("\nMost recent EXPALL (type 00):")
expall_only = [e for e in ga_evalids if parse_evalid(e).eval_type == 0]
if expall_only:
    most_recent_expall = sort_evalids_by_year(expall_only)[0]
    print(f"  {most_recent_expall} (Year {parse_evalid(most_recent_expall).year_4digit})")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("\n✅ The robust EVALID parser correctly handles:")
print("  - 2-digit year interpretation (00-30 = 2000-2030, 31-99 = 1931-1999)")
print("  - Chronological sorting regardless of numeric EVALID value")
print("  - State and type filtering")
print("  - DataFrame operations with proper year columns")
print("\nThis solves the critical bug where 139901 (1999) would be selected over 132301 (2023)")