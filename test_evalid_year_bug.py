#!/usr/bin/env python
"""Test and demonstrate the EVALID year interpretation bug."""

import polars as pl

# Create sample EVALIDs to demonstrate the bug
evalids = [
    132301,  # Georgia 2023
    139901,  # Georgia 1999
    131501,  # Georgia 2015
    130801,  # Georgia 2008
    139801,  # Georgia 1998
]

print("=" * 80)
print("EVALID YEAR INTERPRETATION BUG DEMONSTRATION")
print("=" * 80)

print("\n1. EVALID FORMAT: SSYYTT")
print("   SS = State code (13 for Georgia)")
print("   YY = Year (last 2 digits)")
print("   TT = Evaluation type")

print("\n2. PARSING EVALIDS:")
for evalid in evalids:
    evalid_str = str(evalid)
    state = int(evalid_str[:2])
    year_2digit = int(evalid_str[2:4])
    eval_type = int(evalid_str[4:])

    # Correct interpretation: 00-30 = 2000-2030, 31-99 = 1931-1999
    # This is a common Y2K-style windowing approach
    if year_2digit <= 30:
        year_4digit = 2000 + year_2digit
    else:
        year_4digit = 1900 + year_2digit

    print(f"   {evalid}: State={state:02d}, Year={year_4digit}, Type={eval_type:02d}")

print("\n3. SORTING COMPARISON:")
print("\n   Current (WRONG) - Sort by EVALID as integer:")
sorted_wrong = sorted(evalids, reverse=True)
for evalid in sorted_wrong:
    evalid_str = str(evalid)
    year_2digit = int(evalid_str[2:4])
    year_4digit = 2000 + year_2digit if year_2digit <= 30 else 1900 + year_2digit
    print(f"   {evalid} (year {year_4digit})")

print("\n   Correct - Sort by actual year:")
def get_actual_year(evalid):
    evalid_str = str(evalid)
    year_2digit = int(evalid_str[2:4])
    return 2000 + year_2digit if year_2digit <= 30 else 1900 + year_2digit

sorted_correct = sorted(evalids, key=get_actual_year, reverse=True)
for evalid in sorted_correct:
    year = get_actual_year(evalid)
    print(f"   {evalid} (year {year})")

print("\n4. IMPACT ON clip_most_recent():")
print("   The bug causes us to select older data as 'most recent'!")
print(f"   Current selection: {sorted_wrong[0]} (year {get_actual_year(sorted_wrong[0])})")
print(f"   Should select: {sorted_correct[0]} (year {get_actual_year(sorted_correct[0])})")

print("\n5. TESTING WITH POLARS DATAFRAME (simulating find_evalid):")
# Create a sample dataframe
df = pl.DataFrame({
    "EVALID": evalids,
    "STATECD": [13] * len(evalids),
    "EVAL_TYP": ["EXPALL"] * len(evalids),
    "END_INVYR": [get_actual_year(e) for e in evalids]
})

print("\nDataFrame:")
print(df)

# Current approach (WRONG - sorts by EVALID)
current_result = df.sort(["EVALID"], descending=[True]).head(1)
print(f"\nCurrent approach result: EVALID {current_result['EVALID'][0]} (Year {current_result['END_INVYR'][0]})")

# Correct approach (sorts by END_INVYR)
correct_result = df.sort(["END_INVYR"], descending=[True]).head(1)
print(f"Correct approach result: EVALID {correct_result['EVALID'][0]} (Year {correct_result['END_INVYR'][0]})")

# Even better: Add parsed year column
df_with_parsed = df.with_columns([
    pl.col("EVALID").cast(pl.Utf8).str.slice(2, 2).cast(pl.Int32).alias("year_2digit")
]).with_columns([
    pl.when(pl.col("year_2digit") <= 30)
      .then(2000 + pl.col("year_2digit"))
      .otherwise(1900 + pl.col("year_2digit"))
      .alias("parsed_year")
])

print("\nDataFrame with parsed year:")
print(df_with_parsed)

best = df_with_parsed.sort("parsed_year", descending=True).head(1)
print(f"\nBest approach result: EVALID {best['EVALID'][0]} (Parsed Year {best['parsed_year'][0]})")