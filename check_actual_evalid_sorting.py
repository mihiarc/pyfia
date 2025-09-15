#!/usr/bin/env python
"""Check what's actually happening with EVALID sorting in the database."""

from pyfia import FIA
import polars as pl

db = FIA("data/georgia.duckdb")

# Load the evaluation tables
db.load_table("POP_EVAL")
db.load_table("POP_EVAL_TYP")

pop_eval = db.tables["POP_EVAL"].collect()
pop_eval_typ = db.tables["POP_EVAL_TYP"].collect()

# Join them
df = pop_eval.join(
    pop_eval_typ, left_on="CN", right_on="EVAL_CN", how="left"
)

# Filter to Georgia EXPALL evaluations
df_ga = df.filter(
    (pl.col("STATECD") == 13) &
    (pl.col("EVAL_TYP") == "EXPALL")
)

print("=" * 80)
print("GEORGIA EXPALL EVALUATIONS IN DATABASE")
print("=" * 80)

# Show all Georgia EXPALL evaluations
print("\nAll Georgia EXPALL evaluations:")
result = df_ga.select(["EVALID", "END_INVYR", "EVAL_DESCR"]).sort("END_INVYR", descending=True)
print(result)

print("\n" + "=" * 80)
print("CURRENT find_evalid() BEHAVIOR")
print("=" * 80)

# Simulate current sorting logic
sorted_current = df_ga.sort(
    ["STATECD", "EVAL_TYP", "END_INVYR", "EVALID"],
    descending=[False, False, True, False]
)

print("\nAfter current sorting (END_INVYR desc, then EVALID asc):")
print(sorted_current.select(["EVALID", "END_INVYR", "EVAL_DESCR"]).head(5))

# Group by state and eval_type, take first
most_recent_current = sorted_current.group_by(["STATECD", "EVAL_TYP"]).first()
print(f"\nCurrent 'most recent' selection: EVALID {most_recent_current['EVALID'][0]}, Year {most_recent_current['END_INVYR'][0]}")

print("\n" + "=" * 80)
print("CORRECT BEHAVIOR (using END_INVYR only)")
print("=" * 80)

# Correct sorting - just by END_INVYR
sorted_correct = df_ga.sort("END_INVYR", descending=True)
print("\nAfter correct sorting (END_INVYR desc only):")
print(sorted_correct.select(["EVALID", "END_INVYR", "EVAL_DESCR"]).head(5))

most_recent_correct = sorted_correct.head(1)
print(f"\nCorrect 'most recent' selection: EVALID {most_recent_correct['EVALID'][0]}, Year {most_recent_correct['END_INVYR'][0]}")

# Check if END_INVYR values are unique or if there are ties
year_counts = df_ga.group_by("END_INVYR").agg(pl.count().alias("count")).sort("END_INVYR", descending=True)
print("\n" + "=" * 80)
print("END_INVYR VALUE DISTRIBUTION")
print("=" * 80)
print("\nNumber of evaluations per END_INVYR:")
print(year_counts)

# Check for ties at the most recent year
most_recent_year = year_counts["END_INVYR"][0]
ties = df_ga.filter(pl.col("END_INVYR") == most_recent_year)
print(f"\nEvaluations with END_INVYR = {most_recent_year}:")
print(ties.select(["EVALID", "END_INVYR", "EVAL_DESCR"]))