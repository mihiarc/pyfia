#!/usr/bin/env python
"""Test what clip_most_recent actually returns."""

from pyfia import FIA

# Test 1: Using clip_most_recent
print("=" * 80)
print("TEST 1: Using clip_most_recent(eval_type='ALL')")
print("=" * 80)

db1 = FIA("data/georgia.duckdb")
db1.clip_most_recent(eval_type="ALL")

print(f"Selected EVALIDs: {db1.evalid}")

# Check what this corresponds to
if hasattr(db1, 'conn'):
    conn = db1.conn
elif hasattr(db1, '_reader') and hasattr(db1._reader, 'conn'):
    conn = db1._reader.conn
else:
    import duckdb
    conn = duckdb.connect("data/georgia.duckdb", read_only=True)

for evalid in db1.evalid:
    result = conn.execute("""
        SELECT pe.EVALID, pe.END_INVYR, pe.EVAL_DESCR, pet.EVAL_TYP
        FROM POP_EVAL pe
        JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        WHERE pe.EVALID = ?
    """, [evalid]).fetchone()

    if result:
        print(f"  EVALID {result[0]}: Year {result[1]}, Type {result[3]}")
        print(f"    Description: {result[2]}")

# Test 2: Manual EVALID selection
print("\n" + "=" * 80)
print("TEST 2: Manual clip_by_evalid with different values")
print("=" * 80)

test_evalids = [132300, 132301, 139900, 139901]

for test_evalid in test_evalids:
    # Check if this EVALID exists
    result = conn.execute("""
        SELECT pe.EVALID, pe.END_INVYR, COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pe.EVALID = ?
        GROUP BY pe.EVALID, pe.END_INVYR
    """, [test_evalid]).fetchone()

    if result:
        print(f"EVALID {result[0]}: Year {result[1]}, Plots: {result[2]}")
    else:
        print(f"EVALID {test_evalid}: NOT FOUND in database")

# Test 3: Check what our earlier tests were using
print("\n" + "=" * 80)
print("TEST 3: Query all distinct EVALIDs in POP_PLOT_STRATUM_ASSGN")
print("=" * 80)

all_evalids = conn.execute("""
    SELECT DISTINCT ppsa.EVALID, COUNT(DISTINCT PLT_CN) as plot_count
    FROM POP_PLOT_STRATUM_ASSGN ppsa
    GROUP BY ppsa.EVALID
    ORDER BY ppsa.EVALID DESC
    LIMIT 10
""").fetchall()

print("Top 10 EVALIDs by value (descending):")
for evalid, count in all_evalids:
    # Parse the year
    evalid_str = str(evalid)
    if len(evalid_str) >= 4:
        year_2digit = int(evalid_str[2:4])
        year_4digit = 2000 + year_2digit if year_2digit <= 30 else 1900 + year_2digit
        print(f"  {evalid} ({year_4digit}): {count} plots")
    else:
        print(f"  {evalid}: {count} plots")

# Test 4: See what happens without any filtering
print("\n" + "=" * 80)
print("TEST 4: Default FIA object (no filtering)")
print("=" * 80)

db_default = FIA("data/georgia.duckdb")
print(f"Default EVALIDs: {db_default.evalid}")

# Get some area calculations to see which plots are included
db_default.load_table("POP_PLOT_STRATUM_ASSGN")
ppsa = db_default.tables["POP_PLOT_STRATUM_ASSGN"].collect()
evalid_counts = ppsa.group_by("EVALID").agg(ppsa.columns[0].count().alias("count"))
print(f"\nTotal unique EVALIDs in PPSA: {len(evalid_counts)}")
print(f"Total plot-stratum assignments: {len(ppsa)}")