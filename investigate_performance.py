#!/usr/bin/env python
"""
Investigate why clip_by_state appears faster than direct clip_by_evalid.
Break down the timing of each step.
"""

import time
from pyfia import FIA, area
from rich.console import Console

console = Console()

console.print("\n[bold cyan]Investigating Performance Differences[/bold cyan]")
console.print("=" * 60)

# Test 1: Time just the filtering step (not area calculation)
console.print("\n[yellow]Test 1: Timing just the filtering step[/yellow]")

# clip_by_state timing
start = time.perf_counter()
with FIA("nfi_south.duckdb") as db:
    db.clip_by_state(40, most_recent=True)
    evalid_state = db.evalid
time_clip_state = time.perf_counter() - start

# clip_by_evalid timing
start = time.perf_counter()
with FIA("nfi_south.duckdb") as db:
    db.clip_by_evalid(402300)
    evalid_direct = db.evalid
time_clip_evalid = time.perf_counter() - start

console.print(f"clip_by_state time: {time_clip_state:.4f}s (EVALID: {evalid_state})")
console.print(f"clip_by_evalid time: {time_clip_evalid:.4f}s (EVALID: {evalid_direct})")

# Test 2: Time the area() calculation after filtering
console.print("\n[yellow]Test 2: Timing area() calculation after filtering[/yellow]")

# Setup with clip_by_state
with FIA("nfi_south.duckdb") as db:
    db.clip_by_state(40, most_recent=True)
    
    start = time.perf_counter()
    results1 = area(db, totals=True)
    time_area_state = time.perf_counter() - start

# Setup with clip_by_evalid
with FIA("nfi_south.duckdb") as db:
    db.clip_by_evalid(402300)
    
    start = time.perf_counter()
    results2 = area(db, totals=True)
    time_area_evalid = time.perf_counter() - start

console.print(f"area() after clip_by_state: {time_area_state:.4f}s")
console.print(f"area() after clip_by_evalid: {time_area_evalid:.4f}s")

# Test 3: Check if state_filter affects query performance
console.print("\n[yellow]Test 3: Impact of state_filter on queries[/yellow]")

with FIA("nfi_south.duckdb") as db:
    conn = db._reader._backend._connection
    
    # Query with state filter
    db.state_filter = [40]
    start = time.perf_counter()
    query1 = """
        SELECT COUNT(*) FROM PLOT WHERE STATECD = 40
    """
    count1 = conn.execute(query1).fetchone()[0]
    time_with_filter = time.perf_counter() - start
    
    # Query without explicit filter (relies on EVALID join)
    start = time.perf_counter()
    query2 = """
        SELECT COUNT(DISTINCT p.CN)
        FROM PLOT p
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON p.CN = ppsa.PLT_CN
        WHERE ppsa.EVALID = 402300
    """
    count2 = conn.execute(query2).fetchone()[0]
    time_with_join = time.perf_counter() - start

console.print(f"Query with state filter: {time_with_filter:.4f}s ({count1} plots)")
console.print(f"Query with EVALID join: {time_with_join:.4f}s ({count2} plots)")

# Analysis
console.print("\n[bold]Analysis:[/bold]")
console.print("The performance difference might be due to:")
console.print("1. State filter enables more efficient WHERE clause filtering")
console.print("2. EVALID-only filtering requires joining through POP_PLOT_STRATUM_ASSGN")
console.print("3. The state_filter is applied directly in load_table() calls")
console.print("4. Database query optimization may favor simple WHERE over JOIN")

# Check what tables are actually loaded
console.print("\n[yellow]Test 4: Tables loaded by each method[/yellow]")

with FIA("nfi_south.duckdb") as db:
    db.clip_by_state(40, most_recent=True)
    results = area(db, totals=True)
    tables_state = list(db.tables.keys())

with FIA("nfi_south.duckdb") as db:
    db.clip_by_evalid(402300)
    results = area(db, totals=True)
    tables_evalid = list(db.tables.keys())

console.print(f"Tables loaded with clip_by_state: {tables_state}")
console.print(f"Tables loaded with clip_by_evalid: {tables_evalid}")

if db.state_filter:
    console.print(f"\n[green]State filter {db.state_filter} allows efficient WHERE clauses[/green]")
else:
    console.print(f"\n[yellow]No state filter means all joins go through EVALID[/yellow]")