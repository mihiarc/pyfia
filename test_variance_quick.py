#!/usr/bin/env python
"""Quick test to check actual output structure of area() and volume()."""

from pyfia import FIA, area, volume

# Use the test database
db_path = "data/nfi_south.duckdb"

with FIA(db_path) as db:
    # Get Texas
    db.clip_by_state(48, most_recent=True, eval_type="ALL")

    print("Testing area() output structure:")
    area_results = area(db, land_type="forest", totals=True)
    print(f"Columns: {area_results.columns}")
    if not area_results.is_empty():
        print(f"First row: {area_results.head(1)}")

    print("\n" + "="*50)
    print("Testing volume() output structure:")
    # Need to re-clip for volume
    db.clip_by_state(48, most_recent=True, eval_type="VOL")
    volume_results = volume(db, land_type="forest", totals=True)
    print(f"Columns: {volume_results.columns}")
    if not volume_results.is_empty():
        print(f"First row: {volume_results.head(1)}")