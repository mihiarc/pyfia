#!/usr/bin/env python
"""
Append Georgia FIA data to the nfi_south.duckdb database.

This script uses pyFIA's converter API to append Georgia's SQLite 
FIA database to the existing Southern US regional DuckDB database.
"""

from pathlib import Path
from pyfia.converter import append_state
import time

def main():
    """Append Georgia data to nfi_south.duckdb."""
    
    # Define paths
    source_path = Path("data/SQLite_FIADB_GA.db")
    target_path = Path("data/nfi_south.duckdb")
    
    # Verify source exists
    if not source_path.exists():
        print(f"Error: Source database not found at {source_path}")
        return 1
    
    # Verify target exists
    if not target_path.exists():
        print(f"Error: Target database not found at {target_path}")
        return 1
    
    print("="*80)
    print("APPENDING GEORGIA TO NFI_SOUTH.DUCKDB")
    print("="*80)
    print(f"Source: {source_path} ({source_path.stat().st_size / 1e9:.1f} GB)")
    print(f"Target: {target_path} ({target_path.stat().st_size / 1e9:.1f} GB before append)")
    print(f"State: Georgia (STATECD=13)")
    print("="*80)
    
    # Start timer
    start_time = time.time()
    
    try:
        print("\nStarting append operation...")
        print("This will take a few minutes due to the large data size.")
        
        # Append Georgia data
        # Using dedupe=True with CN as key to handle any duplicate records
        row_counts = append_state(
            source_path=source_path,
            target_path=target_path,
            state_code=13,  # Georgia FIPS code
            dedupe=True,    # Remove duplicates if any exist
            dedupe_keys=["CN"],  # Use CN (control number) as unique identifier
            show_progress=True
        )
        
        # Calculate elapsed time
        elapsed = time.time() - start_time
        
        print("\n" + "="*80)
        print("APPEND COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Time taken: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"Tables processed: {len(row_counts)}")
        print(f"Total rows appended: {sum(row_counts.values()):,}")
        print(f"Target size after: {target_path.stat().st_size / 1e9:.1f} GB")
        
        # Show top tables by row count
        print("\nTop 5 tables by row count:")
        sorted_tables = sorted(row_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for table, count in sorted_tables:
            print(f"  {table}: {count:,} rows")
        
        print("\n✅ Georgia data successfully appended to nfi_south.duckdb")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error during append: {e}")
        return 1

if __name__ == "__main__":
    exit(main())