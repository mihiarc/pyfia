#!/usr/bin/env python
"""
Convert Georgia SQLite FIA database to standalone DuckDB format.

This creates a properly formatted Georgia DuckDB database that can be:
1. Used standalone for Georgia-only analysis
2. Later merged with other states if schemas are compatible
"""

from pathlib import Path
from pyfia.converter import convert_sqlite_to_duckdb
import time

def main():
    """Convert Georgia SQLite to DuckDB with proper schema."""
    
    source_path = Path("data/SQLite_FIADB_GA.db")
    target_path = Path("data/georgia.duckdb")
    
    if not source_path.exists():
        print(f"Error: Source database not found at {source_path}")
        print("Please ensure SQLite_FIADB_GA.db is in the data/ directory")
        return 1
    
    # Remove existing Georgia DuckDB if it exists
    if target_path.exists():
        print(f"Removing existing {target_path}")
        target_path.unlink()
    
    print("="*80)
    print("CONVERTING GEORGIA SQLITE TO DUCKDB")
    print("="*80)
    print(f"Source: {source_path} ({source_path.stat().st_size / 1e9:.1f} GB)")
    print(f"Target: {target_path}")
    print(f"State: Georgia (STATECD=13)")
    print("="*80)
    
    start_time = time.time()
    
    try:
        print("\nStarting conversion...")
        print("This will:")
        print("  1. Read SQLite schema and data")
        print("  2. Create optimized DuckDB format")
        print("  3. Apply compression (expect ~5-6x reduction)")
        print("  4. Preserve all columns including CN for data integrity")
        print()
        
        # Convert with pyFIA's converter
        row_counts = convert_sqlite_to_duckdb(
            source_path=source_path,
            target_path=target_path,
            state_code=13,  # Georgia FIPS code
            show_progress=True
        )
        
        elapsed = time.time() - start_time
        
        print("\n" + "="*80)
        print("CONVERSION COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"Time taken: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"Tables converted: {len(row_counts)}")
        print(f"Total rows: {sum(row_counts.values()):,}")
        print(f"Output size: {target_path.stat().st_size / 1e9:.1f} GB")
        print(f"Compression ratio: {source_path.stat().st_size / target_path.stat().st_size:.1f}x")
        
        # Show key table row counts
        print("\nKey table row counts:")
        key_tables = ['PLOT', 'TREE', 'COND', 'POP_EVAL', 'POP_PLOT_STRATUM_ASSGN']
        for table in key_tables:
            if table in row_counts:
                print(f"  {table}: {row_counts[table]:,} rows")
        
        print("\n✅ Georgia DuckDB database created successfully at:")
        print(f"   {target_path.absolute()}")
        print("\nYou can now:")
        print("  1. Use this database directly with pyFIA for Georgia analysis")
        print("  2. Merge it with other state DuckDB files if needed")
        print("\nExample usage:")
        print("  from pyfia import FIA, area")
        print("  db = FIA('data/georgia.duckdb')")
        print("  result = area(db, land_type='forest')")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())