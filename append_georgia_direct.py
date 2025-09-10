#!/usr/bin/env python
"""
Directly append Georgia FIA data to nfi_south.duckdb using DuckDB SQL.

This script uses a more direct approach to handle schema differences between
the Georgia SQLite database and the existing DuckDB database.
"""

import duckdb
from pathlib import Path
import time

def main():
    """Append Georgia data using direct SQL approach."""
    
    source_path = Path("data/SQLite_FIADB_GA.db")
    target_path = Path("data/nfi_south.duckdb")
    
    if not source_path.exists():
        print(f"Error: Source database not found at {source_path}")
        return 1
    
    if not target_path.exists():
        print(f"Error: Target database not found at {target_path}")
        return 1
    
    print("="*80)
    print("DIRECTLY APPENDING GEORGIA TO NFI_SOUTH.DUCKDB")
    print("="*80)
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    print("="*80)
    
    start_time = time.time()
    
    # Connect to DuckDB
    conn = duckdb.connect(str(target_path))
    
    try:
        # Install and load sqlite extension
        conn.execute("INSTALL sqlite_scanner")
        conn.execute("LOAD sqlite_scanner")
        
        # Attach the SQLite database
        conn.execute(f"ATTACH '{source_path}' AS ga_db (TYPE SQLITE)")
        
        # Get list of tables from Georgia database
        tables = conn.execute("""
            SELECT name FROM ga_db.sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """).fetchall()
        
        print(f"\nFound {len(tables)} tables in Georgia database")
        
        # Critical tables for FIA analysis
        critical_tables = [
            'PLOT', 'COND', 'TREE', 'SEEDLING', 'SUBPLOT',
            'POP_EVAL', 'POP_EVAL_TYP', 'POP_PLOT_STRATUM_ASSGN', 
            'POP_STRATUM', 'POP_ESTN_UNIT'
        ]
        
        success_count = 0
        error_count = 0
        total_rows = 0
        
        for (table_name,) in tables:
            try:
                # Check if table exists in target
                target_exists = conn.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                """).fetchone()[0] > 0
                
                if not target_exists:
                    print(f"  Skipping {table_name} - not in target database")
                    continue
                
                # Get column information from both databases
                ga_cols = conn.execute(f"PRAGMA ga_db.table_info({table_name})").fetchall()
                target_cols = conn.execute(f"DESCRIBE {table_name}").fetchall()
                
                # Get column names
                ga_col_names = [col[1] for col in ga_cols]
                target_col_names = [col[0] for col in target_cols]
                
                # Find common columns
                common_cols = [col for col in ga_col_names if col in target_col_names]
                
                if not common_cols:
                    print(f"  Skipping {table_name} - no common columns")
                    continue
                
                # Build column list for insert
                col_list = ', '.join(common_cols)
                
                # Count existing Georgia records (if STATECD exists)
                if 'STATECD' in common_cols:
                    existing = conn.execute(f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE STATECD = 13
                    """).fetchone()[0]
                    
                    if existing > 0:
                        print(f"  {table_name}: Found {existing:,} existing Georgia records, removing...")
                        conn.execute(f"DELETE FROM {table_name} WHERE STATECD = 13")
                
                # Insert Georgia data
                row_count = conn.execute(f"""
                    INSERT INTO {table_name} ({col_list})
                    SELECT {col_list}
                    FROM ga_db.{table_name}
                    WHERE STATECD = 13 OR STATECD IS NULL
                """).fetchone()[0]
                
                if row_count > 0:
                    is_critical = table_name in critical_tables
                    marker = "✓✓" if is_critical else "✓"
                    print(f"  {marker} {table_name}: {row_count:,} rows added")
                    success_count += 1
                    total_rows += row_count
                
            except Exception as e:
                error_msg = str(e)
                if "STATECD" in error_msg:
                    # Try without STATECD filter for reference tables
                    try:
                        row_count = conn.execute(f"""
                            INSERT INTO {table_name} ({col_list})
                            SELECT {col_list}
                            FROM ga_db.{table_name}
                        """).fetchone()[0]
                        
                        if row_count > 0:
                            print(f"  ✓ {table_name}: {row_count:,} rows added (reference table)")
                            success_count += 1
                            total_rows += row_count
                    except:
                        print(f"  ✗ {table_name}: Error - {error_msg[:60]}")
                        error_count += 1
                else:
                    print(f"  ✗ {table_name}: Error - {error_msg[:60]}")
                    error_count += 1
        
        # Commit changes
        conn.commit()
        
        # Verify Georgia data was added
        print("\n" + "="*80)
        print("VERIFICATION")
        print("="*80)
        
        for table in critical_tables:
            try:
                count = conn.execute(f"""
                    SELECT COUNT(*) FROM {table} 
                    WHERE STATECD = 13
                """).fetchone()[0]
                if count > 0:
                    print(f"  {table}: {count:,} Georgia records")
            except:
                pass
        
        elapsed = time.time() - start_time
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        print(f"Time taken: {elapsed:.1f} seconds")
        print(f"Tables processed successfully: {success_count}")
        print(f"Tables with errors: {error_count}")
        print(f"Total rows added: {total_rows:,}")
        
        if success_count > 0:
            print("\n✅ Georgia data successfully appended to nfi_south.duckdb")
        else:
            print("\n❌ Failed to append Georgia data")
            
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        return 1
    finally:
        conn.close()
    
    return 0

if __name__ == "__main__":
    exit(main())