"""
Debug script to investigate mortality calculation issues.
"""

import duckdb
import polars as pl
from pathlib import Path
from pyfia import FIA, mortality

# Create test database
db_path = Path("test_mortality_debug.duckdb")

# Remove existing database if it exists
if db_path.exists():
    db_path.unlink()

with duckdb.connect(str(db_path)) as conn:
    # Create required FIA tables
    
    # PLOT table
    conn.execute("""
        CREATE TABLE PLOT (
            CN TEXT PRIMARY KEY,
            STATECD INTEGER,
            INVYR INTEGER,
            PLOT_STATUS_CD INTEGER,
            MACRO_BREAKPOINT_DIA DOUBLE
        )
    """)
    
    conn.execute("""
        INSERT INTO PLOT VALUES
        ('1', 40, 2023, 1, 9.0),
        ('2', 40, 2023, 1, 9.0)
    """)
    
    # COND table
    conn.execute("""
        CREATE TABLE COND (
            PLT_CN TEXT,
            CONDID INTEGER,
            COND_STATUS_CD INTEGER,
            CONDPROP_UNADJ DOUBLE,
            OWNGRPCD INTEGER,
            FORTYPCD INTEGER,
            SITECLCD INTEGER,
            RESERVCD INTEGER,
            PRIMARY KEY (PLT_CN, CONDID)
        )
    """)
    
    conn.execute("""
        INSERT INTO COND VALUES
        ('1', 1, 1, 1.0, 10, 101, 1, 0),
        ('2', 1, 1, 1.0, 10, 101, 1, 0)
    """)
    
    # TREE table with dead trees
    conn.execute("""
        CREATE TABLE TREE (
            CN TEXT PRIMARY KEY,
            PLT_CN TEXT,
            CONDID INTEGER,
            STATUSCD INTEGER,
            SPCD INTEGER,
            DIA DOUBLE,
            TPA_UNADJ DOUBLE,
            VOLCFNET DOUBLE,
            DRYBIO_AG DOUBLE,
            DRYBIO_BG DOUBLE,
            MORTYR INTEGER
        )
    """)
    
    # Insert dead trees with recent mortality
    conn.execute("""
        INSERT INTO TREE VALUES
        ('T1', '1', 1, 2, 131, 12.5, 6.018, 800.0, 2000.0, 500.0, 2020),
        ('T2', '1', 1, 2, 131, 15.0, 6.018, 1200.0, 3000.0, 700.0, 2021),
        ('T3', '2', 1, 2, 833, 20.0, 3.0, 2000.0, 5000.0, 1200.0, 2019)
    """)
    
    # POP_PLOT_STRATUM_ASSGN table
    conn.execute("""
        CREATE TABLE POP_PLOT_STRATUM_ASSGN (
            CN TEXT PRIMARY KEY,
            PLT_CN TEXT,
            STRATUM_CN TEXT,
            EVALID INTEGER
        )
    """)
    
    conn.execute("""
        INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES
        ('PSA1', '1', 'S1', 402300),
        ('PSA2', '2', 'S1', 402300)
    """)
    
    # POP_STRATUM table
    conn.execute("""
        CREATE TABLE POP_STRATUM (
            CN TEXT PRIMARY KEY,
            ESTN_UNIT_CN TEXT,
            STRATUMCD INTEGER,
            EXPNS DOUBLE,
            ADJ_FACTOR_MICR DOUBLE,
            ADJ_FACTOR_SUBP DOUBLE,
            ADJ_FACTOR_MACR DOUBLE,
            EVALID INTEGER
        )
    """)
    
    conn.execute("""
        INSERT INTO POP_STRATUM VALUES
        ('S1', 'EU1', 1, 1000.0, 1.0, 1.0, 1.0, 402300)
    """)
    
    # POP_EVAL table
    conn.execute("""
        CREATE TABLE POP_EVAL (
            CN TEXT PRIMARY KEY,
            EVALID INTEGER,
            EVAL_DESCR TEXT,
            STATECD INTEGER,
            LOCATION_NM TEXT,
            START_INVYR INTEGER,
            END_INVYR INTEGER
        )
    """)
    
    conn.execute("""
        INSERT INTO POP_EVAL VALUES
        ('PE1', 402300, 'Oklahoma 2023 All Area', 40, 'Oklahoma', 2018, 2023)
    """)
    
    # POP_EVAL_TYP table
    conn.execute("""
        CREATE TABLE POP_EVAL_TYP (
            CN TEXT PRIMARY KEY,
            EVAL_CN TEXT,
            EVAL_TYP TEXT
        )
    """)
    
    conn.execute("""
        INSERT INTO POP_EVAL_TYP VALUES
        ('PET1', 'PE1', 'EXPALL')
    """)

print("Created test database")

# Now test mortality calculation
print("\n=== Testing mortality calculation ===")
with FIA(str(db_path)) as db:
    db.clip_by_evalid([402300])
    
    # Check what data we have
    print("\n1. Checking dead trees:")
    # Get connection from FIA object's backend
    conn = db._reader._backend._connection
    dead_trees = conn.execute("""
        SELECT COUNT(*) as count, 
               SUM(TPA_UNADJ) as total_tpa,
               SUM(VOLCFNET * TPA_UNADJ) as total_volume
        FROM TREE 
        WHERE STATUSCD = 2 AND MORTYR > 0 AND MORTYR >= 2018
    """).fetchone()
    print(f"   Dead trees (recent): {dead_trees[0]}")
    print(f"   Total TPA: {dead_trees[1]}")
    print(f"   Total Volume: {dead_trees[2]}")
    
    print("\n2. Checking stratification join:")
    strat_check = conn.execute("""
        SELECT 
            t.CN as tree_cn,
            t.PLT_CN,
            t.VOLCFNET * t.TPA_UNADJ as tree_volume,
            ps.EXPNS,
            c.CONDPROP_UNADJ
        FROM TREE t
        JOIN COND c ON t.PLT_CN = c.PLT_CN AND t.CONDID = c.CONDID
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON t.PLT_CN = ppsa.PLT_CN
        JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
        WHERE t.STATUSCD = 2 AND t.MORTYR > 0 AND t.MORTYR >= 2018
    """).fetchall()
    
    print("   Trees with stratification:")
    for row in strat_check:
        print(f"   Tree {row[0]}: PLT_CN={row[1]}, Volume={row[2]}, EXPNS={row[3]}, CONDPROP={row[4]}")
    
    print("\n3. Testing area calculation:")
    area_check = conn.execute("""
        SELECT 
            SUM(c.CONDPROP_UNADJ * ps.EXPNS) as total_area
        FROM COND c
        JOIN POP_PLOT_STRATUM_ASSGN ppsa ON c.PLT_CN = ppsa.PLT_CN
        JOIN POP_STRATUM ps ON ppsa.STRATUM_CN = ps.CN
        WHERE c.COND_STATUS_CD = 1
    """).fetchone()
    print(f"   Total area: {area_check[0]}")
    
    # Now run mortality function
    try:
        print("\n4. Running mortality function:")
        results = mortality(db, measure="volume")
        print("   Results:")
        print(results)
        
        # Show specific values
        if len(results) > 0:
            print(f"\n   MORT_ACRE: {results['MORT_ACRE'][0]}")
            print(f"   MORT_TOTAL: {results['MORT_TOTAL'][0]}")
            print(f"   AREA_TOTAL: {results['AREA_TOTAL'][0]}")
            print(f"   N_DEAD_TREES: {results['N_DEAD_TREES'][0]}")
    except Exception as e:
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()

# Clean up
db_path.unlink()