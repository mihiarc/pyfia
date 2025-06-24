"""
Pytest configuration and shared fixtures for pyFIA tests.

This module provides reusable test fixtures for consistent testing
across all pyFIA modules.
"""

import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, Any
from unittest.mock import MagicMock

import polars as pl
import pytest

from pyfia import FIA
from pyfia.models import EvaluationInfo


@pytest.fixture(scope="session")
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        yield Path(tmp.name)
    # Cleanup happens automatically


@pytest.fixture(scope="session")
def sample_fia_db(temp_db_path):
    """Create a sample FIA SQLite database with test data."""
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    
    # Create simplified FIA tables with essential columns
    cursor.execute("""
        CREATE TABLE PLOT (
            CN TEXT PRIMARY KEY,
            STATECD INTEGER,
            COUNTYCD INTEGER,
            PLOT INTEGER,
            INVYR INTEGER,
            LAT REAL,
            LON REAL,
            PLOT_STATUS_CD INTEGER,
            MACRO_BREAKPOINT_DIA REAL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE COND (
            CN TEXT PRIMARY KEY,
            PLT_CN TEXT,
            CONDID INTEGER,
            COND_STATUS_CD INTEGER,
            CONDPROP_UNADJ REAL,
            LANDCLCD INTEGER,
            FORTYPCD INTEGER,
            OWNGRPCD INTEGER,
            FOREIGN KEY (PLT_CN) REFERENCES PLOT(CN)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE TREE (
            CN TEXT PRIMARY KEY,
            PLT_CN TEXT,
            CONDID INTEGER,
            TREE INTEGER,
            STATUSCD INTEGER,
            SPCD INTEGER,
            DIA REAL,
            HT REAL,
            TPA_UNADJ REAL,
            DRYBIO_AG REAL,
            DRYBIO_BG REAL,
            VOLCFNET REAL,
            VOLCSNET REAL,
            VOLBFNET REAL,
            FOREIGN KEY (PLT_CN) REFERENCES PLOT(CN)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE POP_EVAL (
            CN TEXT PRIMARY KEY,
            EVALID INTEGER UNIQUE,
            EVAL_GRP TEXT,
            EVAL_TYP TEXT,
            EVAL_DESCR TEXT,
            START_INVYR INTEGER,
            END_INVYR INTEGER,
            STATECD INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE POP_PLOT_STRATUM_ASSGN (
            CN TEXT PRIMARY KEY,
            PLT_CN TEXT,
            STRATUM_CN TEXT,
            EVALID INTEGER,
            FOREIGN KEY (PLT_CN) REFERENCES PLOT(CN),
            FOREIGN KEY (EVALID) REFERENCES POP_EVAL(EVALID)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE POP_STRATUM (
            CN TEXT PRIMARY KEY,
            EVALID INTEGER,
            EXPNS REAL,
            ADJ_FACTOR_SUBP REAL,
            ADJ_FACTOR_MICR REAL,
            ADJ_FACTOR_MACR REAL,
            P2POINTCNT INTEGER,
            P1POINTCNT INTEGER,
            P1PNTCNT_EU INTEGER,
            AREA_USED REAL,
            FOREIGN KEY (EVALID) REFERENCES POP_EVAL(EVALID)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE REF_SPECIES (
            SPCD INTEGER PRIMARY KEY,
            COMMON_NAME TEXT,
            SCIENTIFIC_NAME TEXT,
            GENUS TEXT,
            SPECIES TEXT
        )
    """)
    
    # Insert sample data
    # Plots (10 plots in North Carolina)
    plots_data = [
        ("PLT001", 37, 1, 1, 2020, 35.5, -80.5, 1, 24.0),
        ("PLT002", 37, 1, 2, 2020, 35.6, -80.4, 1, 24.0),
        ("PLT003", 37, 1, 3, 2020, 35.7, -80.3, 1, 24.0),
        ("PLT004", 37, 1, 4, 2020, 35.8, -80.2, 1, 24.0),
        ("PLT005", 37, 1, 5, 2020, 35.9, -80.1, 1, 24.0),
        ("PLT006", 37, 1, 6, 2021, 35.5, -80.6, 1, 24.0),
        ("PLT007", 37, 1, 7, 2021, 35.6, -80.7, 1, 24.0),
        ("PLT008", 37, 1, 8, 2021, 35.7, -80.8, 1, 24.0),
        ("PLT009", 37, 1, 9, 2021, 35.8, -80.9, 1, 24.0),
        ("PLT010", 37, 1, 10, 2021, 35.9, -81.0, 1, 24.0),
    ]
    cursor.executemany("INSERT INTO PLOT VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", plots_data)
    
    # Conditions (one per plot, all forest)
    cond_data = [
        ("COND001", "PLT001", 1, 1, 1.0, 1, 220, 10),
        ("COND002", "PLT002", 1, 1, 1.0, 1, 220, 10),
        ("COND003", "PLT003", 1, 1, 1.0, 1, 220, 10),
        ("COND004", "PLT004", 1, 1, 1.0, 1, 220, 10),
        ("COND005", "PLT005", 1, 1, 1.0, 1, 220, 10),
        ("COND006", "PLT006", 1, 1, 1.0, 1, 220, 10),
        ("COND007", "PLT007", 1, 1, 1.0, 1, 220, 10),
        ("COND008", "PLT008", 1, 1, 1.0, 1, 220, 10),
        ("COND009", "PLT009", 1, 1, 1.0, 1, 220, 10),
        ("COND010", "PLT010", 1, 1, 1.0, 1, 220, 10),
    ]
    cursor.executemany("INSERT INTO COND VALUES (?, ?, ?, ?, ?, ?, ?, ?)", cond_data)
    
    # Trees (multiple trees per plot)
    tree_data = []
    tree_id = 1
    for plot_num in range(1, 11):
        plt_cn = f"PLT{plot_num:03d}"
        # 2-5 trees per plot
        num_trees = 2 + (plot_num % 4)
        for tree_num in range(1, num_trees + 1):
            tree_cn = f"TREE{tree_id:04d}"
            # Vary tree characteristics
            spcd = [131, 110, 833, 802][tree_num % 4]  # Loblolly pine, Virginia pine, Chestnut oak, White oak
            dia = 8.0 + (tree_num * 2) + (plot_num * 0.5)
            ht = 30.0 + (tree_num * 5) + (plot_num * 0.3)
            tpa_unadj = 6.0  # Standard subplot TPA
            drybio_ag = dia * 2.5  # Simplified biomass
            drybio_bg = drybio_ag * 0.2  # 20% belowground
            volcfnet = dia * 1.8  # Simplified volume
            volcsnet = volcfnet * 0.7  # Sawlog volume
            volbfnet = volcsnet * 5.5  # Board feet
            
            tree_data.append((
                tree_cn, plt_cn, 1, tree_num, 1, spcd, dia, ht,
                tpa_unadj, drybio_ag, drybio_bg, volcfnet, volcsnet, volbfnet
            ))
            tree_id += 1
    
    cursor.executemany("""
        INSERT INTO TREE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tree_data)
    
    # Evaluation
    eval_data = [
        ("EVAL001", 372301, "NC2023", "VOL", "NC 2023 Volume", 2018, 2023, 37)
    ]
    cursor.executemany("INSERT INTO POP_EVAL VALUES (?, ?, ?, ?, ?, ?, ?, ?)", eval_data)
    
    # Plot-Stratum assignments
    stratum_assgn_data = []
    for plot_num in range(1, 11):
        plt_cn = f"PLT{plot_num:03d}"
        assgn_cn = f"ASSGN{plot_num:03d}"
        stratum_assgn_data.append((assgn_cn, plt_cn, "STRAT001", 372301))
    
    cursor.executemany("""
        INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES (?, ?, ?, ?)
    """, stratum_assgn_data)
    
    # Stratum
    stratum_data = [
        ("STRAT001", 372301, 6000.0, 1.0, 1.1, 0.9, 10, 50, 50, 300000.0)
    ]
    cursor.executemany("""
        INSERT INTO POP_STRATUM VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, stratum_data)
    
    # Species reference
    species_data = [
        (131, "Loblolly pine", "Pinus taeda", "Pinus", "taeda"),
        (110, "Virginia pine", "Pinus virginiana", "Pinus", "virginiana"),
        (833, "Chestnut oak", "Quercus montana", "Quercus", "montana"),
        (802, "White oak", "Quercus alba", "Quercus", "alba"),
    ]
    cursor.executemany("INSERT INTO REF_SPECIES VALUES (?, ?, ?, ?, ?)", species_data)
    
    conn.commit()
    conn.close()
    
    yield temp_db_path


@pytest.fixture
def sample_fia_instance(sample_fia_db):
    """Create a FIA instance with sample data."""
    return FIA(str(sample_fia_db))


@pytest.fixture
def sample_evaluation():
    """Create a sample evaluation info object."""
    return EvaluationInfo(
        evalid=372301,
        statecd=37,
        eval_typ="VOL",
        start_invyr=2018,
        end_invyr=2023,
        nplots=10
    )


@pytest.fixture
def sample_plot_data():
    """Create sample plot data as Polars DataFrame."""
    return pl.DataFrame({
        "PLT_CN": [f"PLT{i:03d}" for i in range(1, 6)],
        "STATECD": [37] * 5,
        "INVYR": [2020] * 5,
        "LAT": [35.5, 35.6, 35.7, 35.8, 35.9],
        "LON": [-80.5, -80.4, -80.3, -80.2, -80.1],
        "PLOT_STATUS_CD": [1] * 5,
    })


@pytest.fixture
def sample_tree_data():
    """Create sample tree data as Polars DataFrame."""
    data = {
        "CN": [],
        "PLT_CN": [],
        "CONDID": [],
        "STATUSCD": [],
        "SPCD": [],
        "DIA": [],
        "TPA_UNADJ": [],
        "DRYBIO_AG": [],
        "VOLCFNET": [],
    }
    
    tree_id = 1
    for plot_num in range(1, 6):
        plt_cn = f"PLT{plot_num:03d}"
        for tree_num in range(1, 4):  # 3 trees per plot
            data["CN"].append(f"TREE{tree_id:04d}")
            data["PLT_CN"].append(plt_cn)
            data["CONDID"].append(1)
            data["STATUSCD"].append(1)  # Live
            data["SPCD"].append([131, 110, 833][tree_num - 1])
            data["DIA"].append(10.0 + tree_num * 2)
            data["TPA_UNADJ"].append(6.0)
            data["DRYBIO_AG"].append(20.0 + tree_num * 5)
            data["VOLCFNET"].append(15.0 + tree_num * 3)
            tree_id += 1
    
    return pl.DataFrame(data)


@pytest.fixture
def sample_condition_data():
    """Create sample condition data as Polars DataFrame."""
    return pl.DataFrame({
        "PLT_CN": [f"PLT{i:03d}" for i in range(1, 6)],
        "CONDID": [1] * 5,
        "COND_STATUS_CD": [1] * 5,  # Forest
        "CONDPROP_UNADJ": [1.0] * 5,
        "FORTYPCD": [220] * 5,  # Loblolly pine
        "OWNGRPCD": [10] * 5,   # National Forest
        "EXPNS": [6000.0] * 5,
    })


@pytest.fixture
def mock_database_connection():
    """Create a mock database connection for testing."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def sample_estimation_data():
    """Create comprehensive estimation test data."""
    return {
        "plot_data": pl.DataFrame({
            "PLT_CN": ["PLT001", "PLT002", "PLT003"],
            "STATECD": [37, 37, 37],
            "INVYR": [2020, 2020, 2020],
            "PLOT_STATUS_CD": [1, 1, 1],
        }),
        "tree_data": pl.DataFrame({
            "CN": ["TREE001", "TREE002", "TREE003", "TREE004"],
            "PLT_CN": ["PLT001", "PLT001", "PLT002", "PLT003"],
            "STATUSCD": [1, 1, 1, 1],
            "SPCD": [131, 110, 131, 833],
            "DIA": [12.0, 8.5, 15.2, 10.1],
            "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0],
            "DRYBIO_AG": [25.5, 18.2, 35.8, 22.4],
            "VOLCFNET": [20.1, 14.6, 28.9, 18.7],
        }),
        "condition_data": pl.DataFrame({
            "PLT_CN": ["PLT001", "PLT002", "PLT003"],
            "CONDID": [1, 1, 1],
            "COND_STATUS_CD": [1, 1, 1],
            "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
            "EXPNS": [6000.0, 6000.0, 6000.0],
        }),
        "evaluation": {
            "evalid": 372301,
            "statecd": 37,
            "start_year": 2018,
            "end_year": 2023,
        }
    }


