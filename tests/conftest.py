"""
Pytest configuration and shared fixtures for pyFIA tests.

This module provides reusable test fixtures for consistent testing
across all pyFIA modules.
"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from pyfia import FIA
from pyfia.models import EvaluationInfo

# Import centralized fixtures to make them available globally
from fixtures import *


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
            VOLCFGRS REAL,
            VOLCSGRS REAL,
            VOLBFGRS REAL,
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
            volcfgrs = volcfnet * 1.1  # Gross volume (10% more than net)
            volcsgrs = volcsnet * 1.1  # Gross sawlog volume
            volbfgrs = volbfnet * 1.1  # Gross board feet

            tree_data.append((
                tree_cn, plt_cn, 1, tree_num, 1, spcd, dia, ht,
                tpa_unadj, drybio_ag, drybio_bg, volcfnet, volcsnet, volbfnet,
                volcfgrs, volcsgrs, volbfgrs
            ))
            tree_id += 1

    cursor.executemany("""
        INSERT INTO TREE VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


# Legacy fixtures removed - use centralized fixtures from fixtures.py instead


# Legacy sample_estimation_data fixture removed - use standard_estimation_dataset or simple_estimation_dataset instead


