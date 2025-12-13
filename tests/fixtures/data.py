"""
Core data fixtures for pyFIA tests.

Provides standardized FIA data structures for testing estimation,
filtering, and aggregation functions.
"""

import polars as pl
import pytest


# =============================================================================
# Standard FIA Table Fixtures
# =============================================================================

@pytest.fixture
def standard_tree_data():
    """Create standardized tree data for testing across modules."""
    return pl.DataFrame({
        "CN": ["T001", "T002", "T003", "T004", "T005", "T006"],
        "PLT_CN": ["P001", "P001", "P002", "P002", "P003", "P003"],
        "CONDID": [1, 1, 1, 2, 1, 1],
        "TREE": [1, 2, 1, 1, 1, 2],
        "STATUSCD": [1, 1, 2, 1, 1, 1],  # Live=1, Dead=2
        "TREECLCD": [2, 2, 2, 1, 3, 2],  # Growing stock=2
        "SPCD": [131, 110, 131, 316, 833, 121],  # Common species
        "DIA": [10.5, 8.2, 15.3, 12.0, 6.5, 25.0],
        "HT": [45.0, 35.0, 55.0, 50.0, 25.0, 75.0],
        "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0, 6.0, 6.0],
        "DRYBIO_AG": [25.5, 18.2, 35.8, 28.0, 12.4, 85.0],
        "DRYBIO_BG": [5.1, 3.6, 7.2, 5.6, 2.5, 17.0],
        "VOLCFNET": [20.1, 14.6, 28.9, 22.4, 10.0, 68.5],
        "VOLCSNET": [15.2, 11.0, 21.8, 16.9, 7.5, 51.8],
        "VOLBFNET": [95.0, 68.5, 136.2, 105.5, 46.8, 323.4],
        "VOLCFGRS": [22.1, 16.1, 31.8, 24.6, 11.0, 75.4],
        "VOLCSGRS": [16.7, 12.1, 24.0, 18.6, 8.3, 57.0],
        "VOLBFGRS": [104.5, 75.4, 149.8, 116.1, 51.5, 355.7],
        "COMPONENT": ["LIVE", "LIVE", "MORTALITY", "LIVE", "LIVE", "LIVE"],
        "AGENTCD": [0, 0, 10, 0, 0, 0],
    })


@pytest.fixture
def standard_condition_data():
    """Create standardized condition data for testing across modules."""
    return pl.DataFrame({
        "CN": ["C001", "C002", "C003", "C004", "C005"],
        "PLT_CN": ["P001", "P001", "P002", "P003", "P004"],
        "CONDID": [1, 2, 1, 1, 1],
        "COND_STATUS_CD": [1, 1, 1, 2, 3],  # Forest=1, Non-forest=2, Water=3
        "CONDPROP_UNADJ": [0.7, 0.3, 1.0, 1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP", "MACR", "SUBP", "SUBP"],
        "SITECLCD": [3, 3, 2, None, None],
        "RESERVCD": [0, 0, 0, 0, 1],
        "OWNGRPCD": [10, 10, 20, 30, 10],
        "FORTYPCD": [161, 161, 406, None, None],
        "STDSZCD": [1, 1, 2, None, None],
        "EXPNS": [6000.0, 6000.0, 6000.0, 6000.0, 6000.0],
    })


@pytest.fixture
def standard_plot_data():
    """Create standardized plot data for testing across modules."""
    return pl.DataFrame({
        "CN": ["P001", "P002", "P003", "P004", "P005"],
        "PLT_CN": ["P001", "P002", "P003", "P004", "P005"],
        "STATECD": [37, 37, 37, 37, 37],
        "COUNTYCD": [1, 1, 1, 1, 1],
        "PLOT": [1, 2, 3, 4, 5],
        "INVYR": [2020, 2020, 2020, 2021, 2021],
        "LAT": [35.5, 35.6, 35.7, 35.8, 35.9],
        "LON": [-80.5, -80.4, -80.3, -80.2, -80.1],
        "PLOT_STATUS_CD": [1, 1, 1, 1, 1],
        "MACRO_BREAKPOINT_DIA": [24.0, 24.0, 24.0, 24.0, 24.0],
    })


@pytest.fixture
def standard_stratum_data():
    """Create standardized stratum data for testing across modules."""
    return pl.DataFrame({
        "CN": ["S001", "S002", "S003"],
        "EVALID": [372301, 372301, 372301],
        "EXPNS": [1000.0, 1500.0, 2000.0],
        "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0],
        "ADJ_FACTOR_MACR": [0.25, 0.25, 0.25],
        "ADJ_FACTOR_MICR": [12.5, 12.5, 12.5],
        "P2POINTCNT": [100, 150, 75],
        "P1POINTCNT": [500, 750, 375],
        "P1PNTCNT_EU": [500, 750, 375],
        "STRATUM_WGT": [0.4, 0.35, 0.25],
        "AREA_USED": [40000.0, 52500.0, 15000.0],
    })


@pytest.fixture
def standard_ppsa_data():
    """Create standardized plot-stratum assignment data for testing."""
    return pl.DataFrame({
        "CN": ["A001", "A002", "A003", "A004", "A005"],
        "PLT_CN": ["P001", "P002", "P003", "P004", "P005"],
        "STRATUM_CN": ["S001", "S001", "S002", "S002", "S003"],
        "EVALID": [372301, 372301, 372301, 372301, 372301],
    })


@pytest.fixture
def standard_evaluation_data():
    """Create standardized evaluation data for testing."""
    return pl.DataFrame({
        "CN": ["E001"],
        "EVALID": [372301],
        "EVAL_GRP": ["NC2023"],
        "EVAL_TYP": ["VOL"],
        "EVAL_DESCR": ["NC 2023 Volume"],
        "START_INVYR": [2018],
        "END_INVYR": [2023],
        "STATECD": [37],
    })


@pytest.fixture
def standard_species_data():
    """Create standardized species reference data for testing."""
    return pl.DataFrame({
        "SPCD": [110, 121, 131, 202, 316, 621, 833, 802],
        "COMMON_NAME": [
            "Virginia pine", "Loblolly pine", "Loblolly pine",
            "Black cherry", "Red maple", "Yellow-poplar",
            "Chestnut oak", "White oak"
        ],
        "SCIENTIFIC_NAME": [
            "Pinus virginiana", "Pinus taeda", "Pinus taeda",
            "Prunus serotina", "Acer rubrum", "Liriodendron tulipifera",
            "Quercus montana", "Quercus alba"
        ],
        "GENUS": ["Pinus", "Pinus", "Pinus", "Prunus", "Acer", "Liriodendron", "Quercus", "Quercus"],
        "SPECIES": ["virginiana", "taeda", "taeda", "serotina", "rubrum", "tulipifera", "montana", "alba"],
    })


# =============================================================================
# Simplified Test Data Fixtures
# =============================================================================

@pytest.fixture
def simple_tree_data():
    """Create simple tree data for basic tests."""
    return pl.DataFrame({
        "CN": ["T1", "T2", "T3", "T4"],
        "PLT_CN": ["P1", "P1", "P2", "P2"],
        "CONDID": [1, 1, 1, 1],
        "STATUSCD": [1, 1, 2, 1],
        "SPCD": [131, 110, 131, 316],
        "DIA": [12.0, 8.5, 15.2, 10.1],
        "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0],
        "DRYBIO_AG": [25.0, 18.0, 35.0, 22.0],
        "VOLCFNET": [20.0, 14.5, 29.0, 18.5],
    })


@pytest.fixture
def simple_condition_data():
    """Create simple condition data for basic tests."""
    return pl.DataFrame({
        "CN": ["C1", "C2"],
        "PLT_CN": ["P1", "P2"],
        "CONDID": [1, 1],
        "COND_STATUS_CD": [1, 1],
        "CONDPROP_UNADJ": [1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP"],
        "EXPNS": [6000.0, 6000.0],
    })


@pytest.fixture
def simple_plot_data():
    """Create simple plot data for basic tests."""
    return pl.DataFrame({
        "CN": ["P1", "P2"],
        "PLT_CN": ["P1", "P2"],
        "STATECD": [37, 37],
        "INVYR": [2020, 2020],
        "PLOT_STATUS_CD": [1, 1],
        "MACRO_BREAKPOINT_DIA": [24.0, 24.0],
    })


# =============================================================================
# Domain-Specific Test Data
# =============================================================================

@pytest.fixture
def mortality_tree_data():
    """Create tree data specific to mortality testing."""
    return pl.DataFrame({
        "CN": ["MT1", "MT2", "MT3", "MT4"],
        "PLT_CN": ["P1", "P1", "P2", "P2"],
        "STATUSCD": [2, 2, 2, 1],
        "AGENTCD": [10, 30, 40, 0],
        "COMPONENT": ["MORTALITY", "MORTALITY", "MORTALITY", "LIVE"],
        "SPCD": [131, 131, 110, 316],
        "DIA": [12.0, 15.5, 8.2, 10.0],
        "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0],
        "DRYBIO_AG": [25.0, 35.0, 18.0, 22.0],
        "VOLCFNET": [20.0, 28.0, 14.5, 18.0],
    })


@pytest.fixture
def growing_stock_tree_data():
    """Create tree data for growing stock tests."""
    return pl.DataFrame({
        "CN": ["GST1", "GST2", "GST3", "GST4", "GST5"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P2"],
        "STATUSCD": [1, 1, 1, 1, 2],
        "TREECLCD": [2, 2, 2, 1, 2],
        "SPCD": [131, 110, 316, 833, 131],
        "DIA": [10.0, 12.5, 8.5, 15.0, 20.0],
        "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0, 6.0],
        "DRYBIO_AG": [22.0, 28.0, 18.5, 35.0, 45.0],
        "VOLCFNET": [18.0, 22.5, 15.0, 28.0, 36.0],
    })


@pytest.fixture
def multi_species_tree_data():
    """Create diverse species tree data for grouping tests."""
    return pl.DataFrame({
        "CN": ["MST1", "MST2", "MST3", "MST4", "MST5", "MST6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "STATUSCD": [1, 1, 1, 1, 1, 1],
        "SPCD": [110, 121, 131, 316, 621, 833],
        "DIA": [8.0, 10.5, 12.0, 9.5, 16.0, 14.5],
        "TPA_UNADJ": [6.0, 6.0, 6.0, 6.0, 6.0, 6.0],
        "DRYBIO_AG": [18.0, 23.0, 26.0, 21.0, 38.0, 32.0],
        "VOLCFNET": [14.5, 18.5, 20.5, 17.0, 30.0, 26.0],
    })


@pytest.fixture
def timber_condition_data():
    """Create condition data for timber land tests."""
    return pl.DataFrame({
        "CN": ["TC1", "TC2", "TC3"],
        "PLT_CN": ["P1", "P2", "P3"],
        "CONDID": [1, 1, 1],
        "COND_STATUS_CD": [1, 1, 1],
        "SITECLCD": [1, 2, 3],
        "RESERVCD": [0, 0, 0],
        "OWNGRPCD": [10, 20, 30],
        "FORTYPCD": [161, 406, 703],
        "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP", "MACR"],
        "EXPNS": [6000.0, 6000.0, 6000.0],
    })


# =============================================================================
# Composite Fixtures
# =============================================================================

@pytest.fixture
def standard_estimation_dataset(
    standard_tree_data,
    standard_condition_data,
    standard_plot_data,
    standard_stratum_data,
    standard_ppsa_data,
    standard_evaluation_data
):
    """Create a complete standardized dataset for estimation tests."""
    return {
        "tree_data": standard_tree_data,
        "condition_data": standard_condition_data,
        "plot_data": standard_plot_data,
        "stratum_data": standard_stratum_data,
        "ppsa_data": standard_ppsa_data,
        "evaluation_data": standard_evaluation_data,
        "evalid": 372301,
        "statecd": 37,
    }


@pytest.fixture
def simple_estimation_dataset(simple_tree_data, simple_condition_data, simple_plot_data):
    """Create a simple dataset for basic estimation tests."""
    return {
        "tree_data": simple_tree_data,
        "condition_data": simple_condition_data,
        "plot_data": simple_plot_data,
        "evalid": 372301,
        "statecd": 37,
    }


# =============================================================================
# Reference Data Fixtures
# =============================================================================

@pytest.fixture
def common_species_codes():
    """Common FIA species codes for testing."""
    return {
        "loblolly_pine": 131,
        "virginia_pine": 110,
        "red_maple": 316,
        "chestnut_oak": 833,
        "white_oak": 802,
        "yellow_poplar": 621,
        "black_cherry": 202,
    }


@pytest.fixture
def forest_type_codes():
    """Common FIA forest type codes for testing."""
    return {
        "loblolly_shortleaf": 161,
        "oak_pine": 406,
        "white_red_black_oak": 703,
        "yellow_poplar": 621,
    }


@pytest.fixture
def test_evalid():
    """Standard evaluation ID for testing."""
    return 372301


@pytest.fixture
def test_statecd():
    """Standard state code for testing (North Carolina)."""
    return 37


