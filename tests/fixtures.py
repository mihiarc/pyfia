"""
Centralized test fixtures for pyFIA tests.

This module provides reusable test fixtures that can be shared across 
multiple test files to reduce code duplication and ensure consistency
in test data structures.
"""

from typing import Dict, Any
from unittest.mock import Mock

import polars as pl
import pytest


# =============================================================================
# Core Data Fixtures
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
        "TREECLCD": [2, 2, 2, 1, 3, 2],  # Growing stock=2, other classes
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
        "AGENTCD": [0, 0, 10, 0, 0, 0],  # Damage agents
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
        "SITECLCD": [3, 3, 2, None, None],  # Site class for timber
        "RESERVCD": [0, 0, 0, 0, 1],  # Reserved status
        "OWNGRPCD": [10, 10, 20, 30, 10],  # Ownership groups
        "FORTYPCD": [161, 161, 406, None, None],  # Forest type codes
        "STDSZCD": [1, 1, 2, None, None],  # Stand size classes
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
def standard_species_data():
    """Create standardized species reference data for testing."""
    return pl.DataFrame({
        "SPCD": [110, 121, 131, 202, 316, 621, 833, 802],
        "COMMON_NAME": [
            "Virginia pine",
            "Loblolly pine", 
            "Loblolly pine",
            "Black cherry",
            "Red maple",
            "Yellow-poplar",
            "Chestnut oak",
            "White oak"
        ],
        "SCIENTIFIC_NAME": [
            "Pinus virginiana",
            "Pinus taeda",
            "Pinus taeda", 
            "Prunus serotina",
            "Acer rubrum",
            "Liriodendron tulipifera",
            "Quercus montana",
            "Quercus alba"
        ],
        "GENUS": ["Pinus", "Pinus", "Pinus", "Prunus", "Acer", "Liriodendron", "Quercus", "Quercus"],
        "SPECIES": ["virginiana", "taeda", "taeda", "serotina", "rubrum", "tulipifera", "montana", "alba"],
    })


@pytest.fixture
def standard_stratum_data():
    """Create standardized stratum data for testing across modules."""
    return pl.DataFrame({
        "CN": ["S001", "S002", "S003"],
        "EVALID": [372301, 372301, 372301],
        "EXPNS": [1000.0, 1500.0, 2000.0],  # Acres per plot
        "ADJ_FACTOR_SUBP": [1.0, 1.0, 1.0],
        "ADJ_FACTOR_MACR": [0.25, 0.25, 0.25],  # Macroplot is 4x subplot
        "ADJ_FACTOR_MICR": [12.5, 12.5, 12.5],
        "P2POINTCNT": [100, 150, 75],  # Number of plots in stratum
        "P1POINTCNT": [500, 750, 375],  # Phase 1 points
        "P1PNTCNT_EU": [500, 750, 375],  # EU phase 1 points
        "STRATUM_WGT": [0.4, 0.35, 0.25],  # Stratum weights
        "AREA_USED": [40000.0, 52500.0, 15000.0],  # Total area in stratum
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
        "STATUSCD": [1, 1, 2, 1],  # Live, Live, Dead, Live
        "SPCD": [131, 110, 131, 316],  # Species codes
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
        "COND_STATUS_CD": [1, 1],  # Forest
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
def simple_estimation_dataset(
    simple_tree_data,
    simple_condition_data,
    simple_plot_data
):
    """Create a simple dataset for basic estimation tests."""
    return {
        "tree_data": simple_tree_data,
        "condition_data": simple_condition_data,
        "plot_data": simple_plot_data,
        "evalid": 372301,
        "statecd": 37,
    }


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_fia_database():
    """Create a mock FIA database object for testing."""
    db = Mock()
    db.evalid = [372301]  # NC 2023 evaluation
    db.statecd = [37]     # North Carolina
    db.tables = {}
    db.load_table = Mock()
    db.get_evaluation_info = Mock()
    # Add _reader mock to support _get_available_columns
    db._reader = Mock()
    db._reader.conn = None  # This will cause _get_available_columns to return None and fallback
    return db


@pytest.fixture
def mock_database_query_interface():
    """Create a mock database query interface for testing."""
    mock_interface = Mock()
    mock_interface.execute_query = Mock()
    mock_interface.get_table_columns = Mock()
    mock_interface.table_exists = Mock(return_value=True)
    return mock_interface


# =============================================================================
# Domain-Specific Fixtures
# =============================================================================

@pytest.fixture
def mortality_tree_data():
    """Create tree data specific to mortality testing."""
    return pl.DataFrame({
        "CN": ["MT1", "MT2", "MT3", "MT4"],
        "PLT_CN": ["P1", "P1", "P2", "P2"],
        "STATUSCD": [2, 2, 2, 1],  # Dead trees for mortality
        "AGENTCD": [10, 30, 40, 0],  # Mortality agents
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
        "STATUSCD": [1, 1, 1, 1, 2],  # Live trees
        "TREECLCD": [2, 2, 2, 1, 2],  # Growing stock class 2
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
        "SPCD": [110, 121, 131, 316, 621, 833],  # Diverse species
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
        "COND_STATUS_CD": [1, 1, 1],  # All forest
        "SITECLCD": [1, 2, 3],  # Productive timber sites
        "RESERVCD": [0, 0, 0],  # Not reserved
        "OWNGRPCD": [10, 20, 30],  # Different ownerships
        "FORTYPCD": [161, 406, 703],  # Different forest types
        "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP", "MACR"],
        "EXPNS": [6000.0, 6000.0, 6000.0],
    })


# =============================================================================
# Test Configuration Fixtures
# =============================================================================

@pytest.fixture
def test_evalid():
    """Standard evaluation ID for testing."""
    return 372301


@pytest.fixture
def test_statecd():
    """Standard state code for testing (North Carolina)."""
    return 37


# =============================================================================
# Fixture Aliases for Backward Compatibility
# =============================================================================

@pytest.fixture
def sample_plot_data(standard_plot_data):
    """Alias for standard_plot_data for backward compatibility."""
    return standard_plot_data


@pytest.fixture
def sample_cond_data(standard_condition_data):
    """Alias for standard_condition_data for backward compatibility."""
    return standard_condition_data


@pytest.fixture
def sample_tree_data(standard_tree_data):
    """Alias for standard_tree_data for backward compatibility."""
    return standard_tree_data


@pytest.fixture
def sample_stratum_data(standard_stratum_data):
    """Alias for standard_stratum_data for backward compatibility."""
    return standard_stratum_data


@pytest.fixture
def sample_ppsa_data(standard_ppsa_data):
    """Alias for standard_ppsa_data for backward compatibility."""
    return standard_ppsa_data


@pytest.fixture
def mock_db(mock_fia_database):
    """Alias for mock_fia_database for backward compatibility."""
    return mock_fia_database


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


# =============================================================================
# GRM (Growth-Removal-Mortality) Test Fixtures
# =============================================================================

@pytest.fixture
def grm_component_data():
    """Create standardized TREE_GRM_COMPONENT data for growth/mortality/removal tests."""
    return pl.DataFrame({
        "TRE_CN": ["GRM_T1", "GRM_T2", "GRM_T3", "GRM_T4", "GRM_T5", "GRM_T6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "DIA_BEGIN": [10.0, None, 8.5, 15.0, None, 25.0],
        "DIA_MIDPT": [11.0, 5.5, 9.2, 15.8, 7.0, 26.5],
        "DIA_END": [12.0, 6.0, 10.0, 16.5, 7.8, 28.0],
        # GS FOREST columns (growing stock on forest land)
        "SUBP_COMPONENT_GS_FOREST": ["SURVIVOR", "INGROWTH", "SURVIVOR", "SURVIVOR", "REVERSION1", "SURVIVOR"],
        "SUBP_TPAGROW_UNADJ_GS_FOREST": [2.5, 1.8, 1.2, 3.2, 0.9, 4.1],
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 2, 1, 0, 1, 3],  # SUBP, MICR, SUBP, None, SUBP, MACR
        # AL FOREST columns (all live on forest land)
        "SUBP_COMPONENT_AL_FOREST": ["SURVIVOR", "INGROWTH", "SURVIVOR", "SURVIVOR", "REVERSION1", "SURVIVOR"],
        "SUBP_TPAGROW_UNADJ_AL_FOREST": [2.5, 1.8, 1.2, 3.2, 0.9, 4.1],
        "SUBP_SUBPTYP_GRM_AL_FOREST": [1, 2, 1, 0, 1, 3],
        # GS TIMBER columns (growing stock on timber land)
        "SUBP_COMPONENT_GS_TIMBER": ["SURVIVOR", "INGROWTH", "SURVIVOR", "SURVIVOR", "REVERSION1", "SURVIVOR"],
        "SUBP_TPAGROW_UNADJ_GS_TIMBER": [2.5, 1.8, 1.2, 3.2, 0.9, 4.1],
        "SUBP_SUBPTYP_GRM_GS_TIMBER": [1, 2, 1, 0, 1, 3],
    })


@pytest.fixture
def grm_midpt_data():
    """Create standardized TREE_GRM_MIDPT data for current measurements."""
    return pl.DataFrame({
        "TRE_CN": ["GRM_T1", "GRM_T2", "GRM_T3", "GRM_T4", "GRM_T5", "GRM_T6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "DIA": [11.0, 5.5, 9.2, 15.8, 7.0, 26.5],
        "SPCD": [131, 110, 316, 131, 833, 621],  # Various species
        "STATUSCD": [1, 1, 1, 1, 1, 1],  # All live
        "VOLCFNET": [25.5, 12.8, 18.6, 45.2, 15.0, 85.7],  # Net cubic foot volume
        "DRYBIO_AG": [180.2, 95.1, 132.5, 320.5, 105.8, 580.3],  # Aboveground biomass
    })


@pytest.fixture
def grm_begin_data():
    """Create standardized TREE_GRM_BEGIN data for beginning measurements."""
    return pl.DataFrame({
        "TRE_CN": ["GRM_T1", "GRM_T3", "GRM_T4", "GRM_T6"],  # Only SURVIVOR trees have beginning data
        "PLT_CN": ["P1", "P2", "P2", "P3"],
        "VOLCFNET": [20.1, 16.8, 40.8, 75.3],  # Beginning volumes (for growth calculation)
        "DRYBIO_AG": [150.8, 120.2, 290.2, 520.1],  # Beginning biomass
    })


@pytest.fixture
def grm_mortality_component_data():
    """Create GRM component data specifically for mortality testing."""
    return pl.DataFrame({
        "TRE_CN": ["MORT_T1", "MORT_T2", "MORT_T3", "MORT_T4", "MORT_T5"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
        "DIA_BEGIN": [12.0, 8.5, 15.0, 10.2, 18.5],
        "DIA_MIDPT": [12.0, 8.5, 15.0, 10.2, 18.5],  # Same as begin for dead trees
        "DIA_END": [12.0, 8.5, 15.0, 10.2, 18.5],
        # Components for mortality
        "SUBP_COMPONENT_GS_FOREST": ["MORTALITY1", "MORTALITY2", "MORTALITY1", "MORTALITY1", "MORTALITY2"],
        "SUBP_TPAMORT_UNADJ_GS_FOREST": [2.2, 1.8, 3.5, 2.1, 4.2],  # Mortality TPA
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 2, 1, 1, 3],  # Various adjustment types
        # Repeat for AL and TIMBER
        "SUBP_COMPONENT_AL_FOREST": ["MORTALITY1", "MORTALITY2", "MORTALITY1", "MORTALITY1", "MORTALITY2"],
        "SUBP_TPAMORT_UNADJ_AL_FOREST": [2.2, 1.8, 3.5, 2.1, 4.2],
        "SUBP_SUBPTYP_GRM_AL_FOREST": [1, 2, 1, 1, 3],
    })


@pytest.fixture
def grm_removal_component_data():
    """Create GRM component data specifically for removal testing."""
    return pl.DataFrame({
        "TRE_CN": ["REM_T1", "REM_T2", "REM_T3", "REM_T4"],
        "PLT_CN": ["P1", "P1", "P2", "P2"],
        "DIA_BEGIN": [14.0, 11.5, 18.0, 22.5],
        "DIA_MIDPT": [14.0, 11.5, 18.0, 22.5],
        "DIA_END": [14.0, 11.5, 18.0, 22.5],
        # Components for removals
        "SUBP_COMPONENT_GS_FOREST": ["CUT1", "CUT2", "DIVERSION1", "CUT1"],
        "SUBP_TPAREMV_UNADJ_GS_FOREST": [3.2, 2.8, 1.5, 4.1],  # Removal TPA
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 1, 2, 3],  # Various adjustment types
        # Repeat for AL and TIMBER
        "SUBP_COMPONENT_AL_FOREST": ["CUT1", "CUT2", "DIVERSION1", "CUT1"],
        "SUBP_TPAREMV_UNADJ_AL_FOREST": [3.2, 2.8, 1.5, 4.1],
        "SUBP_SUBPTYP_GRM_AL_FOREST": [1, 1, 2, 3],
    })


@pytest.fixture
def extended_plot_data_with_remper():
    """Create plot data with REMPER (remeasurement period) for GRM calculations."""
    return pl.DataFrame({
        "CN": ["P1", "P2", "P3", "P4", "P5"],
        "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
        "STATECD": [37, 37, 37, 37, 37],
        "COUNTYCD": [1, 1, 1, 1, 1],
        "PLOT": [1, 2, 3, 4, 5],
        "INVYR": [2020, 2020, 2020, 2021, 2021],
        "LAT": [35.5, 35.6, 35.7, 35.8, 35.9],
        "LON": [-80.5, -80.4, -80.3, -80.2, -80.1],
        "PLOT_STATUS_CD": [1, 1, 1, 1, 1],
        "MACRO_BREAKPOINT_DIA": [24.0, 24.0, 24.0, 24.0, 24.0],
        "REMPER": [5.0, 4.5, None, 5.2, 6.0],  # Remeasurement periods (None for missing)
    })


@pytest.fixture
def alstkcd_condition_data():
    """Create condition data with ALSTKCD (stocking class) for grouping tests."""
    return pl.DataFrame({
        "CN": ["C1", "C2", "C3", "C4", "C5"],
        "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
        "CONDID": [1, 1, 1, 1, 1],
        "COND_STATUS_CD": [1, 1, 1, 1, 1],  # All forest
        "CONDPROP_UNADJ": [1.0, 1.0, 1.0, 1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "MACR", "SUBP"],
        "SITECLCD": [3, 2, 1, 3, 2],  # Site classes
        "RESERVCD": [0, 0, 0, 0, 1],  # Reserved status
        "OWNGRPCD": [40, 40, 10, 20, 30],  # Various ownerships
        "FORTYPCD": [161, 406, 703, 161, 621],  # Forest types
        "STDSZCD": [1, 2, 3, 1, 2],  # Stand size classes
        "ALSTKCD": [1, 2, 3, 4, 5],  # All stocking classes (1=Over, 2=Full, 3=Medium, 4=Poor, 5=Non)
    })


@pytest.fixture
def comprehensive_grm_dataset(
    grm_component_data,
    grm_midpt_data,
    grm_begin_data,
    extended_plot_data_with_remper,
    alstkcd_condition_data,
    standard_stratum_data,
    standard_ppsa_data
):
    """Create a comprehensive GRM dataset for growth/mortality/removal testing."""
    return {
        "grm_component": grm_component_data,
        "grm_midpt": grm_midpt_data,
        "grm_begin": grm_begin_data,
        "plot_data": extended_plot_data_with_remper,
        "condition_data": alstkcd_condition_data,
        "stratum_data": standard_stratum_data,
        "ppsa_data": standard_ppsa_data,
        "evalid": 372301,
        "statecd": 37,
    }


@pytest.fixture
def grm_component_types():
    """Standard GRM component types for validation."""
    return {
        "growth_components": ["SURVIVOR", "INGROWTH", "REVERSION1", "REVERSION2"],
        "mortality_components": ["MORTALITY1", "MORTALITY2"],
        "removal_components": ["CUT1", "CUT2", "CUT3", "DIVERSION1", "DIVERSION2"],
        "all_components": [
            "SURVIVOR", "INGROWTH", "REVERSION1", "REVERSION2",
            "MORTALITY1", "MORTALITY2",
            "CUT1", "CUT2", "CUT3", "DIVERSION1", "DIVERSION2"
        ]
    }


@pytest.fixture
def subptyp_grm_mappings():
    """Standard SUBPTYP_GRM adjustment factor mappings."""
    return {
        0: {"name": "None", "description": "Not sampled/no adjustment"},
        1: {"name": "SUBP", "description": "Subplot adjustment (typically 5.0-24.0\" DBH)"},
        2: {"name": "MICR", "description": "Microplot adjustment (typically <5.0\" DBH)"},
        3: {"name": "MACR", "description": "Macroplot adjustment (typically ≥24.0\" DBH)"}
    }