"""
Growth-Removal-Mortality (GRM) fixtures for pyFIA tests.

Provides specialized fixtures for testing growth, mortality,
and removal estimation functions.
"""

import polars as pl
import pytest


@pytest.fixture
def grm_component_data():
    """Create standardized TREE_GRM_COMPONENT data for growth/mortality/removal tests."""
    return pl.DataFrame({
        "TRE_CN": ["GRM_T1", "GRM_T2", "GRM_T3", "GRM_T4", "GRM_T5", "GRM_T6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "DIA_BEGIN": [10.0, None, 8.5, 15.0, None, 25.0],
        "DIA_MIDPT": [11.0, 5.5, 9.2, 15.8, 7.0, 26.5],
        "DIA_END": [12.0, 6.0, 10.0, 16.5, 7.8, 28.0],
        # GS FOREST columns
        "SUBP_COMPONENT_GS_FOREST": ["SURVIVOR", "INGROWTH", "SURVIVOR", "SURVIVOR", "REVERSION1", "SURVIVOR"],
        "SUBP_TPAGROW_UNADJ_GS_FOREST": [2.5, 1.8, 1.2, 3.2, 0.9, 4.1],
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 2, 1, 0, 1, 3],
        # AL FOREST columns
        "SUBP_COMPONENT_AL_FOREST": ["SURVIVOR", "INGROWTH", "SURVIVOR", "SURVIVOR", "REVERSION1", "SURVIVOR"],
        "SUBP_TPAGROW_UNADJ_AL_FOREST": [2.5, 1.8, 1.2, 3.2, 0.9, 4.1],
        "SUBP_SUBPTYP_GRM_AL_FOREST": [1, 2, 1, 0, 1, 3],
        # GS TIMBER columns
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
        "SPCD": [131, 110, 316, 131, 833, 621],
        "STATUSCD": [1, 1, 1, 1, 1, 1],
        "VOLCFNET": [25.5, 12.8, 18.6, 45.2, 15.0, 85.7],
        "DRYBIO_AG": [180.2, 95.1, 132.5, 320.5, 105.8, 580.3],
    })


@pytest.fixture
def grm_begin_data():
    """Create standardized TREE_GRM_BEGIN data for beginning measurements."""
    return pl.DataFrame({
        "TRE_CN": ["GRM_T1", "GRM_T3", "GRM_T4", "GRM_T6"],
        "PLT_CN": ["P1", "P2", "P2", "P3"],
        "VOLCFNET": [20.1, 16.8, 40.8, 75.3],
        "DRYBIO_AG": [150.8, 120.2, 290.2, 520.1],
    })


@pytest.fixture
def grm_mortality_component_data():
    """Create GRM component data specifically for mortality testing."""
    return pl.DataFrame({
        "TRE_CN": ["MORT_T1", "MORT_T2", "MORT_T3", "MORT_T4", "MORT_T5"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3"],
        "DIA_BEGIN": [12.0, 8.5, 15.0, 10.2, 18.5],
        "DIA_MIDPT": [12.0, 8.5, 15.0, 10.2, 18.5],
        "DIA_END": [12.0, 8.5, 15.0, 10.2, 18.5],
        "SUBP_COMPONENT_GS_FOREST": ["MORTALITY1", "MORTALITY2", "MORTALITY1", "MORTALITY1", "MORTALITY2"],
        "SUBP_TPAMORT_UNADJ_GS_FOREST": [2.2, 1.8, 3.5, 2.1, 4.2],
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 2, 1, 1, 3],
        "SUBP_COMPONENT_AL_FOREST": ["MORTALITY1", "MORTALITY2", "MORTALITY1", "MORTALITY1", "MORTALITY2"],
        "SUBP_TPAMORT_UNADJ_AL_FOREST": [2.2, 1.8, 3.5, 2.1, 4.2],
        "SUBP_SUBPTYP_GRM_AL_FOREST": [1, 2, 1, 1, 3],
    })


@pytest.fixture
def grm_mortality_with_agentcd_data():
    """Create GRM mortality component data with AGENTCD for cause-of-death grouping tests."""
    return pl.DataFrame({
        "TRE_CN": ["MORT_A1", "MORT_A2", "MORT_A3", "MORT_A4", "MORT_A5", "MORT_A6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "DIA_BEGIN": [12.0, 8.5, 15.0, 10.2, 18.5, 14.0],
        "DIA_MIDPT": [12.0, 8.5, 15.0, 10.2, 18.5, 14.0],
        "DIA_END": [12.0, 8.5, 15.0, 10.2, 18.5, 14.0],
        "SUBP_COMPONENT_GS_FOREST": ["MORTALITY1", "MORTALITY2", "MORTALITY1", "MORTALITY1", "MORTALITY2", "MORTALITY1"],
        "SUBP_TPAMORT_UNADJ_GS_FOREST": [2.2, 1.8, 3.5, 2.1, 4.2, 2.5],
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 2, 1, 1, 3, 1],
    })


@pytest.fixture
def tree_data_with_agentcd():
    """Create TREE table data with AGENTCD for mortality cause analysis.

    AGENTCD codes:
    - 10: Insect
    - 20: Disease
    - 30: Fire
    - 40: Animal
    - 50: Weather
    - 60: Vegetation (competition)
    - 70: Unknown/other
    - 80: Silvicultural/land clearing
    """
    return pl.DataFrame({
        "CN": ["MORT_A1", "MORT_A2", "MORT_A3", "MORT_A4", "MORT_A5", "MORT_A6"],
        "PLT_CN": ["P1", "P1", "P2", "P2", "P3", "P3"],
        "AGENTCD": [30, 10, 50, 20, 30, 10],  # Fire, Insect, Weather, Disease, Fire, Insect
    })


@pytest.fixture
def condition_data_with_dstrbcd():
    """Create condition data with DSTRBCD (disturbance codes) for grouping tests.

    DSTRBCD codes (primary):
    - 0: No disturbance
    - 10: Insect damage
    - 20: Disease damage
    - 30: Fire damage
    - 40: Animal damage
    - 50: Weather damage (includes 52=hurricane/wind)
    - 54: Drought
    - 60: Vegetation (competition)
    - 70: Unknown
    - 80: Human (includes harvest, clearing)
    """
    return pl.DataFrame({
        "CN": ["C1", "C2", "C3", "C4", "C5"],
        "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
        "CONDID": [1, 1, 1, 1, 1],
        "COND_STATUS_CD": [1, 1, 1, 1, 1],
        "CONDPROP_UNADJ": [1.0, 1.0, 1.0, 1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "MACR", "SUBP"],
        "SITECLCD": [3, 2, 1, 3, 2],
        "RESERVCD": [0, 0, 0, 0, 1],
        "OWNGRPCD": [40, 40, 10, 20, 30],
        "FORTYPCD": [161, 406, 703, 161, 621],
        "STDSZCD": [1, 2, 3, 1, 2],
        "ALSTKCD": [1, 2, 3, 4, 5],
        "DSTRBCD1": [30, 10, 52, 0, 54],  # Fire, Insect, Hurricane, None, Drought
        "DSTRBCD2": [0, 0, 0, 0, 0],
        "DSTRBCD3": [0, 0, 0, 0, 0],
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
        "SUBP_COMPONENT_GS_FOREST": ["CUT1", "CUT2", "DIVERSION1", "CUT1"],
        "SUBP_TPAREMV_UNADJ_GS_FOREST": [3.2, 2.8, 1.5, 4.1],
        "SUBP_SUBPTYP_GRM_GS_FOREST": [1, 1, 2, 3],
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
        "REMPER": [5.0, 4.5, None, 5.2, 6.0],
    })


@pytest.fixture
def alstkcd_condition_data():
    """Create condition data with ALSTKCD (stocking class) for grouping tests."""
    return pl.DataFrame({
        "CN": ["C1", "C2", "C3", "C4", "C5"],
        "PLT_CN": ["P1", "P2", "P3", "P4", "P5"],
        "CONDID": [1, 1, 1, 1, 1],
        "COND_STATUS_CD": [1, 1, 1, 1, 1],
        "CONDPROP_UNADJ": [1.0, 1.0, 1.0, 1.0, 1.0],
        "PROP_BASIS": ["SUBP", "SUBP", "SUBP", "MACR", "SUBP"],
        "SITECLCD": [3, 2, 1, 3, 2],
        "RESERVCD": [0, 0, 0, 0, 1],
        "OWNGRPCD": [40, 40, 10, 20, 30],
        "FORTYPCD": [161, 406, 703, 161, 621],
        "STDSZCD": [1, 2, 3, 1, 2],
        "ALSTKCD": [1, 2, 3, 4, 5],
    })


@pytest.fixture
def comprehensive_grm_dataset(
    grm_component_data,
    grm_midpt_data,
    grm_begin_data,
    extended_plot_data_with_remper,
    alstkcd_condition_data,
):
    """Create a comprehensive GRM dataset for growth/mortality/removal testing."""
    return {
        "grm_component": grm_component_data,
        "grm_midpt": grm_midpt_data,
        "grm_begin": grm_begin_data,
        "plot_data": extended_plot_data_with_remper,
        "condition_data": alstkcd_condition_data,
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
        3: {"name": "MACR", "description": "Macroplot adjustment (typically >=24.0\" DBH)"}
    }
