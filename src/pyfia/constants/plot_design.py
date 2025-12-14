"""
Plot design and diameter constants.

Contains FIA plot design parameters, diameter breakpoints, size class
definitions, and plot basis types.
"""

from typing import Dict, Tuple


class PlotDesign:
    """FIA plot design parameters."""

    # Plot radii in feet
    MICROPLOT_RADIUS_FT = 6.8
    SUBPLOT_RADIUS_FT = 24.0
    MACROPLOT_RADIUS_FT = 58.9

    # Plot areas as fraction of acre
    MICROPLOT_AREA_ACRES = 1 / 300  # 0.00333 acres
    SUBPLOT_AREA_ACRES = 1 / 24  # 0.04167 acres
    MACROPLOT_AREA_ACRES = 1 / 4  # 0.25 acres

    # Standard plot design code
    STANDARD_DESIGN_CD = 1


class DiameterBreakpoints:
    """Standard diameter breakpoints in inches."""

    # Minimum measurable diameter
    MIN_DBH = 1.0

    # Microplot/Subplot boundary
    MICROPLOT_MAX_DIA = 5.0
    SUBPLOT_MIN_DIA = 5.0

    # Volume thresholds
    BOARD_FOOT_MIN_DIA = 9.0
    SAWLOG_MIN_DIA = 9.0

    # Size class boundaries for grouping
    SIZE_CLASS_BOUNDARIES = [1.0, 5.0, 10.0, 20.0, 30.0]


class PlotBasis:
    """Plot basis identifiers."""

    MICROPLOT = "MICR"
    SUBPLOT = "SUBP"
    MACROPLOT = "MACR"


# Standard size class definitions
STANDARD_SIZE_CLASSES: Dict[str, Tuple[float, float]] = {
    "1.0-4.9": (1.0, 5.0),
    "5.0-9.9": (5.0, 10.0),
    "10.0-19.9": (10.0, 20.0),
    "20.0-29.9": (20.0, 30.0),
    "30.0+": (30.0, float("inf")),
}

# Descriptive size class definitions (alternative labeling)
DESCRIPTIVE_SIZE_CLASSES: Dict[str, Tuple[float, float]] = {
    "Saplings": (1.0, 5.0),
    "Small": (5.0, 10.0),
    "Medium": (10.0, 20.0),
    "Large": (20.0, float("inf")),
}
