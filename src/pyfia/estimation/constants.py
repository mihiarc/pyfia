"""
Physical and statistical constants for FIA estimation.

This module centralizes magic numbers used throughout the estimation module,
providing descriptive names and documenting their derivations.

Unit Conversion Constants
-------------------------
These constants convert between FIA database units and standard reporting units.

Statistical Constants
---------------------
These constants are used for confidence interval calculations.

FIA-Specific Constants
----------------------
These constants are specific to FIA methodology and forestry calculations.
"""

from __future__ import annotations

import math

# =============================================================================
# Unit Conversion Constants
# =============================================================================

# Pounds to short tons conversion factor
# FIA database stores biomass and carbon values in pounds.
# EVALIDator and standard FIA reporting use short tons (US tons = 2000 lbs).
LBS_PER_SHORT_TON: float = 2000.0
LBS_TO_SHORT_TONS: float = 1.0 / LBS_PER_SHORT_TON

# =============================================================================
# Statistical Constants (Z-scores for confidence intervals)
# =============================================================================

# Z-score for 90% confidence interval (alpha = 0.10, two-tailed)
# Corresponds to 95th percentile of standard normal distribution
Z_SCORE_90: float = 1.645

# Z-score for 95% confidence interval (alpha = 0.05, two-tailed)
# Corresponds to 97.5th percentile of standard normal distribution
Z_SCORE_95: float = 1.96

# Z-score for 99% confidence interval (alpha = 0.01, two-tailed)
# Corresponds to 99.5th percentile of standard normal distribution
Z_SCORE_99: float = 2.576

# =============================================================================
# FIA-Specific Constants
# =============================================================================

# Basal area conversion factor for diameter in inches to square feet
# Derivation: pi / (4 * 144) = pi / 576
# Where:
#   - pi/4 converts diameter to cross-sectional area
#   - 144 converts square inches to square feet (12^2)
# Formula: Basal Area (sq ft) = DIA^2 * BASAL_AREA_FACTOR
# Reference: Standard forestry conversion, used in FIA Field Guide
BASAL_AREA_FACTOR: float = math.pi / 576.0  # Approximately 0.005454154

# IPCC standard carbon fraction of dry biomass
# Represents the average proportion of carbon in dry tree biomass.
# Reference: IPCC Guidelines for National Greenhouse Gas Inventories
# Note: FIA uses species-specific factors in CARBON_AG/CARBON_BG columns,
# which are more accurate than this flat rate. Use carbon_pool() for
# species-specific carbon estimation matching EVALIDator.
CARBON_FRACTION: float = 0.47
