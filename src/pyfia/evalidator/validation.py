"""
Validation functions for comparing pyFIA estimates with EVALIDator.

This module provides functions to compare pyFIA estimates against
official USFS EVALIDator values for validation purposes.
"""

from dataclasses import dataclass
from typing import Optional

from .client import EVALIDatorClient, EVALIDatorEstimate


@dataclass
class ValidationResult:
    """Result of comparing pyFIA estimate with EVALIDator."""

    pyfia_estimate: float
    pyfia_se: float
    evalidator_estimate: float
    evalidator_se: float
    evalidator_variance: float
    evalidator_plot_count: int
    pyfia_plot_count: Optional[int]
    absolute_diff: float
    pct_diff: float
    within_1se: bool
    within_2se: bool
    estimate_type: str
    state_code: int
    year: int
    passed: bool
    message: str


def compare_estimates(
    pyfia_value: float,
    pyfia_se: float,
    evalidator_result: EVALIDatorEstimate,
    tolerance_pct: float = 5.0,
    pyfia_plot_count: Optional[int] = None,
) -> ValidationResult:
    """
    Compare a pyFIA estimate with an EVALIDator official estimate.

    Parameters
    ----------
    pyfia_value : float
        The pyFIA estimate value
    pyfia_se : float
        The pyFIA standard error
    evalidator_result : EVALIDatorEstimate
        The EVALIDator official estimate
    tolerance_pct : float
        Acceptable percentage difference (default 5%)
    pyfia_plot_count : int, optional
        Number of plots used by pyFIA (for plot count validation)

    Returns
    -------
    ValidationResult
        Comparison results including pass/fail status

    Example
    -------
    >>> result = compare_estimates(
    ...     pyfia_value=18500000,
    ...     pyfia_se=450000,
    ...     evalidator_result=official_estimate,
    ...     pyfia_plot_count=1500
    ... )
    >>> print(f"Validation {'PASSED' if result.passed else 'FAILED'}: {result.message}")
    """
    ev = evalidator_result

    abs_diff = abs(pyfia_value - ev.estimate)
    pct_diff = (abs_diff / ev.estimate * 100) if ev.estimate != 0 else 0

    # Combined standard error for comparison
    combined_se = (pyfia_se**2 + ev.sampling_error**2) ** 0.5

    within_1se = abs_diff <= combined_se
    within_2se = abs_diff <= 2 * combined_se

    # Pass if within tolerance or within 2 standard errors
    passed = pct_diff <= tolerance_pct or within_2se

    if passed:
        if within_1se:
            message = f"EXCELLENT: Difference ({pct_diff:.2f}%) within 1 SE"
        elif within_2se:
            message = f"GOOD: Difference ({pct_diff:.2f}%) within 2 SE"
        else:
            message = f"ACCEPTABLE: Difference ({pct_diff:.2f}%) within {tolerance_pct}% tolerance"
    else:
        message = f"FAILED: Difference ({pct_diff:.2f}%) exceeds {tolerance_pct}% tolerance and 2 SE"

    return ValidationResult(
        pyfia_estimate=pyfia_value,
        pyfia_se=pyfia_se,
        evalidator_estimate=ev.estimate,
        evalidator_se=ev.sampling_error,
        evalidator_variance=ev.variance,
        evalidator_plot_count=ev.plot_count,
        pyfia_plot_count=pyfia_plot_count,
        absolute_diff=abs_diff,
        pct_diff=pct_diff,
        within_1se=within_1se,
        within_2se=within_2se,
        estimate_type=ev.estimate_type,
        state_code=ev.state_code,
        year=ev.year,
        passed=passed,
        message=message,
    )


def validate_pyfia_estimate(
    pyfia_result,
    state_code: int,
    year: int,
    estimate_type: str,
    client: Optional[EVALIDatorClient] = None,
    **kwargs,
) -> ValidationResult:
    """
    Validate a pyFIA estimate against EVALIDator.

    This is a convenience function that fetches the EVALIDator estimate
    and performs the comparison in one step.

    Parameters
    ----------
    pyfia_result : pl.DataFrame
        pyFIA estimation result DataFrame with estimate and SE columns
    state_code : int
        State FIPS code
    year : int
        Inventory year
    estimate_type : str
        Type of estimate: "area", "volume", "biomass", "carbon", "tpa"
    client : EVALIDatorClient, optional
        Existing client instance (creates new one if not provided)
    **kwargs
        Additional arguments passed to the EVALIDator API call

    Returns
    -------
    ValidationResult
        Comparison result

    Example
    -------
    >>> from pyfia import FIA, area
    >>> from pyfia.evalidator import validate_pyfia_estimate
    >>>
    >>> with FIA("path/to/db.duckdb") as db:
    ...     db.clip_by_state(37)
    ...     db.clip_most_recent(eval_type="EXPALL")
    ...     result = area(db, land_type="forest")
    >>>
    >>> validation = validate_pyfia_estimate(
    ...     result, state_code=37, year=2023, estimate_type="area"
    ... )
    >>> print(validation.message)
    """
    import polars as pl

    if client is None:
        client = EVALIDatorClient()

    # Extract pyFIA values from result DataFrame
    # Assumes standard pyFIA output format with TOTAL and SE columns
    if isinstance(pyfia_result, pl.DataFrame):
        # Look for total/estimate columns
        estimate_cols = [
            c
            for c in pyfia_result.columns
            if "TOTAL" in c.upper() or "ESTIMATE" in c.upper()
        ]
        se_cols = [
            c
            for c in pyfia_result.columns
            if "SE" in c.upper() and "PCT" not in c.upper()
        ]

        if estimate_cols and se_cols:
            pyfia_value = pyfia_result[estimate_cols[0]][0]
            pyfia_se = pyfia_result[se_cols[0]][0]
        else:
            raise ValueError("Could not find estimate and SE columns in pyFIA result")
    else:
        raise TypeError("pyfia_result must be a Polars DataFrame")

    # Fetch EVALIDator estimate based on type
    if estimate_type == "area":
        land_type = kwargs.get("land_type", "forest")
        ev_result = client.get_forest_area(state_code, year, land_type)
    elif estimate_type == "volume":
        vol_type = kwargs.get("vol_type", "net")
        ev_result = client.get_volume(state_code, year, vol_type)
    elif estimate_type == "biomass":
        component = kwargs.get("component", "ag")
        ev_result = client.get_biomass(state_code, year, component)
    elif estimate_type == "carbon":
        pool = kwargs.get("pool", "total")
        ev_result = client.get_carbon(state_code, year, pool)
    elif estimate_type == "tpa":
        min_dia = kwargs.get("min_diameter", 1.0)
        ev_result = client.get_tree_count(state_code, year, min_dia)
    else:
        raise ValueError(f"Unknown estimate_type: {estimate_type}")

    return compare_estimates(pyfia_value, pyfia_se, ev_result)
