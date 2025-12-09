"""
EVALIDator API client for validating pyFIA estimates against official USFS values.

This module provides programmatic access to the USFS EVALIDator API, enabling
automated comparison of pyFIA estimates with official FIA population estimates.

The EVALIDator API is documented at:
https://apps.fs.usda.gov/fiadb-api/

Example
-------
>>> from pyfia.evalidator import EVALIDatorClient, compare_estimates
>>>
>>> # Get official estimate from EVALIDator
>>> client = EVALIDatorClient()
>>> official = client.get_forest_area(state_code=37, year=2023)
>>>
>>> # Compare with pyFIA estimate
>>> from pyfia import FIA, area
>>> with FIA("path/to/db.duckdb") as db:
...     db.clip_by_state(37)
...     db.clip_most_recent(eval_type="EXPALL")
...     pyfia_result = area(db, land_type="forest")
>>>
>>> # Validate
>>> comparison = compare_estimates(pyfia_result, official)
>>> print(f"Difference: {comparison['pct_diff']:.2f}%")

References
----------
- FIADB-API Documentation: https://apps.fs.usda.gov/fiadb-api/
- Arbor Analytics Guide: https://arbor-analytics.com/post/2023-10-25-using-r-and-python-to-get-forest-resource-data-through-the-evalidator-api/
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import requests

# EVALIDator API base URL
EVALIDATOR_API_URL = "https://apps.fs.usda.gov/fiadb-api/fullreport"
EVALIDATOR_PARAMS_URL = "https://apps.fs.usda.gov/fiadb-api/fullreport/parameters"


# Estimate attribute codes (snum values) for common FIA estimates
# Reference: https://apps.fs.usda.gov/fiadb-api/fullreport/parameters/snum
class EstimateType:
    """EVALIDator estimate type codes (snum parameter values)."""

    # Area estimates
    AREA_FOREST = 2  # Area of forest land, in acres
    AREA_TIMBERLAND = 3  # Area of timberland, in acres
    AREA_SAMPLED = 79  # Area of sampled land and water, in acres

    # Tree counts - Live trees (all species, all tree classes)
    TREE_COUNT_1INCH_FOREST = 4  # Live trees >=1" d.b.h. on forest land
    TREE_COUNT_5INCH_FOREST = 5  # Growing-stock trees >=5" d.b.h. on forest land (TREECLCD=2)
    TREE_COUNT_1INCH_TIMBER = 7  # Live trees >=1" d.b.h. on timberland
    TREE_COUNT_5INCH_TIMBER = 8  # Growing-stock trees >=5" d.b.h. on timberland (TREECLCD=2)

    # Legacy aliases (for backwards compatibility)
    TREE_COUNT_1INCH = 4  # Alias for TREE_COUNT_1INCH_FOREST
    TREE_COUNT_5INCH = 5  # Alias for TREE_COUNT_5INCH_FOREST (corrected: was 7)

    # Basal area
    BASAL_AREA_1INCH = 1004  # Basal area of live trees >=1" d.b.h. (sq ft)
    BASAL_AREA_5INCH = 1007  # Basal area of live trees >=5" d.b.h. (sq ft)

    # Volume (net merchantable bole)
    VOLUME_NET_GROWINGSTOCK = 15  # Net volume growing-stock trees (cu ft)
    VOLUME_NET_ALLSPECIES = 18  # Net volume all species (cu ft)

    # Sawlog volume (board feet)
    VOLUME_SAWLOG_DOYLE = 19  # Sawlog volume - Doyle rule
    VOLUME_SAWLOG_INTERNATIONAL = 20  # Sawlog volume - International 1/4" rule
    VOLUME_SAWLOG_SCRIBNER = 21  # Sawlog volume - Scribner rule

    # Biomass (dry short tons)
    BIOMASS_AG_LIVE = 10  # Aboveground biomass live trees
    BIOMASS_AG_LIVE_5INCH = 13  # Aboveground biomass live trees >=5" d.b.h.
    BIOMASS_BG_LIVE = 59  # Belowground biomass live trees
    BIOMASS_BG_LIVE_5INCH = 73  # Belowground biomass live trees >=5" d.b.h.

    # Carbon (metric tonnes)
    CARBON_AG_LIVE = 53000  # Aboveground carbon in live trees
    CARBON_TOTAL_LIVE = 55000  # Above + belowground carbon in live trees
    CARBON_POOL_AG = 98  # Aboveground live tree carbon pool
    CARBON_POOL_BG = 99  # Belowground live tree carbon pool
    CARBON_POOL_DEADWOOD = 100  # Dead wood carbon pool
    CARBON_POOL_LITTER = 101  # Litter carbon pool
    CARBON_POOL_SOIL = 102  # Soil organic carbon pool
    CARBON_POOL_TOTAL = 103  # Total forest ecosystem carbon

    # Growth (annual net growth)
    GROWTH_NET_VOLUME = 202  # Annual net growth volume (cu ft)
    GROWTH_NET_BIOMASS = 311  # Annual net growth biomass

    # Mortality (growing-stock trees on forest land)
    MORTALITY_VOLUME = 214  # Annual mortality volume (cu ft) - growing-stock, forest
    MORTALITY_BIOMASS = 336  # Annual mortality biomass (AG) - growing-stock, forest

    # Removals
    REMOVALS_VOLUME = 226  # Annual removals volume (cu ft)
    REMOVALS_BIOMASS = 369  # Annual removals biomass


@dataclass
class EVALIDatorEstimate:
    """Container for an EVALIDator estimate result."""

    estimate: float
    sampling_error: float
    sampling_error_pct: float
    units: str
    estimate_type: str
    state_code: int
    year: int
    grouping: Optional[Dict[str, Any]] = None
    raw_response: Optional[Dict[str, Any]] = None


class EVALIDatorClient:
    """
    Client for the USFS EVALIDator API.

    This client provides methods to retrieve official FIA population estimates
    for comparison with pyFIA calculations.

    Parameters
    ----------
    timeout : int, optional
        Request timeout in seconds. Default is 30.

    Example
    -------
    >>> client = EVALIDatorClient()
    >>> result = client.get_forest_area(state_code=37, year=2023)
    >>> print(f"Forest area: {result.estimate:,.0f} acres (SE: {result.sampling_error_pct:.1f}%)")
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "pyFIA/1.0 (validation client)"
        })

    def _build_wc(self, state_code: int, year: int) -> int:
        """
        Build the wc (evaluation group code) parameter.

        Format: state FIPS code + 4-digit year (e.g., 372023 for NC 2023)
        """
        return int(f"{state_code}{year}")

    def _make_request(
        self,
        snum: int,
        state_code: int,
        year: int,
        rselected: str = "Total",
        cselected: str = "Total",
        output_format: str = "NJSON",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make a request to the EVALIDator API.

        Parameters
        ----------
        snum : int
            Estimate attribute number (see EstimateType class)
        state_code : int
            State FIPS code
        year : int
            Inventory year (4-digit)
        rselected : str
            Row grouping definition. Default "Total" for state total.
        cselected : str
            Column grouping definition. Default "Total" for ungrouped.
        output_format : str
            Response format. Default "NJSON" for flat JSON with metadata.
        **kwargs
            Additional API parameters (e.g., strFilter for domain filtering)

        Returns
        -------
        dict
            Parsed JSON response from EVALIDator

        Raises
        ------
        requests.RequestException
            If the API request fails
        ValueError
            If the API returns an error response
        """
        params = {
            "snum": snum,
            "wc": self._build_wc(state_code, year),
            "rselected": rselected,
            "cselected": cselected,
            "outputFormat": output_format,
            **kwargs
        }

        response = self.session.post(
            EVALIDATOR_API_URL,
            data=params,
            timeout=self.timeout
        )
        response.raise_for_status()

        data = response.json()

        # Check for API errors in response
        if "error" in data:
            raise ValueError(f"EVALIDator API error: {data['error']}")

        return data

    def _parse_njson_response(
        self,
        data: Dict[str, Any],
        snum: int,
        state_code: int,
        year: int,
        units: str,
        estimate_type: str
    ) -> EVALIDatorEstimate:
        """Parse NJSON format response into EVALIDatorEstimate."""
        # NJSON format has 'estimates' array with flat records
        estimates = data.get("estimates", [])

        if not estimates:
            raise ValueError("No estimates returned from EVALIDator")

        # For total estimates, take the first (and likely only) row
        est = estimates[0]

        return EVALIDatorEstimate(
            estimate=float(est.get("ESTIMATE", 0)),
            sampling_error=float(est.get("SE", 0)),
            sampling_error_pct=float(est.get("SE_PERCENT", 0)),
            units=units,
            estimate_type=estimate_type,
            state_code=state_code,
            year=year,
            raw_response=data
        )

    def get_forest_area(
        self,
        state_code: int,
        year: int,
        land_type: str = "forest"
    ) -> EVALIDatorEstimate:
        """
        Get forest area estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code (e.g., 37 for North Carolina)
        year : int
            Inventory year (e.g., 2023)
        land_type : str
            "forest" for all forestland, "timber" for timberland only

        Returns
        -------
        EVALIDatorEstimate
            Official estimate with sampling error
        """
        snum = EstimateType.AREA_TIMBERLAND if land_type == "timber" else EstimateType.AREA_FOREST

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="acres",
            estimate_type=f"{land_type}_area"
        )

    def get_volume(
        self,
        state_code: int,
        year: int,
        vol_type: str = "net"
    ) -> EVALIDatorEstimate:
        """
        Get volume estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        vol_type : str
            Volume type: "net" for net merchantable, "sawlog" for board feet

        Returns
        -------
        EVALIDatorEstimate
            Official volume estimate
        """
        snum_map = {
            "net": EstimateType.VOLUME_NET_GROWINGSTOCK,
            "sawlog": EstimateType.VOLUME_SAWLOG_INTERNATIONAL,
        }
        snum = snum_map.get(vol_type, EstimateType.VOLUME_NET_GROWINGSTOCK)
        units = "board_feet" if vol_type == "sawlog" else "cubic_feet"

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"volume_{vol_type}"
        )

    def get_biomass(
        self,
        state_code: int,
        year: int,
        component: str = "ag",
        min_diameter: float = 0.0
    ) -> EVALIDatorEstimate:
        """
        Get biomass estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        component : str
            "ag" for aboveground, "bg" for belowground, "total" for both
        min_diameter : float, default 0.0
            Minimum DBH threshold. Use 0.0 for all trees, 5.0 for trees ≥5" DBH.

        Returns
        -------
        EVALIDatorEstimate
            Official biomass estimate in dry short tons
        """
        # Select snum based on component and diameter threshold
        if min_diameter >= 5.0:
            snum_map = {
                "ag": EstimateType.BIOMASS_AG_LIVE_5INCH,
                "bg": EstimateType.BIOMASS_BG_LIVE_5INCH,
            }
        else:
            snum_map = {
                "ag": EstimateType.BIOMASS_AG_LIVE,
                "bg": EstimateType.BIOMASS_BG_LIVE,
            }
        snum = snum_map.get(component, snum_map["ag"])

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="dry_short_tons",
            estimate_type=f"biomass_{component}"
        )

    def get_carbon(
        self,
        state_code: int,
        year: int,
        pool: str = "total"
    ) -> EVALIDatorEstimate:
        """
        Get carbon estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        pool : str
            Carbon pool: "ag", "bg", "total", or ecosystem pools

        Returns
        -------
        EVALIDatorEstimate
            Official carbon estimate in metric tonnes
        """
        snum_map = {
            "ag": EstimateType.CARBON_AG_LIVE,
            "total": EstimateType.CARBON_TOTAL_LIVE,
            "ecosystem": EstimateType.CARBON_POOL_TOTAL,
        }
        snum = snum_map.get(pool, EstimateType.CARBON_TOTAL_LIVE)

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="metric_tonnes",
            estimate_type=f"carbon_{pool}"
        )

    def get_tree_count(
        self,
        state_code: int,
        year: int,
        min_diameter: float = 1.0,
        land_type: str = "forest"
    ) -> EVALIDatorEstimate:
        """
        Get tree count estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        min_diameter : float
            Minimum DBH in inches (1.0 or 5.0).
            Note: 5" threshold returns growing-stock trees (TREECLCD=2) only,
            while 1" threshold returns all live trees.
        land_type : str
            "forest" for forest land, "timber" for timberland only

        Returns
        -------
        EVALIDatorEstimate
            Official tree count estimate

        Notes
        -----
        snum values used:
        - snum=4: Live trees >=1" d.b.h. on forest land (all tree classes)
        - snum=5: Growing-stock trees >=5" d.b.h. on forest land (TREECLCD=2)
        - snum=7: Live trees >=1" d.b.h. on timberland (all tree classes)
        - snum=8: Growing-stock trees >=5" d.b.h. on timberland (TREECLCD=2)
        """
        if land_type == "timber":
            snum = EstimateType.TREE_COUNT_5INCH_TIMBER if min_diameter >= 5.0 else EstimateType.TREE_COUNT_1INCH_TIMBER
        else:
            snum = EstimateType.TREE_COUNT_5INCH_FOREST if min_diameter >= 5.0 else EstimateType.TREE_COUNT_1INCH_FOREST

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="trees",
            estimate_type=f"tree_count_{int(min_diameter)}inch_{land_type}"
        )

    def get_growth(
        self,
        state_code: int,
        year: int,
        measure: str = "volume",
        land_type: str = "forest",
        **kwargs
    ) -> EVALIDatorEstimate:
        """
        Get annual net growth estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        measure : {'volume', 'biomass'}, default 'volume'
            Measurement type:
            - 'volume': Net cubic foot growth of merchantable bole wood
            - 'biomass': Net biomass growth of aboveground trees
        land_type : {'forest', 'timber'}, default 'forest'
            Land classification
        **kwargs
            Additional API parameters

        Returns
        -------
        EVALIDatorEstimate
            Growth estimate with standard error
        """
        if measure == "volume":
            snum = EstimateType.GROWTH_NET_VOLUME
            units = "cubic_feet_per_year"
        elif measure == "biomass":
            snum = EstimateType.GROWTH_NET_BIOMASS
            units = "dry_tons_per_year"
        else:
            raise ValueError(f"Invalid measure: {measure}. Use 'volume' or 'biomass'")

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year,
            **kwargs
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"growth_{measure}_{land_type}"
        )

    def get_mortality(
        self,
        state_code: int,
        year: int,
        measure: str = "volume",
        land_type: str = "forest",
        **kwargs
    ) -> EVALIDatorEstimate:
        """
        Get annual mortality estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        measure : {'volume', 'biomass'}, default 'volume'
            Measurement type:
            - 'volume': Net cubic foot mortality of merchantable bole wood
            - 'biomass': Net biomass mortality of aboveground trees
        land_type : {'forest', 'timber'}, default 'forest'
            Land classification
        **kwargs
            Additional API parameters

        Returns
        -------
        EVALIDatorEstimate
            Mortality estimate with standard error
        """
        if measure == "volume":
            snum = EstimateType.MORTALITY_VOLUME
            units = "cubic_feet_per_year"
        elif measure == "biomass":
            snum = EstimateType.MORTALITY_BIOMASS
            units = "dry_tons_per_year"
        else:
            raise ValueError(f"Invalid measure: {measure}. Use 'volume' or 'biomass'")

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year,
            **kwargs
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"mortality_{measure}_{land_type}"
        )

    def get_removals(
        self,
        state_code: int,
        year: int,
        measure: str = "volume",
        land_type: str = "forest",
        **kwargs
    ) -> EVALIDatorEstimate:
        """
        Get annual removals (harvest) estimate from EVALIDator.

        Parameters
        ----------
        state_code : int
            State FIPS code
        year : int
            Inventory year
        measure : {'volume', 'biomass'}, default 'volume'
            Measurement type:
            - 'volume': Net cubic foot removals of merchantable bole wood
            - 'biomass': Net biomass removals of aboveground trees
        land_type : {'forest', 'timber'}, default 'forest'
            Land classification
        **kwargs
            Additional API parameters

        Returns
        -------
        EVALIDatorEstimate
            Removals estimate with standard error
        """
        if measure == "volume":
            snum = EstimateType.REMOVALS_VOLUME
            units = "cubic_feet_per_year"
        elif measure == "biomass":
            snum = EstimateType.REMOVALS_BIOMASS
            units = "dry_tons_per_year"
        else:
            raise ValueError(f"Invalid measure: {measure}. Use 'volume' or 'biomass'")

        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year,
            **kwargs
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"removals_{measure}_{land_type}"
        )

    def get_custom_estimate(
        self,
        snum: int,
        state_code: int,
        year: int,
        units: str,
        estimate_type: str,
        **kwargs
    ) -> EVALIDatorEstimate:
        """
        Get a custom estimate using any snum value.

        Parameters
        ----------
        snum : int
            Estimate attribute number from EstimateType or FIADB-API docs
        state_code : int
            State FIPS code
        year : int
            Inventory year
        units : str
            Units description for the estimate
        estimate_type : str
            Description of the estimate type
        **kwargs
            Additional API parameters

        Returns
        -------
        EVALIDatorEstimate
            Custom estimate result
        """
        data = self._make_request(
            snum=snum,
            state_code=state_code,
            year=year,
            **kwargs
        )

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=estimate_type
        )


@dataclass
class ValidationResult:
    """Result of comparing pyFIA estimate with EVALIDator."""

    pyfia_estimate: float
    pyfia_se: float
    evalidator_estimate: float
    evalidator_se: float
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
    tolerance_pct: float = 5.0
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

    Returns
    -------
    ValidationResult
        Comparison results including pass/fail status

    Example
    -------
    >>> result = compare_estimates(
    ...     pyfia_value=18500000,
    ...     pyfia_se=450000,
    ...     evalidator_result=official_estimate
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
        absolute_diff=abs_diff,
        pct_diff=pct_diff,
        within_1se=within_1se,
        within_2se=within_2se,
        estimate_type=ev.estimate_type,
        state_code=ev.state_code,
        year=ev.year,
        passed=passed,
        message=message
    )


def validate_pyfia_estimate(
    pyfia_result,
    state_code: int,
    year: int,
    estimate_type: str,
    client: Optional[EVALIDatorClient] = None,
    **kwargs
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
        estimate_cols = [c for c in pyfia_result.columns if "TOTAL" in c.upper() or "ESTIMATE" in c.upper()]
        se_cols = [c for c in pyfia_result.columns if "SE" in c.upper() and "PCT" not in c.upper()]

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
