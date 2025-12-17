"""
EVALIDator API client for retrieving official USFS estimates.

This module provides the EVALIDatorClient class for accessing the FIADB-API
and retrieving official population estimates for validation.

Reference: https://apps.fs.usda.gov/fiadb-api/
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from .estimate_types import EstimateType

# EVALIDator API base URL
EVALIDATOR_API_URL = "https://apps.fs.usda.gov/fiadb-api/fullreport"
EVALIDATOR_PARAMS_URL = "https://apps.fs.usda.gov/fiadb-api/fullreport/parameters"


@dataclass
class EVALIDatorEstimate:
    """Container for an EVALIDator estimate result."""

    estimate: float
    sampling_error: float
    sampling_error_pct: float
    variance: float
    plot_count: int
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
        self.session.headers.update({"User-Agent": "pyFIA/1.0 (validation client)"})

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
        **kwargs,
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
            **kwargs,
        }

        response = self.session.post(
            EVALIDATOR_API_URL, data=params, timeout=self.timeout
        )
        response.raise_for_status()

        data = response.json()

        # Check for API errors in response
        if "error" in data:
            raise ValueError(f"EVALIDator API error: {data['error']}")

        result: Dict[str, Any] = data
        return result

    def _parse_njson_response(
        self,
        data: Dict[str, Any],
        snum: int,
        state_code: int,
        year: int,
        units: str,
        estimate_type: str,
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
            variance=float(est.get("VARIANCE", 0)),
            plot_count=int(est.get("PLOT_COUNT", 0)),
            units=units,
            estimate_type=estimate_type,
            state_code=state_code,
            year=year,
            raw_response=data,
        )

    def get_forest_area(
        self, state_code: int, year: int, land_type: str = "forest"
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
        snum = (
            EstimateType.AREA_TIMBERLAND
            if land_type == "timber"
            else EstimateType.AREA_FOREST
        )

        data = self._make_request(snum=snum, state_code=state_code, year=year)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="acres",
            estimate_type=f"{land_type}_area",
        )

    def get_area_change(
        self,
        state_code: int,
        year: int,
        land_type: str = "forest",
        annual: bool = True,
        measurement: str = "both",
    ) -> EVALIDatorEstimate:
        """
        Get area change estimate from EVALIDator.

        Area change estimates require remeasured plots (plots measured at two
        points in time) and represent the net change in land classification.

        Parameters
        ----------
        state_code : int
            State FIPS code (e.g., 37 for North Carolina)
        year : int
            Inventory year (e.g., 2023)
        land_type : str, default "forest"
            Land classification:
            - "forest": Forest land area change
            - "timber": Timberland area change
            - "sampled": All sampled land area change
        annual : bool, default True
            If True, return annualized rate of change (acres/year).
            If False, return total change over remeasurement period.
        measurement : str, default "both"
            Which measurement to use for defining land class:
            - "both": Land must meet criteria at both measurements
            - "either": Land meets criteria at either measurement
            - "remeasured": Only remeasured conditions (for non-annual)

        Returns
        -------
        EVALIDatorEstimate
            Official area change estimate with sampling error

        Notes
        -----
        Area change estimates track transitions between land use categories.
        Positive values indicate net gain, negative values indicate net loss.

        The "both" measurement option is more conservative, requiring land
        to be classified consistently at both time points. The "either"
        option captures land that was in the category at any point.

        Examples
        --------
        >>> client = EVALIDatorClient()
        >>> # Annual forest area change
        >>> result = client.get_area_change(37, 2023, land_type="forest")
        >>> print(f"Annual change: {result.estimate:+,.0f} acres/year")

        >>> # Total timberland change over remeasurement period
        >>> result = client.get_area_change(37, 2023, land_type="timber", annual=False)
        """
        # Select appropriate snum based on parameters
        if annual:
            if land_type == "sampled":
                snum = EstimateType.AREA_CHANGE_ANNUAL_SAMPLED
            elif land_type == "timber":
                snum = (
                    EstimateType.AREA_CHANGE_ANNUAL_TIMBERLAND_BOTH
                    if measurement == "both"
                    else EstimateType.AREA_CHANGE_ANNUAL_TIMBERLAND_EITHER
                )
            else:  # forest
                snum = (
                    EstimateType.AREA_CHANGE_ANNUAL_FOREST_BOTH
                    if measurement == "both"
                    else EstimateType.AREA_CHANGE_ANNUAL_FOREST_EITHER
                )
            units = "acres_per_year"
        else:
            if land_type == "sampled":
                snum = EstimateType.AREA_CHANGE_SAMPLED
            elif land_type == "timber":
                snum = (
                    EstimateType.AREA_CHANGE_TIMBERLAND_REMEASURED
                    if measurement == "remeasured"
                    else EstimateType.AREA_CHANGE_TIMBERLAND_EITHER
                )
            else:  # forest
                snum = (
                    EstimateType.AREA_CHANGE_FOREST_REMEASURED
                    if measurement == "remeasured"
                    else EstimateType.AREA_CHANGE_FOREST_EITHER
                )
            units = "acres"

        data = self._make_request(snum=snum, state_code=state_code, year=year)

        estimate_type = f"{land_type}_area_change"
        if annual:
            estimate_type += "_annual"

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=estimate_type,
        )

    def get_volume(
        self, state_code: int, year: int, vol_type: str = "net"
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

        data = self._make_request(snum=snum, state_code=state_code, year=year)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"volume_{vol_type}",
        )

    def get_biomass(
        self,
        state_code: int,
        year: int,
        component: str = "ag",
        min_diameter: float = 0.0,
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
            Minimum DBH threshold. Use 0.0 for all trees, 5.0 for trees >=5" DBH.

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

        data = self._make_request(snum=snum, state_code=state_code, year=year)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="dry_short_tons",
            estimate_type=f"biomass_{component}",
        )

    def get_carbon(
        self, state_code: int, year: int, pool: str = "total"
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

        data = self._make_request(snum=snum, state_code=state_code, year=year)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="metric_tonnes",
            estimate_type=f"carbon_{pool}",
        )

    def get_tree_count(
        self,
        state_code: int,
        year: int,
        min_diameter: float = 1.0,
        land_type: str = "forest",
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
            snum = (
                EstimateType.TREE_COUNT_5INCH_TIMBER
                if min_diameter >= 5.0
                else EstimateType.TREE_COUNT_1INCH_TIMBER
            )
        else:
            snum = (
                EstimateType.TREE_COUNT_5INCH_FOREST
                if min_diameter >= 5.0
                else EstimateType.TREE_COUNT_1INCH_FOREST
            )

        data = self._make_request(snum=snum, state_code=state_code, year=year)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units="trees",
            estimate_type=f"tree_count_{int(min_diameter)}inch_{land_type}",
        )

    def get_growth(
        self,
        state_code: int,
        year: int,
        measure: str = "volume",
        land_type: str = "forest",
        **kwargs,
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

        data = self._make_request(snum=snum, state_code=state_code, year=year, **kwargs)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"growth_{measure}_{land_type}",
        )

    def get_mortality(
        self,
        state_code: int,
        year: int,
        measure: str = "volume",
        land_type: str = "forest",
        **kwargs,
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

        data = self._make_request(snum=snum, state_code=state_code, year=year, **kwargs)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"mortality_{measure}_{land_type}",
        )

    def get_removals(
        self,
        state_code: int,
        year: int,
        measure: str = "volume",
        land_type: str = "forest",
        **kwargs,
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

        data = self._make_request(snum=snum, state_code=state_code, year=year, **kwargs)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=f"removals_{measure}_{land_type}",
        )

    def get_custom_estimate(
        self,
        snum: int,
        state_code: int,
        year: int,
        units: str,
        estimate_type: str,
        **kwargs,
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
        data = self._make_request(snum=snum, state_code=state_code, year=year, **kwargs)

        return self._parse_njson_response(
            data=data,
            snum=snum,
            state_code=state_code,
            year=year,
            units=units,
            estimate_type=estimate_type,
        )
