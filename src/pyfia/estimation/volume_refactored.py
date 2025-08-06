"""
Volume estimation functions for pyFIA using the BaseEstimator architecture.

This module implements volume estimation following FIA procedures,
matching the functionality of rFIA::volume() while using the new
BaseEstimator architecture for cleaner, more maintainable code.
"""

from typing import Dict, List, Optional, Union

import polars as pl

from ..core import FIA
from .base import BaseEstimator, EstimatorConfig


class VolumeEstimator(BaseEstimator):
    """
    Volume estimator implementing FIA volume calculation procedures.

    This class calculates cubic foot and board foot volume estimates
    for forest inventory data, supporting multiple volume types (net,
    gross, sound, sawlog) and various grouping options.

    The estimator follows the standard FIA estimation workflow:
    1. Filter trees and conditions based on criteria
    2. Join trees with condition data
    3. Calculate volume per acre (VOL * TPA_UNADJ)
    4. Aggregate to plot level
    5. Apply stratification and expansion
    6. Calculate population estimates with variance

    Attributes
    ----------
    vol_type : str
        Type of volume to calculate (net, gross, sound, sawlog)
    volume_columns : Dict[str, str]
        Mapping of FIA column names to internal calculation columns
    """

    def __init__(self, db: Union[str, FIA], config: EstimatorConfig):
        """
        Initialize the volume estimator.

        Parameters
        ----------
        db : Union[str, FIA]
            FIA database object or path to database
        config : EstimatorConfig
            Configuration with estimation parameters including vol_type
        """
        super().__init__(db, config)

        # Extract volume-specific parameters
        self.vol_type = config.extra_params.get("vol_type", "net").upper()
        self.volume_columns = self._get_volume_columns()

    def get_required_tables(self) -> List[str]:
        """
        Return required database tables for volume estimation.

        Returns
        -------
        List[str]
            ["PLOT", "TREE", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]
        """
        return ["PLOT", "TREE", "COND", "POP_STRATUM", "POP_PLOT_STRATUM_ASSGN"]

    def get_response_columns(self) -> Dict[str, str]:
        """
        Define volume response columns based on volume type.

        Returns mapping from calculation columns to output columns.
        For example, for net volume:
        {"VOL_BOLE_CF": "VOLCFNET_ACRE", "VOL_SAW_CF": "VOLCSNET_ACRE", ...}

        Returns
        -------
        Dict[str, str]
            Mapping of internal calculation names to output names
        """
        # Map FIA columns to standardized internal names, then to output names
        response_mapping = {}

        for fia_col, internal_col in self.volume_columns.items():
            output_col = self._get_output_column_name(internal_col)
            # Use internal column name as key for consistency with base class
            response_mapping[internal_col] = output_col

        return response_mapping

    def calculate_values(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate volume values per acre.

        Multiplies volume columns by TPA_UNADJ to get per-acre values,
        following the standard FIA volume calculation methodology.

        Parameters
        ----------
        data : pl.DataFrame
            Trees joined with conditions containing volume and TPA columns

        Returns
        -------
        pl.DataFrame
            Data with calculated volume per acre columns
        """
        # Calculate volume per acre: VOL * TPA_UNADJ
        vol_calculations = []

        for fia_col, internal_col in self.volume_columns.items():
            if fia_col in data.columns:
                vol_calculations.append(
                    (pl.col(fia_col) * pl.col("TPA_UNADJ")).alias(internal_col)
                )

        if not vol_calculations:
            available_cols = [col for col in self.volume_columns.keys() if col in data.columns]
            raise ValueError(
                f"No volume columns found for vol_type '{self.vol_type}'. "
                f"Expected columns: {list(self.volume_columns.keys())}, "
                f"Available: {available_cols}"
            )

        return data.with_columns(vol_calculations)

    def get_output_columns(self) -> List[str]:
        """
        Define the output column structure for volume estimates.

        Returns
        -------
        List[str]
            Standard output columns including estimates, SE, and metadata
        """
        output_cols = ["YEAR", "nPlots_TREE", "nPlots_AREA", "N"]

        # Add volume estimate columns and their standard errors
        for _, output_col in self.get_response_columns().items():
            output_cols.append(output_col)
            # Add SE or VAR column based on config
            if self.config.variance:
                output_cols.append(f"{output_col}_VAR")
            else:
                output_cols.append(f"{output_col}_SE")

        # Add totals if requested
        if self.config.totals:
            for _, output_col in self.get_response_columns().items():
                output_cols.append(f"{output_col}_TOTAL")

        return output_cols

    def apply_module_filters(self, tree_df: Optional[pl.DataFrame],
                            cond_df: pl.DataFrame) -> tuple[Optional[pl.DataFrame], pl.DataFrame]:
        """
        Apply volume-specific filtering requirements.

        Volume estimation requires valid volume data (VOLCFGRS not null)
        in addition to the standard filters.

        Parameters
        ----------
        tree_df : Optional[pl.DataFrame]
            Tree dataframe after common filters
        cond_df : pl.DataFrame
            Condition dataframe after common filters

        Returns
        -------
        tuple[Optional[pl.DataFrame], pl.DataFrame]
            Filtered tree and condition dataframes
        """
        # Volume requires valid volume data
        if tree_df is not None:
            # Filter for trees with valid volume measurements
            tree_df = tree_df.filter(pl.col("VOLCFGRS").is_not_null())

        return tree_df, cond_df

    def format_output(self, estimates: pl.DataFrame) -> pl.DataFrame:
        """
        Format output to match rFIA volume() function structure.

        Ensures compatibility with existing code expecting the original
        volume() function output format.

        Parameters
        ----------
        estimates : pl.DataFrame
            Raw estimation results

        Returns
        -------
        pl.DataFrame
            Formatted output matching rFIA structure
        """
        # Start with base formatting
        formatted = super().format_output(estimates)

        # Ensure nPlots columns are properly named for compatibility
        if "nPlots" in formatted.columns and "nPlots_TREE" not in formatted.columns:
            formatted = formatted.rename({"nPlots": "nPlots_TREE"})

        if "nPlots_TREE" in formatted.columns and "nPlots_AREA" not in formatted.columns:
            formatted = formatted.with_columns(
                pl.col("nPlots_TREE").alias("nPlots_AREA")
            )

        return formatted

    def _get_volume_columns(self) -> Dict[str, str]:
        """
        Get the volume column mapping for the specified volume type.

        Returns
        -------
        Dict[str, str]
            Mapping from FIA column names to internal calculation names
        """
        if self.vol_type == "NET":
            return {
                "VOLCFNET": "BOLE_CF_ACRE",  # Bole cubic feet (net)
                "VOLCSNET": "SAW_CF_ACRE",   # Sawlog cubic feet (net)
                "VOLBFNET": "SAW_BF_ACRE",   # Sawlog board feet (net)
            }
        elif self.vol_type == "GROSS":
            return {
                "VOLCFGRS": "BOLE_CF_ACRE",  # Bole cubic feet (gross)
                "VOLCSGRS": "SAW_CF_ACRE",   # Sawlog cubic feet (gross)
                "VOLBFGRS": "SAW_BF_ACRE",   # Sawlog board feet (gross)
            }
        elif self.vol_type == "SOUND":
            return {
                "VOLCFSND": "BOLE_CF_ACRE",  # Bole cubic feet (sound)
                "VOLCSSND": "SAW_CF_ACRE",   # Sawlog cubic feet (sound)
                # VOLBFSND not available in FIA
            }
        elif self.vol_type == "SAWLOG":
            return {
                "VOLCSNET": "SAW_CF_ACRE",   # Sawlog cubic feet (net)
                "VOLBFNET": "SAW_BF_ACRE",   # Sawlog board feet (net)
            }
        else:
            raise ValueError(
                f"Unknown volume type: {self.vol_type}. "
                f"Valid types are: NET, GROSS, SOUND, SAWLOG"
            )

    def _get_output_column_name(self, internal_col: str) -> str:
        """
        Get the output column name for rFIA compatibility.

        Maps internal calculation column names to the expected
        output column names that match rFIA conventions.

        Parameters
        ----------
        internal_col : str
            Internal column name (e.g., "BOLE_CF_ACRE")

        Returns
        -------
        str
            Output column name (e.g., "VOLCFNET_ACRE")
        """
        # Map internal names to rFIA output names based on volume type
        if internal_col == "BOLE_CF_ACRE":
            if self.vol_type == "NET":
                return "VOLCFNET_ACRE"
            elif self.vol_type == "GROSS":
                return "VOLCFGRS_ACRE"
            elif self.vol_type == "SOUND":
                return "VOLCFSND_ACRE"
        elif internal_col == "SAW_CF_ACRE":
            if self.vol_type == "NET":
                return "VOLCSNET_ACRE"
            elif self.vol_type == "GROSS":
                return "VOLCSGRS_ACRE"
            elif self.vol_type == "SOUND":
                return "VOLCSSND_ACRE"
            elif self.vol_type == "SAWLOG":
                return "VOLCSNET_ACRE"
        elif internal_col == "SAW_BF_ACRE":
            if self.vol_type in ["NET", "SAWLOG"]:
                return "VOLBFNET_ACRE"
            elif self.vol_type == "GROSS":
                return "VOLBFGRS_ACRE"

        # Fallback to internal name if no mapping found
        return internal_col


def volume(
    db: Union[str, FIA],
    grp_by: Optional[Union[str, List[str]]] = None,
    by_species: bool = False,
    by_size_class: bool = False,
    land_type: str = "forest",
    tree_type: str = "live",
    vol_type: str = "net",
    method: str = "TI",
    lambda_: float = 0.5,
    tree_domain: Optional[str] = None,
    area_domain: Optional[str] = None,
    totals: bool = False,
    variance: bool = False,
    by_plot: bool = False,
    cond_list: bool = False,
    n_cores: int = 1,
    remote: bool = False,
    mr: bool = False,
) -> pl.DataFrame:
    """
    Estimate volume from FIA data following rFIA methodology.

    This is a wrapper function that maintains backward compatibility with
    the original volume() API while using the new VolumeEstimator class
    internally for cleaner implementation.

    Parameters
    ----------
    db : FIA or str
        FIA database object or path to database
    grp_by : list of str, optional
        Columns to group estimates by
    by_species : bool, default False
        Group by species
    by_size_class : bool, default False
        Group by size classes
    land_type : str, default "forest"
        Land type filter: "forest" or "timber"
    tree_type : str, default "live"
        Tree type filter: "live", "dead", "gs", "all"
    vol_type : str, default "net"
        Volume type: "net", "gross", "sound", "sawlog"
    method : str, default "TI"
        Estimation method (currently only "TI" supported)
    lambda_ : float, default 0.5
        Temporal weighting parameter (not used for TI)
    tree_domain : str, optional
        SQL-like condition to filter trees
    area_domain : str, optional
        SQL-like condition to filter area
    totals : bool, default False
        Include population totals in addition to per-acre estimates
    variance : bool, default False
        Return variance instead of standard error
    by_plot : bool, default False
        Return plot-level estimates
    cond_list : bool, default False
        Return condition list
    n_cores : int, default 1
        Number of cores (not implemented)
    remote : bool, default False
        Use remote database (not implemented)
    mr : bool, default False
        Use most recent evaluation

    Returns
    -------
    pl.DataFrame
        DataFrame with volume estimates

    Examples
    --------
    >>> # Basic volume estimation
    >>> vol_results = volume(db, vol_type="net")

    >>> # Volume by species with totals
    >>> vol_results = volume(
    ...     db,
    ...     by_species=True,
    ...     totals=True,
    ...     vol_type="gross"
    ... )

    >>> # Volume for large trees by forest type
    >>> vol_results = volume(
    ...     db,
    ...     grp_by="FORTYPCD",
    ...     tree_domain="DIA >= 20.0",
    ...     land_type="timber"
    ... )
    """
    # Create configuration from parameters
    config = EstimatorConfig(
        grp_by=grp_by,
        by_species=by_species,
        by_size_class=by_size_class,
        land_type=land_type,
        tree_type=tree_type,
        tree_domain=tree_domain,
        area_domain=area_domain,
        method=method,
        lambda_=lambda_,
        totals=totals,
        variance=variance,
        by_plot=by_plot,
        most_recent=mr,
        extra_params={"vol_type": vol_type}
    )

    # Create estimator and run estimation
    with VolumeEstimator(db, config) as estimator:
        results = estimator.estimate()

    # Handle special cases for backward compatibility
    if by_plot:
        # TODO: Implement plot-level results
        # For now, return standard results
        pass

    if cond_list:
        # TODO: Implement condition list functionality
        # For now, return standard results
        pass

    return results
