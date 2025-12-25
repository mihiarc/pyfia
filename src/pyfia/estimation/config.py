"""
Configuration for FIA estimators.

Simple configuration using dataclasses instead of complex Pydantic models
with unnecessary validation and abstraction layers.
"""

from dataclasses import dataclass
from typing import List, Optional, Type, Union


@dataclass
class EstimatorConfig:
    """
    Configuration for FIA estimators.

    Simple dataclass without complex validation - just the parameters needed
    for estimation without over-engineering.
    """

    # Grouping parameters
    grp_by: Optional[Union[str, List[str]]] = None
    by_species: bool = False
    by_size_class: bool = False

    # Land and tree type filters
    land_type: str = "forest"  # "forest", "timber", or "all"
    tree_type: str = "live"  # "live", "dead", "gs", or "all"

    # Domain filters (SQL-like conditions)
    tree_domain: Optional[str] = None
    area_domain: Optional[str] = None
    plot_domain: Optional[str] = None

    # Estimation method
    method: str = "TI"  # Temporally Indifferent
    lambda_: float = 0.5  # For weighted methods

    # Output options
    totals: bool = True  # Include population totals
    variance: bool = False  # Return variance instead of SE
    by_plot: bool = False  # Plot-level estimates

    # Database options
    most_recent: bool = False  # Use most recent evaluation
    eval_type: Optional[str] = None  # EXPVOL, EXPALL, etc.

    # Performance options (simplified)
    use_cache: bool = True
    show_progress: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for compatibility."""
        return {
            "grp_by": self.grp_by,
            "by_species": self.by_species,
            "by_size_class": self.by_size_class,
            "land_type": self.land_type,
            "tree_type": self.tree_type,
            "tree_domain": self.tree_domain,
            "area_domain": self.area_domain,
            "plot_domain": self.plot_domain,
            "method": self.method,
            "lambda_": self.lambda_,
            "totals": self.totals,
            "variance": self.variance,
            "by_plot": self.by_plot,
            "most_recent": self.most_recent,
            "eval_type": self.eval_type,
            "use_cache": self.use_cache,
            "show_progress": self.show_progress,
        }


@dataclass
class VolumeConfig(EstimatorConfig):
    """Configuration specific to volume estimation."""

    vol_type: str = "net"  # "net", "gross", "sound", "sawlog"

    def to_dict(self) -> dict:
        """Include volume-specific parameters."""
        config = super().to_dict()
        config["vol_type"] = self.vol_type
        return config


@dataclass
class BiomassConfig(EstimatorConfig):
    """Configuration specific to biomass estimation."""

    component: str = "AG"  # "AG", "BG", "TOTAL", etc.
    model_snag: bool = True

    def to_dict(self) -> dict:
        """Include biomass-specific parameters."""
        config = super().to_dict()
        config["component"] = self.component
        config["model_snag"] = self.model_snag
        return config


@dataclass
class MortalityConfig(EstimatorConfig):
    """Configuration specific to mortality estimation."""

    proportion: bool = False  # Return as proportion vs total

    def to_dict(self) -> dict:
        """Include mortality-specific parameters."""
        config = super().to_dict()
        config["proportion"] = self.proportion
        return config


def create_config(estimation_type: str, **kwargs) -> EstimatorConfig:
    """
    Factory function to create appropriate config.

    Parameters
    ----------
    estimation_type : str
        Type of estimation: "area", "volume", "biomass", "tpa", "mortality", "growth"
    **kwargs
        Parameters to pass to config

    Returns
    -------
    EstimatorConfig
        Appropriate configuration object
    """
    config_map: dict[str, Type[EstimatorConfig]] = {
        "volume": VolumeConfig,
        "biomass": BiomassConfig,
        "mortality": MortalityConfig,
        "area": EstimatorConfig,
        "tpa": EstimatorConfig,
        "growth": EstimatorConfig,
    }

    config_class = config_map.get(estimation_type, EstimatorConfig)
    return config_class(**kwargs)
