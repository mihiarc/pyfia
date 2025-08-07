"""
Land type classification strategies for FIA area estimation.

This module implements the Strategy pattern for land type classification,
providing different classification approaches based on FIA land classification
standards and user requirements.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Protocol, runtime_checkable

import polars as pl

from ...constants.constants import (
    LandStatus,
    SiteClass,
    ReserveStatus,
)


class LandTypeCategory(str, Enum):
    """Standard FIA land type categories."""
    TIMBER = "Timber"
    NON_TIMBER_FOREST = "Non-Timber Forest"
    NON_FOREST = "Non-Forest"
    WATER = "Water"
    OTHER = "Other"


@runtime_checkable
class LandTypeStrategy(Protocol):
    """Protocol for land type classification strategies."""
    
    def classify(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Classify land types in the dataframe.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with land type classifications added
        """
        ...
    
    def create_domain_indicator(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Create domain indicator for this land type.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with landD domain indicator added
        """
        ...


class BaseLandTypeStrategy(ABC):
    """Base class for land type classification strategies."""
    
    @abstractmethod
    def classify(self, data: pl.DataFrame) -> pl.DataFrame:
        """Classify land types in the dataframe."""
        pass
    
    @abstractmethod
    def create_domain_indicator(self, data: pl.DataFrame) -> pl.DataFrame:
        """Create domain indicator for this land type."""
        pass
    
    def add_land_type_categories(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Add comprehensive land type categories for grouping analysis.
        
        This method adds the standard FIA land type categories that are
        used in by_land_type analysis.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with condition status and site class data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with LAND_TYPE column added
        """
        return data.with_columns(
            pl.when(
                (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
            .then(pl.lit(LandTypeCategory.TIMBER))
            .when(pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            .then(pl.lit(LandTypeCategory.NON_TIMBER_FOREST))
            .when(pl.col("COND_STATUS_CD") == LandStatus.NONFOREST)
            .then(pl.lit(LandTypeCategory.NON_FOREST))
            .when(pl.col("COND_STATUS_CD").is_in([LandStatus.WATER, LandStatus.CENSUS_WATER]))
            .then(pl.lit(LandTypeCategory.WATER))
            .otherwise(pl.lit(LandTypeCategory.OTHER))
            .alias("LAND_TYPE")
        )


class ForestLandStrategy(BaseLandTypeStrategy):
    """Strategy for forest land classification."""
    
    def classify(self, data: pl.DataFrame) -> pl.DataFrame:
        """Add forest land classification."""
        return self.add_land_type_categories(data)
    
    def create_domain_indicator(self, data: pl.DataFrame) -> pl.DataFrame:
        """Create domain indicator for forest land only."""
        return data.with_columns(
            (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
            .cast(pl.Int32)
            .alias("landD")
        )


class TimberLandStrategy(BaseLandTypeStrategy):
    """Strategy for timber land classification."""
    
    def classify(self, data: pl.DataFrame) -> pl.DataFrame:
        """Add timber land classification."""
        return self.add_land_type_categories(data)
    
    def create_domain_indicator(self, data: pl.DataFrame) -> pl.DataFrame:
        """Create domain indicator for timber land only."""
        return data.with_columns(
            (
                (pl.col("COND_STATUS_CD") == LandStatus.FOREST)
                & pl.col("SITECLCD").is_in(SiteClass.PRODUCTIVE_CLASSES)
                & (pl.col("RESERVCD") == ReserveStatus.NOT_RESERVED)
            )
            .cast(pl.Int32)
            .alias("landD")
        )


class AllLandStrategy(BaseLandTypeStrategy):
    """Strategy for all land classification (no filtering)."""
    
    def classify(self, data: pl.DataFrame) -> pl.DataFrame:
        """Add all land classification."""
        return self.add_land_type_categories(data)
    
    def create_domain_indicator(self, data: pl.DataFrame) -> pl.DataFrame:
        """Create domain indicator for all land (always 1)."""
        return data.with_columns(pl.lit(1).alias("landD"))


class ByLandTypeStrategy(BaseLandTypeStrategy):
    """Strategy for by-land-type analysis."""
    
    def classify(self, data: pl.DataFrame) -> pl.DataFrame:
        """Add comprehensive land type categories for grouping."""
        return self.add_land_type_categories(data)
    
    def create_domain_indicator(self, data: pl.DataFrame) -> pl.DataFrame:
        """Create domain indicator for by-land-type analysis (always 1)."""
        return data.with_columns(pl.lit(1).alias("landD"))


class LandTypeClassifier:
    """
    Factory class for creating and managing land type classification strategies.
    
    This class implements the Factory pattern to create appropriate land type
    strategies based on configuration parameters and provides a unified
    interface for land type classification operations.
    """
    
    def __init__(self):
        """Initialize the land type classifier with available strategies."""
        self._strategies: Dict[str, BaseLandTypeStrategy] = {
            "forest": ForestLandStrategy(),
            "timber": TimberLandStrategy(),
            "all": AllLandStrategy(),
        }
        self._by_land_type_strategy = ByLandTypeStrategy()
    
    def get_strategy(self, land_type: str, by_land_type: bool = False) -> BaseLandTypeStrategy:
        """
        Get the appropriate land type strategy.
        
        Parameters
        ----------
        land_type : str
            Type of land classification ("forest", "timber", or "all")
        by_land_type : bool, default False
            Whether this is a by-land-type analysis
            
        Returns
        -------
        BaseLandTypeStrategy
            Appropriate strategy for the requested classification
            
        Raises
        ------
        ValueError
            If the requested land type is not supported
        """
        if by_land_type:
            return self._by_land_type_strategy
        
        if land_type not in self._strategies:
            raise ValueError(
                f"Unknown land type: {land_type}. "
                f"Available options: {list(self._strategies.keys())}"
            )
        
        return self._strategies[land_type]
    
    def classify_land_types(
        self, 
        data: pl.DataFrame, 
        land_type: str, 
        by_land_type: bool = False
    ) -> pl.DataFrame:
        """
        Classify land types using the appropriate strategy.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with condition data
        land_type : str
            Type of land classification
        by_land_type : bool, default False
            Whether this is a by-land-type analysis
            
        Returns
        -------
        pl.DataFrame
            DataFrame with land type classifications added
        """
        strategy = self.get_strategy(land_type, by_land_type)
        return strategy.classify(data)
    
    def create_land_domain_indicator(
        self,
        data: pl.DataFrame,
        land_type: str,
        by_land_type: bool = False
    ) -> pl.DataFrame:
        """
        Create land domain indicator using the appropriate strategy.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with condition data
        land_type : str
            Type of land classification
        by_land_type : bool, default False
            Whether this is a by-land-type analysis
            
        Returns
        -------
        pl.DataFrame
            DataFrame with landD domain indicator added
        """
        strategy = self.get_strategy(land_type, by_land_type)
        return strategy.create_domain_indicator(data)
    
    def get_available_land_types(self) -> list[str]:
        """
        Get list of available land type classifications.
        
        Returns
        -------
        list[str]
            List of supported land type names
        """
        return list(self._strategies.keys())
    
    def validate_land_type(self, land_type: str) -> bool:
        """
        Validate that a land type is supported.
        
        Parameters
        ----------
        land_type : str
            Land type to validate
            
        Returns
        -------
        bool
            True if land type is supported, False otherwise
        """
        return land_type in self._strategies