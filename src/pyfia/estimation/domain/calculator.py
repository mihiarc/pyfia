"""
Domain indicator calculation for FIA area estimation.

This module provides the DomainIndicatorCalculator class which handles the
calculation of all domain indicators needed for FIA area estimation, including
land domain, area domain, tree domain, and composite indicators.
"""

from typing import Dict, Optional, Any

import polars as pl

from ...constants.constants import LandStatus
from .land_types import LandTypeClassifier


class DomainIndicatorCalculator:
    """
    Calculates domain indicators for FIA area estimation.
    
    This class encapsulates the logic for calculating various domain indicators
    used in FIA area estimation, including:
    - landD: Land domain indicator (based on land type filtering)
    - aD: Area domain indicator (based on area domain filtering)
    - tD: Tree domain indicator (based on tree domain filtering)
    - aDI: Comprehensive area domain indicator (numerator)
    - pDI: Partial domain indicator (denominator)
    
    The calculator uses composition with a LandTypeClassifier to handle
    land type specific logic and supports both regular and by-land-type
    analysis modes.
    """
    
    def __init__(
        self,
        land_type: str = "forest",
        by_land_type: bool = False,
        tree_domain: Optional[str] = None,
        area_domain: Optional[str] = None,
        data_cache: Optional[Dict[str, pl.DataFrame]] = None
    ):
        """
        Initialize the domain indicator calculator.
        
        Parameters
        ----------
        land_type : str, default "forest"
            Type of land classification ("forest", "timber", or "all")
        by_land_type : bool, default False
            Whether this is a by-land-type analysis
        tree_domain : Optional[str], default None
            SQL-like expression for tree domain filtering
        area_domain : Optional[str], default None
            SQL-like expression for area domain filtering
        data_cache : Optional[Dict[str, pl.DataFrame]], default None
            Cache of loaded data tables
        """
        self.land_type = land_type
        self.by_land_type = by_land_type
        self.tree_domain = tree_domain
        self.area_domain = area_domain
        # Preserve the reference to the shared data_cache
        if data_cache is not None:
            self.data_cache = data_cache
        else:
            self.data_cache = {}
        
        # Initialize the land type classifier
        self.land_type_classifier = LandTypeClassifier()
    
    def calculate_all_indicators(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate all domain indicators for area estimation.
        
        This is the main method that orchestrates the calculation of all
        required domain indicators by calling the individual calculation
        methods in the correct order.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with all domain indicators added
        """
        # Apply tree domain filtering if needed
        if self.tree_domain is not None:
            cond_df = self._apply_tree_domain_filtering(cond_df)
        
        # Add land type categories if doing by-land-type analysis
        if self.by_land_type:
            cond_df = self.land_type_classifier.classify_land_types(
                cond_df, self.land_type, self.by_land_type
            )
        
        # Calculate individual domain indicators
        cond_df = self._calculate_land_domain_indicator(cond_df)
        cond_df = self._calculate_area_domain_indicator(cond_df)
        cond_df = self._calculate_tree_domain_indicator(cond_df)
        
        # Calculate composite indicators
        cond_df = self._calculate_comprehensive_indicator(cond_df)
        cond_df = self._calculate_partial_indicator(cond_df)
        
        return cond_df
    
    def _apply_tree_domain_filtering(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply tree domain filtering at the condition level.
        
        This method identifies conditions that contain trees meeting the
        tree domain criteria and adds a flag for use in domain indicator
        calculation.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with HAS_QUALIFYING_TREE column added
        """
        if "TREE" not in self.data_cache:
            return cond_df
        
        tree_df = self.data_cache["TREE"]
        
        # Filter trees by domain
        qualifying_trees = tree_df.filter(pl.sql_expr(self.tree_domain))
        
        # Get unique PLT_CN/CONDID combinations with qualifying trees
        qualifying_conds = (
            qualifying_trees.select(["PLT_CN", "CONDID"])
            .unique()
            .with_columns(pl.lit(1).alias("HAS_QUALIFYING_TREE"))
        )
        
        # Join back to conditions
        result = cond_df.join(
            qualifying_conds,
            on=["PLT_CN", "CONDID"],
            how="left"
        ).with_columns(
            pl.col("HAS_QUALIFYING_TREE").fill_null(0)
        )
        
        return result
    
    def _calculate_land_domain_indicator(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate land domain indicator (landD).
        
        The land domain indicator identifies conditions that meet the
        land type criteria for the analysis.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with landD column added
        """
        return self.land_type_classifier.create_land_domain_indicator(
            cond_df, self.land_type, self.by_land_type
        )
    
    def _calculate_area_domain_indicator(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate area domain indicator (aD).
        
        The area domain indicator identifies conditions that meet the
        area domain criteria specified by the user.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with aD column added
        """
        if self.area_domain is not None:
            # Apply area domain filtering as an indicator
            return cond_df.with_columns(
                pl.when(pl.sql_expr(self.area_domain))
                .then(1)
                .otherwise(0)
                .alias("aD")
            )
        else:
            # No area domain filtering - all conditions qualify
            return cond_df.with_columns(pl.lit(1).alias("aD"))
    
    def _calculate_tree_domain_indicator(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate tree domain indicator (tD).
        
        The tree domain indicator identifies conditions that contain
        trees meeting the tree domain criteria.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data
            
        Returns
        -------
        pl.DataFrame
            DataFrame with tD column added
        """
        if "HAS_QUALIFYING_TREE" in cond_df.columns:
            return cond_df.with_columns(
                pl.col("HAS_QUALIFYING_TREE").alias("tD")
            )
        else:
            # No tree domain filtering applied
            return cond_df.with_columns(pl.lit(1).alias("tD"))
    
    def _calculate_comprehensive_indicator(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate comprehensive area domain indicator (aDI).
        
        This is the numerator for area calculations and represents
        conditions that meet all domain criteria.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data and individual domain indicators
            
        Returns
        -------
        pl.DataFrame
            DataFrame with aDI column added
        """
        if self.by_land_type:
            # For by_land_type: use only area domain
            return cond_df.with_columns(pl.col("aD").alias("aDI"))
        else:
            # Regular: combine all domain indicators
            return cond_df.with_columns(
                (pl.col("landD") * pl.col("aD") * pl.col("tD")).alias("aDI")
            )
    
    def _calculate_partial_indicator(self, cond_df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculate partial domain indicator (pDI).
        
        This is the denominator for area calculations and typically
        represents a broader set of conditions than the numerator.
        
        Parameters
        ----------
        cond_df : pl.DataFrame
            DataFrame with condition data and individual domain indicators
            
        Returns
        -------
        pl.DataFrame
            DataFrame with pDI column added
        """
        if self.by_land_type:
            # For by_land_type: use only land conditions (excludes water)
            return cond_df.with_columns(
                pl.when(
                    pl.col("COND_STATUS_CD").is_in([LandStatus.FOREST, LandStatus.NONFOREST])
                )
                .then(pl.col("aD"))
                .otherwise(0)
                .alias("pDI")
            )
        else:
            # Regular: denominator excludes area domain (for percentage calculations)
            # pDI represents the broader population we're calculating percentages relative to
            return cond_df.with_columns(
                pl.col("landD").alias("pDI")
            )
    
    def get_indicator_descriptions(self) -> Dict[str, str]:
        """
        Get descriptions of all domain indicators.
        
        Returns
        -------
        Dict[str, str]
            Dictionary mapping indicator names to their descriptions
        """
        return {
            "landD": "Land domain indicator (land type filtering)",
            "aD": "Area domain indicator (area filtering)", 
            "tD": "Tree domain indicator (tree filtering)",
            "aDI": "Comprehensive area domain indicator (numerator)",
            "pDI": "Partial domain indicator (denominator)",
        }
    
    def validate_indicators(self, data: pl.DataFrame) -> Dict[str, Any]:
        """
        Validate domain indicators for consistency.
        
        Parameters
        ----------
        data : pl.DataFrame
            DataFrame with calculated domain indicators
            
        Returns
        -------
        Dict[str, Any]
            Dictionary with validation results
        """
        validation_results = {
            "is_valid": True,
            "warnings": [],
            "statistics": {}
        }
        
        required_indicators = ["landD", "aD", "tD", "aDI", "pDI"]
        missing_indicators = [ind for ind in required_indicators if ind not in data.columns]
        
        if missing_indicators:
            validation_results["is_valid"] = False
            validation_results["warnings"].append(
                f"Missing indicators: {missing_indicators}"
            )
        
        if validation_results["is_valid"]:
            # Calculate basic statistics for each indicator
            for indicator in required_indicators:
                stats = data.select([
                    pl.col(indicator).sum().alias("sum"),
                    pl.col(indicator).mean().alias("mean"),
                    pl.col(indicator).min().alias("min"),
                    pl.col(indicator).max().alias("max")
                ]).to_dicts()[0]
                validation_results["statistics"][indicator] = stats
                
                # Check for invalid values
                if stats["min"] < 0 or stats["max"] > 1:
                    validation_results["warnings"].append(
                        f"Invalid values in {indicator}: min={stats['min']}, max={stats['max']}"
                    )
        
        return validation_results
    
    def update_configuration(self, **kwargs) -> None:
        """
        Update calculator configuration.
        
        Parameters
        ----------
        **kwargs
            Configuration parameters to update
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise ValueError(f"Unknown configuration parameter: {key}")