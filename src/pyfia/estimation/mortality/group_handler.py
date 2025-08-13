"""
Group handling for mortality estimates.

This module manages grouping operations and validation for mortality
estimation with support for multiple grouping variables.
"""

from typing import Dict, List, Optional
import polars as pl


class MortalityGroupHandler:
    """
    Manages grouping operations for mortality estimates.
    
    Handles validation and application of grouping variables including
    species, ownership, mortality agents, and disturbance codes.
    """
    
    # Valid grouping variables for mortality
    VALID_GROUP_VARS = {
        "SPCD": "Species code",
        "SPGRPCD": "Species group code", 
        "OWNGRPCD": "Ownership group code",
        "UNITCD": "Unit code",
        "AGENTCD": "Mortality agent code",
        "DSTRBCD1": "Primary disturbance code",
        "DSTRBCD2": "Secondary disturbance code",
        "DSTRBCD3": "Tertiary disturbance code",
        "FORTYPCD": "Forest type code",
        "STDSZCD": "Stand size class code",
        "EVALID": "Evaluation ID",
        "STATECD": "State code",
        "COUNTYCD": "County code",
    }
    
    # Grouping variables that need special handling
    REFERENCE_GROUPS = {
        "SPCD": "REF_SPECIES",
        "SPGRPCD": "REF_SPECIES_GROUP",
        "OWNGRPCD": "REF_OWNGRP",
        "AGENTCD": "REF_AGENT",
        "FORTYPCD": "REF_FOREST_TYPE",
    }
    
    def __init__(self, db=None):
        """
        Initialize the group handler.
        
        Parameters
        ----------
        db : FIA, optional
            FIA database for loading reference tables
        """
        self.db = db
        self._reference_cache = {}
    
    def validate_groups(self, groups: List[str]) -> None:
        """
        Validate grouping variables.
        
        Parameters
        ----------
        groups : List[str]
            List of grouping variables to validate
            
        Raises
        ------
        ValueError
            If any grouping variable is invalid
        """
        invalid_groups = [g for g in groups if g not in self.VALID_GROUP_VARS]
        if invalid_groups:
            raise ValueError(
                f"Invalid grouping variables: {invalid_groups}. "
                f"Valid options are: {list(self.VALID_GROUP_VARS.keys())}"
            )
    
    def apply_grouping(
        self, 
        data: pl.DataFrame, 
        groups: List[str],
        include_names: bool = True
    ) -> pl.DataFrame:
        """
        Apply grouping with proper aggregations and reference lookups.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to apply grouping to
        groups : List[str]
            List of grouping variables
        include_names : bool, default True
            Whether to include descriptive names from reference tables
            
        Returns
        -------
        pl.DataFrame
            Data with grouping applied and reference names added
        """
        # Validate groups first
        self.validate_groups(groups)
        
        # Add reference names if requested
        if include_names and self.db is not None:
            data = self._add_reference_names(data, groups)
        
        return data
    
    def _add_reference_names(
        self, 
        data: pl.DataFrame, 
        groups: List[str]
    ) -> pl.DataFrame:
        """
        Add descriptive names from reference tables.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to enhance with reference names
        groups : List[str]
            Grouping variables that may need reference lookups
            
        Returns
        -------
        pl.DataFrame
            Data with reference names added
        """
        for group in groups:
            if group in self.REFERENCE_GROUPS:
                ref_table = self.REFERENCE_GROUPS[group]
                
                # Load reference table if not cached
                if ref_table not in self._reference_cache:
                    try:
                        self.db.load_table(ref_table)
                        ref_df = self.db.tables[ref_table].collect()
                        self._reference_cache[ref_table] = ref_df
                    except Exception:
                        # Skip if reference table not available
                        continue
                else:
                    ref_df = self._reference_cache[ref_table]
                
                # Join reference data based on group type
                if group == "SPCD" and "SPCD" in ref_df.columns:
                    # Species reference
                    data = data.join(
                        ref_df.select(["SPCD", "COMMON_NAME", "SCIENTIFIC_NAME"]),
                        on="SPCD",
                        how="left"
                    )
                elif group == "SPGRPCD" and "SPGRPCD" in ref_df.columns:
                    # Species group reference
                    data = data.join(
                        ref_df.select(["SPGRPCD", "NAME"]).rename({"NAME": "SPGRP_NAME"}),
                        on="SPGRPCD",
                        how="left"
                    )
                elif group == "OWNGRPCD" and "OWNGRPCD" in ref_df.columns:
                    # Ownership group reference
                    data = data.join(
                        ref_df.select(["OWNGRPCD", "OWNGRPNM"]),
                        on="OWNGRPCD",
                        how="left"
                    )
                elif group == "AGENTCD" and "AGENTCD" in ref_df.columns:
                    # Mortality agent reference
                    data = data.join(
                        ref_df.select(["AGENTCD", "AGENTNM"]),
                        on="AGENTCD",
                        how="left"
                    )
                elif group == "FORTYPCD" and "VALUE" in ref_df.columns:
                    # Forest type reference
                    data = data.join(
                        ref_df.select(["VALUE", "MEANING"]).rename({
                            "VALUE": "FORTYPCD",
                            "MEANING": "FORTYP_NAME"
                        }),
                        on="FORTYPCD",
                        how="left"
                    )
        
        return data
    
    def get_group_summary(
        self,
        data: pl.DataFrame,
        groups: List[str],
        value_col: str
    ) -> pl.DataFrame:
        """
        Get summary statistics by groups.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to summarize
        groups : List[str]
            Grouping variables
        value_col : str
            Value column to summarize
            
        Returns
        -------
        pl.DataFrame
            Summary statistics by group
        """
        # Validate groups
        self.validate_groups(groups)
        
        # Calculate summary statistics
        summary = data.group_by(groups).agg([
            pl.count().alias("N_RECORDS"),
            pl.col(value_col).sum().alias(f"{value_col}_TOTAL"),
            pl.col(value_col).mean().alias(f"{value_col}_MEAN"),
            pl.col(value_col).std().alias(f"{value_col}_STD"),
            pl.col(value_col).min().alias(f"{value_col}_MIN"),
            pl.col(value_col).max().alias(f"{value_col}_MAX"),
        ])
        
        # Sort by total descending
        summary = summary.sort(f"{value_col}_TOTAL", descending=True)
        
        return summary
    
    def create_hierarchical_groups(
        self,
        data: pl.DataFrame,
        hierarchy: Dict[str, List[str]]
    ) -> pl.DataFrame:
        """
        Create hierarchical grouping structure.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data to group
        hierarchy : Dict[str, List[str]]
            Hierarchical grouping structure, e.g.:
            {"STATE": ["STATECD"], "COUNTY": ["STATECD", "COUNTYCD"]}
            
        Returns
        -------
        pl.DataFrame
            Data with hierarchical group identifiers
        """
        for level_name, level_groups in hierarchy.items():
            # Validate groups
            self.validate_groups(level_groups)
            
            # Create concatenated group identifier
            group_expr = pl.concat_str(
                [pl.col(g).cast(pl.Utf8) for g in level_groups],
                separator="_"
            ).alias(f"{level_name}_GROUP")
            
            data = data.with_columns(group_expr)
        
        return data
    
    def filter_significant_groups(
        self,
        data: pl.DataFrame,
        groups: List[str],
        value_col: str,
        min_plots: int = 10,
        min_value: float = 0.0
    ) -> pl.DataFrame:
        """
        Filter to only significant groups based on criteria.
        
        Parameters
        ----------
        data : pl.DataFrame
            Data with groups
        groups : List[str]
            Grouping variables
        value_col : str
            Value column to check
        min_plots : int, default 10
            Minimum number of plots required
        min_value : float, default 0.0
            Minimum total value required
            
        Returns
        -------
        pl.DataFrame
            Data filtered to significant groups only
        """
        # Calculate group statistics
        group_stats = data.group_by(groups).agg([
            pl.n_unique("PLT_CN").alias("N_PLOTS"),
            pl.col(value_col).sum().alias(f"{value_col}_TOTAL"),
        ])
        
        # Filter significant groups
        significant = group_stats.filter(
            (pl.col("N_PLOTS") >= min_plots) &
            (pl.col(f"{value_col}_TOTAL") > min_value)
        )
        
        # Join back to filter original data
        data_filtered = data.join(
            significant.select(groups),
            on=groups,
            how="inner"
        )
        
        return data_filtered