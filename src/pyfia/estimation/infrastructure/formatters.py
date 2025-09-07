"""
Output formatting utilities for consistent estimation results.

This module provides centralized formatting for all estimation outputs,
ensuring consistent column naming, variance/SE handling, and metadata.
"""

from typing import Dict, List, Optional, Union
import polars as pl

from ...constants.constants import EstimatorType


class OutputFormatter:
    """
    Centralized formatter for estimation outputs.
    
    Ensures consistent column naming, variance/SE conversion, and metadata
    across all estimators (area, biomass, volume, tpa, mortality, etc).
    """
    
    # Standard column name mappings per estimator type
    COLUMN_MAPPINGS = {
        EstimatorType.AREA: {
            "primary": "AREA",
            "per_unit": "AREA_PERC",
            "total": "AREA",
            "variance_suffix": "_VAR",
            "se_suffix": "_SE",
        },
        EstimatorType.BIOMASS: {
            "primary": "BIOMASS",
            "per_unit": "BIOMASS_ACRE", 
            "total": "BIOMASS",
            "variance_suffix": "_VAR",
            "se_suffix": "_SE",
            "secondary": {
                "carbon_per_unit": "CARB_ACRE",
                "carbon_total": "CARB",
            }
        },
        EstimatorType.VOLUME: {
            "primary": "VOLUME",
            "per_unit": "VOLUME_ACRE",
            "total": "VOLUME", 
            "variance_suffix": "_VAR",
            "se_suffix": "_SE",
        },
        EstimatorType.TPA: {
            "primary": "TPA",
            "per_unit": "TPA",
            "total": "TPA_TOTAL",  # Changed to avoid conflict
            "secondary": {
                "basal_per_unit": "BAA",
                "basal_total": "BA_TOTAL",
                "tree_total": "TREE_TOTAL",  # Keep original name
            },
            "variance_suffix": "_VAR", 
            "se_suffix": "_SE",
        },
        EstimatorType.MORTALITY: {
            "primary": "MORTALITY",
            "per_unit": "MORTALITY_TPA",
            "total": "MORTALITY_TOTAL",
            "secondary": {
                "volume_per_unit": "MORTALITY_VOL",
                "volume_total": "MORTALITY_VOL_TOTAL",
                "basal_per_unit": "MORTALITY_BA",
                "basal_total": "MORTALITY_BA_TOTAL",
            },
            "variance_suffix": "_VAR",
            "se_suffix": "_SE",
        },
        EstimatorType.GROWTH: {
            "primary": "GROWTH",
            "per_unit": "GROSS_GROWTH_ACRE",
            "total": "GROSS_GROWTH",
            "secondary": {
                "net_per_unit": "NET_GROWTH_ACRE",
                "net_total": "NET_GROWTH",
            },
            "variance_suffix": "_VAR",
            "se_suffix": "_SE",
        }
    }
    
    def __init__(self, estimator_type: EstimatorType):
        """
        Initialize formatter for specific estimator type.
        
        Parameters
        ----------
        estimator_type : EstimatorType
            Type of estimator (AREA, BIOMASS, VOLUME, etc)
        """
        self.estimator_type = estimator_type
        self.mapping = self.COLUMN_MAPPINGS.get(estimator_type, {})
        
    def standardize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Standardize column names based on estimator type.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame with raw estimation results
            
        Returns
        -------
        pl.DataFrame
            DataFrame with standardized column names
        """
        # Create rename mapping for existing columns
        rename_map = {}
        
        # Handle various naming patterns
        for col in df.columns:
            col_upper = col.upper()
            
            # Skip already standardized columns
            if col in rename_map.values():
                continue
                
            # Handle specific patterns based on estimator type
            if self.estimator_type == EstimatorType.AREA:
                # FA_TOTAL -> AREA
                if col == "FA_TOTAL" and self.mapping.get("total"):
                    rename_map[col] = self.mapping["total"]
                # Don't rename AREA_PERC_VAR - it's already standardized
                elif col in ["AREA_PERC", "AREA_PERC_VAR", "AREA_PERC_SE", "AREA_VAR", "AREA_SE"]:
                    continue
                    
            # Handle per-acre/per-unit columns
            elif any(pattern in col_upper for pattern in ["_ACRE", "_PERC", "_PER_", "_PA"]):
                if "VAR" in col_upper and col_upper.endswith("_VAR"):
                    base = self._extract_base_name(col_upper)
                    new_name = f"{base}{self.mapping.get('variance_suffix', '_VAR')}"
                    if new_name != col:
                        rename_map[col] = new_name
                elif "SE" in col_upper and col_upper.endswith("_SE"):
                    base = self._extract_base_name(col_upper)
                    new_name = f"{base}{self.mapping.get('se_suffix', '_SE')}"
                    if new_name != col:
                        rename_map[col] = new_name
                    
            # Handle total columns
            elif "TOTAL" in col_upper and "SE" not in col_upper and "VAR" not in col_upper:
                # Special handling for certain estimators
                if self.estimator_type == EstimatorType.TPA and col in ["TREE_TOTAL", "BA_TOTAL"]:
                    continue
                elif self.estimator_type == EstimatorType.BIOMASS and col in ["BIO_TOTAL", "CARB_TOTAL"]:
                    continue
                elif self.mapping.get("total") and col != self.mapping["total"]:
                    rename_map[col] = self.mapping["total"]
                    
        if rename_map:
            df = df.rename(rename_map)
            
        return df
    
    def _extract_base_name(self, col_name: str) -> str:
        """Extract base column name without suffixes."""
        original_name = col_name
        
        # Remove common suffixes
        for suffix in ["_VAR", "_SE", "_ACRE", "_PERC", "_TOTAL", "_PA"]:
            if col_name.endswith(suffix):
                col_name = col_name[:-len(suffix)]
        
        # Map to standard base name
        if self.estimator_type == EstimatorType.AREA:
            # Already standardized AREA columns should be returned as-is
            if original_name in ["AREA_PERC", "AREA"]:
                return original_name
            return "AREA_PERC" if "PERC" in original_name else "AREA"
        elif self.estimator_type == EstimatorType.BIOMASS:
            if "CARB" in col_name:
                return "CARB_ACRE"
            return "BIOMASS_ACRE" if "ACRE" in col_name else "BIOMASS"
        elif self.estimator_type == EstimatorType.VOLUME:
            return "VOLUME_ACRE" if "ACRE" in col_name else "VOLUME"
        elif self.estimator_type == EstimatorType.TPA:
            if "BA" in col_name:
                return "BAA" if "ACRE" in col_name or "AA" in col_name else "BA_TOTAL"
            return "TPA" if "ACRE" not in col_name else "TPA"
        elif self.estimator_type == EstimatorType.MORTALITY:
            if "VOL" in col_name:
                return "MORTALITY_VOL"
            elif "BA" in col_name:
                return "MORTALITY_BA"
            return "MORTALITY_TPA"
        
        return col_name
    
    def convert_variance_to_se(self, df: pl.DataFrame, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert variance columns to standard error.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame with variance columns
        columns : List[str], optional
            Specific columns to convert. If None, converts all variance columns.
            
        Returns
        -------
        pl.DataFrame
            DataFrame with SE columns added/replaced
        """
        if columns is None:
            # Find all variance columns
            columns = [col for col in df.columns if col.endswith("_VAR")]
            
        expr_list = []
        rename_map = {}
        
        for var_col in columns:
            if var_col in df.columns:
                # Extract base name and create SE column name
                base = var_col[:-4]  # Remove _VAR
                se_col = f"{base}_SE"
                
                # Calculate SE as sqrt(variance)
                expr_list.append(
                    pl.col(var_col).sqrt().alias(se_col)
                )
                
                # Mark variance column for removal
                rename_map[var_col] = None
                
        if expr_list:
            # Add SE columns
            df = df.with_columns(expr_list)
            
            # Remove variance columns if requested
            df = df.drop([col for col in rename_map.keys() if col in df.columns])
            
        return df
    
    def convert_se_to_variance(self, df: pl.DataFrame, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """
        Convert standard error columns to variance.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame with SE columns
        columns : List[str], optional
            Specific columns to convert. If None, converts all SE columns.
            
        Returns
        -------
        pl.DataFrame
            DataFrame with variance columns added/replaced
        """
        if columns is None:
            # Find all SE columns
            columns = [col for col in df.columns if col.endswith("_SE")]
            
        expr_list = []
        rename_map = {}
        
        for se_col in columns:
            if se_col in df.columns:
                # Extract base name and create variance column name
                base = se_col[:-3]  # Remove _SE
                var_col = f"{base}_VAR"
                
                # Calculate variance as SE^2
                expr_list.append(
                    (pl.col(se_col) ** 2).alias(var_col)
                )
                
                # Mark SE column for removal
                rename_map[se_col] = None
                
        if expr_list:
            # Add variance columns
            df = df.with_columns(expr_list)
            
            # Remove SE columns if requested
            df = df.drop([col for col in rename_map.keys() if col in df.columns])
            
        return df
    
    def add_metadata_columns(
        self, 
        df: pl.DataFrame,
        year: Optional[int] = None,
        n_plots: Optional[Union[int, pl.Expr]] = None,
        additional_metadata: Optional[Dict[str, Union[int, float, str, pl.Expr]]] = None
    ) -> pl.DataFrame:
        """
        Add standard metadata columns.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame to add metadata to
        year : int, optional
            Year of estimation
        n_plots : int or pl.Expr, optional
            Number of plots used
        additional_metadata : dict, optional
            Additional metadata columns to add
            
        Returns
        -------
        pl.DataFrame
            DataFrame with metadata columns added
        """
        expr_list = []
        
        # Add year if provided
        if year is not None and "YEAR" not in df.columns:
            expr_list.append(pl.lit(year).alias("YEAR"))
            
        # Add plot count if provided
        if n_plots is not None:
            if isinstance(n_plots, pl.Expr):
                expr_list.append(n_plots.alias("N_PLOTS"))
            else:
                expr_list.append(pl.lit(n_plots).alias("N_PLOTS"))
                
        # Add N column (total record count) if not present
        if "N" not in df.columns:
            expr_list.append(pl.len().alias("N"))
            
        # Add any additional metadata
        if additional_metadata:
            for col_name, value in additional_metadata.items():
                if col_name not in df.columns:
                    if isinstance(value, pl.Expr):
                        expr_list.append(value.alias(col_name))
                    else:
                        expr_list.append(pl.lit(value).alias(col_name))
                        
        if expr_list:
            df = df.with_columns(expr_list)
            
        return df
    
    def format_grouped_results(
        self,
        df: pl.DataFrame,
        group_cols: Optional[List[str]] = None,
        totals_row: bool = False
    ) -> pl.DataFrame:
        """
        Format grouped results with consistent ordering and optional totals row.
        
        Parameters
        ----------
        df : pl.DataFrame
            DataFrame with grouped results
        group_cols : List[str], optional
            Grouping columns to put first
        totals_row : bool, default False
            Whether to add a totals row
            
        Returns
        -------
        pl.DataFrame
            Formatted DataFrame
        """
        if group_cols:
            # Ensure group columns come first
            other_cols = [col for col in df.columns if col not in group_cols]
            df = df.select(group_cols + other_cols)
            
        if totals_row and group_cols:
            # Calculate totals row
            numeric_cols = []
            for col in df.columns:
                if col not in group_cols and df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
                    numeric_cols.append(col)
                    
            if numeric_cols:
                # Create aggregation expressions
                agg_exprs = []
                for col in numeric_cols:
                    # Sum for totals, mean for per-unit values
                    if any(suffix in col.upper() for suffix in ["_TOTAL", "N_PLOTS", "_N"]):
                        agg_exprs.append(pl.sum(col).alias(col))
                    else:
                        # For per-unit values, calculate weighted average if possible
                        agg_exprs.append(pl.mean(col).alias(col))
                        
                # Calculate totals
                totals = df.select(agg_exprs)
                
                # Add group column values as "Total"
                for col in group_cols:
                    totals = totals.with_columns(pl.lit("Total").alias(col))
                    
                # Append totals row
                df = pl.concat([df, totals])
                
        return df
    
    def format_output(
        self,
        df: pl.DataFrame,
        variance: bool = False,
        totals: bool = False,
        group_cols: Optional[List[str]] = None,
        year: Optional[int] = None,
        standardize: bool = True
    ) -> pl.DataFrame:
        """
        Apply complete formatting to estimation output.
        
        Parameters
        ----------
        df : pl.DataFrame
            Raw estimation output
        variance : bool, default False
            Whether to output variance (True) or standard error (False)
        totals : bool, default False
            Whether to include total columns
        group_cols : List[str], optional
            Grouping columns
        year : int, optional
            Year for metadata
        standardize : bool, default True
            Whether to standardize column names
            
        Returns
        -------
        pl.DataFrame
            Fully formatted output
        """
        # Standardize column names if requested
        if standardize:
            df = self.standardize_columns(df)
            
        # Handle variance/SE conversion
        if variance:
            # Convert any SE columns to variance
            df = self.convert_se_to_variance(df)
        else:
            # Convert any variance columns to SE
            df = self.convert_variance_to_se(df)
            
        # Add metadata
        df = self.add_metadata_columns(df, year=year)
        
        # Format grouped results
        df = self.format_grouped_results(df, group_cols=group_cols)
        
        # Select final columns based on options
        output_cols = self._get_output_columns(df, variance=variance, totals=totals, group_cols=group_cols)
        
        # Ensure all expected columns exist
        available_cols = [col for col in output_cols if col in df.columns]
        
        return df.select(available_cols)
    
    def _get_output_columns(
        self,
        df: pl.DataFrame,
        variance: bool = False,
        totals: bool = False,
        group_cols: Optional[List[str]] = None
    ) -> List[str]:
        """Determine output columns based on options and estimator type."""
        cols = []
        
        # Group columns first
        if group_cols:
            cols.extend(group_cols)
            
        # Standard metadata columns
        for meta_col in ["YEAR", "N", "N_PLOTS"]:
            if meta_col in df.columns and meta_col not in cols:
                cols.append(meta_col)
                
        # Primary per-unit estimate
        per_unit_col = self.mapping.get("per_unit")
        if per_unit_col and per_unit_col in df.columns:
            cols.append(per_unit_col)
            
        # Primary uncertainty (variance or SE)
        if variance:
            var_col = f"{per_unit_col}_VAR"
            if var_col in df.columns:
                cols.append(var_col)
        else:
            se_col = f"{per_unit_col}_SE"
            if se_col in df.columns:
                cols.append(se_col)
                
        # Secondary per-unit estimates (e.g., carbon, basal area)
        secondary = self.mapping.get("secondary", {})
        for key, col_name in secondary.items():
            if "per_unit" in key and col_name in df.columns:
                cols.append(col_name)
                # Add uncertainty
                if variance:
                    var_col = f"{col_name}_VAR"
                    if var_col in df.columns:
                        cols.append(var_col)
                else:
                    se_col = f"{col_name}_SE"
                    if se_col in df.columns:
                        cols.append(se_col)
                        
        # Total columns if requested
        if totals:
            # For TPA and BIOMASS, handle specific total columns
            if self.estimator_type == EstimatorType.TPA:
                for total_col in ["TREE_TOTAL", "BA_TOTAL"]:
                    if total_col in df.columns:
                        cols.append(total_col)
                        # Add uncertainty if exists
                        if variance:
                            var_col = f"{total_col}_VAR"
                            if var_col in df.columns:
                                cols.append(var_col)
                        else:
                            se_col = f"{total_col}_SE"
                            if se_col in df.columns:
                                cols.append(se_col)
            elif self.estimator_type == EstimatorType.BIOMASS:
                for total_col in ["BIO_TOTAL", "CARB_TOTAL"]:
                    if total_col in df.columns:
                        cols.append(total_col)
                        # Add uncertainty if exists
                        if variance:
                            var_col = f"{total_col}_VAR"
                            if var_col in df.columns:
                                cols.append(var_col)
                        else:
                            se_col = f"{total_col}_SE"
                            if se_col in df.columns:
                                cols.append(se_col)
            else:
                # Primary total
                total_col = self.mapping.get("total")
                if total_col and total_col in df.columns:
                    cols.append(total_col)
                    # Add uncertainty
                    if variance:
                        var_col = f"{total_col}_VAR"
                        if var_col in df.columns:
                            cols.append(var_col)
                    else:
                        se_col = f"{total_col}_SE"
                        if se_col in df.columns:
                            cols.append(se_col)
                            
                # Secondary totals
                for key, col_name in secondary.items():
                    if "total" in key and col_name in df.columns:
                        cols.append(col_name)
                        # Add uncertainty
                        if variance:
                            var_col = f"{col_name}_VAR"
                            if var_col in df.columns:
                                cols.append(var_col)
                        else:
                            se_col = f"{col_name}_SE"
                            if se_col in df.columns:
                                cols.append(se_col)
                            
        # Additional plot count columns if present
        for plot_col in ["nPlots_TREE", "nPlots_AREA"]:
            if plot_col in df.columns and plot_col not in cols:
                cols.append(plot_col)
                
        return cols


def format_estimation_output(
    df: pl.DataFrame,
    estimator_type: EstimatorType,
    variance: bool = False,
    totals: bool = False,
    group_cols: Optional[List[str]] = None,
    year: Optional[int] = None
) -> pl.DataFrame:
    """
    Convenience function to format estimation output.
    
    Parameters
    ----------
    df : pl.DataFrame
        Raw estimation output
    estimator_type : EstimatorType
        Type of estimator
    variance : bool, default False
        Whether to output variance or standard error
    totals : bool, default False
        Whether to include total columns
    group_cols : List[str], optional
        Grouping columns
    year : int, optional
        Year for metadata
        
    Returns
    -------
    pl.DataFrame
        Formatted estimation output
    """
    formatter = OutputFormatter(estimator_type)
    return formatter.format_output(
        df,
        variance=variance,
        totals=totals,
        group_cols=group_cols,
        year=year
    )