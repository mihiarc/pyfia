"""
Data filtering steps for the pyFIA estimation pipeline.

This module provides pipeline steps for applying various domain filters to
FIA data, including tree domain, area domain, plot domain, land type, and
EVALID filtering. These steps ensure that only relevant data is processed
in the estimation pipeline.

Steps:
- ApplyTreeDomainStep: Apply tree domain filters
- ApplyAreaDomainStep: Apply area domain filters  
- FilterByLandTypeStep: Filter by land type classification
- FilterByEvalidStep: Apply EVALID filtering for statistical validity
- ApplyCombinedDomainsStep: Apply multiple domain filters in one step
"""

from typing import Dict, List, Optional, Type, Union
import warnings

import polars as pl

from ....filters.common import (
    apply_tree_filters_common,
    apply_area_filters_common,
    parse_domain_expression
)
from ....filters.evalid_filter import EvalidFilter
from ...lazy_evaluation import LazyFrameWrapper
from ..core import ExecutionContext, PipelineException
from ..contracts import RawTablesContract, FilteredDataContract
from ..base_steps import FilteringStep


class ApplyTreeDomainStep(FilteringStep):
    """
    Apply tree domain filters to tree data.
    
    This step applies tree-level filters based on SQL-like domain expressions,
    supporting conditions on STATUSCD, DIA, SPCD, and other tree attributes.
    It tracks filter efficiency and maintains data lineage.
    
    Examples
    --------
    >>> # Filter to live trees >= 5 inches DBH
    >>> step = ApplyTreeDomainStep(
    ...     tree_domain="STATUSCD == 1 AND DIA >= 5.0",
    ...     track_efficiency=True
    ... )
    >>> 
    >>> # Filter by species and status
    >>> step = ApplyTreeDomainStep(
    ...     tree_domain="SPCD IN (131, 110) AND STATUSCD == 1"
    ... )
    """
    
    def __init__(
        self,
        tree_domain: str,
        validate_expression: bool = True,
        track_efficiency: bool = True,
        preserve_unfiltered: bool = False,
        **kwargs
    ):
        """
        Initialize tree domain filtering step.
        
        Parameters
        ----------
        tree_domain : str
            SQL-like expression for filtering trees
        validate_expression : bool
            Whether to validate the domain expression syntax
        track_efficiency : bool
            Whether to track filter efficiency metrics
        preserve_unfiltered : bool
            Whether to preserve unfiltered data for comparison
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(tree_domain=tree_domain, **kwargs)
        self.validate_expression = validate_expression
        self.track_efficiency = track_efficiency
        self.preserve_unfiltered = preserve_unfiltered
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> FilteredDataContract:
        """
        Execute tree domain filtering.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract with raw tables
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FilteredDataContract
            Contract with filtered tree data
        """
        try:
            # Get tree data from input
            if not input_data.has_table("TREE"):
                raise PipelineException(
                    "TREE table not found in input data",
                    step_id=self.step_id
                )
            
            tree_data = input_data.get_table("TREE")
            
            # Validate expression if requested
            if self.validate_expression:
                self._validate_tree_domain_expression(self.tree_domain)
            
            # Get initial record count if tracking efficiency
            before_count = None
            if self.track_efficiency:
                if isinstance(tree_data.frame, pl.LazyFrame):
                    before_count = tree_data.frame.select(pl.count()).collect().item()
                else:
                    before_count = len(tree_data.frame)
            
            # Apply tree domain filter
            filtered_tree = self.apply_tree_filters(tree_data, context.config)
            
            # Get filtered record count if tracking efficiency
            after_count = None
            if self.track_efficiency:
                if isinstance(filtered_tree.frame, pl.LazyFrame):
                    after_count = filtered_tree.frame.select(pl.count()).collect().item()
                else:
                    after_count = len(filtered_tree.frame)
            
            # Get condition and plot data if available
            condition_data = input_data.get_table("COND") if input_data.has_table("COND") else None
            plot_data = input_data.get_table("PLOT") if input_data.has_table("PLOT") else None
            
            # Create output contract
            output = FilteredDataContract(
                tree_data=filtered_tree,
                condition_data=condition_data or LazyFrameWrapper(pl.LazyFrame()),
                plot_data=plot_data,
                tree_domain=self.tree_domain,
                step_id=self.step_id
            )
            
            # Add filter metadata
            if self.track_efficiency and before_count is not None and after_count is not None:
                output.add_filter_metadata("TREE", before_count, after_count)
                
                # Log efficiency
                efficiency = (after_count / before_count * 100) if before_count > 0 else 0
                if context.debug:
                    warnings.warn(
                        f"Tree domain filter retained {after_count:,} of {before_count:,} "
                        f"records ({efficiency:.1f}%)",
                        category=UserWarning
                    )
            
            # Preserve unfiltered data if requested
            if self.preserve_unfiltered:
                output.add_processing_metadata("unfiltered_tree_data", tree_data)
            
            # Track performance
            self.track_performance(
                context,
                records_before=before_count,
                records_after=after_count,
                filter_efficiency=after_count / before_count if before_count else 0
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply tree domain filter: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _validate_tree_domain_expression(self, expression: str) -> None:
        """Validate tree domain expression syntax."""
        # Check for valid tree columns
        valid_columns = {
            "STATUSCD", "DIA", "SPCD", "HT", "ACTUALHT", "CR", "TREECLCD",
            "AGENTCD", "TOTAGE", "TPA_UNADJ", "CARBON_AG", "CARBON_BG",
            "DRYBIO_AG", "DRYBIO_BG", "VOLCFNET", "VOLCFGRS"
        }
        
        # Parse expression to check for invalid columns
        try:
            parsed = parse_domain_expression(expression)
            # This is a simplified validation - could be more sophisticated
            for token in expression.split():
                if token.upper() in ["AND", "OR", "NOT", "IN", "BETWEEN", "IS", "NULL"]:
                    continue
                if any(op in token for op in ["==", "!=", ">=", "<=", ">", "<", "(", ")", ","]):
                    continue
                # Check if it's a number
                try:
                    float(token)
                    continue
                except ValueError:
                    pass
                # Check if it's a valid column
                if token.upper() not in valid_columns and not token.isdigit():
                    warnings.warn(
                        f"Potential invalid column in tree domain: {token}",
                        category=UserWarning
                    )
        except Exception as e:
            raise ValueError(f"Invalid tree domain expression: {e}")


class ApplyAreaDomainStep(FilteringStep):
    """
    Apply area domain filters to condition data.
    
    This step applies condition-level filters based on SQL-like domain expressions,
    supporting conditions on COND_STATUS_CD, OWNCD, FORTYPCD, and other attributes.
    
    Examples
    --------
    >>> # Filter to forest land
    >>> step = ApplyAreaDomainStep(
    ...     area_domain="COND_STATUS_CD == 1",
    ...     track_efficiency=True
    ... )
    >>> 
    >>> # Filter by ownership and forest type
    >>> step = ApplyAreaDomainStep(
    ...     area_domain="OWNGRPCD == 10 AND FORTYPCD >= 100"
    ... )
    """
    
    def __init__(
        self,
        area_domain: str,
        validate_expression: bool = True,
        track_efficiency: bool = True,
        **kwargs
    ):
        """
        Initialize area domain filtering step.
        
        Parameters
        ----------
        area_domain : str
            SQL-like expression for filtering conditions
        validate_expression : bool
            Whether to validate the domain expression syntax
        track_efficiency : bool
            Whether to track filter efficiency metrics
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(area_domain=area_domain, **kwargs)
        self.validate_expression = validate_expression
        self.track_efficiency = track_efficiency
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> FilteredDataContract:
        """
        Execute area domain filtering.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract with raw tables
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FilteredDataContract
            Contract with filtered condition data
        """
        try:
            # Get condition data from input
            if not input_data.has_table("COND"):
                raise PipelineException(
                    "COND table not found in input data",
                    step_id=self.step_id
                )
            
            condition_data = input_data.get_table("COND")
            
            # Validate expression if requested
            if self.validate_expression:
                self._validate_area_domain_expression(self.area_domain)
            
            # Get initial record count if tracking efficiency
            before_count = None
            if self.track_efficiency:
                if isinstance(condition_data.frame, pl.LazyFrame):
                    before_count = condition_data.frame.select(pl.count()).collect().item()
                else:
                    before_count = len(condition_data.frame)
            
            # Apply area domain filter
            filtered_condition = self.apply_area_filters(condition_data, context.config)
            
            # Get filtered record count if tracking efficiency
            after_count = None
            if self.track_efficiency:
                if isinstance(filtered_condition.frame, pl.LazyFrame):
                    after_count = filtered_condition.frame.select(pl.count()).collect().item()
                else:
                    after_count = len(filtered_condition.frame)
            
            # Get tree and plot data if available
            tree_data = input_data.get_table("TREE") if input_data.has_table("TREE") else None
            plot_data = input_data.get_table("PLOT") if input_data.has_table("PLOT") else None
            
            # Create output contract
            output = FilteredDataContract(
                tree_data=tree_data,
                condition_data=filtered_condition,
                plot_data=plot_data,
                area_domain=self.area_domain,
                step_id=self.step_id
            )
            
            # Add filter metadata
            if self.track_efficiency and before_count is not None and after_count is not None:
                output.add_filter_metadata("COND", before_count, after_count)
                
                # Log efficiency
                efficiency = (after_count / before_count * 100) if before_count > 0 else 0
                if context.debug:
                    warnings.warn(
                        f"Area domain filter retained {after_count:,} of {before_count:,} "
                        f"records ({efficiency:.1f}%)",
                        category=UserWarning
                    )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply area domain filter: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _validate_area_domain_expression(self, expression: str) -> None:
        """Validate area domain expression syntax."""
        # Check for valid condition columns
        valid_columns = {
            "COND_STATUS_CD", "OWNCD", "OWNGRPCD", "FORTYPCD", "FLDTYPCD",
            "RESERVED", "CONDPROP_UNADJ", "MICRPROP_UNADJ", "SUBPPROP_UNADJ",
            "SLOPE", "ASPECT", "STDAGE", "STDSZCD", "SITECLCD", "SICOND",
            "SIBASE", "SISP", "BALIVE"
        }
        
        # Similar validation as tree domain
        try:
            parsed = parse_domain_expression(expression)
        except Exception as e:
            raise ValueError(f"Invalid area domain expression: {e}")


class FilterByLandTypeStep(FilteringStep):
    """
    Filter data by land type classification.
    
    This step filters condition data based on land type classification
    (forest, timber, or all land), which is a common requirement in FIA
    estimation workflows.
    
    Examples
    --------
    >>> # Filter to forest land only
    >>> step = FilterByLandTypeStep(land_type="forest")
    >>> 
    >>> # Filter to timberland (productive forest)
    >>> step = FilterByLandTypeStep(
    ...     land_type="timber",
    ...     include_reserved=False
    ... )
    """
    
    def __init__(
        self,
        land_type: str = "forest",
        include_reserved: Optional[bool] = None,
        track_efficiency: bool = True,
        **kwargs
    ):
        """
        Initialize land type filtering step.
        
        Parameters
        ----------
        land_type : str
            Land type to filter ("forest", "timber", or "all")
        include_reserved : Optional[bool]
            Whether to include reserved lands (None = no filter)
        track_efficiency : bool
            Whether to track filter efficiency metrics
        **kwargs
            Additional arguments passed to base class
        """
        # Build area domain based on land type
        area_domain = self._build_land_type_domain(land_type, include_reserved)
        super().__init__(area_domain=area_domain, **kwargs)
        self.land_type = land_type
        self.include_reserved = include_reserved
        self.track_efficiency = track_efficiency
    
    def _build_land_type_domain(self, land_type: str, include_reserved: Optional[bool]) -> str:
        """Build area domain expression for land type."""
        land_type_lower = land_type.lower()
        
        if land_type_lower == "all":
            # No filtering for all land
            domain = None
        elif land_type_lower == "forest":
            # Forest land: COND_STATUS_CD = 1
            domain = "COND_STATUS_CD == 1"
        elif land_type_lower == "timber":
            # Timberland: Forest land capable of producing timber
            # Site class 1-5 (productive), 6-7 (unproductive)
            domain = "COND_STATUS_CD == 1 AND SITECLCD >= 1 AND SITECLCD <= 5"
            
            # Exclude reserved land if specified
            if include_reserved is False:
                domain += " AND RESERVED == 0"
        else:
            raise ValueError(
                f"Invalid land type: {land_type}. "
                "Must be 'forest', 'timber', or 'all'"
            )
        
        # Add reserved filter if specified and not already included
        if include_reserved is not None and land_type_lower != "timber":
            if domain:
                domain += f" AND RESERVED == {1 if include_reserved else 0}"
            else:
                domain = f"RESERVED == {1 if include_reserved else 0}"
        
        return domain
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> FilteredDataContract:
        """
        Execute land type filtering.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract with raw tables
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FilteredDataContract
            Contract with filtered condition data
        """
        try:
            # Get condition data
            if not input_data.has_table("COND"):
                raise PipelineException(
                    "COND table required for land type filtering",
                    step_id=self.step_id
                )
            
            condition_data = input_data.get_table("COND")
            
            # Track initial count
            before_count = None
            if self.track_efficiency:
                if isinstance(condition_data.frame, pl.LazyFrame):
                    before_count = condition_data.frame.select(pl.count()).collect().item()
                else:
                    before_count = len(condition_data.frame)
            
            # Apply land type filter
            if self.area_domain:
                filtered_condition = self.apply_area_filters(condition_data, context.config)
            else:
                # No filtering for "all" land type
                filtered_condition = condition_data
            
            # Track filtered count
            after_count = None
            if self.track_efficiency:
                if isinstance(filtered_condition.frame, pl.LazyFrame):
                    after_count = filtered_condition.frame.select(pl.count()).collect().item()
                else:
                    after_count = len(filtered_condition.frame)
            
            # Get other data if available
            tree_data = input_data.get_table("TREE") if input_data.has_table("TREE") else None
            plot_data = input_data.get_table("PLOT") if input_data.has_table("PLOT") else None
            
            # Create output contract
            output = FilteredDataContract(
                tree_data=tree_data,
                condition_data=filtered_condition,
                plot_data=plot_data,
                area_domain=self.area_domain,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("land_type", self.land_type)
            output.add_processing_metadata("include_reserved", self.include_reserved)
            
            if self.track_efficiency and before_count and after_count:
                output.add_filter_metadata("COND", before_count, after_count)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to filter by land type: {e}",
                step_id=self.step_id,
                cause=e
            )


class FilterByEvalidStep(FilteringStep):
    """
    Apply EVALID filtering for statistical validity.
    
    This step ensures that only data from statistically valid evaluation groups
    is included in the analysis. It can filter to most recent evaluations or
    specific EVALID values.
    
    Examples
    --------
    >>> # Filter to most recent volume evaluation
    >>> step = FilterByEvalidStep(
    ...     most_recent=True,
    ...     eval_type="VOL"
    ... )
    >>> 
    >>> # Filter to specific EVALIDs
    >>> step = FilterByEvalidStep(
    ...     evalid=[371501, 371502]
    ... )
    """
    
    def __init__(
        self,
        evalid: Optional[Union[int, List[int]]] = None,
        most_recent: bool = False,
        eval_type: str = "VOL",
        apply_to_tables: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize EVALID filtering step.
        
        Parameters
        ----------
        evalid : Optional[Union[int, List[int]]]
            Specific EVALID(s) to filter to
        most_recent : bool
            Whether to use most recent evaluation
        eval_type : str
            Evaluation type if using most_recent (VOL, GRM, CHNG)
        apply_to_tables : Optional[List[str]]
            Tables to apply EVALID filter to (default: all with EVALID column)
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(**kwargs)
        self.evalid = evalid
        self.most_recent = most_recent
        self.eval_type = eval_type
        self.apply_to_tables = apply_to_tables
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> FilteredDataContract:
        """
        Execute EVALID filtering.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract with raw tables
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FilteredDataContract
            Contract with EVALID-filtered data
        """
        try:
            # Determine EVALID to use
            if self.evalid:
                evalid_to_use = self.evalid
            elif self.most_recent:
                evalid_filter = EvalidFilter(context.db)
                evalid_to_use = evalid_filter.get_most_recent_evalid(self.eval_type)
                
                if context.debug:
                    warnings.warn(
                        f"Using most recent {self.eval_type} EVALID: {evalid_to_use}",
                        category=UserWarning
                    )
            else:
                # No EVALID filtering
                return self._create_output_without_filtering(input_data)
            
            # Tables that typically have EVALID column
            evalid_tables = {
                "POP_PLOT_STRATUM_ASSGN", "POP_STRATUM", "POP_ESTN_UNIT", 
                "POP_EVAL", "TREE", "COND", "PLOT"
            }
            
            # Determine which tables to filter
            tables_to_filter = self.apply_to_tables or list(evalid_tables)
            
            # Filter each table
            filtered_data = {}
            filter_metadata = {}
            
            for table_name in input_data.tables:
                if table_name not in tables_to_filter:
                    # Keep table as-is
                    filtered_data[table_name] = input_data.tables[table_name]
                    continue
                
                table_data = input_data.tables[table_name]
                
                # Check if table has EVALID column
                if isinstance(table_data.frame, pl.LazyFrame):
                    columns = table_data.frame.collect_schema().names()
                else:
                    columns = table_data.frame.columns
                
                if "EVALID" not in columns:
                    # Table doesn't have EVALID, keep as-is
                    filtered_data[table_name] = table_data
                    continue
                
                # Get before count
                if isinstance(table_data.frame, pl.LazyFrame):
                    before_count = table_data.frame.select(pl.count()).collect().item()
                else:
                    before_count = len(table_data.frame)
                
                # Apply EVALID filter
                if isinstance(evalid_to_use, list):
                    filter_expr = pl.col("EVALID").is_in(evalid_to_use)
                else:
                    filter_expr = pl.col("EVALID") == evalid_to_use
                
                filtered_frame = table_data.frame.filter(filter_expr)
                filtered_data[table_name] = LazyFrameWrapper(filtered_frame)
                
                # Get after count
                if isinstance(filtered_frame, pl.LazyFrame):
                    after_count = filtered_frame.select(pl.count()).collect().item()
                else:
                    after_count = len(filtered_frame)
                
                filter_metadata[table_name] = {
                    "before": before_count,
                    "after": after_count
                }
            
            # Create output contract
            # Extract specific tables for FilteredDataContract
            tree_data = filtered_data.get("TREE")
            condition_data = filtered_data.get("COND", LazyFrameWrapper(pl.LazyFrame()))
            plot_data = filtered_data.get("PLOT")
            
            output = FilteredDataContract(
                tree_data=tree_data,
                condition_data=condition_data,
                plot_data=plot_data,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("evalid_used", evalid_to_use)
            output.add_processing_metadata("eval_type", self.eval_type)
            output.add_processing_metadata("filter_metadata", filter_metadata)
            
            # Add filter efficiency metadata
            for table_name, counts in filter_metadata.items():
                output.add_filter_metadata(table_name, counts["before"], counts["after"])
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply EVALID filter: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _create_output_without_filtering(self, input_data: RawTablesContract) -> FilteredDataContract:
        """Create output contract without applying any filtering."""
        tree_data = input_data.get_table("TREE") if input_data.has_table("TREE") else None
        condition_data = input_data.get_table("COND") if input_data.has_table("COND") else LazyFrameWrapper(pl.LazyFrame())
        plot_data = input_data.get_table("PLOT") if input_data.has_table("PLOT") else None
        
        return FilteredDataContract(
            tree_data=tree_data,
            condition_data=condition_data,
            plot_data=plot_data,
            step_id=self.step_id
        )


class ApplyCombinedDomainsStep(FilteringStep):
    """
    Apply multiple domain filters in a single optimized step.
    
    This step combines tree domain, area domain, and plot domain filtering
    into a single operation for improved performance and consistency.
    
    Examples
    --------
    >>> # Apply all domain filters at once
    >>> step = ApplyCombinedDomainsStep(
    ...     tree_domain="STATUSCD == 1 AND DIA >= 5.0",
    ...     area_domain="COND_STATUS_CD == 1",
    ...     plot_domain="LAT > 35.0",
    ...     land_type="forest"
    ... )
    """
    
    def __init__(
        self,
        tree_domain: Optional[str] = None,
        area_domain: Optional[str] = None,
        plot_domain: Optional[str] = None,
        land_type: Optional[str] = None,
        track_efficiency: bool = True,
        **kwargs
    ):
        """
        Initialize combined domain filtering step.
        
        Parameters
        ----------
        tree_domain : Optional[str]
            Tree domain filter expression
        area_domain : Optional[str]
            Area domain filter expression
        plot_domain : Optional[str]
            Plot domain filter expression
        land_type : Optional[str]
            Land type filter ("forest", "timber", "all")
        track_efficiency : bool
            Whether to track filter efficiency for each domain
        **kwargs
            Additional arguments passed to base class
        """
        # Combine area domain with land type if both specified
        combined_area_domain = self._combine_area_domains(area_domain, land_type)
        
        super().__init__(
            tree_domain=tree_domain,
            area_domain=combined_area_domain,
            plot_domain=plot_domain,
            **kwargs
        )
        self.land_type = land_type
        self.track_efficiency = track_efficiency
        self._original_area_domain = area_domain
    
    def _combine_area_domains(self, area_domain: Optional[str], land_type: Optional[str]) -> Optional[str]:
        """Combine area domain with land type filter."""
        if not land_type or land_type.lower() == "all":
            return area_domain
        
        # Build land type domain
        if land_type.lower() == "forest":
            land_domain = "COND_STATUS_CD == 1"
        elif land_type.lower() == "timber":
            land_domain = "COND_STATUS_CD == 1 AND SITECLCD >= 1 AND SITECLCD <= 5 AND RESERVED == 0"
        else:
            return area_domain
        
        # Combine with existing area domain
        if area_domain:
            return f"({area_domain}) AND ({land_domain})"
        else:
            return land_domain
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> FilteredDataContract:
        """
        Execute combined domain filtering.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract with raw tables
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        FilteredDataContract
            Contract with all filtered data
        """
        try:
            filter_metadata = {}
            
            # Filter tree data if domain specified and table exists
            tree_data = None
            if self.tree_domain and input_data.has_table("TREE"):
                tree_data_raw = input_data.get_table("TREE")
                
                # Track before count
                if self.track_efficiency:
                    if isinstance(tree_data_raw.frame, pl.LazyFrame):
                        before_count = tree_data_raw.frame.select(pl.count()).collect().item()
                    else:
                        before_count = len(tree_data_raw.frame)
                else:
                    before_count = None
                
                # Apply filter
                tree_data = self.apply_tree_filters(tree_data_raw, context.config)
                
                # Track after count
                if self.track_efficiency and before_count is not None:
                    if isinstance(tree_data.frame, pl.LazyFrame):
                        after_count = tree_data.frame.select(pl.count()).collect().item()
                    else:
                        after_count = len(tree_data.frame)
                    
                    filter_metadata["TREE"] = {"before": before_count, "after": after_count}
            elif input_data.has_table("TREE"):
                tree_data = input_data.get_table("TREE")
            
            # Filter condition data if domain specified and table exists
            condition_data = None
            if self.area_domain and input_data.has_table("COND"):
                condition_data_raw = input_data.get_table("COND")
                
                # Track before count
                if self.track_efficiency:
                    if isinstance(condition_data_raw.frame, pl.LazyFrame):
                        before_count = condition_data_raw.frame.select(pl.count()).collect().item()
                    else:
                        before_count = len(condition_data_raw.frame)
                else:
                    before_count = None
                
                # Apply filter
                condition_data = self.apply_area_filters(condition_data_raw, context.config)
                
                # Track after count
                if self.track_efficiency and before_count is not None:
                    if isinstance(condition_data.frame, pl.LazyFrame):
                        after_count = condition_data.frame.select(pl.count()).collect().item()
                    else:
                        after_count = len(condition_data.frame)
                    
                    filter_metadata["COND"] = {"before": before_count, "after": after_count}
            elif input_data.has_table("COND"):
                condition_data = input_data.get_table("COND")
            else:
                # Condition data is required
                condition_data = LazyFrameWrapper(pl.LazyFrame())
            
            # Filter plot data if domain specified and table exists
            plot_data = None
            if self.plot_domain and input_data.has_table("PLOT"):
                plot_data_raw = input_data.get_table("PLOT")
                
                # Track before count
                if self.track_efficiency:
                    if isinstance(plot_data_raw.frame, pl.LazyFrame):
                        before_count = plot_data_raw.frame.select(pl.count()).collect().item()
                    else:
                        before_count = len(plot_data_raw.frame)
                else:
                    before_count = None
                
                # Apply filter
                plot_data = self.apply_plot_filters(plot_data_raw)
                
                # Track after count
                if self.track_efficiency and before_count is not None:
                    if isinstance(plot_data.frame, pl.LazyFrame):
                        after_count = plot_data.frame.select(pl.count()).collect().item()
                    else:
                        after_count = len(plot_data.frame)
                    
                    filter_metadata["PLOT"] = {"before": before_count, "after": after_count}
            elif input_data.has_table("PLOT"):
                plot_data = input_data.get_table("PLOT")
            
            # Create output contract
            output = FilteredDataContract(
                tree_data=tree_data,
                condition_data=condition_data,
                plot_data=plot_data,
                tree_domain=self.tree_domain,
                area_domain=self._original_area_domain,
                plot_domain=self.plot_domain,
                step_id=self.step_id
            )
            
            # Add all filter metadata
            for table_name, counts in filter_metadata.items():
                output.add_filter_metadata(table_name, counts["before"], counts["after"])
            
            # Add processing metadata
            output.add_processing_metadata("land_type", self.land_type)
            output.add_processing_metadata("combined_filtering", True)
            
            # Log overall efficiency if debug mode
            if context.debug and filter_metadata:
                for table_name, counts in filter_metadata.items():
                    efficiency = (counts["after"] / counts["before"] * 100) if counts["before"] > 0 else 0
                    warnings.warn(
                        f"{table_name} filter retained {counts['after']:,} of {counts['before']:,} "
                        f"records ({efficiency:.1f}%)",
                        category=UserWarning
                    )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to apply combined domain filters: {e}",
                step_id=self.step_id,
                cause=e
            )


# Export all filtering step classes
__all__ = [
    "ApplyTreeDomainStep",
    "ApplyAreaDomainStep",
    "FilterByLandTypeStep",
    "FilterByEvalidStep",
    "ApplyCombinedDomainsStep",
]