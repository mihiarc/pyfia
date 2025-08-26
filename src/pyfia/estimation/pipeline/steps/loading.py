"""
Data loading steps for the pyFIA estimation pipeline.

This module provides pipeline steps for loading FIA database tables with
various optimization strategies, EVALID filtering, and lazy evaluation support.
These steps form the foundation of any estimation pipeline by efficiently
loading the required data from the database.

Steps:
- LoadTablesStep: Load multiple FIA tables with optimization
- LoadPlotDataStep: Optimized plot data loading with EVALID filtering
- LoadTreeDataStep: Tree data with domain filtering support
- LoadConditionDataStep: Condition/area data with land type filtering
- LoadStratificationDataStep: Stratification tables loading
"""

from typing import Dict, List, Optional, Type, Union
import warnings

import polars as pl

from ....core import FIA
# from ....filters.evalid_filter import EvalidFilter  # Module doesn't exist yet
from ...lazy_evaluation import LazyFrameWrapper
from ...query_builders import QueryBuilderFactory, TreeQueryBuilder, PlotQueryBuilder
from ..core import ExecutionContext, PipelineException
from ..contracts import RawTablesContract
from ..base_steps import DataLoadingStep


class LoadTablesStep(DataLoadingStep):
    """
    Load multiple FIA tables with optimization and caching.
    
    This step loads the specified FIA tables from the database, applying
    EVALID filtering and query optimization as configured. It serves as the
    primary data ingestion step for most estimation pipelines.
    
    Examples
    --------
    >>> # Load standard estimation tables
    >>> step = LoadTablesStep(
    ...     tables=["TREE", "COND", "PLOT"],
    ...     apply_evalid_filter=True,
    ...     cache_results=True
    ... )
    >>> 
    >>> # Load with specific EVALID
    >>> step = LoadTablesStep(
    ...     tables=["TREE", "COND"],
    ...     evalid=[371501, 371502],
    ...     optimize_columns=True
    ... )
    """
    
    def __init__(
        self,
        tables: List[str],
        evalid: Optional[Union[int, List[int]]] = None,
        state_filter: Optional[Union[int, List[int]]] = None,
        optimize_columns: bool = True,
        cache_results: bool = True,
        parallel_loading: bool = False,
        **kwargs
    ):
        """
        Initialize the table loading step.
        
        Parameters
        ----------
        tables : List[str]
            Names of FIA tables to load (e.g., ["TREE", "COND", "PLOT"])
        evalid : Optional[Union[int, List[int]]]
            Specific EVALID(s) to filter by
        state_filter : Optional[Union[int, List[int]]]
            State code(s) to filter by
        optimize_columns : bool
            Whether to load only necessary columns for estimation
        cache_results : bool
            Whether to cache loaded tables for reuse
        parallel_loading : bool
            Whether to load tables in parallel (if supported)
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(tables=tables, **kwargs)
        self.evalid = evalid
        self.state_filter = state_filter
        self.optimize_columns = optimize_columns
        self.cache_results = cache_results
        self.parallel_loading = parallel_loading
        
        # Column optimization mapping for common FIA tables
        self.optimized_columns = {
            "TREE": [
                "CN", "PLT_CN", "PREV_TRE_CN", "INVYR", "STATECD", "UNITCD", 
                "COUNTYCD", "PLOT", "SUBP", "TREE", "CONDID", "STATUSCD", 
                "SPCD", "DIA", "HT", "ACTUALHT", "HTCD", "TREECLCD", "CR", 
                "CCLCD", "AGENTCD", "TOTAGE", "TPA_UNADJ", "CARBON_AG", 
                "CARBON_BG", "DRYBIO_AG", "DRYBIO_BG", "VOLCFNET", "VOLCFGRS",
                "VOLCSNET", "VOLCSGRS", "VOLBFNET", "VOLBFGRS"
            ],
            "COND": [
                "CN", "PLT_CN", "PREV_COND_CN", "INVYR", "STATECD", "UNITCD",
                "COUNTYCD", "PLOT", "CONDID", "COND_STATUS_CD", "OWNCD",
                "OWNGRPCD", "FORTYPCD", "FLDTYPCD", "RESERVED", "CONDPROP_UNADJ",
                "MICRPROP_UNADJ", "SUBPPROP_UNADJ", "SLOPE", "ASPECT", "STDAGE",
                "STDSZCD", "SITECLCD", "SICOND", "SIBASE", "SISP", "BALIVE"
            ],
            "PLOT": [
                "CN", "PREV_PLT_CN", "INVYR", "STATECD", "UNITCD", "COUNTYCD",
                "PLOT", "LAT", "LON", "ELEV", "PLOT_STATUS_CD", "KINDCD",
                "DESIGNCD", "RDDISTCD", "WATERCD", "ECOSUBCD", "CONGCD",
                "MANUAL", "MICROPLOT_LOC", "MACRO_BREAKPOINT_DIA"
            ],
            "POP_PLOT_STRATUM_ASSGN": [
                "CN", "PLT_CN", "EVALID", "STATECD", "ESTN_UNIT", 
                "STRATUMCD", "P1POINTCNT"
            ],
            "POP_STRATUM": [
                "CN", "EVALID", "ESTN_UNIT", "STRATUMCD", "P1POINTCNT",
                "P2POINTCNT", "ACRES", "ADJ_FACTOR_MICR", "ADJ_FACTOR_SUBP",
                "ADJ_FACTOR_MACR", "EXPNS"
            ],
            "POP_ESTN_UNIT": [
                "CN", "EVALID", "ESTN_UNIT", "AREA_USED", "P1PNTCNT_EU"
            ],
            "POP_EVAL": [
                "CN", "EVALID", "EVAL_GRP", "EVAL_TYP", "STATECD", "REPORT_YEAR_NM"
            ]
        }
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> RawTablesContract:
        """
        Execute table loading with optimization.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract (may contain previously loaded tables)
        context : ExecutionContext
            Execution context with database connection
            
        Returns
        -------
        RawTablesContract
            Contract containing loaded tables
        """
        try:
            # Start with existing tables if any
            loaded_tables = input_data.tables.copy() if input_data.tables else {}
            
            # Get EVALID to use
            evalid_to_use = self.evalid or input_data.evalid
            if evalid_to_use is None and context.config.most_recent:
                # Get most recent EVALID if needed
                evalid_filter = EvalidFilter(context.db)
                evalid_to_use = evalid_filter.get_most_recent_evalid("VOL")
            
            # Track loading metrics
            load_times = {}
            record_counts = {}
            
            # Load each requested table
            for table_name in self.tables:
                # Skip if already loaded and caching is enabled
                if self.cache_results and table_name in loaded_tables:
                    context.set_context_data(f"table_{table_name}_cached", True)
                    continue
                
                # Load table with optimization
                start_time = context.total_execution_time
                
                # Determine columns to load
                columns = None
                if self.optimize_columns and table_name in self.optimized_columns:
                    columns = self.optimized_columns[table_name]
                
                # Build query with optimization
                table_data = self._load_optimized_table(
                    context.db,
                    table_name,
                    evalid_to_use,
                    self.state_filter,
                    columns
                )
                
                # Track metrics
                load_times[table_name] = context.total_execution_time - start_time
                
                # Get record count for tracking
                if isinstance(table_data.frame, pl.LazyFrame):
                    record_counts[table_name] = table_data.frame.select(
                        pl.count().alias("n")
                    ).collect().item()
                else:
                    record_counts[table_name] = len(table_data.frame)
                
                loaded_tables[table_name] = table_data
            
            # Create output contract
            output = RawTablesContract(
                tables=loaded_tables,
                evalid=evalid_to_use,
                state_filter=self.state_filter,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("load_times", load_times)
            output.add_processing_metadata("record_counts", record_counts)
            output.add_processing_metadata("optimized_loading", self.optimize_columns)
            
            # Track performance
            self.track_performance(
                context,
                tables_loaded=len(self.tables),
                total_records=sum(record_counts.values()),
                load_time=sum(load_times.values())
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to load tables: {e}",
                step_id=self.step_id,
                cause=e
            )
    
    def _load_optimized_table(
        self,
        db: FIA,
        table_name: str,
        evalid: Optional[Union[int, List[int]]],
        state_filter: Optional[Union[int, List[int]]],
        columns: Optional[List[str]] = None
    ) -> LazyFrameWrapper:
        """Load a table with query optimization."""
        # Build WHERE clause components
        where_components = []
        
        if evalid:
            if isinstance(evalid, list):
                evalid_str = ",".join(map(str, evalid))
                where_components.append(f"EVALID IN ({evalid_str})")
            else:
                where_components.append(f"EVALID = {evalid}")
        
        if state_filter:
            if isinstance(state_filter, list):
                state_str = ",".join(map(str, state_filter))
                where_components.append(f"STATECD IN ({state_str})")
            else:
                where_components.append(f"STATECD = {state_filter}")
        
        # Combine WHERE clauses
        where_clause = " AND ".join(where_components) if where_components else None
        
        # Load with column selection if specified
        if columns:
            # Build SELECT clause
            select_clause = f"SELECT {', '.join(columns)} FROM {table_name}"
            if where_clause:
                select_clause += f" WHERE {where_clause}"
            
            # Execute query
            data = db.data_reader.query(select_clause)
        else:
            # Load entire table with WHERE clause
            data = db.data_reader.load_table(table_name, where_clause=where_clause)
        
        return LazyFrameWrapper(data)


class LoadPlotDataStep(DataLoadingStep):
    """
    Optimized plot data loading with EVALID filtering and spatial optimization.
    
    This step specializes in loading PLOT table data with optimizations for
    spatial queries, EVALID filtering, and plot status filtering.
    
    Examples
    --------
    >>> # Load plot data for specific evaluation
    >>> step = LoadPlotDataStep(
    ...     evalid=371501,
    ...     include_spatial=True,
    ...     filter_sampled_plots=True
    ... )
    >>> 
    >>> # Load with bounding box filter
    >>> step = LoadPlotDataStep(
    ...     lat_bounds=(35.0, 36.0),
    ...     lon_bounds=(-84.0, -83.0)
    ... )
    """
    
    def __init__(
        self,
        evalid: Optional[Union[int, List[int]]] = None,
        include_spatial: bool = True,
        filter_sampled_plots: bool = True,
        lat_bounds: Optional[tuple[float, float]] = None,
        lon_bounds: Optional[tuple[float, float]] = None,
        plot_status_filter: Optional[List[int]] = None,
        **kwargs
    ):
        """
        Initialize plot data loading step.
        
        Parameters
        ----------
        evalid : Optional[Union[int, List[int]]]
            EVALID(s) to filter by
        include_spatial : bool
            Whether to include LAT/LON columns
        filter_sampled_plots : bool
            Whether to filter to only sampled plots (PLOT_STATUS_CD = 1)
        lat_bounds : Optional[tuple[float, float]]
            Latitude bounds for spatial filtering (min, max)
        lon_bounds : Optional[tuple[float, float]]
            Longitude bounds for spatial filtering (min, max)
        plot_status_filter : Optional[List[int]]
            Specific plot status codes to include
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(tables=["PLOT"], **kwargs)
        self.evalid = evalid
        self.include_spatial = include_spatial
        self.filter_sampled_plots = filter_sampled_plots
        self.lat_bounds = lat_bounds
        self.lon_bounds = lon_bounds
        self.plot_status_filter = plot_status_filter or ([1] if filter_sampled_plots else None)
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> RawTablesContract:
        """
        Execute optimized plot data loading.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        RawTablesContract
            Contract with loaded plot data
        """
        try:
            # Use plot query builder for optimization
            plot_builder = PlotQueryBuilder()
            
            # Apply EVALID filter
            evalid_to_use = self.evalid or input_data.evalid
            if evalid_to_use:
                plot_builder.add_evalid_filter(evalid_to_use)
            
            # Apply plot status filter
            if self.plot_status_filter:
                status_str = ",".join(map(str, self.plot_status_filter))
                plot_builder.add_where_clause(f"PLOT_STATUS_CD IN ({status_str})")
            
            # Apply spatial bounds if specified
            if self.lat_bounds:
                plot_builder.add_where_clause(
                    f"LAT >= {self.lat_bounds[0]} AND LAT <= {self.lat_bounds[1]}"
                )
            if self.lon_bounds:
                plot_builder.add_where_clause(
                    f"LON >= {self.lon_bounds[0]} AND LON <= {self.lon_bounds[1]}"
                )
            
            # Select columns based on requirements
            if not self.include_spatial:
                plot_builder.columns = [
                    col for col in plot_builder.columns 
                    if col not in ["LAT", "LON"]
                ]
            
            # Execute query
            plot_data = plot_builder.build_and_execute(context.db)
            
            # Create output with plot data
            output_tables = input_data.tables.copy() if input_data.tables else {}
            output_tables["PLOT"] = LazyFrameWrapper(plot_data)
            
            # Create output contract
            output = RawTablesContract(
                tables=output_tables,
                evalid=evalid_to_use,
                state_filter=input_data.state_filter,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("spatial_filtering", bool(self.lat_bounds or self.lon_bounds))
            output.add_processing_metadata("plot_status_filter", self.plot_status_filter)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to load plot data: {e}",
                step_id=self.step_id,
                cause=e
            )


class LoadTreeDataStep(DataLoadingStep):
    """
    Load tree data with domain filtering support and optimization.
    
    This step specializes in loading TREE table data with tree domain
    filtering, species filtering, and diameter class filtering built in.
    
    Examples
    --------
    >>> # Load live trees only
    >>> step = LoadTreeDataStep(
    ...     tree_domain="STATUSCD == 1",
    ...     species_filter=[131, 110],  # Loblolly and Virginia pine
    ...     dia_limits=(5.0, None)  # Trees >= 5 inches DBH
    ... )
    """
    
    def __init__(
        self,
        evalid: Optional[Union[int, List[int]]] = None,
        tree_domain: Optional[str] = None,
        species_filter: Optional[List[int]] = None,
        dia_limits: Optional[tuple[Optional[float], Optional[float]]] = None,
        status_filter: Optional[List[int]] = None,
        optimize_for_volume: bool = False,
        optimize_for_biomass: bool = False,
        **kwargs
    ):
        """
        Initialize tree data loading step.
        
        Parameters
        ----------
        evalid : Optional[Union[int, List[int]]]
            EVALID(s) to filter by
        tree_domain : Optional[str]
            Tree domain filter expression
        species_filter : Optional[List[int]]
            Species codes to include
        dia_limits : Optional[tuple]
            Diameter limits (min, max) - None means no limit
        status_filter : Optional[List[int]]
            Status codes to include (e.g., [1] for live trees)
        optimize_for_volume : bool
            Include volume-specific columns
        optimize_for_biomass : bool
            Include biomass-specific columns
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(tables=["TREE"], **kwargs)
        self.evalid = evalid
        self.tree_domain = tree_domain
        self.species_filter = species_filter
        self.dia_limits = dia_limits
        self.status_filter = status_filter
        self.optimize_for_volume = optimize_for_volume
        self.optimize_for_biomass = optimize_for_biomass
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> RawTablesContract:
        """
        Execute optimized tree data loading.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        RawTablesContract
            Contract with loaded tree data
        """
        try:
            # Use tree query builder for optimization
            tree_builder = TreeQueryBuilder()
            
            # Apply EVALID filter
            evalid_to_use = self.evalid or input_data.evalid
            if evalid_to_use:
                tree_builder.add_evalid_filter(evalid_to_use)
            
            # Apply tree domain filter
            if self.tree_domain:
                tree_builder.add_where_clause(self.tree_domain)
            
            # Apply species filter
            if self.species_filter:
                species_str = ",".join(map(str, self.species_filter))
                tree_builder.add_where_clause(f"SPCD IN ({species_str})")
            
            # Apply diameter limits
            if self.dia_limits:
                if self.dia_limits[0] is not None:
                    tree_builder.add_where_clause(f"DIA >= {self.dia_limits[0]}")
                if self.dia_limits[1] is not None:
                    tree_builder.add_where_clause(f"DIA <= {self.dia_limits[1]}")
            
            # Apply status filter
            if self.status_filter:
                status_str = ",".join(map(str, self.status_filter))
                tree_builder.add_where_clause(f"STATUSCD IN ({status_str})")
            
            # Optimize columns based on calculation type
            if self.optimize_for_volume:
                tree_builder.add_columns([
                    "VOLCFNET", "VOLCFGRS", "VOLCSNET", "VOLCSGRS",
                    "VOLBFNET", "VOLBFGRS", "VOLBFNET_SND", "VOLBFGRS_SND"
                ])
            
            if self.optimize_for_biomass:
                tree_builder.add_columns([
                    "CARBON_AG", "CARBON_BG", "DRYBIO_AG", "DRYBIO_BG",
                    "DRYBIO_BOLE", "DRYBIO_STUMP", "DRYBIO_TOP", "DRYBIO_SAPLING"
                ])
            
            # Execute query
            tree_data = tree_builder.build_and_execute(context.db)
            
            # Create output with tree data
            output_tables = input_data.tables.copy() if input_data.tables else {}
            output_tables["TREE"] = LazyFrameWrapper(tree_data)
            
            # Create output contract
            output = RawTablesContract(
                tables=output_tables,
                evalid=evalid_to_use,
                state_filter=input_data.state_filter,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("tree_domain", self.tree_domain)
            output.add_processing_metadata("species_filter", self.species_filter)
            output.add_processing_metadata("dia_limits", self.dia_limits)
            
            # Track record count
            if isinstance(tree_data, pl.LazyFrame):
                record_count = tree_data.select(pl.count()).collect().item()
            else:
                record_count = len(tree_data)
            
            self.track_performance(
                context,
                tree_records_loaded=record_count,
                filters_applied=bool(self.tree_domain or self.species_filter or self.dia_limits)
            )
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to load tree data: {e}",
                step_id=self.step_id,
                cause=e
            )


class LoadConditionDataStep(DataLoadingStep):
    """
    Load condition/area data with land type filtering.
    
    This step specializes in loading COND table data with area domain
    filtering, land type classification, and ownership filtering.
    
    Examples
    --------
    >>> # Load forest land conditions
    >>> step = LoadConditionDataStep(
    ...     land_type="forest",
    ...     ownership_groups=[10, 20],  # Federal ownership
    ...     reserved_filter=False  # Exclude reserved land
    ... )
    """
    
    def __init__(
        self,
        evalid: Optional[Union[int, List[int]]] = None,
        area_domain: Optional[str] = None,
        land_type: Optional[str] = None,  # "forest", "timber", "all"
        ownership_filter: Optional[List[int]] = None,
        ownership_groups: Optional[List[int]] = None,
        reserved_filter: Optional[bool] = None,
        forest_type_filter: Optional[List[int]] = None,
        **kwargs
    ):
        """
        Initialize condition data loading step.
        
        Parameters
        ----------
        evalid : Optional[Union[int, List[int]]]
            EVALID(s) to filter by
        area_domain : Optional[str]
            Area domain filter expression
        land_type : Optional[str]
            Land type classification ("forest", "timber", "all")
        ownership_filter : Optional[List[int]]
            Owner codes to include
        ownership_groups : Optional[List[int]]
            Owner group codes to include
        reserved_filter : Optional[bool]
            Whether to filter reserved lands (None = no filter)
        forest_type_filter : Optional[List[int]]
            Forest type codes to include
        **kwargs
            Additional arguments passed to base class
        """
        super().__init__(tables=["COND"], **kwargs)
        self.evalid = evalid
        self.area_domain = area_domain
        self.land_type = land_type
        self.ownership_filter = ownership_filter
        self.ownership_groups = ownership_groups
        self.reserved_filter = reserved_filter
        self.forest_type_filter = forest_type_filter
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> RawTablesContract:
        """
        Execute condition data loading with filtering.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        RawTablesContract
            Contract with loaded condition data
        """
        try:
            # Build WHERE clause components
            where_components = []
            
            # Apply EVALID filter
            evalid_to_use = self.evalid or input_data.evalid
            if evalid_to_use:
                if isinstance(evalid_to_use, list):
                    evalid_str = ",".join(map(str, evalid_to_use))
                    where_components.append(f"EVALID IN ({evalid_str})")
                else:
                    where_components.append(f"EVALID = {evalid_to_use}")
            
            # Apply land type filter
            if self.land_type:
                if self.land_type.lower() == "forest":
                    where_components.append("COND_STATUS_CD = 1")
                elif self.land_type.lower() == "timber":
                    where_components.append("COND_STATUS_CD = 1 AND SITECLCD IN (1, 2, 3, 4, 5)")
            
            # Apply area domain
            if self.area_domain:
                where_components.append(f"({self.area_domain})")
            
            # Apply ownership filters
            if self.ownership_filter:
                owner_str = ",".join(map(str, self.ownership_filter))
                where_components.append(f"OWNCD IN ({owner_str})")
            
            if self.ownership_groups:
                group_str = ",".join(map(str, self.ownership_groups))
                where_components.append(f"OWNGRPCD IN ({group_str})")
            
            # Apply reserved filter
            if self.reserved_filter is not None:
                where_components.append(f"RESERVED = {1 if self.reserved_filter else 0}")
            
            # Apply forest type filter
            if self.forest_type_filter:
                type_str = ",".join(map(str, self.forest_type_filter))
                where_components.append(f"FORTYPCD IN ({type_str})")
            
            # Combine WHERE clauses
            where_clause = " AND ".join(where_components) if where_components else None
            
            # Load condition data
            cond_data = context.db.data_reader.load_table("COND", where_clause=where_clause)
            
            # Create output with condition data
            output_tables = input_data.tables.copy() if input_data.tables else {}
            output_tables["COND"] = LazyFrameWrapper(cond_data)
            
            # Create output contract
            output = RawTablesContract(
                tables=output_tables,
                evalid=evalid_to_use,
                state_filter=input_data.state_filter,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("area_domain", self.area_domain)
            output.add_processing_metadata("land_type", self.land_type)
            output.add_processing_metadata("ownership_filter", self.ownership_filter)
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to load condition data: {e}",
                step_id=self.step_id,
                cause=e
            )


class LoadStratificationDataStep(DataLoadingStep):
    """
    Load FIA stratification tables for variance calculation.
    
    This step loads the stratification tables (POP_STRATUM, POP_PLOT_STRATUM_ASSGN,
    POP_ESTN_UNIT, POP_EVAL) required for proper variance calculation and 
    expansion factor application.
    
    Examples
    --------
    >>> # Load standard stratification tables
    >>> step = LoadStratificationDataStep(
    ...     evalid=371501,
    ...     estimation_units=[1, 2, 3]
    ... )
    >>> 
    >>> # Load with evaluation type filter
    >>> step = LoadStratificationDataStep(
    ...     eval_type="VOL",
    ...     include_pop_eval=True
    ... )
    """
    
    def __init__(
        self,
        evalid: Optional[Union[int, List[int]]] = None,
        estimation_units: Optional[List[int]] = None,
        eval_type: Optional[str] = None,
        include_pop_eval: bool = True,
        include_adjustment_factors: bool = True,
        **kwargs
    ):
        """
        Initialize stratification data loading step.
        
        Parameters
        ----------
        evalid : Optional[Union[int, List[int]]]
            EVALID(s) to filter by
        estimation_units : Optional[List[int]]
            Estimation units to include
        eval_type : Optional[str]
            Evaluation type filter (VOL, GRM, CHNG, etc.)
        include_pop_eval : bool
            Whether to load POP_EVAL table
        include_adjustment_factors : bool
            Whether to include adjustment factor columns
        **kwargs
            Additional arguments passed to base class
        """
        # Determine tables to load
        tables = ["POP_PLOT_STRATUM_ASSGN", "POP_STRATUM", "POP_ESTN_UNIT"]
        if include_pop_eval:
            tables.append("POP_EVAL")
        
        super().__init__(tables=tables, **kwargs)
        self.evalid = evalid
        self.estimation_units = estimation_units
        self.eval_type = eval_type
        self.include_pop_eval = include_pop_eval
        self.include_adjustment_factors = include_adjustment_factors
    
    def execute_step(
        self, 
        input_data: RawTablesContract, 
        context: ExecutionContext
    ) -> RawTablesContract:
        """
        Execute stratification data loading.
        
        Parameters
        ----------
        input_data : RawTablesContract
            Input contract
        context : ExecutionContext
            Execution context
            
        Returns
        -------
        RawTablesContract
            Contract with loaded stratification data
        """
        try:
            output_tables = input_data.tables.copy() if input_data.tables else {}
            
            # Get EVALID to use
            evalid_to_use = self.evalid or input_data.evalid
            if evalid_to_use is None and context.config.most_recent:
                # Get most recent EVALID for the eval type
                evalid_filter = EvalidFilter(context.db)
                eval_type = self.eval_type or "VOL"
                evalid_to_use = evalid_filter.get_most_recent_evalid(eval_type)
            
            # Load POP_PLOT_STRATUM_ASSGN
            ppsa_where = []
            if evalid_to_use:
                if isinstance(evalid_to_use, list):
                    evalid_str = ",".join(map(str, evalid_to_use))
                    ppsa_where.append(f"EVALID IN ({evalid_str})")
                else:
                    ppsa_where.append(f"EVALID = {evalid_to_use}")
            
            if self.estimation_units:
                unit_str = ",".join(map(str, self.estimation_units))
                ppsa_where.append(f"ESTN_UNIT IN ({unit_str})")
            
            ppsa_clause = " AND ".join(ppsa_where) if ppsa_where else None
            ppsa_data = context.db.data_reader.load_table(
                "POP_PLOT_STRATUM_ASSGN", 
                where_clause=ppsa_clause
            )
            output_tables["POP_PLOT_STRATUM_ASSGN"] = LazyFrameWrapper(ppsa_data)
            
            # Load POP_STRATUM
            ps_where = []
            if evalid_to_use:
                if isinstance(evalid_to_use, list):
                    evalid_str = ",".join(map(str, evalid_to_use))
                    ps_where.append(f"EVALID IN ({evalid_str})")
                else:
                    ps_where.append(f"EVALID = {evalid_to_use}")
            
            if self.estimation_units:
                unit_str = ",".join(map(str, self.estimation_units))
                ps_where.append(f"ESTN_UNIT IN ({unit_str})")
            
            ps_clause = " AND ".join(ps_where) if ps_where else None
            
            # Select columns based on requirements
            if self.include_adjustment_factors:
                ps_columns = None  # Load all columns
            else:
                ps_columns = ["CN", "EVALID", "ESTN_UNIT", "STRATUMCD", 
                             "P1POINTCNT", "P2POINTCNT", "ACRES", "EXPNS"]
            
            if ps_columns:
                select_clause = f"SELECT {', '.join(ps_columns)} FROM POP_STRATUM"
                if ps_clause:
                    select_clause += f" WHERE {ps_clause}"
                ps_data = context.db.data_reader.query(select_clause)
            else:
                ps_data = context.db.data_reader.load_table(
                    "POP_STRATUM",
                    where_clause=ps_clause
                )
            
            output_tables["POP_STRATUM"] = LazyFrameWrapper(ps_data)
            
            # Load POP_ESTN_UNIT
            peu_where = []
            if evalid_to_use:
                if isinstance(evalid_to_use, list):
                    evalid_str = ",".join(map(str, evalid_to_use))
                    peu_where.append(f"EVALID IN ({evalid_str})")
                else:
                    peu_where.append(f"EVALID = {evalid_to_use}")
            
            if self.estimation_units:
                unit_str = ",".join(map(str, self.estimation_units))
                peu_where.append(f"ESTN_UNIT IN ({unit_str})")
            
            peu_clause = " AND ".join(peu_where) if peu_where else None
            peu_data = context.db.data_reader.load_table(
                "POP_ESTN_UNIT",
                where_clause=peu_clause
            )
            output_tables["POP_ESTN_UNIT"] = LazyFrameWrapper(peu_data)
            
            # Load POP_EVAL if requested
            if self.include_pop_eval:
                pe_where = []
                if evalid_to_use:
                    if isinstance(evalid_to_use, list):
                        evalid_str = ",".join(map(str, evalid_to_use))
                        pe_where.append(f"EVALID IN ({evalid_str})")
                    else:
                        pe_where.append(f"EVALID = {evalid_to_use}")
                
                if self.eval_type:
                    pe_where.append(f"EVAL_TYP = '{self.eval_type}'")
                
                pe_clause = " AND ".join(pe_where) if pe_where else None
                pe_data = context.db.data_reader.load_table(
                    "POP_EVAL",
                    where_clause=pe_clause
                )
                output_tables["POP_EVAL"] = LazyFrameWrapper(pe_data)
            
            # Create output contract
            output = RawTablesContract(
                tables=output_tables,
                evalid=evalid_to_use,
                state_filter=input_data.state_filter,
                step_id=self.step_id
            )
            
            # Add metadata
            output.add_processing_metadata("estimation_units", self.estimation_units)
            output.add_processing_metadata("eval_type", self.eval_type)
            output.add_processing_metadata("stratification_tables_loaded", len(output_tables))
            
            return output
            
        except Exception as e:
            self.handle_step_error(e, context)
            raise PipelineException(
                f"Failed to load stratification data: {e}",
                step_id=self.step_id,
                cause=e
            )


# Export all loading step classes
__all__ = [
    "LoadTablesStep",
    "LoadPlotDataStep",
    "LoadTreeDataStep",
    "LoadConditionDataStep",
    "LoadStratificationDataStep",
]