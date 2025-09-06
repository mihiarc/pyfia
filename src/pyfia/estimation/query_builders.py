"""
Query builders for optimized FIA data access.

This module provides specialized query builders that optimize query generation
and execution for common FIA patterns. It implements filter push-down,
column selection optimization, query plan caching, and integrates with the
lazy evaluation system from Phase 2.

Key features:
- Filter push-down to database level (WHERE clauses)
- Optimized column selection to minimize data transfer
- Query plan caching with LRU eviction
- Join strategy optimization based on data characteristics
- Integration with LazyFrameWrapper for deferred execution
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import hashlib
import json
import warnings

import polars as pl
from pydantic import BaseModel, Field, ConfigDict, field_validator

from ..core import FIA
from .config import EstimatorConfig, LazyEvaluationConfig
from .lazy_evaluation import LazyFrameWrapper, CollectionStrategy
from .caching import CacheKey, MemoryCache


# === Query Optimization Enums ===

class JoinStrategy(Enum):
    """Join strategies for query optimization."""
    HASH = auto()        # Hash join for large datasets
    SORT_MERGE = auto()  # Sort-merge for pre-sorted data
    NESTED_LOOP = auto() # Nested loop for small datasets
    BROADCAST = auto()   # Broadcast join for small lookup tables
    AUTO = auto()        # Let Polars decide


class FilterPushDownLevel(Enum):
    """Level of filter push-down optimization."""
    NONE = auto()        # No push-down
    PARTIAL = auto()     # Push down simple filters
    AGGRESSIVE = auto()  # Push down all possible filters
    AUTO = auto()        # Automatic based on filter complexity


# === Query Plan Components ===

@dataclass
class QueryColumn:
    """Represents a column in a query with metadata."""
    
    name: str
    table: str
    dtype: Optional[str] = None
    is_required: bool = True
    is_grouping: bool = False
    is_filter: bool = False
    alias: Optional[str] = None
    
    @property
    def full_name(self) -> str:
        """Get fully qualified column name."""
        return f"{self.table}.{self.name}"
    
    @property
    def output_name(self) -> str:
        """Get output column name (alias or original)."""
        return self.alias or self.name


@dataclass
class QueryFilter:
    """Represents a filter condition in a query."""
    
    column: str
    operator: str
    value: Any
    table: Optional[str] = None
    can_push_down: bool = True
    
    def to_sql(self) -> str:
        """Convert to SQL WHERE clause fragment."""
        col_name = f"{self.table}.{self.column}" if self.table else self.column
        
        if self.operator == "IN":
            if isinstance(self.value, (list, tuple)):
                values = ", ".join(str(v) for v in self.value)
                return f"{col_name} IN ({values})"
            return f"{col_name} IN ({self.value})"
        elif self.operator == "BETWEEN":
            return f"{col_name} BETWEEN {self.value[0]} AND {self.value[1]}"
        elif self.operator in ["IS NULL", "IS NOT NULL"]:
            return f"{col_name} {self.operator}"
        else:
            return f"{col_name} {self.operator} {self.value}"
    
    def to_polars_expr(self) -> pl.Expr:
        """Convert to Polars expression."""
        col = pl.col(self.column)
        
        if self.operator == "==":
            return col == self.value
        elif self.operator == "!=":
            return col != self.value
        elif self.operator == ">":
            return col > self.value
        elif self.operator == ">=":
            return col >= self.value
        elif self.operator == "<":
            return col < self.value
        elif self.operator == "<=":
            return col <= self.value
        elif self.operator == "IN":
            return col.is_in(self.value)
        elif self.operator == "BETWEEN":
            return (col >= self.value[0]) & (col <= self.value[1])
        elif self.operator == "IS NULL":
            return col.is_null()
        elif self.operator == "IS NOT NULL":
            return col.is_not_null()
        else:
            raise ValueError(f"Unsupported operator: {self.operator}")


@dataclass
class QueryJoin:
    """Represents a join operation in a query."""
    
    left_table: str
    right_table: str
    left_on: Union[str, List[str]]
    right_on: Union[str, List[str]]
    how: str = "inner"
    strategy: JoinStrategy = JoinStrategy.AUTO
    
    def get_join_keys(self) -> Tuple[List[str], List[str]]:
        """Get normalized join keys."""
        left_keys = [self.left_on] if isinstance(self.left_on, str) else self.left_on
        right_keys = [self.right_on] if isinstance(self.right_on, str) else self.right_on
        return left_keys, right_keys


@dataclass
class QueryPlan:
    """
    Represents a complete query execution plan.
    
    This class encapsulates all components of a query including tables,
    columns, filters, joins, and optimization hints.
    """
    
    tables: List[str]
    columns: List[QueryColumn]
    filters: List[QueryFilter] = field(default_factory=list)
    joins: List[QueryJoin] = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    order_by: List[Tuple[str, str]] = field(default_factory=list)  # (column, direction)
    limit: Optional[int] = None
    
    # Optimization hints
    estimated_rows: Optional[int] = None
    filter_selectivity: Optional[float] = None
    preferred_strategy: Optional[JoinStrategy] = None
    cache_key: Optional[str] = None
    
    def __post_init__(self):
        """Generate cache key for the query plan."""
        if not self.cache_key:
            plan_dict = {
                "tables": self.tables,
                "columns": [(c.table, c.name) for c in self.columns],
                "filters": [(f.column, f.operator, str(f.value)) for f in self.filters],
                "joins": [(j.left_table, j.right_table, j.left_on, j.right_on) for j in self.joins],
                "group_by": self.group_by,
                "order_by": self.order_by,
                "limit": self.limit
            }
            plan_str = json.dumps(plan_dict, sort_keys=True)
            self.cache_key = hashlib.md5(plan_str.encode()).hexdigest()[:16]
    
    def get_required_columns(self, table: str) -> Set[str]:
        """Get required columns for a specific table."""
        required = set()
        
        # Add selected columns
        for col in self.columns:
            if col.table == table and col.is_required:
                required.add(col.name)
        
        # Add filter columns
        for filt in self.filters:
            if filt.table == table:
                required.add(filt.column)
        
        # Add join columns
        for join in self.joins:
            if join.left_table == table:
                keys = [join.left_on] if isinstance(join.left_on, str) else join.left_on
                required.update(keys)
            if join.right_table == table:
                keys = [join.right_on] if isinstance(join.right_on, str) else join.right_on
                required.update(keys)
        
        # Add grouping columns
        for col in self.group_by:
            if "." in col:
                tbl, col_name = col.split(".", 1)
                if tbl == table:
                    required.add(col_name)
            else:
                # Assume column could be from this table
                required.add(col)
        
        return required
    
    def get_pushdown_filters(self, table: str) -> List[QueryFilter]:
        """Get filters that can be pushed down to a specific table."""
        return [
            f for f in self.filters
            if f.table == table and f.can_push_down
        ]


# === Base Query Builder ===

class BaseQueryBuilder(ABC):
    """
    Abstract base class for query builders.
    
    Provides common functionality for all specialized query builders including
    caching, optimization hints, and integration with lazy evaluation.
    """
    
    def __init__(self, 
                 db: FIA,
                 config: EstimatorConfig,
                 cache: Optional[MemoryCache] = None):
        """
        Initialize base query builder.
        
        Parameters
        ----------
        db : FIA
            FIA database instance
        config : EstimatorConfig
            Estimator configuration
        cache : Optional[MemoryCache]
            Cache for query plans and results
        """
        self.db = db
        self.config = config
        self.cache = cache or MemoryCache(max_size_mb=256, max_entries=100)
        
        # Query optimization settings
        self.enable_pushdown = getattr(
            config.lazy_config, 'enable_predicate_pushdown', True
        ) if config.lazy_config else True
        
        self.enable_projection = getattr(
            config.lazy_config, 'enable_projection_pushdown', True
        ) if config.lazy_config else True
        
        # Statistics for optimization decisions
        self._table_stats: Dict[str, Dict[str, Any]] = {}
        self._filter_stats: Dict[str, float] = {}  # Selectivity estimates
    
    @abstractmethod
    def build_query_plan(self, **kwargs) -> QueryPlan:
        """
        Build optimized query plan for the specific use case.
        
        Returns
        -------
        QueryPlan
            Optimized query execution plan
        """
        pass
    
    @abstractmethod
    def execute(self, plan: QueryPlan) -> LazyFrameWrapper:
        """
        Execute query plan and return results.
        
        Parameters
        ----------
        plan : QueryPlan
            Query plan to execute
            
        Returns
        -------
        LazyFrameWrapper
            Query results wrapped for lazy evaluation
        """
        pass
    
    # === Common Helper Methods ===
    
    def _parse_domain_filter(self, domain: str) -> List[QueryFilter]:
        """
        Parse domain filter expression into QueryFilter objects.
        
        Parameters
        ----------
        domain : str
            SQL-like filter expression (e.g., "DIA > 10 AND STATUSCD == 1")
            
        Returns
        -------
        List[QueryFilter]
            Parsed filter conditions
        """
        filters = []
        
        # Handle BETWEEN specially since it contains AND
        # First, temporarily replace BETWEEN...AND with a placeholder
        import re
        between_pattern = r'(\w+)\s+BETWEEN\s+([^\s]+)\s+AND\s+([^\s]+)'
        between_matches = list(re.finditer(between_pattern, domain, re.IGNORECASE))
        
        # Replace BETWEEN clauses with placeholders
        temp_domain = domain
        placeholders = []
        for i, match in enumerate(between_matches):
            placeholder = f"__BETWEEN_{i}__"
            placeholders.append((placeholder, match.groups()))
            temp_domain = temp_domain.replace(match.group(), placeholder)
        
        # Now split by AND
        conditions = re.split(r'\s+AND\s+', temp_domain, flags=re.IGNORECASE)
        
        for condition in conditions:
            condition = condition.strip()
            
            # Check if this is a BETWEEN placeholder
            if condition.startswith("__BETWEEN_"):
                idx = int(condition.replace("__BETWEEN_", "").replace("__", ""))
                col, low, high = placeholders[idx][1]
                
                # Try to convert to numbers
                try:
                    low = float(low)
                    if low.is_integer():
                        low = int(low)
                except ValueError:
                    pass
                
                try:
                    high = float(high)
                    if high.is_integer():
                        high = int(high)
                except ValueError:
                    pass
                
                filters.append(QueryFilter(col.upper(), "BETWEEN", [low, high]))
                
            # Parse different operator patterns
            elif " IN " in condition.upper():
                col, values = re.split(r'\s+IN\s+', condition, flags=re.IGNORECASE)
                col = col.strip().upper()
                values = values.strip("()").split(",")
                values = [v.strip().strip("'\"") for v in values]
                filters.append(QueryFilter(col, "IN", values))
                
            elif re.search(r'\s+IS\s+NULL', condition, re.IGNORECASE):
                col = re.sub(r'\s+IS\s+NULL', '', condition, flags=re.IGNORECASE).strip()
                filters.append(QueryFilter(col.upper(), "IS NULL", None))
                
            elif re.search(r'\s+IS\s+NOT\s+NULL', condition, re.IGNORECASE):
                col = re.sub(r'\s+IS\s+NOT\s+NULL', '', condition, flags=re.IGNORECASE).strip()
                filters.append(QueryFilter(col.upper(), "IS NOT NULL", None))
                
            else:
                # Handle comparison operators
                for op in [">=", "<=", "!=", "==", ">", "<", "="]:
                    if op in condition:
                        col, value = condition.split(op, 1)
                        col = col.strip().upper()
                        value = value.strip().strip("'\"")
                        
                        # Convert = to ==
                        if op == "=":
                            op = "=="
                            
                        # Try to convert value to appropriate type
                        try:
                            value = float(value)
                            if value.is_integer():
                                value = int(value)
                        except ValueError:
                            pass  # Keep as string
                            
                        filters.append(QueryFilter(col, op, value))
                        break
        
        return filters
    
    def _estimate_filter_selectivity(self, filters: List[QueryFilter]) -> float:
        """
        Estimate selectivity of filters for optimization.
        
        Parameters
        ----------
        filters : List[QueryFilter]
            List of filters to evaluate
            
        Returns
        -------
        float
            Estimated selectivity (0.0 to 1.0)
        """
        if not filters:
            return 1.0
        
        # Simple heuristic-based selectivity estimation
        selectivity = 1.0
        
        for filt in filters:
            if filt.operator == "==":
                # Equality is usually highly selective
                selectivity *= 0.1
            elif filt.operator in [">", "<", ">=", "<="]:
                # Range filters are moderately selective
                selectivity *= 0.3
            elif filt.operator == "IN":
                # IN clause selectivity depends on number of values
                n_values = len(filt.value) if isinstance(filt.value, (list, tuple)) else 1
                selectivity *= min(0.1 * n_values, 0.5)
            elif filt.operator == "BETWEEN":
                # BETWEEN is moderately selective
                selectivity *= 0.2
            elif filt.operator == "IS NULL":
                # NULL checks are usually selective in FIA data
                selectivity *= 0.05
            elif filt.operator == "IS NOT NULL":
                # NOT NULL is usually not very selective
                selectivity *= 0.95
        
        return max(selectivity, 0.001)  # Avoid zero selectivity
    
    def _optimize_join_strategy(self, 
                               left_size: int,
                               right_size: int,
                               join_type: str = "inner") -> JoinStrategy:
        """
        Determine optimal join strategy based on table sizes.
        
        Parameters
        ----------
        left_size : int
            Estimated rows in left table
        right_size : int
            Estimated rows in right table
        join_type : str
            Type of join (inner, left, etc.)
            
        Returns
        -------
        JoinStrategy
            Recommended join strategy
        """
        ratio = right_size / left_size if left_size > 0 else 1.0
        
        # Heuristics for join strategy selection
        if right_size < 10000 and ratio < 0.1:
            # Small right table - use broadcast join
            return JoinStrategy.BROADCAST
        elif left_size < 100000 and right_size < 100000:
            # Both tables small - use hash join
            return JoinStrategy.HASH
        elif ratio > 100 or ratio < 0.1:  # Changed from 0.01 to 0.1
            # Very skewed sizes - use broadcast for very small or hash for moderate
            if right_size < 10000:
                return JoinStrategy.BROADCAST
            else:
                return JoinStrategy.HASH
        else:
            # Large, similar-sized tables - use sort-merge
            return JoinStrategy.SORT_MERGE
    
    @lru_cache(maxsize=128)
    def _get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get cached statistics for a table.
        
        Parameters
        ----------
        table_name : str
            Name of the table
            
        Returns
        -------
        Dict[str, Any]
            Table statistics including row count, columns, etc.
        """
        if table_name not in self._table_stats:
            # Load table lazily to get schema
            if table_name not in self.db.tables:
                self.db.load_table(table_name)
            
            table = self.db.tables[table_name]
            
            # Get basic stats
            stats = {
                "columns": list(table.columns) if hasattr(table, 'columns') else [],
                "schema": dict(table.schema) if hasattr(table, 'schema') else {},
                "estimated_rows": None  # Would need to query for actual count
            }
            
            self._table_stats[table_name] = stats
        
        return self._table_stats[table_name]


# === Specialized Query Builders ===

class StratificationQueryBuilder(BaseQueryBuilder):
    """
    Query builder optimized for stratification queries.
    
    Handles efficient querying of POP_STRATUM, POP_PLOT_STRATUM_ASSGN,
    and related stratification tables with appropriate caching and optimization.
    """
    
    def build_query_plan(self,
                        evalid: Optional[List[int]] = None,
                        state_cd: Optional[List[int]] = None,
                        include_adjustment_factors: bool = True,
                        **kwargs) -> QueryPlan:
        """
        Build optimized query plan for stratification data.
        
        Parameters
        ----------
        evalid : Optional[List[int]]
            EVALID filter
        state_cd : Optional[List[int]]
            State code filter
        include_adjustment_factors : bool
            Whether to include adjustment factors
            
        Returns
        -------
        QueryPlan
            Optimized stratification query plan
        """
        # Define required columns
        columns = [
            QueryColumn("CN", "POP_STRATUM", is_required=True),
            QueryColumn("EVALID", "POP_STRATUM", is_required=True),
            QueryColumn("ESTN_UNIT", "POP_STRATUM", is_required=True),
            QueryColumn("STRATUMCD", "POP_STRATUM", is_required=True),
            QueryColumn("P1POINTCNT", "POP_STRATUM", is_required=True),
            QueryColumn("P2POINTCNT", "POP_STRATUM", is_required=True),
            QueryColumn("EXPNS", "POP_STRATUM", is_required=True),  # Expansion factor from sample query
        ]
        
        if include_adjustment_factors:
            columns.extend([
                QueryColumn("ADJ_FACTOR_MACR", "POP_STRATUM"),
                QueryColumn("ADJ_FACTOR_SUBP", "POP_STRATUM"),
                QueryColumn("ADJ_FACTOR_MICR", "POP_STRATUM"),
            ])
        
        # Build filters
        filters = []
        
        if evalid:
            filters.append(QueryFilter("EVALID", "IN", evalid, "POP_STRATUM"))
        
        if state_cd:
            filters.append(QueryFilter("STATECD", "IN", state_cd, "POP_STRATUM"))
        
        # Create query plan
        plan = QueryPlan(
            tables=["POP_STRATUM"],
            columns=columns,
            filters=filters,
            filter_selectivity=self._estimate_filter_selectivity(filters)
        )
        
        return plan
    
    def execute(self, plan: QueryPlan) -> LazyFrameWrapper:
        """Execute stratification query plan."""
        # Check cache
        cache_key = CacheKey("stratification", {"plan": plan.cache_key})
        cached_result = self.cache.get(cache_key.key)
        if cached_result is not None:
            return LazyFrameWrapper(cached_result)
        
        # Load base table with column selection
        required_cols = list(plan.get_required_columns("POP_STRATUM"))
        
        # Build WHERE clause for push-down
        where_clauses = []
        for filt in plan.get_pushdown_filters("POP_STRATUM"):
            where_clauses.append(filt.to_sql())
        
        where_clause = " AND ".join(where_clauses) if where_clauses else None
        
        # Load with optimized column selection and filter push-down
        df = self.db._reader.read_table(
            "POP_STRATUM",
            columns=required_cols,
            where=where_clause,
            lazy=True
        )
        
        # Apply any remaining filters that couldn't be pushed down
        for filt in plan.filters:
            if not filt.can_push_down:
                df = df.filter(filt.to_polars_expr())
        
        # Cache and return
        result = LazyFrameWrapper(df)
        self.cache.put(cache_key.key, df, ttl_seconds=300)
        
        return result


class TreeQueryBuilder(BaseQueryBuilder):
    """
    Query builder optimized for tree data queries.
    
    Implements efficient filtering, joins with plot data, and
    optimization for common tree-level queries.
    """
    
    def build_query_plan(self,
                        tree_domain: Optional[str] = None,
                        status_cd: Optional[List[int]] = None,
                        species: Optional[List[int]] = None,
                        dia_range: Optional[Tuple[float, float]] = None,
                        include_seedlings: bool = False,
                        columns: Optional[List[str]] = None,
                        **kwargs) -> QueryPlan:
        """
        Build optimized query plan for tree data.
        
        Parameters
        ----------
        tree_domain : Optional[str]
            Tree domain filter expression
        status_cd : Optional[List[int]]
            Status codes to include
        species : Optional[List[int]]
            Species codes to include
        dia_range : Optional[Tuple[float, float]]
            Diameter range (min, max)
        include_seedlings : bool
            Whether to include seedlings
        columns : Optional[List[str]]
            Specific columns to select
            
        Returns
        -------
        QueryPlan
            Optimized tree query plan
        """
        # Define base columns
        base_columns = [
            QueryColumn("CN", "TREE", is_required=True),
            QueryColumn("PLT_CN", "TREE", is_required=True),
            QueryColumn("PLOT", "TREE", is_required=True),
            QueryColumn("SUBP", "TREE", is_required=True),
            QueryColumn("TREE", "TREE", is_required=True),
            QueryColumn("CONDID", "TREE", is_required=True),
            QueryColumn("STATUSCD", "TREE", is_required=True),
            QueryColumn("SPCD", "TREE", is_required=True),
            QueryColumn("DIA", "TREE", is_required=True),
            QueryColumn("HT", "TREE"),
            QueryColumn("ACTUALHT", "TREE"),
            QueryColumn("TREECLCD", "TREE"),
            QueryColumn("CR", "TREE"),
            QueryColumn("CCLCD", "TREE"),
            QueryColumn("TPA_UNADJ", "TREE", is_required=True),  # Required for TPA calculations
        ]
        
        # Add requested columns
        if columns:
            for col in columns:
                if not any(c.name == col for c in base_columns):
                    base_columns.append(QueryColumn(col, "TREE"))
        
        # Parse and build filters
        filters = []
        
        # Parse tree domain
        if tree_domain:
            domain_filters = self._parse_domain_filter(tree_domain)
            for f in domain_filters:
                f.table = "TREE"
            filters.extend(domain_filters)
        
        # Add specific filters
        if status_cd:
            filters.append(QueryFilter("STATUSCD", "IN", status_cd, "TREE"))
        
        if species:
            filters.append(QueryFilter("SPCD", "IN", species, "TREE"))
        
        if dia_range:
            filters.append(QueryFilter("DIA", "BETWEEN", dia_range, "TREE"))
        
        if not include_seedlings:
            # Seedlings typically have DIA = 0 or NULL
            filters.append(QueryFilter("DIA", ">", 0.0, "TREE"))
        
        # Create query plan
        plan = QueryPlan(
            tables=["TREE"],
            columns=base_columns,
            filters=filters,
            filter_selectivity=self._estimate_filter_selectivity(filters)
        )
        
        return plan
    
    def execute(self, plan: QueryPlan) -> LazyFrameWrapper:
        """Execute tree query plan with optimizations."""
        # Check cache
        cache_key = CacheKey("tree", {"plan": plan.cache_key})
        cached_result = self.cache.get(cache_key.key)
        if cached_result is not None:
            return LazyFrameWrapper(cached_result)
        
        # Get required columns
        required_cols = list(plan.get_required_columns("TREE"))
        
        # Build WHERE clause for push-down
        where_clauses = []
        for filt in plan.get_pushdown_filters("TREE"):
            # Special handling for tree filters
            if filt.column == "DIA" and filt.operator == "BETWEEN":
                where_clauses.append(
                    f"DIA >= {filt.value[0]} AND DIA <= {filt.value[1]}"
                )
            else:
                where_clauses.append(filt.to_sql())
        
        where_clause = " AND ".join(where_clauses) if where_clauses else None
        
        # Load with optimization
        df = self.db._reader.read_table(
            "TREE",
            columns=required_cols,
            where=where_clause,
            lazy=True
        )
        
        # Apply remaining filters
        for filt in plan.filters:
            if not filt.can_push_down:
                df = df.filter(filt.to_polars_expr())
        
        # Cache and return
        result = LazyFrameWrapper(df)
        self.cache.put(cache_key.key, df, ttl_seconds=300)
        
        return result


class ConditionQueryBuilder(BaseQueryBuilder):
    """
    Query builder optimized for condition/area queries.
    
    Handles efficient filtering of condition data including land type,
    forest type, and other condition-level attributes.
    """
    
    def build_query_plan(self,
                        area_domain: Optional[str] = None,
                        land_class: Optional[List[int]] = None,
                        forest_type: Optional[List[int]] = None,
                        ownership: Optional[List[int]] = None,
                        reserved: Optional[bool] = None,
                        columns: Optional[List[str]] = None,
                        **kwargs) -> QueryPlan:
        """
        Build optimized query plan for condition data.
        
        Parameters
        ----------
        area_domain : Optional[str]
            Area domain filter expression
        land_class : Optional[List[int]]
            Land class codes
        forest_type : Optional[List[int]]
            Forest type codes
        ownership : Optional[List[int]]
            Ownership codes
        reserved : Optional[bool]
            Reserved land filter
        columns : Optional[List[str]]
            Specific columns to select
            
        Returns
        -------
        QueryPlan
            Optimized condition query plan
        """
        # Define base columns
        base_columns = [
            QueryColumn("CN", "COND", is_required=True),
            QueryColumn("PLT_CN", "COND", is_required=True),
            QueryColumn("PLOT", "COND", is_required=True),
            QueryColumn("CONDID", "COND", is_required=True),
            QueryColumn("COND_STATUS_CD", "COND", is_required=True),
            QueryColumn("OWNCD", "COND", is_required=True),
            QueryColumn("OWNGRPCD", "COND", is_required=True),
            QueryColumn("FORTYPCD", "COND"),
            QueryColumn("STDSZCD", "COND"),
            QueryColumn("SITECLCD", "COND"),
            QueryColumn("CONDPROP_UNADJ", "COND", is_required=True),
            QueryColumn("MICRPROP_UNADJ", "COND"),
            QueryColumn("SUBPPROP_UNADJ", "COND"),
        ]
        
        # Add requested columns
        if columns:
            for col in columns:
                if not any(c.name == col for c in base_columns):
                    base_columns.append(QueryColumn(col, "COND"))
        
        # Build filters
        filters = []
        
        # Parse area domain
        if area_domain:
            domain_filters = self._parse_domain_filter(area_domain)
            for f in domain_filters:
                f.table = "COND"
            filters.extend(domain_filters)
        
        # Add specific filters
        if land_class:
            filters.append(QueryFilter("LAND_COVER_CLASS_CD", "IN", land_class, "COND"))  # Use actual column name
        
        if forest_type:
            filters.append(QueryFilter("FORTYPCD", "IN", forest_type, "COND"))
        
        if ownership:
            filters.append(QueryFilter("OWNGRPCD", "IN", ownership, "COND"))
        
        if reserved is not None:
            if reserved:
                filters.append(QueryFilter("RESERVCD", "==", 1, "COND"))
            else:
                filters.append(QueryFilter("RESERVCD", "==", 0, "COND"))
        
        # Create query plan
        plan = QueryPlan(
            tables=["COND"],
            columns=base_columns,
            filters=filters,
            filter_selectivity=self._estimate_filter_selectivity(filters)
        )
        
        return plan
    
    def execute(self, plan: QueryPlan) -> LazyFrameWrapper:
        """Execute condition query plan."""
        # Check cache
        cache_key = CacheKey("condition", {"plan": plan.cache_key})
        cached_result = self.cache.get(cache_key.key)
        if cached_result is not None:
            return LazyFrameWrapper(cached_result)
        
        # Get required columns
        required_cols = list(plan.get_required_columns("COND"))
        
        # Build WHERE clause
        where_clauses = []
        for filt in plan.get_pushdown_filters("COND"):
            where_clauses.append(filt.to_sql())
        
        where_clause = " AND ".join(where_clauses) if where_clauses else None
        
        # Load with optimization
        df = self.db._reader.read_table(
            "COND",
            columns=required_cols,
            where=where_clause,
            lazy=True
        )
        
        # Apply remaining filters
        for filt in plan.filters:
            if not filt.can_push_down:
                df = df.filter(filt.to_polars_expr())
        
        # Cache and return
        result = LazyFrameWrapper(df)
        self.cache.put(cache_key.key, df, ttl_seconds=300)
        
        return result


class PlotQueryBuilder(BaseQueryBuilder):
    """
    Query builder optimized for plot data queries.
    
    Implements EVALID-based filtering and efficient joins with
    stratification tables.
    """
    
    def build_query_plan(self,
                        evalid: Optional[List[int]] = None,
                        plot_domain: Optional[str] = None,
                        state_cd: Optional[List[int]] = None,
                        county_cd: Optional[List[int]] = None,
                        include_strata: bool = True,
                        columns: Optional[List[str]] = None,
                        **kwargs) -> QueryPlan:
        """
        Build optimized query plan for plot data.
        
        Parameters
        ----------
        evalid : Optional[List[int]]
            EVALID filter
        plot_domain : Optional[str]
            Plot domain filter expression
        state_cd : Optional[List[int]]
            State codes
        county_cd : Optional[List[int]]
            County codes
        include_strata : bool
            Whether to join with stratification data
        columns : Optional[List[str]]
            Specific columns to select
            
        Returns
        -------
        QueryPlan
            Optimized plot query plan
        """
        # Define base columns
        base_columns = [
            QueryColumn("CN", "PLOT", is_required=True),
            QueryColumn("PREV_PLT_CN", "PLOT"),
            QueryColumn("INVYR", "PLOT", is_required=True),
            QueryColumn("STATECD", "PLOT", is_required=True),
            QueryColumn("UNITCD", "PLOT"),
            QueryColumn("COUNTYCD", "PLOT"),
            QueryColumn("PLOT", "PLOT", is_required=True),
            QueryColumn("LAT", "PLOT"),
            QueryColumn("LON", "PLOT"),
            QueryColumn("DESIGNCD", "PLOT"),
            QueryColumn("MEASMON", "PLOT"),
            QueryColumn("MEASDAY", "PLOT"),
            QueryColumn("MEASYEAR", "PLOT"),
            QueryColumn("MACRO_BREAKPOINT_DIA", "PLOT"),  # Required for TPA calculations
        ]
        
        # Add requested columns
        if columns:
            for col in columns:
                if not any(c.name == col for c in base_columns):
                    base_columns.append(QueryColumn(col, "PLOT"))
        
        # Build filters
        filters = []
        
        # Parse plot domain
        if plot_domain:
            domain_filters = self._parse_domain_filter(plot_domain)
            for f in domain_filters:
                f.table = "PLOT"
            filters.extend(domain_filters)
        
        # Add specific filters
        if state_cd:
            filters.append(QueryFilter("STATECD", "IN", state_cd, "PLOT"))
        
        if county_cd:
            filters.append(QueryFilter("COUNTYCD", "IN", county_cd, "PLOT"))
        
        # Build joins if needed
        joins = []
        tables = ["PLOT"]
        
        if include_strata and evalid:
            # Need to join with POP_PLOT_STRATUM_ASSGN for EVALID filtering
            tables.append("POP_PLOT_STRATUM_ASSGN")
            
            joins.append(QueryJoin(
                left_table="PLOT",
                right_table="POP_PLOT_STRATUM_ASSGN",
                left_on="CN",
                right_on="PLT_CN",
                how="inner",
                strategy=JoinStrategy.HASH  # Usually efficient for this join
            ))
            
            # Add EVALID filter on the assignment table
            filters.append(
                QueryFilter("EVALID", "IN", evalid, "POP_PLOT_STRATUM_ASSGN")
            )
            
            # Add stratification columns
            base_columns.extend([
                QueryColumn("EVALID", "POP_PLOT_STRATUM_ASSGN", is_required=True),
                QueryColumn("ESTN_UNIT", "POP_PLOT_STRATUM_ASSGN"),
                QueryColumn("STRATUMCD", "POP_PLOT_STRATUM_ASSGN"),
                QueryColumn("STRATUM_CN", "POP_PLOT_STRATUM_ASSGN"),
            ])
        
        # Create query plan
        plan = QueryPlan(
            tables=tables,
            columns=base_columns,
            filters=filters,
            joins=joins,
            filter_selectivity=self._estimate_filter_selectivity(filters)
        )
        
        return plan
    
    def execute(self, plan: QueryPlan) -> LazyFrameWrapper:
        """Execute plot query plan with joins."""
        # Check cache
        cache_key = CacheKey("plot", {"plan": plan.cache_key})
        cached_result = self.cache.get(cache_key.key)
        if cached_result is not None:
            return LazyFrameWrapper(cached_result)
        
        # Load PLOT table
        plot_cols = list(plan.get_required_columns("PLOT"))
        plot_filters = plan.get_pushdown_filters("PLOT")
        
        where_clauses = [f.to_sql() for f in plot_filters]
        where_clause = " AND ".join(where_clauses) if where_clauses else None
        
        plot_df = self.db._reader.read_table(
            "PLOT",
            columns=plot_cols,
            where=where_clause,
            lazy=True
        )
        
        # Apply joins if needed
        result_df = plot_df
        
        for join in plan.joins:
            if join.right_table == "POP_PLOT_STRATUM_ASSGN":
                # Load assignment table
                assgn_cols = list(plan.get_required_columns("POP_PLOT_STRATUM_ASSGN"))
                assgn_filters = plan.get_pushdown_filters("POP_PLOT_STRATUM_ASSGN")
                
                where_clauses = [f.to_sql() for f in assgn_filters]
                where_clause = " AND ".join(where_clauses) if where_clauses else None
                
                assgn_df = self.db._reader.read_table(
                    "POP_PLOT_STRATUM_ASSGN",
                    columns=assgn_cols,
                    where=where_clause,
                    lazy=True
                )
                
                # Perform join
                left_keys, right_keys = join.get_join_keys()
                result_df = result_df.join(
                    assgn_df,
                    left_on=left_keys,
                    right_on=right_keys,
                    how=join.how
                )
        
        # Apply remaining filters
        for filt in plan.filters:
            if not filt.can_push_down:
                result_df = result_df.filter(filt.to_polars_expr())
        
        # Don't select columns for now - let the caller handle column selection
        # This avoids issues with lazy frames where we can't check columns
        
        # Cache and return
        result = LazyFrameWrapper(result_df)
        self.cache.put(cache_key.key, result_df, ttl_seconds=300)
        
        return result


# === Query Builder Factory ===

class QueryBuilderFactory:
    """
    Factory for creating appropriate query builders.
    
    This factory creates the right query builder based on the query type
    and maintains a registry of available builders.
    """
    
    # Registry of available builders
    _builders = {
        "stratification": StratificationQueryBuilder,
        "tree": TreeQueryBuilder,
        "condition": ConditionQueryBuilder,
        "plot": PlotQueryBuilder,
    }
    
    @classmethod
    def create_builder(cls,
                      builder_type: str,
                      db: FIA,
                      config: EstimatorConfig,
                      cache: Optional[MemoryCache] = None) -> BaseQueryBuilder:
        """
        Create a query builder of the specified type.
        
        Parameters
        ----------
        builder_type : str
            Type of builder ('stratification', 'tree', 'condition', 'plot')
        db : FIA
            FIA database instance
        config : EstimatorConfig
            Estimator configuration
        cache : Optional[MemoryCache]
            Shared cache instance
            
        Returns
        -------
        BaseQueryBuilder
            Appropriate query builder instance
            
        Raises
        ------
        ValueError
            If builder type is not recognized
        """
        if builder_type not in cls._builders:
            raise ValueError(
                f"Unknown builder type: {builder_type}. "
                f"Available types: {list(cls._builders.keys())}"
            )
        
        builder_class = cls._builders[builder_type]
        return builder_class(db, config, cache)
    
    @classmethod
    def register_builder(cls, name: str, builder_class: type):
        """
        Register a new query builder type.
        
        Parameters
        ----------
        name : str
            Name for the builder type
        builder_class : type
            Builder class (must inherit from BaseQueryBuilder)
        """
        if not issubclass(builder_class, BaseQueryBuilder):
            raise TypeError(
                f"Builder class must inherit from BaseQueryBuilder, "
                f"got {builder_class.__name__}"
            )
        
        cls._builders[name] = builder_class
    
    @classmethod
    def get_available_builders(cls) -> List[str]:
        """Get list of available builder types."""
        return list(cls._builders.keys())


# === Composite Query Builder ===

class CompositeQueryBuilder:
    """
    Composite query builder that combines multiple specialized builders.
    
    This class orchestrates multiple query builders to create complex
    queries that span multiple tables with optimized execution plans.
    """
    
    def __init__(self,
                 db: FIA,
                 config: EstimatorConfig,
                 cache: Optional[MemoryCache] = None):
        """
        Initialize composite query builder.
        
        Parameters
        ----------
        db : FIA
            FIA database instance
        config : EstimatorConfig
            Estimator configuration
        cache : Optional[MemoryCache]
            Shared cache instance
        """
        self.db = db
        self.config = config
        self.cache = cache or MemoryCache(max_size_mb=512, max_entries=200)
        
        # Create specialized builders
        self.builders = {
            name: QueryBuilderFactory.create_builder(name, db, config, self.cache)
            for name in QueryBuilderFactory.get_available_builders()
        }
    
    def build_estimation_query(self,
                              estimation_type: str,
                              evalid: Optional[List[int]] = None,
                              tree_domain: Optional[str] = None,
                              area_domain: Optional[str] = None,
                              plot_domain: Optional[str] = None,
                              **kwargs) -> Dict[str, LazyFrameWrapper]:
        """
        Build complete query for an estimation task.
        
        Parameters
        ----------
        estimation_type : str
            Type of estimation ('volume', 'biomass', 'tpa', etc.)
        evalid : Optional[List[int]]
            EVALID filter
        tree_domain : Optional[str]
            Tree-level filter
        area_domain : Optional[str]
            Area-level filter
        plot_domain : Optional[str]
            Plot-level filter
        **kwargs
            Additional parameters
            
        Returns
        -------
        Dict[str, LazyFrameWrapper]
            Dictionary of query results by table name
        """
        results = {}
        
        # Build plot query with stratification
        plot_plan = self.builders["plot"].build_query_plan(
            evalid=evalid,
            plot_domain=plot_domain,
            include_strata=True,
            **kwargs
        )
        results["plots"] = self.builders["plot"].execute(plot_plan)
        
        # Build stratification query if needed
        if evalid:
            strat_plan = self.builders["stratification"].build_query_plan(
                evalid=evalid,
                **kwargs
            )
            results["strata"] = self.builders["stratification"].execute(strat_plan)
        
        # Build condition query if area domain specified or needed for estimation
        # TPA, volume, biomass, and growth estimations need condition data for proper expansion factors
        if area_domain or estimation_type in ["area", "carbon", "tpa", "volume", "biomass", "growth", "mortality"]:
            cond_plan = self.builders["condition"].build_query_plan(
                area_domain=area_domain,
                **kwargs
            )
            results["conditions"] = self.builders["condition"].execute(cond_plan)
        
        # Build tree query if tree domain specified or needed for estimation
        if tree_domain or estimation_type in ["volume", "biomass", "tpa", "growth", "mortality"]:
            tree_plan = self.builders["tree"].build_query_plan(
                tree_domain=tree_domain,
                **kwargs
            )
            results["trees"] = self.builders["tree"].execute(tree_plan)
        
        return results
    
    def optimize_join_order(self,
                           tables: List[str],
                           joins: List[QueryJoin]) -> List[QueryJoin]:
        """
        Optimize join order for multi-table queries.
        
        Parameters
        ----------
        tables : List[str]
            List of tables to join
        joins : List[QueryJoin]
            List of join operations
            
        Returns
        -------
        List[QueryJoin]
            Optimized join order
        """
        # Simple heuristic: join smaller tables first
        # In FIA, typically: STRATA < PLOT < COND < TREE
        table_order = {
            "POP_STRATUM": 1,
            "POP_PLOT_STRATUM_ASSGN": 2,
            "PLOT": 3,
            "COND": 4,
            "TREE": 5,
        }
        
        # Sort joins by table order
        sorted_joins = sorted(
            joins,
            key=lambda j: (
                table_order.get(j.left_table, 99),
                table_order.get(j.right_table, 99)
            )
        )
        
        return sorted_joins


# === Export public API ===

__all__ = [
    # Base classes
    "BaseQueryBuilder",
    "QueryPlan",
    "QueryColumn",
    "QueryFilter",
    "QueryJoin",
    
    # Specialized builders
    "StratificationQueryBuilder",
    "TreeQueryBuilder",
    "ConditionQueryBuilder",
    "PlotQueryBuilder",
    
    # Factory and composite
    "QueryBuilderFactory",
    "CompositeQueryBuilder",
    
    # Enums
    "JoinStrategy",
    "FilterPushDownLevel",
]