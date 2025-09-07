"""
Tests for query builders module.

This module tests the query builder framework including filter push-down,
column optimization, caching, and integration with lazy evaluation.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Any

import polars as pl

from pyfia.core import FIA
from pyfia.estimation.config import EstimatorConfig, LazyEvaluationConfig
from pyfia.estimation.builder import (
    # Base classes
    BaseQueryBuilder,
    QueryPlan,
    QueryColumn,
    QueryFilter,
    QueryJoin,
    
    # Specialized builders
    StratificationQueryBuilder,
    TreeQueryBuilder,
    ConditionQueryBuilder,
    PlotQueryBuilder,
    
    # Factory and composite
    QueryBuilderFactory,
    CompositeQueryBuilder,
    
    # Enums
    QueryJoinStrategy,
    FilterPushDownLevel,
)
from pyfia.estimation.lazy_evaluation import LazyFrameWrapper
from pyfia.estimation.caching import MemoryCache


# === Fixtures ===

@pytest.fixture
def mock_fia_db():
    """Create mock FIA database."""
    db = Mock(spec=FIA)
    db.tables = {}
    db._reader = Mock()
    
    # Mock read_table to return lazy frames
    def mock_read_table(table_name, columns=None, where=None, lazy=True):
        # Create mock data based on table
        if table_name == "TREE":
            data = {
                "CN": [1, 2, 3, 4, 5],
                "PLT_CN": [10, 10, 20, 20, 30],
                "PLOT": [1, 1, 2, 2, 3],
                "SUBP": [1, 2, 1, 2, 1],
                "TREE": [1, 2, 1, 2, 1],
                "CONDID": [1, 1, 1, 1, 1],
                "STATUSCD": [1, 1, 2, 1, 1],
                "SPCD": [131, 131, 110, 833, 802],
                "DIA": [10.5, 15.2, 0.0, 22.1, 18.7],
                "HT": [65, 72, 0, 85, 78],
            }
        elif table_name == "PLOT":
            data = {
                "CN": [10, 20, 30],
                "INVYR": [2020, 2020, 2021],
                "STATECD": [37, 37, 37],
                "COUNTYCD": [1, 1, 2],
                "PLOT": [1, 2, 3],
                "LAT": [35.1, 35.2, 35.3],
                "LON": [-78.1, -78.2, -78.3],
            }
        elif table_name == "COND":
            data = {
                "CN": [100, 200, 300],
                "PLT_CN": [10, 20, 30],
                "PLOT": [1, 2, 3],
                "CONDID": [1, 1, 1],
                "COND_STATUS_CD": [1, 1, 1],
                "OWNCD": [11, 11, 21],
                "OWNGRPCD": [10, 10, 20],
                "FORTYPCD": [161, 161, 171],
                "CONDPROP_UNADJ": [1.0, 1.0, 1.0],
            }
        elif table_name == "POP_STRATUM":
            data = {
                "CN": [1000, 2000, 3000],
                "EVALID": [371801, 371801, 371801],
                "ESTN_UNIT": [1, 1, 2],
                "STRATUMCD": [1, 2, 1],
                "P1POINTCNT": [100, 150, 120],
                "P2POINTCNT": [100, 150, 120],
                "ACRES": [50000, 75000, 60000],
                "ADJ_FACTOR_MACR": [1.0, 1.0, 1.0],
            }
        elif table_name == "POP_PLOT_STRATUM_ASSGN":
            data = {
                "PLT_CN": [10, 20, 30],
                "EVALID": [371801, 371801, 371801],
                "ESTN_UNIT": [1, 1, 2],
                "STRATUMCD": [1, 2, 1],
                "STRATUM_CN": [1000, 2000, 3000],
            }
        else:
            data = {"CN": [1, 2, 3]}
        
        # Apply column selection
        if columns:
            data = {k: v for k, v in data.items() if k in columns}
        
        df = pl.DataFrame(data)
        
        # Apply WHERE clause filtering
        if where:
            # Simple WHERE clause parsing for tests
            if "STATUSCD IN" in where:
                df = df.filter(pl.col("STATUSCD").is_in([1, 2]))
            elif "DIA >" in where:
                df = df.filter(pl.col("DIA") > 0)
            elif "EVALID IN" in where:
                df = df.filter(pl.col("EVALID") == 371801)
        
        return df.lazy() if lazy else df
    
    db._reader.read_table = mock_read_table
    
    return db


@pytest.fixture
def base_config():
    """Create base estimator configuration."""
    return EstimatorConfig(
        lazy_config=LazyEvaluationConfig(
            enable_predicate_pushdown=True,
            enable_projection_pushdown=True,
        )
    )


@pytest.fixture
def memory_cache():
    """Create memory cache for testing."""
    return MemoryCache(max_size_mb=10, max_entries=100)


# === Test Query Components ===

class TestQueryComponents:
    """Test query component classes."""
    
    def test_query_column_creation(self):
        """Test QueryColumn creation and properties."""
        col = QueryColumn(
            name="DIA",
            table="TREE",
            dtype="float",
            is_required=True,
            is_grouping=False,
            alias="diameter"
        )
        
        assert col.full_name == "TREE.DIA"
        assert col.output_name == "diameter"
        
        # Test without alias
        col2 = QueryColumn(name="SPCD", table="TREE")
        assert col2.output_name == "SPCD"
    
    def test_query_filter_sql_generation(self):
        """Test QueryFilter SQL generation."""
        # Equality filter
        f1 = QueryFilter("STATUSCD", "==", 1, "TREE")
        assert f1.to_sql() == "TREE.STATUSCD == 1"
        
        # IN filter
        f2 = QueryFilter("SPCD", "IN", [131, 110], "TREE")
        assert f2.to_sql() == "TREE.SPCD IN (131, 110)"
        
        # BETWEEN filter
        f3 = QueryFilter("DIA", "BETWEEN", [10, 20], "TREE")
        assert f3.to_sql() == "TREE.DIA BETWEEN 10 AND 20"
        
        # NULL filter
        f4 = QueryFilter("HT", "IS NULL", None, "TREE")
        assert f4.to_sql() == "TREE.HT IS NULL"
    
    def test_query_filter_polars_expr(self):
        """Test QueryFilter Polars expression generation."""
        # Equality filter
        f1 = QueryFilter("STATUSCD", "==", 1)
        expr = f1.to_polars_expr()
        assert isinstance(expr, pl.Expr)
        
        # Range filter
        f2 = QueryFilter("DIA", ">", 10.0)
        expr = f2.to_polars_expr()
        assert isinstance(expr, pl.Expr)
        
        # IN filter
        f3 = QueryFilter("SPCD", "IN", [131, 110])
        expr = f3.to_polars_expr()
        assert isinstance(expr, pl.Expr)
    
    def test_query_join_keys(self):
        """Test QueryJoin key normalization."""
        # Single key join
        j1 = QueryJoin(
            left_table="PLOT",
            right_table="TREE",
            left_on="CN",
            right_on="PLT_CN"
        )
        left_keys, right_keys = j1.get_join_keys()
        assert left_keys == ["CN"]
        assert right_keys == ["PLT_CN"]
        
        # Multi-key join
        j2 = QueryJoin(
            left_table="TREE",
            right_table="COND",
            left_on=["PLT_CN", "CONDID"],
            right_on=["PLT_CN", "CONDID"]
        )
        left_keys, right_keys = j2.get_join_keys()
        assert left_keys == ["PLT_CN", "CONDID"]
        assert right_keys == ["PLT_CN", "CONDID"]
    
    def test_query_plan_cache_key(self):
        """Test QueryPlan cache key generation."""
        plan = QueryPlan(
            tables=["TREE"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE"),
            ],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE"),
            ],
        )
        
        assert plan.cache_key is not None
        assert len(plan.cache_key) == 16  # MD5 hash truncated
        
        # Same plan should generate same key
        plan2 = QueryPlan(
            tables=["TREE"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE"),
            ],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE"),
            ],
        )
        
        assert plan.cache_key == plan2.cache_key
    
    def test_query_plan_required_columns(self):
        """Test QueryPlan required columns extraction."""
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[
                QueryColumn("CN", "TREE"),
                QueryColumn("DIA", "TREE"),
                QueryColumn("INVYR", "PLOT"),
            ],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE"),
                QueryFilter("STATECD", "==", 37, "PLOT"),
            ],
            joins=[
                QueryJoin("TREE", "PLOT", "PLT_CN", "CN"),
            ],
            group_by=["TREE.SPCD", "PLOT.INVYR"],
        )
        
        # Get required columns for TREE
        tree_cols = plan.get_required_columns("TREE")
        assert "CN" in tree_cols
        assert "DIA" in tree_cols
        assert "STATUSCD" in tree_cols  # From filter
        assert "PLT_CN" in tree_cols    # From join
        assert "SPCD" in tree_cols      # From group by
        
        # Get required columns for PLOT
        plot_cols = plan.get_required_columns("PLOT")
        assert "INVYR" in plot_cols
        assert "STATECD" in plot_cols  # From filter
        assert "CN" in plot_cols       # From join
    
    def test_query_plan_pushdown_filters(self):
        """Test QueryPlan filter push-down selection."""
        plan = QueryPlan(
            tables=["TREE"],
            columns=[QueryColumn("CN", "TREE")],
            filters=[
                QueryFilter("STATUSCD", "==", 1, "TREE", can_push_down=True),
                QueryFilter("CUSTOM", "==", "value", "TREE", can_push_down=False),
            ],
        )
        
        # Get pushdown filters
        pushdown = plan.get_pushdown_filters("TREE")
        assert len(pushdown) == 1
        assert pushdown[0].column == "STATUSCD"
        
        # Non-pushdown filter should not be included
        assert not any(f.column == "CUSTOM" for f in pushdown)


# === Test Base Query Builder ===

class TestBaseQueryBuilder:
    """Test base query builder functionality."""
    
    def test_domain_filter_parsing(self, mock_fia_db, base_config):
        """Test parsing of domain filter expressions."""
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                return QueryPlan(tables=["TEST"], columns=[])
            
            def execute(self, plan):
                return LazyFrameWrapper(pl.DataFrame().lazy())
        
        builder = TestBuilder(mock_fia_db, base_config)
        
        # Test simple equality
        filters = builder._parse_domain_filter("STATUSCD == 1")
        assert len(filters) == 1
        assert filters[0].column == "STATUSCD"
        assert filters[0].operator == "=="
        assert filters[0].value == 1
        
        # Test AND conditions
        filters = builder._parse_domain_filter("DIA > 10 AND STATUSCD == 1")
        assert len(filters) == 2
        assert filters[0].column == "DIA"
        assert filters[0].operator == ">"
        assert filters[1].column == "STATUSCD"
        
        # Test IN clause
        filters = builder._parse_domain_filter("SPCD IN (131, 110, 833)")
        assert len(filters) == 1
        assert filters[0].operator == "IN"
        assert len(filters[0].value) == 3
        
        # Test BETWEEN
        filters = builder._parse_domain_filter("DIA BETWEEN 10 AND 20")
        assert len(filters) == 1
        assert filters[0].operator == "BETWEEN"
        assert filters[0].value == [10, 20]  # Should be converted to numbers
        
        # Test IS NULL
        filters = builder._parse_domain_filter("HT IS NULL")
        assert len(filters) == 1
        assert filters[0].operator == "IS NULL"
    
    def test_filter_selectivity_estimation(self, mock_fia_db, base_config):
        """Test filter selectivity estimation."""
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                return QueryPlan(tables=["TEST"], columns=[])
            
            def execute(self, plan):
                return LazyFrameWrapper(pl.DataFrame().lazy())
        
        builder = TestBuilder(mock_fia_db, base_config)
        
        # Test equality filter (high selectivity)
        filters = [QueryFilter("STATUSCD", "==", 1)]
        selectivity = builder._estimate_filter_selectivity(filters)
        assert selectivity < 0.2
        
        # Test range filter (moderate selectivity)
        filters = [QueryFilter("DIA", ">", 10)]
        selectivity = builder._estimate_filter_selectivity(filters)
        assert 0.2 < selectivity < 0.5
        
        # Test multiple filters (compound selectivity)
        filters = [
            QueryFilter("STATUSCD", "==", 1),
            QueryFilter("DIA", ">", 10),
        ]
        selectivity = builder._estimate_filter_selectivity(filters)
        assert selectivity < 0.1
        
        # Test no filters (no selectivity)
        selectivity = builder._estimate_filter_selectivity([])
        assert selectivity == 1.0
    
    def test_join_strategy_optimization(self, mock_fia_db, base_config):
        """Test join strategy selection."""
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                return QueryPlan(tables=["TEST"], columns=[])
            
            def execute(self, plan):
                return LazyFrameWrapper(pl.DataFrame().lazy())
        
        builder = TestBuilder(mock_fia_db, base_config)
        
        # Small right table - broadcast join
        strategy = builder._optimize_join_strategy(100000, 1000)
        assert strategy == QueryJoinStrategy.BROADCAST
        
        # Small both tables - hash join
        strategy = builder._optimize_join_strategy(5000, 5000)
        assert strategy == QueryJoinStrategy.HASH
        
        # Large similar-sized tables - sort-merge
        strategy = builder._optimize_join_strategy(1000000, 800000)
        assert strategy == QueryJoinStrategy.SORT_MERGE
        
        # Very skewed sizes with tiny right table - broadcast join
        strategy = builder._optimize_join_strategy(1000000, 100)
        assert strategy == QueryJoinStrategy.BROADCAST  # 100 rows is small enough for broadcast
        
        # Very skewed sizes with larger right table - hash join
        strategy = builder._optimize_join_strategy(1000000, 50000)
        assert strategy == QueryJoinStrategy.HASH


# === Test Specialized Query Builders ===

class TestStratificationQueryBuilder:
    """Test stratification query builder."""
    
    def test_build_query_plan(self, mock_fia_db, base_config, memory_cache):
        """Test building stratification query plan."""
        builder = StratificationQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(
            evalid=[371801, 371802],
            state_cd=[37],
            include_adjustment_factors=True
        )
        
        assert "POP_STRATUM" in plan.tables
        assert len(plan.columns) > 0
        assert any(col.name == "EVALID" for col in plan.columns)
        assert any(col.name == "ADJ_FACTOR_MACR" for col in plan.columns)
        
        # Check filters
        assert len(plan.filters) == 2
        evalid_filter = next(f for f in plan.filters if f.column == "EVALID")
        assert evalid_filter.operator == "IN"
        assert 371801 in evalid_filter.value
    
    def test_execute_with_caching(self, mock_fia_db, base_config, memory_cache):
        """Test stratification query execution with caching."""
        builder = StratificationQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(evalid=[371801])
        
        # First execution
        result1 = builder.execute(plan)
        assert isinstance(result1, LazyFrameWrapper)
        
        # Second execution should hit cache
        with patch.object(memory_cache, 'get') as mock_get:
            mock_get.return_value = pl.DataFrame({"cached": [1]}).lazy()
            result2 = builder.execute(plan)
            mock_get.assert_called_once()


class TestTreeQueryBuilder:
    """Test tree query builder."""
    
    def test_build_query_plan_with_domain(self, mock_fia_db, base_config, memory_cache):
        """Test building tree query plan with domain filter."""
        builder = TreeQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(
            tree_domain="DIA > 10 AND STATUSCD == 1",
            species=[131, 110],
            include_seedlings=False
        )
        
        assert "TREE" in plan.tables
        
        # Check domain filters were parsed
        dia_filter = next((f for f in plan.filters if f.column == "DIA" and f.operator == ">"), None)
        assert dia_filter is not None
        assert dia_filter.value == 10
        
        # Check specific filters
        species_filter = next((f for f in plan.filters if f.column == "SPCD"), None)
        assert species_filter is not None
        assert species_filter.operator == "IN"
        assert 131 in species_filter.value
        
        # Check seedling exclusion
        seedling_filter = next((f for f in plan.filters if f.column == "DIA" and f.operator == ">"), None)
        assert seedling_filter is not None
    
    def test_execute_with_pushdown(self, mock_fia_db, base_config, memory_cache):
        """Test tree query execution with filter push-down."""
        builder = TreeQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(
            status_cd=[1, 2],
            dia_range=(10.0, 30.0)
        )
        
        # Execute and verify read_table was called with WHERE clause
        with patch.object(mock_fia_db._reader, 'read_table', wraps=mock_fia_db._reader.read_table) as mock_read:
            result = builder.execute(plan)
            
            # Verify WHERE clause was constructed
            mock_read.assert_called_once()
            call_args = mock_read.call_args
            assert call_args[1]['where'] is not None
            assert "DIA" in call_args[1]['where']


class TestConditionQueryBuilder:
    """Test condition query builder."""
    
    def test_build_query_plan(self, mock_fia_db, base_config, memory_cache):
        """Test building condition query plan."""
        builder = ConditionQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(
            area_domain="FORTYPCD == 161",
            ownership=[10, 20],
            reserved=False
        )
        
        assert "COND" in plan.tables
        
        # Check area domain parsing
        fortyp_filter = next((f for f in plan.filters if f.column == "FORTYPCD"), None)
        assert fortyp_filter is not None
        assert fortyp_filter.value == 161
        
        # Check ownership filter
        own_filter = next((f for f in plan.filters if f.column == "OWNGRPCD"), None)
        assert own_filter is not None
        assert own_filter.operator == "IN"
        
        # Check reserved filter
        reserved_filter = next((f for f in plan.filters if f.column == "RESERVCD"), None)
        assert reserved_filter is not None
        assert reserved_filter.value == 0


class TestPlotQueryBuilder:
    """Test plot query builder."""
    
    def test_build_query_plan_with_strata(self, mock_fia_db, base_config, memory_cache):
        """Test building plot query plan with stratification join."""
        builder = PlotQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(
            evalid=[371801],
            state_cd=[37],
            include_strata=True
        )
        
        assert "PLOT" in plan.tables
        assert "POP_PLOT_STRATUM_ASSGN" in plan.tables
        
        # Check join was created
        assert len(plan.joins) == 1
        join = plan.joins[0]
        assert join.left_table == "PLOT"
        assert join.right_table == "POP_PLOT_STRATUM_ASSGN"
        assert join.left_on == "CN"
        assert join.right_on == "PLT_CN"
        
        # Check EVALID filter on assignment table
        evalid_filter = next(
            (f for f in plan.filters if f.column == "EVALID" and f.table == "POP_PLOT_STRATUM_ASSGN"),
            None
        )
        assert evalid_filter is not None
    
    def test_execute_with_join(self, mock_fia_db, base_config, memory_cache):
        """Test plot query execution with join."""
        builder = PlotQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        plan = builder.build_query_plan(
            evalid=[371801],
            include_strata=True
        )
        
        result = builder.execute(plan)
        assert isinstance(result, LazyFrameWrapper)
        
        # Collect and verify join was performed
        df = result.collect()
        assert "EVALID" in df.columns  # From POP_PLOT_STRATUM_ASSGN


# === Test Query Builder Factory ===

class TestQueryBuilderFactory:
    """Test query builder factory."""
    
    def test_create_builder(self, mock_fia_db, base_config):
        """Test creating builders through factory."""
        # Test each builder type
        for builder_type in ["stratification", "tree", "condition", "plot"]:
            builder = QueryBuilderFactory.create_builder(
                builder_type,
                mock_fia_db,
                base_config
            )
            assert isinstance(builder, BaseQueryBuilder)
        
        # Test invalid type
        with pytest.raises(ValueError):
            QueryBuilderFactory.create_builder(
                "invalid",
                mock_fia_db,
                base_config
            )
    
    def test_register_builder(self, mock_fia_db, base_config):
        """Test registering custom builder."""
        class CustomBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                return QueryPlan(tables=["CUSTOM"], columns=[])
            
            def execute(self, plan):
                return LazyFrameWrapper(pl.DataFrame().lazy())
        
        # Register custom builder
        QueryBuilderFactory.register_builder("custom", CustomBuilder)
        
        # Create instance
        builder = QueryBuilderFactory.create_builder(
            "custom",
            mock_fia_db,
            base_config
        )
        assert isinstance(builder, CustomBuilder)
        
        # Verify in available builders
        assert "custom" in QueryBuilderFactory.get_available_builders()
    
    def test_register_invalid_builder(self):
        """Test registering invalid builder class."""
        class NotABuilder:
            pass
        
        with pytest.raises(TypeError):
            QueryBuilderFactory.register_builder("invalid", NotABuilder)


# === Test Composite Query Builder ===

class TestCompositeQueryBuilder:
    """Test composite query builder."""
    
    def test_build_estimation_query(self, mock_fia_db, base_config, memory_cache):
        """Test building complete estimation query."""
        builder = CompositeQueryBuilder(mock_fia_db, base_config, memory_cache)
        
        results = builder.build_estimation_query(
            estimation_type="volume",
            evalid=[371801],
            tree_domain="DIA > 10",
            area_domain="FORTYPCD == 161"
        )
        
        # Should have plots with stratification
        assert "plots" in results
        assert isinstance(results["plots"], LazyFrameWrapper)
        
        # Should have stratification data
        assert "strata" in results
        
        # Should have conditions (area domain specified)
        assert "conditions" in results
        
        # Should have trees (volume estimation)
        assert "trees" in results
    
    def test_optimize_join_order(self, mock_fia_db, base_config):
        """Test join order optimization."""
        builder = CompositeQueryBuilder(mock_fia_db, base_config)
        
        joins = [
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN"),
            QueryJoin("PLOT", "POP_PLOT_STRATUM_ASSGN", "CN", "PLT_CN"),
            QueryJoin("TREE", "COND", ["PLT_CN", "CONDID"], ["PLT_CN", "CONDID"]),
        ]
        
        optimized = builder.optimize_join_order(
            ["TREE", "PLOT", "COND", "POP_PLOT_STRATUM_ASSGN"],
            joins
        )
        
        # Should reorder to join smaller tables first
        # Expected order: STRATA -> PLOT -> COND -> TREE
        assert optimized[0].right_table == "POP_PLOT_STRATUM_ASSGN"


# === Integration Tests ===

class TestQueryBuilderIntegration:
    """Integration tests for query builders."""
    
    def test_end_to_end_tree_query(self, mock_fia_db, base_config):
        """Test end-to-end tree query with caching."""
        cache = MemoryCache(max_size_mb=10)
        builder = TreeQueryBuilder(mock_fia_db, base_config, cache)
        
        # Build and execute query
        plan = builder.build_query_plan(
            tree_domain="DIA > 10 AND STATUSCD == 1",
            species=[131, 110]
        )
        
        result = builder.execute(plan)
        df = result.collect()
        
        # Verify results
        assert len(df) > 0
        assert "DIA" in df.columns
        assert "SPCD" in df.columns
        
        # All DIAs should be > 10 (except seedlings which are 0)
        non_zero_dias = df.filter(pl.col("DIA") > 0)["DIA"]
        assert all(d > 10 for d in non_zero_dias)
    
    def test_composite_volume_estimation(self, mock_fia_db, base_config):
        """Test composite query for volume estimation."""
        cache = MemoryCache(max_size_mb=50)
        builder = CompositeQueryBuilder(mock_fia_db, base_config, cache)
        
        # Build complete estimation query
        results = builder.build_estimation_query(
            estimation_type="volume",
            evalid=[371801],
            tree_domain="DIA > 5",
            state_cd=[37]
        )
        
        # Collect all results
        collected = {}
        for name, wrapper in results.items():
            collected[name] = wrapper.collect()
        
        # Verify we have all necessary data
        assert len(collected["plots"]) > 0
        assert len(collected["trees"]) > 0
        if "strata" in collected:
            assert len(collected["strata"]) > 0
    
    def test_query_plan_caching(self, mock_fia_db, base_config):
        """Test that query plans are properly cached."""
        cache = MemoryCache(max_size_mb=10)
        builder = TreeQueryBuilder(mock_fia_db, base_config, cache)
        
        # Build same query twice
        plan1 = builder.build_query_plan(tree_domain="DIA > 10")
        plan2 = builder.build_query_plan(tree_domain="DIA > 10")
        
        # Should have same cache key
        assert plan1.cache_key == plan2.cache_key
        
        # Execute first query
        result1 = builder.execute(plan1)
        
        # Second execution should use cache
        # Mock the cache get to verify it's called
        original_get = cache.get
        get_called = False
        
        def mock_get(key):
            nonlocal get_called
            get_called = True
            return original_get(key)
        
        cache.get = mock_get
        result2 = builder.execute(plan2)
        
        assert get_called  # Cache was checked


# === Performance Tests ===

@pytest.mark.slow
class TestQueryBuilderPerformance:
    """Performance tests for query builders."""
    
    def test_large_filter_optimization(self, mock_fia_db, base_config):
        """Test optimization with many filters."""
        builder = TreeQueryBuilder(mock_fia_db, base_config)
        
        # Create complex domain with many conditions
        domain = " AND ".join([
            "DIA > 10",
            "STATUSCD == 1",
            "SPCD IN (131, 110, 833, 802, 837)",
            "HT > 50",
            "TREECLCD == 2",
            "CR > 30",
            "CCLCD IN (1, 2, 3)",
        ])
        
        plan = builder.build_query_plan(tree_domain=domain)
        
        # Should parse all filters
        assert len(plan.filters) >= 7
        
        # Should estimate low selectivity
        assert plan.filter_selectivity < 0.01
    
    def test_query_plan_cache_performance(self, mock_fia_db, base_config):
        """Test cache performance with many queries."""
        cache = MemoryCache(max_size_mb=50)
        builder = TreeQueryBuilder(mock_fia_db, base_config, cache)
        
        # Execute many similar queries
        import time
        
        # First pass - no cache
        start = time.time()
        for i in range(100):
            plan = builder.build_query_plan(
                tree_domain=f"DIA > {i % 20}",
                species=[131]
            )
            builder.execute(plan)
        uncached_time = time.time() - start
        
        # Second pass - with cache
        start = time.time()
        for i in range(100):
            plan = builder.build_query_plan(
                tree_domain=f"DIA > {i % 20}",  # Repeats every 20
                species=[131]
            )
            builder.execute(plan)
        cached_time = time.time() - start
        
        # Cached should be faster (though in tests with mocks, difference may be small)
        assert cached_time <= uncached_time * 1.5  # Allow some variance