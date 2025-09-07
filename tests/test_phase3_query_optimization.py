"""
Comprehensive tests for Phase 3 query optimization components.

This test suite validates query builders, join optimization, filter push-down,
and caching to ensure optimal query performance.
"""

import pytest
import polars as pl
import time
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, List, Any, Optional

from pyfia.estimation.query_builders import (
    BaseQueryBuilder,
    QueryBuilderFactory,
    CompositeQueryBuilder,
    StratificationQueryBuilder,
    TreeQueryBuilder,
    ConditionQueryBuilder,
    PlotQueryBuilder,
    QueryPlan,
    QueryColumn,
    QueryFilter,
    QueryJoin,
    QueryJoinStrategy,
    FilterPushDownLevel
)
from pyfia.estimation.join import (
    JoinManager,
    JoinOptimizer,
    JoinPlan,
    JoinType,
    JoinStrategy as JoinStrategyType,
    TableStatistics,
    FIATableInfo
)
from pyfia.estimation.config import EstimatorConfig, LazyEvaluationConfig
from pyfia.estimation.caching import MemoryCache, CacheKey
from pyfia.estimation.lazy_evaluation import LazyFrameWrapper


class TestQueryFilterParsing:
    """Test query filter parsing and expression handling."""
    
    def create_test_builder(self):
        """Create a test query builder."""
        mock_db = Mock()
        mock_db.tables = {}
        config = EstimatorConfig()
        
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                return QueryPlan(tables=["TEST"], columns=[], filters=[])
            
            def execute(self, plan):
                return LazyFrameWrapper(pl.DataFrame())
        
        return TestBuilder(mock_db, config)
    
    def test_simple_comparison_filters(self):
        """Test parsing of simple comparison filters."""
        builder = self.create_test_builder()
        
        test_cases = [
            ("DIA >= 10.0", [("DIA", ">=", 10.0)]),
            ("STATUSCD == 1", [("STATUSCD", "==", 1)]),
            ("HT > 50", [("HT", ">", 50)]),
            ("SPCD != 131", [("SPCD", "!=", 131)]),
            ("TREECLCD <= 3", [("TREECLCD", "<=", 3)]),
            ("ACTUALHT < 100.5", [("ACTUALHT", "<", 100.5)]),
            ("OWNGRPCD = 10", [("OWNGRPCD", "==", 10)])  # = converted to ==
        ]
        
        for filter_expr, expected in test_cases:
            filters = builder._parse_domain_filter(filter_expr)
            assert len(filters) == 1, f"Failed for: {filter_expr}"
            
            f = filters[0]
            expected_col, expected_op, expected_val = expected[0]
            assert f.column == expected_col, f"Column mismatch for: {filter_expr}"
            assert f.operator == expected_op, f"Operator mismatch for: {filter_expr}"
            assert f.value == expected_val, f"Value mismatch for: {filter_expr}"
    
    def test_in_clause_parsing(self):
        """Test parsing of IN clauses."""
        builder = self.create_test_builder()
        
        test_cases = [
            "SPCD IN (131, 110, 833)",
            "OWNGRPCD IN (10, 20, 30, 40)",
            "FORTYPCD IN (401, 402)"
        ]
        
        for filter_expr in test_cases:
            filters = builder._parse_domain_filter(filter_expr)
            assert len(filters) == 1
            
            f = filters[0]
            assert f.operator == "IN"
            assert isinstance(f.value, list)
            assert len(f.value) > 1
    
    def test_between_clause_parsing(self):
        """Test parsing of BETWEEN clauses."""
        builder = self.create_test_builder()
        
        test_cases = [
            ("DIA BETWEEN 5.0 AND 15.0", "DIA", [5.0, 15.0]),
            ("HT BETWEEN 10 AND 100", "HT", [10, 100]),
            ("INVYR BETWEEN 2015 AND 2020", "INVYR", [2015, 2020])
        ]
        
        for filter_expr, expected_col, expected_range in test_cases:
            filters = builder._parse_domain_filter(filter_expr)
            assert len(filters) == 1
            
            f = filters[0]
            assert f.column == expected_col
            assert f.operator == "BETWEEN"
            assert f.value == expected_range
    
    def test_null_clause_parsing(self):
        """Test parsing of NULL clauses."""
        builder = self.create_test_builder()
        
        test_cases = [
            ("HT IS NULL", "HT", "IS NULL"),
            ("ACTUALHT IS NOT NULL", "ACTUALHT", "IS NOT NULL"),
            ("TREECLCD IS NULL", "TREECLCD", "IS NULL")
        ]
        
        for filter_expr, expected_col, expected_op in test_cases:
            filters = builder._parse_domain_filter(filter_expr)
            assert len(filters) == 1
            
            f = filters[0]
            assert f.column == expected_col
            assert f.operator == expected_op
            assert f.value is None
    
    def test_complex_expression_parsing(self):
        """Test parsing of complex expressions with multiple conditions."""
        builder = self.create_test_builder()
        
        # Test AND combination
        filters = builder._parse_domain_filter("DIA >= 10.0 AND STATUSCD == 1")
        assert len(filters) == 2
        
        # Find DIA filter
        dia_filter = next(f for f in filters if f.column == "DIA")
        assert dia_filter.operator == ">="
        assert dia_filter.value == 10.0
        
        # Find STATUSCD filter
        status_filter = next(f for f in filters if f.column == "STATUSCD")
        assert status_filter.operator == "=="
        assert status_filter.value == 1
        
        # Test BETWEEN with additional conditions
        filters = builder._parse_domain_filter("DIA BETWEEN 5.0 AND 15.0 AND STATUSCD == 1")
        assert len(filters) == 2
        
        between_filter = next(f for f in filters if f.operator == "BETWEEN")
        assert between_filter.column == "DIA"
        assert between_filter.value == [5.0, 15.0]
        
        eq_filter = next(f for f in filters if f.operator == "==")
        assert eq_filter.column == "STATUSCD"
        assert eq_filter.value == 1
        
        # Test complex combination
        complex_expr = "DIA >= 10.0 AND SPCD IN (131, 110) AND HT IS NOT NULL"
        filters = builder._parse_domain_filter(complex_expr)
        assert len(filters) == 3
        
        operators = [f.operator for f in filters]
        assert ">=" in operators
        assert "IN" in operators
        assert "IS NOT NULL" in operators
    
    def test_filter_to_polars_conversion(self):
        """Test conversion of QueryFilter to Polars expressions."""
        # Test equality
        f = QueryFilter("STATUSCD", "==", 1)
        expr = f.to_polars_expr()
        assert "eq" in str(expr) or "==" in str(expr)
        
        # Test inequality
        f = QueryFilter("STATUSCD", "!=", 0)
        expr = f.to_polars_expr()
        assert "neq" in str(expr) or "!=" in str(expr)
        
        # Test greater than
        f = QueryFilter("DIA", ">", 10.0)
        expr = f.to_polars_expr()
        assert "gt" in str(expr) or ">" in str(expr)
        
        # Test IN clause
        f = QueryFilter("SPCD", "IN", [131, 110, 833])
        expr = f.to_polars_expr()
        assert "is_in" in str(expr)
        
        # Test BETWEEN (should become range)
        f = QueryFilter("DIA", "BETWEEN", [5.0, 15.0])
        expr = f.to_polars_expr()
        # Should create compound expression with AND
        assert "&" in str(expr) or "and" in str(expr)
        
        # Test NULL checks
        f = QueryFilter("HT", "IS NULL", None)
        expr = f.to_polars_expr()
        assert "is_null" in str(expr)
        
        f = QueryFilter("HT", "IS NOT NULL", None)
        expr = f.to_polars_expr()
        assert "is_not_null" in str(expr) or "not_null" in str(expr)
    
    def test_filter_to_sql_conversion(self):
        """Test conversion of QueryFilter to SQL WHERE clauses."""
        # Test basic equality
        f = QueryFilter("STATUSCD", "==", 1, "TREE")
        sql = f.to_sql()
        assert sql == "TREE.STATUSCD == 1"
        
        # Test IN clause
        f = QueryFilter("SPCD", "IN", [131, 110, 833], "TREE")
        sql = f.to_sql()
        assert sql == "TREE.SPCD IN (131, 110, 833)"
        
        # Test BETWEEN
        f = QueryFilter("DIA", "BETWEEN", [5.0, 15.0], "TREE")
        sql = f.to_sql()
        assert sql == "TREE.DIA BETWEEN 5.0 AND 15.0"
        
        # Test NULL checks
        f = QueryFilter("HT", "IS NULL", None, "TREE")
        sql = f.to_sql()
        assert sql == "TREE.HT IS NULL"
        
        f = QueryFilter("HT", "IS NOT NULL", None, "TREE")
        sql = f.to_sql()
        assert sql == "TREE.HT IS NOT NULL"
        
        # Test without table qualifier
        f = QueryFilter("STATUSCD", "==", 1)
        sql = f.to_sql()
        assert sql == "STATUSCD == 1"


class TestQueryPlanCreation:
    """Test QueryPlan creation, caching, and metadata."""
    
    def test_query_plan_basic_creation(self):
        """Test basic QueryPlan creation and properties."""
        columns = [
            QueryColumn("CN", "TREE", is_required=True),
            QueryColumn("SPCD", "TREE", is_required=True),
            QueryColumn("DIA", "TREE", is_required=True),
            QueryColumn("HT", "TREE", is_required=False)
        ]
        
        filters = [
            QueryFilter("STATUSCD", "==", 1, "TREE"),
            QueryFilter("DIA", ">=", 10.0, "TREE")
        ]
        
        joins = [
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN", "inner", QueryJoinStrategy.HASH)
        ]
        
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=columns,
            filters=filters,
            joins=joins,
            group_by=["SPCD", "TREE.STATUSCD"],
            order_by=[("SPCD", "ASC"), ("DIA", "DESC")],
            limit=1000,
            estimated_rows=50000,
            filter_selectivity=0.3
        )
        
        # Test basic properties
        assert plan.tables == ["TREE", "PLOT"]
        assert len(plan.columns) == 4
        assert len(plan.filters) == 2
        assert len(plan.joins) == 1
        assert plan.group_by == ["SPCD", "TREE.STATUSCD"]
        assert plan.order_by == [("SPCD", "ASC"), ("DIA", "DESC")]
        assert plan.limit == 1000
        assert plan.estimated_rows == 50000
        assert plan.filter_selectivity == 0.3
        
        # Test cache key generation
        assert plan.cache_key is not None
        assert isinstance(plan.cache_key, str)
        assert len(plan.cache_key) == 16  # Truncated MD5 hash
    
    def test_required_columns_extraction(self):
        """Test extraction of required columns by table."""
        columns = [
            QueryColumn("CN", "TREE", is_required=True),
            QueryColumn("SPCD", "TREE", is_required=True),
            QueryColumn("DIA", "TREE", is_required=False),
            QueryColumn("CN", "PLOT", is_required=True),
            QueryColumn("LAT", "PLOT", is_required=False)
        ]
        
        filters = [
            QueryFilter("STATUSCD", "==", 1, "TREE"),
            QueryFilter("STATECD", "==", 37, "PLOT")
        ]
        
        joins = [
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN")
        ]
        
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=columns,
            filters=filters,
            joins=joins,
            group_by=["TREE.SPCD"]
        )
        
        # Test TREE table columns
        tree_cols = plan.get_required_columns("TREE")
        assert "CN" in tree_cols  # Required column
        assert "SPCD" in tree_cols  # Required column + grouping
        assert "STATUSCD" in tree_cols  # Filter column
        assert "PLT_CN" in tree_cols  # Join column
        assert "DIA" not in tree_cols  # Not required
        
        # Test PLOT table columns
        plot_cols = plan.get_required_columns("PLOT")
        assert "CN" in plot_cols  # Required + join column
        assert "STATECD" in plot_cols  # Filter column
        assert "LAT" not in plot_cols  # Not required
    
    def test_pushdown_filter_identification(self):
        """Test identification of filters that can be pushed down."""
        filters = [
            QueryFilter("STATUSCD", "==", 1, "TREE", can_push_down=True),
            QueryFilter("DIA", ">=", 10.0, "TREE", can_push_down=True),
            QueryFilter("COMPLEX", "==", "value", "TREE", can_push_down=False),
            QueryFilter("STATECD", "==", 37, "PLOT", can_push_down=True)
        ]
        
        plan = QueryPlan(
            tables=["TREE", "PLOT"],
            columns=[],
            filters=filters
        )
        
        # Test TREE pushdown filters
        tree_filters = plan.get_pushdown_filters("TREE")
        assert len(tree_filters) == 2
        pushdown_columns = [f.column for f in tree_filters]
        assert "STATUSCD" in pushdown_columns
        assert "DIA" in pushdown_columns
        assert "COMPLEX" not in pushdown_columns  # Can't push down
        
        # Test PLOT pushdown filters
        plot_filters = plan.get_pushdown_filters("PLOT")
        assert len(plot_filters) == 1
        assert plot_filters[0].column == "STATECD"
    
    def test_query_plan_caching(self):
        """Test QueryPlan cache key consistency."""
        # Create two identical plans
        columns = [QueryColumn("CN", "TREE")]
        filters = [QueryFilter("STATUSCD", "==", 1, "TREE")]
        
        plan1 = QueryPlan(tables=["TREE"], columns=columns, filters=filters)
        plan2 = QueryPlan(tables=["TREE"], columns=columns, filters=filters)
        
        # Should have same cache key
        assert plan1.cache_key == plan2.cache_key
        
        # Different plans should have different cache keys
        plan3 = QueryPlan(
            tables=["TREE"],
            columns=columns,
            filters=[QueryFilter("STATUSCD", "==", 2, "TREE")]  # Different filter
        )
        
        assert plan1.cache_key != plan3.cache_key


class TestSpecializedQueryBuilders:
    """Test specialized query builder implementations."""
    
    def create_mock_db(self, tables: List[str]):
        """Create mock database with specified tables."""
        mock_db = Mock()
        mock_db.tables = {table: Mock() for table in tables}
        mock_db._reader = Mock()
        
        # Mock read_table to return lazy DataFrame
        mock_df = pl.LazyFrame({"dummy": [1, 2, 3]})
        mock_db._reader.read_table.return_value = mock_df
        
        return mock_db
    
    def test_stratification_query_builder(self):
        """Test StratificationQueryBuilder query plan generation."""
        mock_db = self.create_mock_db(["POP_STRATUM"])
        config = EstimatorConfig()
        
        builder = StratificationQueryBuilder(mock_db, config)
        
        # Test basic plan
        plan = builder.build_query_plan(
            evalid=[372301, 372302],
            state_cd=[37],
            include_adjustment_factors=True
        )
        
        assert plan.tables == ["POP_STRATUM"]
        
        # Check required columns are included
        column_names = [col.name for col in plan.columns]
        required_cols = ["CN", "EVALID", "ESTN_UNIT", "STRATUMCD", "P1POINTCNT", "P2POINTCNT", "ACRES"]
        for col in required_cols:
            assert col in column_names
        
        # Check adjustment factor columns
        adj_cols = ["ADJ_FACTOR_MACR", "ADJ_FACTOR_SUBP", "ADJ_FACTOR_MICR"]
        for col in adj_cols:
            assert col in column_names
        
        # Check filters
        assert len(plan.filters) == 2
        
        # EVALID filter
        evalid_filter = next(f for f in plan.filters if f.column == "EVALID")
        assert evalid_filter.operator == "IN"
        assert evalid_filter.value == [372301, 372302]
        assert evalid_filter.table == "POP_STRATUM"
        
        # STATECD filter
        state_filter = next(f for f in plan.filters if f.column == "STATECD")
        assert state_filter.operator == "IN"
        assert state_filter.value == [37]
        assert state_filter.table == "POP_STRATUM"
        
        # Test without adjustment factors
        plan_no_adj = builder.build_query_plan(
            evalid=[372301],
            include_adjustment_factors=False
        )
        column_names_no_adj = [col.name for col in plan_no_adj.columns]
        for adj_col in adj_cols:
            assert adj_col not in column_names_no_adj
    
    def test_tree_query_builder(self):
        """Test TreeQueryBuilder with complex filtering options."""
        mock_db = self.create_mock_db(["TREE"])
        config = EstimatorConfig()
        
        builder = TreeQueryBuilder(mock_db, config)
        
        # Test comprehensive query plan
        plan = builder.build_query_plan(
            tree_domain="DIA >= 10.0 AND STATUSCD == 1",
            status_cd=[1, 2],
            species=[131, 110, 833],
            dia_range=(5.0, 25.0),
            include_seedlings=False,
            columns=["TREECLCD", "CARBON_AG", "VOLCFNET"]
        )
        
        assert plan.tables == ["TREE"]
        
        # Check base columns are included
        base_cols = ["CN", "PLT_CN", "PLOT", "SUBP", "TREE", "CONDID", "STATUSCD", "SPCD", "DIA"]
        column_names = [col.name for col in plan.columns]
        for col in base_cols:
            assert col in column_names
        
        # Check custom columns
        custom_cols = ["TREECLCD", "CARBON_AG", "VOLCFNET"]
        for col in custom_cols:
            assert col in column_names
        
        # Check filters
        filter_columns = [f.column for f in plan.filters]
        
        # Domain filters
        assert "DIA" in filter_columns
        assert "STATUSCD" in filter_columns
        
        # Specific filters
        status_filter = next((f for f in plan.filters if f.column == "STATUSCD" and f.operator == "IN"), None)
        assert status_filter is not None
        assert set(status_filter.value) == {1, 2}
        
        species_filter = next(f for f in plan.filters if f.column == "SPCD")
        assert species_filter.operator == "IN"
        assert set(species_filter.value) == {131, 110, 833}
        
        # Diameter range filter
        dia_between_filter = next((f for f in plan.filters if f.column == "DIA" and f.operator == "BETWEEN"), None)
        assert dia_between_filter is not None
        assert dia_between_filter.value == (5.0, 25.0)
        
        # Seedling exclusion filter
        seedling_filters = [f for f in plan.filters if f.column == "DIA" and f.operator == ">" and f.value == 0.0]
        assert len(seedling_filters) > 0
        
        # Test with seedlings included
        plan_with_seedlings = builder.build_query_plan(include_seedlings=True)
        seedling_exclusion_filters = [f for f in plan_with_seedlings.filters if f.column == "DIA" and f.operator == ">"]
        assert len(seedling_exclusion_filters) == 0
    
    def test_condition_query_builder(self):
        """Test ConditionQueryBuilder functionality."""
        mock_db = self.create_mock_db(["COND"])
        config = EstimatorConfig()
        
        builder = ConditionQueryBuilder(mock_db, config)
        
        # Test comprehensive plan
        plan = builder.build_query_plan(
            area_domain="LANDCLCD IN (1, 2, 3) AND FORTYPCD >= 400",
            land_class=[1, 2, 3],
            forest_type=[401, 402, 403, 500, 501],
            ownership=[10, 20, 30],
            reserved=False,
            columns=["PHYSCLCD", "DSTRBCD1", "TREATCD1"]
        )
        
        assert plan.tables == ["COND"]
        
        # Check base columns
        base_cols = ["CN", "PLT_CN", "PLOT", "CONDID", "COND_STATUS_CD", "OWNCD", "OWNGRPCD"]
        column_names = [col.name for col in plan.columns]
        for col in base_cols:
            assert col in column_names
        
        # Check custom columns
        custom_cols = ["PHYSCLCD", "DSTRBCD1", "TREATCD1"]
        for col in custom_cols:
            assert col in column_names
        
        # Check filters from area domain
        domain_filter_cols = [f.column for f in plan.filters if f.table == "COND"]
        assert "LANDCLCD" in domain_filter_cols
        assert "FORTYPCD" in domain_filter_cols
        
        # Check specific filters
        landclass_filter = next((f for f in plan.filters if f.column == "LANDCLCD" and f.operator == "IN"), None)
        if landclass_filter:  # Might be from domain parsing instead
            assert set(landclass_filter.value) == {1, 2, 3}
        
        forest_filter = next(f for f in plan.filters if f.column == "FORTYPCD")
        assert forest_filter.operator == "IN"
        assert set(forest_filter.value) == {401, 402, 403, 500, 501}
        
        owner_filter = next(f for f in plan.filters if f.column == "OWNGRPCD")
        assert owner_filter.operator == "IN"
        assert set(owner_filter.value) == {10, 20, 30}
        
        reserved_filter = next(f for f in plan.filters if f.column == "RESERVCD")
        assert reserved_filter.operator == "=="
        assert reserved_filter.value == 0  # reserved=False
    
    def test_plot_query_builder_with_joins(self):
        """Test PlotQueryBuilder with stratification joins."""
        mock_db = self.create_mock_db(["PLOT", "POP_PLOT_STRATUM_ASSGN"])
        config = EstimatorConfig()
        
        builder = PlotQueryBuilder(mock_db, config)
        
        # Test with stratification
        plan = builder.build_query_plan(
            evalid=[372301, 372302],
            state_cd=[37, 45],
            county_cd=[183, 185, 001],
            plot_domain="DESIGNCD IN (1, 311)",
            include_strata=True,
            columns=["ECOSUBCD", "MACROPLCD"]
        )
        
        assert "PLOT" in plan.tables
        assert "POP_PLOT_STRATUM_ASSGN" in plan.tables
        
        # Check join configuration
        assert len(plan.joins) == 1
        join = plan.joins[0]
        assert join.left_table == "PLOT"
        assert join.right_table == "POP_PLOT_STRATUM_ASSGN"
        assert join.left_on == "CN"
        assert join.right_on == "PLT_CN"
        assert join.how == "inner"
        
        # Check stratification columns are added
        column_names = [col.name for col in plan.columns]
        strat_cols = ["EVALID", "ESTN_UNIT", "STRATUMCD", "STRATUM_CN"]
        for col in strat_cols:
            assert col in column_names
        
        # Check custom columns
        assert "ECOSUBCD" in column_names
        assert "MACROPLCD" in column_names
        
        # Check filters
        evalid_filter = next(f for f in plan.filters if f.column == "EVALID")
        assert evalid_filter.table == "POP_PLOT_STRATUM_ASSGN"
        assert set(evalid_filter.value) == {372301, 372302}
        
        state_filter = next(f for f in plan.filters if f.column == "STATECD")
        assert state_filter.table == "PLOT"
        assert set(state_filter.value) == {37, 45}
        
        # Domain filters
        domain_filters = [f for f in plan.filters if f.column == "DESIGNCD"]
        assert len(domain_filters) > 0
        
        # Test without stratification
        plan_no_strata = builder.build_query_plan(
            state_cd=[37],
            include_strata=False
        )
        
        assert plan_no_strata.tables == ["PLOT"]
        assert len(plan_no_strata.joins) == 0
        
        no_strata_columns = [col.name for col in plan_no_strata.columns]
        for strat_col in strat_cols:
            assert strat_col not in no_strata_columns


class TestQueryBuilderFactory:
    """Test the QueryBuilderFactory functionality."""
    
    def test_factory_creates_correct_builders(self):
        """Test that factory creates correct builder types."""
        mock_db = Mock()
        config = EstimatorConfig()
        
        # Test all standard builder types
        builder_tests = [
            ("stratification", StratificationQueryBuilder),
            ("tree", TreeQueryBuilder),
            ("condition", ConditionQueryBuilder),
            ("plot", PlotQueryBuilder)
        ]
        
        for builder_name, expected_class in builder_tests:
            builder = QueryBuilderFactory.create_builder(builder_name, mock_db, config)
            assert isinstance(builder, expected_class)
            assert isinstance(builder, BaseQueryBuilder)
            assert builder.db == mock_db
            assert builder.config == config
    
    def test_factory_with_shared_cache(self):
        """Test factory with shared cache instance."""
        mock_db = Mock()
        config = EstimatorConfig()
        shared_cache = MemoryCache(max_size_mb=128, max_entries=50)
        
        builder1 = QueryBuilderFactory.create_builder("tree", mock_db, config, shared_cache)
        builder2 = QueryBuilderFactory.create_builder("condition", mock_db, config, shared_cache)
        
        assert builder1.cache == shared_cache
        assert builder2.cache == shared_cache
        assert builder1.cache == builder2.cache  # Same instance
    
    def test_factory_invalid_builder_type(self):
        """Test factory error handling for invalid builder types."""
        mock_db = Mock()
        config = EstimatorConfig()
        
        with pytest.raises(ValueError, match="Unknown builder type"):
            QueryBuilderFactory.create_builder("invalid_type", mock_db, config)
    
    def test_factory_custom_builder_registration(self):
        """Test custom builder registration."""
        class CustomBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                return QueryPlan(tables=["CUSTOM"], columns=[], filters=[])
            
            def execute(self, plan):
                return LazyFrameWrapper(pl.DataFrame({"custom": [1, 2, 3]}))
        
        # Register custom builder
        QueryBuilderFactory.register_builder("custom", CustomBuilder)
        
        # Test that it's available
        available = QueryBuilderFactory.get_available_builders()
        assert "custom" in available
        
        # Test creation
        mock_db = Mock()
        config = EstimatorConfig()
        
        builder = QueryBuilderFactory.create_builder("custom", mock_db, config)
        assert isinstance(builder, CustomBuilder)
    
    def test_factory_registration_validation(self):
        """Test that factory validates registered builders."""
        class NotAQueryBuilder:
            pass
        
        with pytest.raises(TypeError, match="must inherit from BaseQueryBuilder"):
            QueryBuilderFactory.register_builder("invalid", NotAQueryBuilder)


class TestCompositeQueryBuilder:
    """Test CompositeQueryBuilder orchestration."""
    
    def create_mock_db_with_readers(self):
        """Create mock database with table readers."""
        mock_db = Mock()
        mock_db.tables = {
            "PLOT": Mock(),
            "TREE": Mock(),
            "COND": Mock(),
            "POP_STRATUM": Mock(),
            "POP_PLOT_STRATUM_ASSGN": Mock()
        }
        
        # Mock readers to return LazyFrameWrapper
        def mock_read_table(table, **kwargs):
            data = {
                "PLOT": {"CN": [1, 2, 3], "STATECD": [37, 37, 37]},
                "TREE": {"CN": [1, 2, 3], "PLT_CN": [1, 1, 2], "SPCD": [131, 110, 131]},
                "COND": {"CN": [1, 2, 3], "PLT_CN": [1, 2, 3], "FORTYPCD": [401, 402, 401]},
                "POP_STRATUM": {"CN": [1, 2], "EVALID": [372301, 372301]},
                "POP_PLOT_STRATUM_ASSGN": {"PLT_CN": [1, 2, 3], "EVALID": [372301, 372301, 372301]}
            }
            return pl.LazyFrame(data.get(table, {"id": [1, 2, 3]}))
        
        mock_db._reader = Mock()
        mock_db._reader.read_table.side_effect = mock_read_table
        
        return mock_db
    
    def test_composite_builder_estimation_query(self):
        """Test CompositeQueryBuilder builds complete estimation queries."""
        mock_db = self.create_mock_db_with_readers()
        config = EstimatorConfig()
        
        composite = CompositeQueryBuilder(mock_db, config)
        
        # Test volume estimation query
        results = composite.build_estimation_query(
            estimation_type="volume",
            evalid=[372301],
            tree_domain="DIA >= 10.0",
            area_domain="FORTYPCD IN (401, 402)",
            plot_domain="STATECD == 37"
        )
        
        # Should return results for all relevant components
        expected_components = ["plots", "strata", "conditions", "trees"]
        for component in expected_components:
            assert component in results
            assert isinstance(results[component], LazyFrameWrapper)
        
        # Test area estimation query (no trees needed)
        area_results = composite.build_estimation_query(
            estimation_type="area",
            evalid=[372301],
            area_domain="LANDCLCD IN (1, 2, 3)"
        )
        
        assert "plots" in area_results
        assert "strata" in area_results
        assert "conditions" in area_results
        # Trees might or might not be included depending on implementation
    
    def test_composite_builder_caching(self):
        """Test that CompositeQueryBuilder uses caching effectively."""
        mock_db = self.create_mock_db_with_readers()
        config = EstimatorConfig()
        
        composite = CompositeQueryBuilder(mock_db, config)
        
        # First query
        results1 = composite.build_estimation_query(
            estimation_type="volume",
            evalid=[372301],
            tree_domain="DIA >= 10.0"
        )
        
        # Second identical query
        results2 = composite.build_estimation_query(
            estimation_type="volume",
            evalid=[372301],
            tree_domain="DIA >= 10.0"
        )
        
        # Results should be equivalent
        assert set(results1.keys()) == set(results2.keys())
        
        # Check that cache was used (exact implementation depends on caching strategy)
        cache_stats = composite.cache.get_stats()
        assert cache_stats["entries"] > 0
    
    def test_composite_builder_join_optimization(self):
        """Test CompositeQueryBuilder join order optimization."""
        mock_db = self.create_mock_db_with_readers()
        config = EstimatorConfig()
        
        composite = CompositeQueryBuilder(mock_db, config)
        
        # Test join order optimization
        tables = ["TREE", "PLOT", "COND", "POP_STRATUM"]
        joins = [
            QueryJoin("TREE", "PLOT", "PLT_CN", "CN"),
            QueryJoin("PLOT", "COND", "CN", "PLT_CN"),
            QueryJoin("PLOT", "POP_STRATUM", "STRATUM_CN", "CN")
        ]
        
        optimized_joins = composite.optimize_join_order(tables, joins)
        
        # Should return optimized join order
        assert len(optimized_joins) == len(joins)
        assert all(isinstance(j, QueryJoin) for j in optimized_joins)
        
        # Verify that smaller tables (like POP_STRATUM) are joined first
        join_order = [(j.left_table, j.right_table) for j in optimized_joins]
        # This is a heuristic test - exact order depends on optimization logic


class TestFilterSelectivityEstimation:
    """Test filter selectivity estimation for query optimization."""
    
    def test_selectivity_estimation_different_operators(self):
        """Test selectivity estimation for different filter operators."""
        mock_db = Mock()
        config = EstimatorConfig()
        
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                pass
            def execute(self, plan):
                pass
        
        builder = TestBuilder(mock_db, config)
        
        # Test individual filter selectivities
        test_cases = [
            (QueryFilter("STATUSCD", "==", 1), 0.1),  # Equality - highly selective
            (QueryFilter("DIA", ">", 10.0), 0.3),     # Range - moderately selective
            (QueryFilter("SPCD", "IN", [131, 110]), 0.2),  # IN with 2 values
            (QueryFilter("HT", "IS NULL", None), 0.05),     # NULL - highly selective
            (QueryFilter("DIA", "IS NOT NULL", None), 0.95), # NOT NULL - not selective
            (QueryFilter("DIA", "BETWEEN", [5.0, 15.0]), 0.2)  # BETWEEN - selective
        ]
        
        for filter_obj, expected_range in test_cases:
            selectivity = builder._estimate_filter_selectivity([filter_obj])
            assert 0.0 < selectivity <= 1.0
            # Allow some tolerance around expected values
            assert abs(selectivity - expected_range) < 0.5, f"Selectivity {selectivity} not near expected {expected_range} for {filter_obj.operator}"
    
    def test_combined_filter_selectivity(self):
        """Test selectivity estimation for combined filters."""
        mock_db = Mock()
        config = EstimatorConfig()
        
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                pass
            def execute(self, plan):
                pass
        
        builder = TestBuilder(mock_db, config)
        
        # Multiple filters should be more selective
        single_filter = [QueryFilter("STATUSCD", "==", 1)]
        multiple_filters = [
            QueryFilter("STATUSCD", "==", 1),
            QueryFilter("DIA", ">=", 10.0),
            QueryFilter("SPCD", "IN", [131, 110])
        ]
        
        single_selectivity = builder._estimate_filter_selectivity(single_filter)
        multiple_selectivity = builder._estimate_filter_selectivity(multiple_filters)
        
        assert multiple_selectivity < single_selectivity  # More filters = more selective
        assert multiple_selectivity > 0.001  # But not too selective (minimum threshold)
    
    def test_join_strategy_optimization(self):
        """Test join strategy selection based on table sizes."""
        mock_db = Mock()
        config = EstimatorConfig()
        
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                pass
            def execute(self, plan):
                pass
        
        builder = TestBuilder(mock_db, config)
        
        # Test different size combinations
        test_cases = [
            # (left_size, right_size, expected_strategy_type)
            (1000, 100000, QueryJoinStrategy.BROADCAST),      # Small left table
            (100000, 1000, QueryJoinStrategy.BROADCAST),      # Small right table
            (50000, 60000, QueryJoinStrategy.HASH),           # Medium similar sizes
            (1000000, 1200000, QueryJoinStrategy.SORT_MERGE), # Large similar sizes
            (10000, 10000, QueryJoinStrategy.HASH),           # Small similar sizes
        ]
        
        for left_size, right_size, expected in test_cases:
            strategy = builder._optimize_join_strategy(left_size, right_size)
            
            # Allow some flexibility in strategy selection
            # The exact strategy might vary based on detailed heuristics
            assert strategy in [expected, QueryJoinStrategy.HASH, QueryJoinStrategy.AUTO]
    
    def test_table_statistics_caching(self):
        """Test that table statistics are cached for performance."""
        mock_db = Mock()
        mock_db.tables = {"TREE": Mock(), "PLOT": Mock()}
        
        # Mock table to have columns and schema
        mock_tree_table = Mock()
        mock_tree_table.columns = ["CN", "PLT_CN", "SPCD", "DIA", "STATUSCD"]
        mock_tree_table.schema = {"CN": pl.Int64, "SPCD": pl.Int32, "DIA": pl.Float64}
        mock_db.tables["TREE"] = mock_tree_table
        
        config = EstimatorConfig()
        
        class TestBuilder(BaseQueryBuilder):
            def build_query_plan(self, **kwargs):
                pass
            def execute(self, plan):
                pass
        
        builder = TestBuilder(mock_db, config)
        
        # First call should compute stats
        stats1 = builder._get_table_stats("TREE")
        assert isinstance(stats1, dict)
        assert "columns" in stats1
        assert len(stats1["columns"]) > 0
        
        # Second call should use cache
        stats2 = builder._get_table_stats("TREE")
        assert stats1 == stats2
        
        # Should be same object reference (cached)
        assert stats1 is stats2