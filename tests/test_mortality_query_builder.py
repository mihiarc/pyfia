"""Tests for the mortality query builder module."""

import pytest
from pyfia.estimation.mortality.query_builder import MortalityQueryBuilder


class TestMortalityQueryBuilder:
    """Test the MortalityQueryBuilder class."""
    
    def test_init(self):
        """Test query builder initialization."""
        builder = MortalityQueryBuilder()
        assert builder.db_type == "duckdb"
        
        builder_sqlite = MortalityQueryBuilder(db_type="sqlite")
        assert builder_sqlite.db_type == "sqlite"
        
    def test_plot_query_basic(self):
        """Test basic plot-level query generation."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_plot_query(
            evalid_list=[1, 2, 3],
            groups=[],
            mortality_col="SUBP_TPAMORT_UNADJ_AL_FOREST"
        )
        
        # Check structure
        assert "WITH plot_mortality AS" in query
        assert "EVALID IN (1, 2, 3)" in query
        assert "SUBP_TPAMORT_UNADJ_AL_FOREST" in query
        assert "GROUP BY" in query
        
    def test_plot_query_with_groups(self):
        """Test plot query with grouping variables."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_plot_query(
            evalid_list=[1],
            groups=["SPCD", "OWNGRPCD"],
            tree_domain="DIA >= 10.0",
            area_domain="LANDCLCD = 11"
        )
        
        # Check grouping columns
        assert "t.SPCD AS SPCD" in query
        assert "c.OWNGRPCD AS OWNGRPCD" in query
        
        # Check filters
        assert "DIA >= 10.0" in query
        assert "LANDCLCD = 11" in query
        
        # Check GROUP BY
        assert "GROUP BY" in query
        assert "t.SPCD" in query
        assert "c.OWNGRPCD" in query
        
    def test_plot_query_all_groups(self):
        """Test plot query with all supported grouping variables."""
        builder = MortalityQueryBuilder()
        
        all_groups = ["SPCD", "SPGRPCD", "OWNGRPCD", "UNITCD", 
                      "AGENTCD", "DSTRBCD1", "DSTRBCD2", "DSTRBCD3"]
        
        query = builder.build_plot_query(
            evalid_list=[1],
            groups=all_groups
        )
        
        # All groups should be selected
        for group in all_groups:
            assert f" AS {group}" in query
            
    def test_plot_query_invalid_groups(self):
        """Test plot query with invalid grouping variables."""
        builder = MortalityQueryBuilder()
        
        with pytest.raises(ValueError, match="Invalid grouping variables"):
            builder.build_plot_query(
                evalid_list=[1],
                groups=["INVALID_COL", "SPCD"]
            )
            
    def test_stratum_query(self):
        """Test stratum-level query generation."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_stratum_query(
            groups=["SPCD", "OWNGRPCD"]
        )
        
        assert "stratum_mortality AS" in query
        assert "SUM(MORTALITY_EXPANDED)" in query
        assert "COUNT(DISTINCT PLT_CN)" in query
        assert "MORT_SQUARED_SUM" in query  # For variance calc
        assert "GROUP BY" in query
        assert "SPCD" in query
        assert "OWNGRPCD" in query
        
    def test_population_query_basic(self):
        """Test basic population-level query."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_population_query(
            groups=["SPCD"],
            include_variance=False,
            include_totals=False
        )
        
        assert "population_estimates AS" in query
        assert "MORTALITY_TOTAL" in query
        assert "MORTALITY_PER_ACRE" in query
        assert "N_PLOTS" in query
        assert "N_TREES" in query
        
        # No variance columns
        assert "VARIANCE" not in query
        assert "SE" not in query
        
    def test_population_query_with_variance(self):
        """Test population query with variance calculation."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_population_query(
            groups=["SPCD"],
            include_variance=True
        )
        
        if builder.db_type == "duckdb":
            assert "VARIANCE" in query
            assert "SE" in query
            assert "SE_PERCENT" in query
        else:
            # SQLite uses different approach
            assert "MORT_SQUARED_SUM" in query
            
    def test_population_query_with_totals(self):
        """Test population query with totals."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_population_query(
            groups=["SPCD", "OWNGRPCD"],
            include_totals=True
        )
        
        assert "group_totals" in query
        assert "UNION ALL" in query
        assert "ORDER BY" in query
        
    def test_variance_select_duckdb(self):
        """Test variance column generation for DuckDB."""
        builder = MortalityQueryBuilder(db_type="duckdb")
        
        variance_sql = builder._build_variance_select()
        
        assert "VARIANCE" in variance_sql
        assert "SQRT(VARIANCE" in variance_sql
        assert "SE_PERCENT" in variance_sql
        
    def test_variance_select_sqlite(self):
        """Test variance column generation for SQLite."""
        builder = MortalityQueryBuilder(db_type="sqlite")
        
        variance_sql = builder._build_variance_select()
        
        # SQLite doesn't have VARIANCE function
        assert "VARIANCE" not in variance_sql
        assert "MORT_SQUARED_SUM" in variance_sql
        assert "TOTAL_PLOTS" in variance_sql
        
    def test_totals_query_duckdb(self):
        """Test totals query for DuckDB."""
        builder = MortalityQueryBuilder(db_type="duckdb")
        
        totals_sql = builder._build_totals_query(
            ["SPCD", "OWNGRPCD"],
            "base_table"
        )
        
        assert "group_totals" in totals_sql
        assert "ROLLUP" in totals_sql
        
    def test_totals_query_sqlite(self):
        """Test totals query for SQLite."""
        builder = MortalityQueryBuilder(db_type="sqlite")
        
        totals_sql = builder._build_totals_query(
            ["SPCD", "OWNGRPCD"],
            "base_table"
        )
        
        # SQLite doesn't support ROLLUP
        assert "ROLLUP" not in totals_sql
        assert "NULL AS SPCD" in totals_sql
        assert "NULL AS OWNGRPCD" in totals_sql
        
    def test_complete_query(self):
        """Test complete query generation."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_complete_query(
            evalid_list=[1, 2, 3],
            groups=["SPCD", "OWNGRPCD"],
            tree_domain="DIA > 5.0",
            mortality_col="SUBP_TPAMORT_UNADJ_AL_FOREST",
            include_variance=True,
            include_totals=True
        )
        
        # Should contain all CTEs
        assert "WITH plot_mortality AS" in query
        assert "stratum_mortality AS" in query
        assert "population_estimates AS" in query
        
        # Should have proper structure
        assert query.count("WITH") == 1  # Only one WITH
        assert query.count("SELECT") >= 3  # Multiple selects
        
    def test_clean_query(self):
        """Test query cleaning functionality."""
        builder = MortalityQueryBuilder()
        
        messy_query = """
        
        SELECT   *   
        FROM     table   
        
        WHERE    col = 1
        
        """
        
        clean = builder._clean_query(messy_query)
        
        # Should remove extra whitespace
        assert not clean.startswith("\n")
        assert not clean.endswith("\n")
        assert "   " not in clean
        
    def test_reference_table_joins(self):
        """Test reference table join generation."""
        builder = MortalityQueryBuilder()
        
        # Test species join
        joins = builder.get_reference_table_joins(["SPCD"])
        assert "species" in joins
        assert "REF_SPECIES" in joins["species"]
        assert "t.SPCD = species.SPCD" in joins["species"]
        
        # Test multiple joins
        joins = builder.get_reference_table_joins(
            ["SPCD", "OWNGRPCD", "AGENTCD"]
        )
        assert len(joins) == 3
        assert "species" in joins
        assert "owner" in joins
        assert "agent" in joins
        
        # Test all possible joins
        all_joins = builder.get_reference_table_joins(
            ["SPCD", "SPGRPCD", "OWNGRPCD", "AGENTCD"]
        )
        assert len(all_joins) == 4
        
    def test_mortality_column_variations(self):
        """Test different mortality column options."""
        builder = MortalityQueryBuilder()
        
        # Test with volume mortality column
        query = builder.build_plot_query(
            evalid_list=[1],
            groups=["SPCD"],
            mortality_col="SUBP_VOLCFMORT_AL_FOREST"
        )
        
        assert "SUBP_VOLCFMORT_AL_FOREST" in query
        assert "SUBP_TPAMORT_UNADJ_AL_FOREST" not in query
        
    def test_empty_evalid_list(self):
        """Test query generation with empty EVALID list."""
        builder = MortalityQueryBuilder()
        
        query = builder.build_plot_query(
            evalid_list=[],
            groups=["SPCD"]
        )
        
        # Should not have EVALID filter
        assert "EVALID IN" not in query
        # But should still have valid WHERE clause
        assert "WHERE" in query
        
    @pytest.mark.parametrize("db_type,expected", [
        ("duckdb", "VARIANCE"),
        ("sqlite", "MORT_SQUARED_SUM")
    ])
    def test_db_type_differences(self, db_type, expected):
        """Test database-specific query differences."""
        builder = MortalityQueryBuilder(db_type=db_type)
        
        query = builder.build_population_query(
            groups=["SPCD"],
            include_variance=True
        )
        
        assert expected in query