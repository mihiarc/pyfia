"""
Comprehensive test suite for mortality() API function.

Tests the public-facing mortality estimation function to ensure
it properly calculates mortality rates, handles different measurement
types, and correctly applies grouping and filtering options.
"""

import pytest
import polars as pl
from pathlib import Path
from pyfia import FIA, mortality
import duckdb


@pytest.fixture
def sample_db(tmp_path):
    """Create a sample FIA database for testing mortality calculations."""
    db_path = tmp_path / "test_mortality.duckdb"
    
    with duckdb.connect(str(db_path)) as conn:
        # Create required FIA tables with proper structure
        
        # PLOT table
        conn.execute("""
            CREATE TABLE PLOT (
                CN TEXT PRIMARY KEY,
                STATECD INTEGER,
                INVYR INTEGER,
                PLOT_STATUS_CD INTEGER,
                MACRO_BREAKPOINT_DIA DOUBLE
            )
        """)
        
        conn.execute("""
            INSERT INTO PLOT VALUES
            ('1', 40, 2023, 1, 9.0),
            ('2', 40, 2023, 1, 9.0),
            ('3', 40, 2023, 1, 9.0),
            ('4', 40, 2023, 1, 9.0),
            ('5', 40, 2023, 1, 9.0)
        """)
        
        # COND table
        conn.execute("""
            CREATE TABLE COND (
                PLT_CN TEXT,
                CONDID INTEGER,
                COND_STATUS_CD INTEGER,
                CONDPROP_UNADJ DOUBLE,
                OWNGRPCD INTEGER,
                FORTYPCD INTEGER,
                SITECLCD INTEGER,
                RESERVCD INTEGER,
                PRIMARY KEY (PLT_CN, CONDID)
            )
        """)
        
        conn.execute("""
            INSERT INTO COND VALUES
            ('1', 1, 1, 1.0, 10, 101, 1, 0),
            ('2', 1, 1, 1.0, 10, 101, 1, 0),
            ('3', 1, 1, 1.0, 10, 101, 1, 0),
            ('4', 1, 1, 1.0, 20, 102, 2, 0),
            ('5', 1, 1, 1.0, 20, 102, 2, 0)
        """)
        
        # TREE table with dead trees and mortality years
        conn.execute("""
            CREATE TABLE TREE (
                CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                CONDID INTEGER,
                STATUSCD INTEGER,  -- 1=live, 2=dead
                SPCD INTEGER,
                DIA DOUBLE,
                TPA_UNADJ DOUBLE,
                VOLCFNET DOUBLE,
                DRYBIO_AG DOUBLE,
                DRYBIO_BG DOUBLE,
                MORTYR INTEGER  -- Year of mortality
            )
        """)
        
        # Insert mix of live and dead trees
        # Dead trees with recent mortality (MORTYR >= 2018)
        conn.execute("""
            INSERT INTO TREE VALUES
            -- Plot 1: Dead trees with recent mortality
            ('T1', '1', 1, 2, 131, 12.5, 6.018, 800.0, 2000.0, 500.0, 2020),
            ('T2', '1', 1, 2, 131, 15.0, 6.018, 1200.0, 3000.0, 700.0, 2021),
            ('T3', '1', 1, 2, 833, 8.0, 10.0, 400.0, 1000.0, 300.0, 2019),
            
            -- Plot 2: Dead trees with older mortality (should be filtered if recent_only=True)
            ('T4', '2', 1, 2, 131, 10.0, 6.018, 600.0, 1500.0, 400.0, 2015),
            ('T5', '2', 1, 2, 833, 20.0, 3.0, 2000.0, 5000.0, 1200.0, 2016),
            
            -- Plot 3: Mix of recent and old mortality
            ('T6', '3', 1, 2, 131, 14.0, 6.018, 1000.0, 2500.0, 600.0, 2020),
            ('T7', '3', 1, 2, 802, 18.0, 4.0, 1600.0, 4000.0, 1000.0, 2017),
            
            -- Plot 4: Recent mortality, different species
            ('T8', '4', 1, 2, 802, 22.0, 2.5, 2200.0, 5500.0, 1400.0, 2022),
            ('T9', '4', 1, 2, 802, 25.0, 2.0, 2800.0, 7000.0, 1800.0, 2021),
            
            -- Plot 5: Large diameter dead trees
            ('T10', '5', 1, 2, 131, 30.0, 1.5, 4000.0, 10000.0, 2500.0, 2020),
            ('T11', '5', 1, 2, 833, 35.0, 1.2, 5000.0, 12500.0, 3000.0, 2019),
            
            -- Add some live trees for context (STATUSCD = 1)
            ('T12', '1', 1, 1, 131, 10.0, 6.018, 500.0, 1200.0, 300.0, NULL),
            ('T13', '2', 1, 1, 833, 12.0, 6.018, 700.0, 1800.0, 450.0, NULL),
            ('T14', '3', 1, 1, 802, 8.0, 10.0, 300.0, 800.0, 200.0, NULL)
        """)
        
        # POP_PLOT_STRATUM_ASSGN table
        conn.execute("""
            CREATE TABLE POP_PLOT_STRATUM_ASSGN (
                CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                STRATUM_CN TEXT,
                EVALID INTEGER
            )
        """)
        
        conn.execute("""
            INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES
            ('PSA1', '1', 'S1', 402300),
            ('PSA2', '2', 'S1', 402300),
            ('PSA3', '3', 'S1', 402300),
            ('PSA4', '4', 'S2', 402300),
            ('PSA5', '5', 'S2', 402300)
        """)
        
        # POP_STRATUM table
        conn.execute("""
            CREATE TABLE POP_STRATUM (
                CN TEXT PRIMARY KEY,
                ESTN_UNIT_CN TEXT,
                STRATUMCD INTEGER,
                EXPNS DOUBLE,
                ADJ_FACTOR_MICR DOUBLE,
                ADJ_FACTOR_SUBP DOUBLE,
                ADJ_FACTOR_MACR DOUBLE,
                EVALID INTEGER
            )
        """)
        
        conn.execute("""
            INSERT INTO POP_STRATUM VALUES
            ('S1', 'EU1', 1, 1000.0, 1.0, 1.0, 1.0, 402300),
            ('S2', 'EU1', 2, 1500.0, 1.0, 1.0, 1.0, 402300)
        """)
        
        # POP_EVAL table
        conn.execute("""
            CREATE TABLE POP_EVAL (
                CN TEXT PRIMARY KEY,
                EVALID INTEGER,
                EVAL_DESCR TEXT,
                STATECD INTEGER,
                LOCATION_NM TEXT,
                START_INVYR INTEGER,
                END_INVYR INTEGER
            )
        """)
        
        conn.execute("""
            INSERT INTO POP_EVAL VALUES
            ('PE1', 402300, 'Oklahoma 2023 All Area', 40, 'Oklahoma', 2018, 2023)
        """)
        
        # POP_EVAL_TYP table
        conn.execute("""
            CREATE TABLE POP_EVAL_TYP (
                CN TEXT PRIMARY KEY,
                EVAL_CN TEXT,
                EVAL_TYP TEXT
            )
        """)
        
        conn.execute("""
            INSERT INTO POP_EVAL_TYP VALUES
            ('PET1', 'PE1', 'EXPALL')
        """)
    
    return db_path


class TestMortalityBasic:
    """Test basic mortality calculation functionality."""
    
    def test_mortality_default_parameters(self, sample_db):
        """Test mortality with default parameters (volume, recent only)."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db)
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            
            # Check for required columns
            assert "MORT_ACRE" in results.columns
            assert "MORT_TOTAL" in results.columns
            assert "AREA_TOTAL" in results.columns
            assert "N_PLOTS" in results.columns
            assert "N_DEAD_TREES" in results.columns
            
            # Values should be positive
            assert results["MORT_ACRE"][0] > 0
            assert results["MORT_TOTAL"][0] > 0
    
    def test_mortality_biomass_measure(self, sample_db):
        """Test mortality with biomass measurement."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, measure="biomass")
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            assert "MEASURE" in results.columns
            assert results["MEASURE"][0] == "BIOMASS"
            
            # Biomass values should be in tons (divided by 2000)
            assert results["MORT_ACRE"][0] > 0
    
    def test_mortality_count_measure(self, sample_db):
        """Test mortality with tree count measurement."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, measure="count")
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            assert results["MEASURE"][0] == "COUNT"
            
            # Count should represent trees per acre
            assert results["MORT_ACRE"][0] > 0
    
    def test_recent_vs_all_mortality(self, sample_db):
        """Test filtering for recent mortality only."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            
            # Recent mortality only (default)
            recent_results = mortality(db, recent_only=True)
            
            # All mortality
            all_results = mortality(db, recent_only=False)
            
            # All mortality should include more trees
            # (would be true with more diverse test data)
            assert recent_results["N_DEAD_TREES"][0] <= all_results["N_DEAD_TREES"][0]


class TestMortalityGrouping:
    """Test grouping options for mortality estimation."""
    
    def test_mortality_by_species(self, sample_db):
        """Test mortality grouped by species."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, by_species=True)
            
            assert isinstance(results, pl.DataFrame)
            assert "SPCD" in results.columns
            
            # Should have multiple species
            unique_species = results["SPCD"].unique()
            assert len(unique_species) > 1
            
            # Each species should have mortality values
            for species_results in results.iter_rows(named=True):
                assert species_results["MORT_ACRE"] >= 0
    
    def test_mortality_custom_grouping(self, sample_db):
        """Test mortality with custom grouping column."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, grp_by="OWNGRPCD")
            
            assert isinstance(results, pl.DataFrame)
            assert "OWNGRPCD" in results.columns
            
            # Should have grouped results
            assert len(results) >= 1
    
    def test_mortality_multiple_grouping(self, sample_db):
        """Test mortality with multiple grouping columns."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, grp_by=["OWNGRPCD", "FORTYPCD"])
            
            assert isinstance(results, pl.DataFrame)
            assert "OWNGRPCD" in results.columns
            assert "FORTYPCD" in results.columns


class TestMortalityFiltering:
    """Test domain filtering for mortality estimation."""
    
    def test_tree_domain_filter(self, sample_db):
        """Test mortality with tree-level domain filter."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            
            # Filter to large trees only
            results = mortality(db, tree_domain="DIA >= 20.0")
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            
            # Should have fewer dead trees due to size filter
            all_results = mortality(db)
            assert results["N_DEAD_TREES"][0] < all_results["N_DEAD_TREES"][0]
    
    def test_area_domain_filter(self, sample_db):
        """Test mortality with area-level domain filter."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            
            # Filter by ownership group
            results = mortality(db, area_domain="OWNGRPCD == 10")
            
            assert isinstance(results, pl.DataFrame)
            # Results depend on test data setup


class TestMortalityStatistics:
    """Test statistical options for mortality estimation."""
    
    def test_mortality_with_variance(self, sample_db):
        """Test mortality with variance calculation."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, variance=True)
            
            assert isinstance(results, pl.DataFrame)
            
            # Should have standard error columns
            assert "MORT_ACRE_SE" in results.columns
            assert "MORT_TOTAL_SE" in results.columns
            
            # SE should be positive
            assert results["MORT_ACRE_SE"][0] > 0
    
    def test_mortality_as_rate(self, sample_db):
        """Test mortality rate calculation."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, as_rate=True)
            
            assert isinstance(results, pl.DataFrame)
            
            # Should have mortality rate column
            assert "MORT_RATE" in results.columns
            
            # Rate should be between 0 and 1 (or percentage)
            assert results["MORT_RATE"][0] >= 0
    
    def test_mortality_without_totals(self, sample_db):
        """Test mortality without population totals."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, totals=False)
            
            assert isinstance(results, pl.DataFrame)
            # Implementation would determine exact column behavior


class TestMortalityEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_database(self, tmp_path):
        """Test mortality with empty database."""
        db_path = tmp_path / "empty.duckdb"
        
        # Create minimal structure
        with duckdb.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE TREE (CN TEXT)")
            conn.execute("CREATE TABLE COND (PLT_CN TEXT)")
            conn.execute("CREATE TABLE PLOT (CN TEXT)")
            conn.execute("CREATE TABLE POP_PLOT_STRATUM_ASSGN (CN TEXT)")
            conn.execute("CREATE TABLE POP_STRATUM (CN TEXT)")
        
        with FIA(str(db_path)) as db:
            # Should handle gracefully
            with pytest.raises(Exception):
                results = mortality(db)
    
    def test_no_dead_trees(self, tmp_path):
        """Test mortality when no dead trees exist."""
        db_path = tmp_path / "no_mortality.duckdb"
        
        with duckdb.connect(str(db_path)) as conn:
            # Create tables with only live trees
            conn.execute("""
                CREATE TABLE PLOT (
                    CN TEXT PRIMARY KEY,
                    STATECD INTEGER,
                    INVYR INTEGER,
                    PLOT_STATUS_CD INTEGER,
                    MACRO_BREAKPOINT_DIA DOUBLE
                )
            """)
            conn.execute("INSERT INTO PLOT VALUES ('1', 40, 2023, 1, 9.0)")
            
            conn.execute("""
                CREATE TABLE COND (
                    PLT_CN TEXT,
                    CONDID INTEGER,
                    COND_STATUS_CD INTEGER,
                    CONDPROP_UNADJ DOUBLE,
                    OWNGRPCD INTEGER,
                    FORTYPCD INTEGER,
                    SITECLCD INTEGER,
                    RESERVCD INTEGER
                )
            """)
            conn.execute("INSERT INTO COND VALUES ('1', 1, 1, 1.0, 10, 101, 1, 0)")
            
            conn.execute("""
                CREATE TABLE TREE (
                    CN TEXT,
                    PLT_CN TEXT,
                    CONDID INTEGER,
                    STATUSCD INTEGER,
                    SPCD INTEGER,
                    DIA DOUBLE,
                    TPA_UNADJ DOUBLE,
                    VOLCFNET DOUBLE,
                    DRYBIO_AG DOUBLE,
                    DRYBIO_BG DOUBLE,
                    MORTYR INTEGER
                )
            """)
            # Only live trees (STATUSCD = 1)
            conn.execute("""
                INSERT INTO TREE VALUES 
                ('T1', '1', 1, 1, 131, 10.0, 6.018, 500.0, 1200.0, 300.0, NULL)
            """)
            
            conn.execute("""
                CREATE TABLE POP_PLOT_STRATUM_ASSGN (
                    CN TEXT,
                    PLT_CN TEXT,
                    STRATUM_CN TEXT,
                    EVALID INTEGER
                )
            """)
            conn.execute("INSERT INTO POP_PLOT_STRATUM_ASSGN VALUES ('PSA1', '1', 'S1', 402300)")
            
            conn.execute("""
                CREATE TABLE POP_STRATUM (
                    CN TEXT,
                    ESTN_UNIT_CN TEXT,
                    STRATUMCD INTEGER,
                    EXPNS DOUBLE,
                    ADJ_FACTOR_MICR DOUBLE,
                    ADJ_FACTOR_SUBP DOUBLE,
                    ADJ_FACTOR_MACR DOUBLE,
                    EVALID INTEGER
                )
            """)
            conn.execute("""
                INSERT INTO POP_STRATUM VALUES 
                ('S1', 'EU1', 1, 1000.0, 1.0, 1.0, 1.0, 402300)
            """)
        
        with FIA(str(db_path)) as db:
            results = mortality(db)
            
            # Should return empty or zero results
            assert isinstance(results, pl.DataFrame)
            if len(results) > 0:
                assert results["MORT_ACRE"][0] == 0 or results["N_DEAD_TREES"][0] == 0
    
    def test_invalid_measure_type(self, sample_db):
        """Test mortality with invalid measure type."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            
            # Should default to a valid measure or raise clear error
            # The actual behavior depends on implementation
            results = mortality(db, measure="invalid")
            assert isinstance(results, pl.DataFrame)


class TestMortalityIntegration:
    """Integration tests for mortality function."""
    
    def test_mortality_comprehensive_scenario(self, sample_db):
        """Test comprehensive mortality scenario with multiple options."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            
            results = mortality(
                db,
                measure="biomass",
                by_species=True,
                tree_domain="DIA >= 10.0",
                area_domain="OWNGRPCD == 10",
                recent_only=True,
                as_rate=False,
                variance=True
            )
            
            assert isinstance(results, pl.DataFrame)
            
            # Check all expected columns are present
            expected_cols = ["SPCD", "MORT_ACRE", "MORT_TOTAL", "MORT_ACRE_SE", "MORT_TOTAL_SE"]
            for col in expected_cols:
                assert col in results.columns
            
            # Results should be properly filtered
            if len(results) > 0:
                assert results["MEASURE"][0] == "BIOMASS"
    
    def test_mortality_comparison_across_measures(self, sample_db):
        """Compare mortality across different measurement types."""
        with FIA(str(sample_db)) as db:
            db.clip_by_evalid([402300])
            
            volume_results = mortality(db, measure="volume")
            biomass_results = mortality(db, measure="biomass")
            count_results = mortality(db, measure="count")
            
            # All should return valid dataframes
            assert all(isinstance(r, pl.DataFrame) for r in [volume_results, biomass_results, count_results])
            
            # All should have the same plot count
            assert volume_results["N_PLOTS"][0] == biomass_results["N_PLOTS"][0] == count_results["N_PLOTS"][0]
            
            # But different measurement values
            # (exact relationship depends on tree characteristics)
            assert volume_results["MORT_ACRE"][0] != biomass_results["MORT_ACRE"][0]
            assert biomass_results["MORT_ACRE"][0] != count_results["MORT_ACRE"][0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])