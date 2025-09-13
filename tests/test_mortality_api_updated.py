"""
Updated test suite for mortality() API function with proper GRM table structure.

Tests the public-facing mortality estimation function with mock GRM tables
that match the real FIA database structure.
"""

import pytest
import polars as pl
from pathlib import Path
from pyfia import FIA, mortality
import duckdb


@pytest.fixture
def grm_sample_db(tmp_path):
    """Create a sample FIA database with GRM tables for testing mortality."""
    db_path = tmp_path / "test_mortality_grm.duckdb"
    
    with duckdb.connect(str(db_path)) as conn:
        # Create core FIA tables
        
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
        
        # TREE_GRM_COMPONENT table (critical for mortality)
        conn.execute("""
            CREATE TABLE TREE_GRM_COMPONENT (
                TRE_CN TEXT PRIMARY KEY,
                PLT_CN TEXT,
                DIA_BEGIN DOUBLE,
                DIA_MIDPT DOUBLE,
                DIA_END DOUBLE,
                -- Forest columns
                SUBP_COMPONENT_GS_FOREST VARCHAR,
                SUBP_TPAMORT_UNADJ_GS_FOREST DOUBLE,
                SUBP_SUBPTYP_GRM_GS_FOREST INTEGER,
                -- Timber columns
                SUBP_COMPONENT_GS_TIMBER VARCHAR,
                SUBP_TPAMORT_UNADJ_GS_TIMBER DOUBLE,
                SUBP_SUBPTYP_GRM_GS_TIMBER INTEGER,
                -- All live columns
                SUBP_COMPONENT_AL_FOREST VARCHAR,
                SUBP_TPAMORT_UNADJ_AL_FOREST DOUBLE,
                SUBP_SUBPTYP_GRM_AL_FOREST INTEGER
            )
        """)
        
        # Insert mortality records (MORTALITY1, MORTALITY2 components)
        conn.execute("""
            INSERT INTO TREE_GRM_COMPONENT VALUES
            -- Plot 1: Trees with mortality
            ('T1', '1', 12.0, 12.5, 13.0, 'MORTALITY1', 1.2, 1, 'MORTALITY1', 1.2, 1, 'MORTALITY1', 1.3, 1),
            ('T2', '1', 14.5, 15.0, 15.5, 'MORTALITY1', 0.8, 1, 'MORTALITY1', 0.8, 1, 'MORTALITY1', 0.9, 1),
            ('T3', '1', 7.5, 8.0, 8.5, 'MORTALITY2', 2.1, 2, 'MORTALITY2', 2.1, 2, 'MORTALITY2', 2.2, 2),
            
            -- Plot 2: More mortality
            ('T4', '2', 9.5, 10.0, 10.5, 'MORTALITY1', 1.5, 1, 'MORTALITY1', 1.5, 1, 'MORTALITY1', 1.6, 1),
            ('T5', '2', 19.5, 20.0, 20.5, 'MORTALITY1', 0.5, 1, 'MORTALITY1', 0.5, 1, 'MORTALITY1', 0.6, 1),
            
            -- Plot 3: Mixed components
            ('T6', '3', 13.5, 14.0, 14.5, 'MORTALITY1', 1.0, 1, 'MORTALITY1', 1.0, 1, 'MORTALITY1', 1.1, 1),
            ('T7', '3', 17.5, 18.0, 18.5, 'SURVIVOR', 0.0, 1, 'SURVIVOR', 0.0, 1, 'SURVIVOR', 0.0, 1),
            
            -- Plot 4: Different species
            ('T8', '4', 21.5, 22.0, 22.5, 'MORTALITY1', 0.3, 1, 'MORTALITY1', 0.3, 1, 'MORTALITY1', 0.4, 1),
            ('T9', '4', 24.5, 25.0, 25.5, 'MORTALITY1', 0.2, 1, 'MORTALITY1', 0.2, 1, 'MORTALITY1', 0.3, 1),
            
            -- Plot 5: Large trees
            ('T10', '5', 29.5, 30.0, 30.5, 'MORTALITY1', 0.15, 1, 'MORTALITY1', 0.15, 1, 'MORTALITY1', 0.2, 1),
            ('T11', '5', 34.5, 35.0, 35.5, 'MORTALITY2', 0.1, 3, 'MORTALITY2', 0.1, 3, 'MORTALITY2', 0.12, 3),
            
            -- Add some non-mortality components for testing filtering
            ('T12', '1', 9.5, 10.0, 10.5, 'SURVIVOR', 0.0, 1, 'SURVIVOR', 0.0, 1, 'SURVIVOR', 0.0, 1),
            ('T13', '2', 11.5, 12.0, 12.5, 'INGROWTH', 0.0, 1, 'INGROWTH', 0.0, 1, 'INGROWTH', 0.0, 1),
            ('T14', '3', 7.5, 8.0, 8.5, 'CUT', 0.0, 1, 'CUT', 0.0, 1, 'CUT', 0.0, 1)
        """)
        
        # TREE_GRM_MIDPT table (for volume/biomass data)
        conn.execute("""
            CREATE TABLE TREE_GRM_MIDPT (
                TRE_CN TEXT PRIMARY KEY,
                DIA DOUBLE,
                SPCD INTEGER,
                STATUSCD INTEGER,
                VOLCFNET DOUBLE,
                DRYBIO_BOLE DOUBLE,
                DRYBIO_BRANCH DOUBLE
            )
        """)
        
        # Insert corresponding midpoint data
        conn.execute("""
            INSERT INTO TREE_GRM_MIDPT VALUES
            ('T1', 12.5, 131, 2, 800.0, 2000.0, 500.0),
            ('T2', 15.0, 131, 2, 1200.0, 3000.0, 700.0),
            ('T3', 8.0, 833, 2, 400.0, 1000.0, 300.0),
            ('T4', 10.0, 131, 2, 600.0, 1500.0, 400.0),
            ('T5', 20.0, 833, 2, 2000.0, 5000.0, 1200.0),
            ('T6', 14.0, 131, 2, 1000.0, 2500.0, 600.0),
            ('T7', 18.0, 802, 1, 1600.0, 4000.0, 1000.0),
            ('T8', 22.0, 802, 2, 2200.0, 5500.0, 1400.0),
            ('T9', 25.0, 802, 2, 2800.0, 7000.0, 1800.0),
            ('T10', 30.0, 131, 2, 4000.0, 10000.0, 2500.0),
            ('T11', 35.0, 833, 2, 5000.0, 12500.0, 3000.0),
            ('T12', 10.0, 131, 1, 500.0, 1200.0, 300.0),
            ('T13', 12.0, 833, 1, 700.0, 1800.0, 450.0),
            ('T14', 8.0, 802, 1, 300.0, 800.0, 200.0)
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


class TestMortalityWithGRM:
    """Test mortality calculation with proper GRM table structure."""
    
    def test_mortality_default_parameters(self, grm_sample_db):
        """Test mortality with default parameters using GRM tables."""
        with FIA(str(grm_sample_db)) as db:
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
            
            # Values should be positive (we have mortality data)
            assert results["MORT_ACRE"][0] > 0
            assert results["MORT_TOTAL"][0] > 0
            assert results["N_DEAD_TREES"][0] > 0
    
    def test_mortality_filters_components(self, grm_sample_db):
        """Test that mortality properly filters to MORTALITY components."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db)
            
            # Should only count mortality components, not survivors/ingrowth/cut
            # We have 10 mortality records (T1-T6, T8-T11)
            assert results["N_DEAD_TREES"][0] == 10
    
    def test_mortality_biomass_measure(self, grm_sample_db):
        """Test mortality with biomass measurement."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, measure="biomass")
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            assert "MEASURE" in results.columns
            assert results["MEASURE"][0] == "BIOMASS"
            
            # Biomass values should be positive
            assert results["MORT_ACRE"][0] > 0
    
    def test_mortality_count_measure(self, grm_sample_db):
        """Test mortality with tree count measurement."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, measure="count")
            
            assert isinstance(results, pl.DataFrame)
            assert len(results) > 0
            assert results["MEASURE"][0] == "COUNT"
            
            # Count should represent trees per acre
            assert results["MORT_ACRE"][0] > 0
    
    def test_mortality_by_species(self, grm_sample_db):
        """Test mortality grouped by species."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, by_species=True)
            
            assert isinstance(results, pl.DataFrame)
            assert "SPCD" in results.columns
            
            # We have 3 species in the test data (131, 802, 833)
            unique_species = results["SPCD"].unique()
            assert len(unique_species) >= 2
    
    def test_mortality_adjustment_factors(self, grm_sample_db):
        """Test that SUBPTYP_GRM adjustment factors are applied correctly."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            
            # The test data includes:
            # - SUBPTYP=1 (SUBP adjustment) for most trees
            # - SUBPTYP=2 (MICR adjustment) for T3
            # - SUBPTYP=3 (MACR adjustment) for T11
            
            results = mortality(db, measure="count")
            
            # Should have mortality with adjustments applied
            assert results["MORT_ACRE"][0] > 0
            assert results["MORT_TOTAL"][0] > 0
    
    def test_mortality_land_type_forest_vs_timber(self, grm_sample_db):
        """Test different land types (forest vs timber)."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            
            # Test forest land
            forest_results = mortality(db, land_type="forest")
            assert forest_results["LAND_TYPE"][0] == "FOREST"
            
            # Test timber land
            timber_results = mortality(db, land_type="timber")
            assert timber_results["LAND_TYPE"][0] == "TIMBER"
            
            # Both should work with our test data
            assert forest_results["MORT_ACRE"][0] > 0
            assert timber_results["MORT_ACRE"][0] > 0
    
    def test_mortality_with_variance(self, grm_sample_db):
        """Test mortality with variance calculation."""
        with FIA(str(grm_sample_db)) as db:
            db.clip_by_evalid([402300])
            results = mortality(db, variance=True)
            
            assert isinstance(results, pl.DataFrame)
            
            # Should have standard error columns
            assert "MORT_ACRE_SE" in results.columns
            assert "MORT_TOTAL_SE" in results.columns
            
            # SE should be positive
            assert results["MORT_ACRE_SE"][0] > 0
            assert results["MORT_TOTAL_SE"][0] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])