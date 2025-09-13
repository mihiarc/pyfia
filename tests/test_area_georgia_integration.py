"""
Integration tests for area() function using real Georgia FIA data.

These tests validate that the optimized area() function produces correct
results with the actual georgia.duckdb database, including the lazy loading
optimizations.
"""

import pytest
from pathlib import Path
import polars as pl

from pyfia import FIA, area


# Skip all tests if database not available
GEORGIA_DB = Path("data/georgia.duckdb")
pytestmark = pytest.mark.skipif(
    not GEORGIA_DB.exists(),
    reason="georgia.duckdb not found - real data tests require this database"
)


class TestAreaGeorgiaIntegration:
    """Integration tests with real Georgia FIA data."""
    
    def test_basic_forest_area(self):
        """Test basic forest area calculation with real data."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)  # Georgia
            db.clip_most_recent(eval_type="ALL")
            
            result = area(db, land_type="forest")
            
            # Validate structure
            assert isinstance(result, pl.DataFrame)
            assert "AREA" in result.columns
            assert "AREA_SE" in result.columns
            assert "N_PLOTS" in result.columns
            
            # Validate values - Georgia forest area
            total_area = result["AREA"].sum()
            assert 20_000_000 < total_area < 30_000_000, \
                f"Georgia forest area {total_area:,.0f} outside expected range"
            
            # Should have processed many plots
            n_plots = result["N_PLOTS"].sum()
            assert n_plots > 1000, f"Only {n_plots} plots processed"
    
    def test_area_by_ownership(self):
        """Test grouping by ownership with real data."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            result = area(db, grp_by="OWNGRPCD", land_type="forest")
            
            # Should have multiple ownership groups
            assert len(result) > 1
            assert "OWNGRPCD" in result.columns
            
            # Check standard ownership codes
            ownership_codes = set(result["OWNGRPCD"].to_list())
            # 10=National Forest, 20=Other Federal, 30=State/Local, 40=Private
            assert 40 in ownership_codes, "Private ownership (40) should be present"
            
            # Private should be majority in Georgia
            private_area = result.filter(pl.col("OWNGRPCD") == 40)["AREA"].sum()
            total_area = result["AREA"].sum()
            assert private_area > total_area * 0.5, \
                "Private ownership should be >50% in Georgia"
    
    def test_area_by_forest_type(self):
        """Test grouping by forest type with real data."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            result = area(db, grp_by="FORTYPCD", land_type="forest")
            
            # Should have many forest types
            assert len(result) > 10, "Georgia should have many forest types"
            assert "FORTYPCD" in result.columns
            
            # Loblolly pine (161) should be significant in Georgia
            if 161 in result["FORTYPCD"].to_list():
                loblolly_area = result.filter(pl.col("FORTYPCD") == 161)["AREA"].sum()
                assert loblolly_area > 1_000_000, \
                    "Loblolly pine should cover >1M acres in Georgia"
    
    def test_timber_vs_forest_area(self):
        """Test timber land filtering with real data."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            forest_result = area(db, land_type="forest")
            timber_result = area(db, land_type="timber")
            
            forest_area = forest_result["AREA"].sum()
            timber_area = timber_result["AREA"].sum()
            
            # Timber should be less than total forest
            assert timber_area < forest_area, \
                "Timber area should be less than total forest area"
            
            # But timber should be majority of forest in Georgia
            assert timber_area > forest_area * 0.8, \
                "Timber should be >80% of forest in Georgia"
    
    def test_area_with_domain_filter(self):
        """Test domain filtering with real data."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            # Get all forest area
            all_forest = area(db, land_type="forest")
            
            # Get mature forest (>50 years)
            mature_forest = area(db, 
                               land_type="forest",
                               area_domain="STDAGE > 50")
            
            all_area = all_forest["AREA"].sum()
            mature_area = mature_forest["AREA"].sum()
            
            # Mature forest should be less than total
            assert mature_area < all_area, \
                "Mature forest should be subset of all forest"
            
            # But should still be substantial
            assert mature_area > 1_000_000, \
                "Georgia should have >1M acres of mature forest"
    
    def test_multiple_grouping_columns(self):
        """Test multiple group-by columns with real data."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            result = area(db, 
                        grp_by=["OWNGRPCD", "STDSZCD"],
                        land_type="forest")
            
            # Should have many combinations
            assert len(result) > 5
            assert "OWNGRPCD" in result.columns
            assert "STDSZCD" in result.columns
            
            # Each ownership should have multiple size classes
            ownership_groups = result.group_by("OWNGRPCD").agg(
                pl.col("STDSZCD").n_unique().alias("n_size_classes")
            )
            assert all(ownership_groups["n_size_classes"] > 1), \
                "Each ownership should have multiple size classes"
    
    def test_column_loading_efficiency(self):
        """Test that lazy loading is working efficiently."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            # Simple query should load minimal columns
            # This is hard to test directly, but we can verify it works
            result = area(db, land_type="forest")
            assert result is not None
            
            # Complex query with domain should still work
            result = area(db,
                        grp_by=["FORTYPCD", "OWNGRPCD"],
                        land_type="timber",
                        area_domain="STDAGE > 30 AND SITECLCD < 4")
            assert result is not None
            assert len(result) > 0
    
    def test_known_value_validation(self):
        """Test against a known value from our earlier testing."""
        with FIA(GEORGIA_DB) as db:
            db.clip_by_state(13)
            db.clip_most_recent(eval_type="ALL")
            
            result = area(db, land_type="forest")
            total_area = result["AREA"].sum()
            
            # From our testing, Georgia forest area is approximately 23,673,198 acres
            # Allow 1% tolerance for floating point differences
            expected = 23_673_198
            tolerance = expected * 0.01
            
            assert abs(total_area - expected) < tolerance, \
                f"Forest area {total_area:,.0f} differs from expected {expected:,}"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v"])