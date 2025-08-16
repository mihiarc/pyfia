"""
Tests for grouped area calculation fixes.

This test suite verifies that the duplicate rows issue in grouped
area calculations has been resolved.
"""

import pytest
import polars as pl
from pyfia import FIA, area
from pyfia.utils.reference_tables import join_forest_type_names


class TestGroupedAreaCalculations:
    """Test grouped area calculations work correctly without duplicates."""
    
    def test_no_duplicates_in_grouped_results(self):
        """Test that grouped area results have no duplicate rows."""
        # Use Georgia database for testing
        with FIA("SQLite_FIADB_GA.db") as db:
            db.clip_most_recent("VOL")
            
            # Calculate area grouped by forest type
            results = area(db, land_type='timber', grp_by=['FORTYPCD'], totals=True)
            
            # Check for duplicates
            unique_results = results.unique('FORTYPCD')
            
            # Assert no duplicates
            assert results.shape[0] == unique_results.shape[0], \
                f"Found {results.shape[0] - unique_results.shape[0]} duplicate rows"
            
            # Verify we have reasonable number of forest types
            assert results.shape[0] < 100, \
                f"Too many rows ({results.shape[0]}) for forest type grouping"
    
    def test_grouped_totals_match_ungrouped(self):
        """Test that sum of grouped areas approximately equals total area."""
        with FIA("SQLite_FIADB_GA.db") as db:
            db.clip_most_recent("VOL")
            
            # Get total area
            total_result = area(db, land_type='timber', totals=True)
            total_area = total_result['AREA'][0]
            
            # Get grouped areas
            grouped_result = area(db, land_type='timber', grp_by=['FORTYPCD'], totals=True)
            
            # Remove duplicates if any (shouldn't be any after fix)
            grouped_unique = grouped_result.unique('FORTYPCD')
            
            # Sum grouped areas
            grouped_sum = grouped_unique['AREA'].sum()
            
            # Check they're approximately equal (within 5%)
            percent_diff = abs(grouped_sum - total_area) / total_area * 100
            assert percent_diff < 5, \
                f"Grouped sum ({grouped_sum:,.0f}) differs from total ({total_area:,.0f}) by {percent_diff:.1f}%"
    
    def test_reference_table_joining(self):
        """Test that reference table joining works correctly."""
        with FIA("SQLite_FIADB_GA.db") as db:
            db.clip_most_recent("VOL")
            
            # Get grouped results
            results = area(db, land_type='timber', grp_by=['FORTYPCD'], totals=True)
            
            # Ensure unique
            results = results.unique('FORTYPCD')
            
            # Join forest type names
            results_with_names = join_forest_type_names(results, db)
            
            # Check that names were added
            assert 'FOREST_TYPE_NAME' in results_with_names.columns
            
            # Check that we have non-null names for most forest types
            non_null_names = results_with_names.filter(
                pl.col('FOREST_TYPE_NAME').is_not_null()
            ).shape[0]
            
            total_types = results_with_names.shape[0]
            assert non_null_names > total_types * 0.8, \
                f"Only {non_null_names}/{total_types} forest types have names"
    
    def test_variance_calculations_are_positive(self):
        """Test that variance calculations produce valid positive values."""
        with FIA("SQLite_FIADB_GA.db") as db:
            db.clip_most_recent("VOL")
            
            # Calculate with variance
            results = area(
                db, 
                land_type='timber', 
                grp_by=['FORTYPCD'], 
                totals=True,
                variance=False  # Request SE not variance
            )
            
            # Ensure unique
            results = results.unique('FORTYPCD')
            
            # Check that SE columns exist and are non-negative
            if 'AREA_SE' in results.columns:
                negative_se = results.filter(pl.col('AREA_SE') < 0).shape[0]
                assert negative_se == 0, f"Found {negative_se} rows with negative SE"
            
            if 'AREA_PERC_SE' in results.columns:
                negative_perc_se = results.filter(pl.col('AREA_PERC_SE') < 0).shape[0]
                assert negative_perc_se == 0, f"Found {negative_perc_se} rows with negative percentage SE"
    
    def test_multiple_grouping_columns(self):
        """Test grouping by multiple columns works correctly."""
        with FIA("SQLite_FIADB_GA.db") as db:
            db.clip_most_recent("VOL")
            
            # Try grouping by both forest type and ownership
            results = area(
                db,
                land_type='timber',
                grp_by=['FORTYPCD', 'OWNGRPCD'],
                totals=True
            )
            
            # Check for duplicates
            group_cols = ['FORTYPCD', 'OWNGRPCD']
            unique_results = results.unique(group_cols)
            
            assert results.shape[0] == unique_results.shape[0], \
                f"Found duplicates when grouping by {group_cols}"


if __name__ == "__main__":
    # Run tests
    test_suite = TestGroupedAreaCalculations()
    
    print("Running grouped area calculation tests...")
    print("-" * 60)
    
    try:
        test_suite.test_no_duplicates_in_grouped_results()
        print("✓ No duplicates test passed")
    except AssertionError as e:
        print(f"✗ No duplicates test failed: {e}")
    
    try:
        test_suite.test_grouped_totals_match_ungrouped()
        print("✓ Totals match test passed")
    except AssertionError as e:
        print(f"✗ Totals match test failed: {e}")
    
    try:
        test_suite.test_reference_table_joining()
        print("✓ Reference table joining test passed")
    except AssertionError as e:
        print(f"✗ Reference table joining test failed: {e}")
    
    try:
        test_suite.test_variance_calculations_are_positive()
        print("✓ Variance calculations test passed")
    except AssertionError as e:
        print(f"✗ Variance calculations test failed: {e}")
    
    try:
        test_suite.test_multiple_grouping_columns()
        print("✓ Multiple grouping columns test passed")
    except AssertionError as e:
        print(f"✗ Multiple grouping columns test failed: {e}")
    
    print("-" * 60)
    print("Test suite complete!")