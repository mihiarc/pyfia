"""
Real integration test for Texas loblolly pine tree count.

This test requires a real FIA database and verifies the actual count
matches the expected value of 1,747,270,660 trees.

This is NOT a unit test - it's an integration test that validates
the complete workflow with real data.
"""

import os
from pathlib import Path

import pytest

from pyfia.core import FIA
from pyfia.database.query_interface import DuckDBQueryInterface
from pyfia.estimation.tree import tree_count
from pyfia.filters.evalid import get_recommended_evalid

# Mark this as an integration test that requires database
pytestmark = pytest.mark.integration


class TestTexasLoblollyRealData:
    """Integration test for Texas loblolly pine count with real database."""

    @pytest.fixture(scope="class")
    def db_path(self):
        """Find and validate real FIA database."""
        possible_paths = [
            "fia.duckdb",
            "fia_datamart.db",
            "data/fia.duckdb",
            "data/fia_datamart.db",
            "../fia.duckdb",
            "../fia_datamart.db",
            os.path.expanduser("~/fia.duckdb"),
            os.path.expanduser("~/fia_datamart.db"),
            os.path.expanduser("~/Downloads/fia.duckdb"),
            os.path.expanduser("~/Downloads/fia_datamart.db"),
            "/tmp/fia.duckdb",
            "/tmp/fia_datamart.db",
        ]

        for path in possible_paths:
            if Path(path).exists() and Path(path).stat().st_size > 1000000:  # At least 1MB
                return str(path)

        pytest.skip(
            "No FIA database available for integration test. "
            "Please place fia_datamart.db in one of these locations:\n" +
            "\n".join(f"  - {p}" for p in possible_paths)
        )

    @pytest.fixture(scope="class")
    def query_interface(self, db_path):
        """Initialize real database query interface."""
        return DuckDBQueryInterface(db_path)

    @pytest.fixture(scope="class")
    def fia_instance(self, db_path):
        """Initialize real FIA instance."""
        return FIA(db_path)

    def test_database_has_required_tables(self, query_interface):
        """Verify database has all required tables for tree counting."""

        required_tables = [
            'POP_EVAL',
            'POP_EVAL_TYP',
            'POP_STRATUM',
            'POP_PLOT_STRATUM_ASSGN',
            'PLOT',
            'COND',
            'TREE',
            'REF_SPECIES'
        ]

        for table in required_tables:
            result = query_interface.execute_query(f"SELECT COUNT(*) as count FROM {table} LIMIT 1")
            count = result.row(0, named=True)['count']
            assert count > 0, f"Table {table} is empty or missing"
            print(f"✅ {table}: {count:,} records")

    def test_texas_has_loblolly_pine_data(self, query_interface):
        """Verify Texas has loblolly pine tree data."""

        # Check for Texas plots
        texas_plots_query = """
        SELECT COUNT(DISTINCT CN) as plot_count
        FROM PLOT
        WHERE STATECD = 48
        """
        result = query_interface.execute_query(texas_plots_query)
        texas_plots = result.row(0, named=True)['plot_count']
        assert texas_plots > 0, "No Texas plots found"
        print(f"✅ Texas plots: {texas_plots:,}")

        # Check for loblolly pine trees in Texas
        loblolly_query = """
        SELECT COUNT(*) as tree_count
        FROM TREE t
        JOIN PLOT p ON t.PLT_CN = p.CN
        WHERE p.STATECD = 48
          AND t.SPCD = 131
          AND t.STATUSCD = 1
        """
        result = query_interface.execute_query(loblolly_query)
        loblolly_trees = result.row(0, named=True)['tree_count']
        assert loblolly_trees > 0, "No loblolly pine trees found in Texas"
        print(f"✅ Texas loblolly pine tree records: {loblolly_trees:,}")

    def test_evalid_selection_returns_valid_id(self, query_interface):
        """Test that EVALID selection returns a valid evaluation for Texas."""

        evalid, explanation = get_recommended_evalid(
            query_interface,
            48,  # Texas
            "tree_count"
        )

        assert evalid is not None, "Should find a valid EVALID for Texas"
        assert isinstance(evalid, int), "EVALID should be integer"
        assert evalid > 0, "EVALID should be positive"

        # Verify this EVALID actually exists and has data
        validation_query = f"""
        SELECT
            pe.EVALID,
            pe.EVAL_DESCR,
            pe.STATECD,
            COUNT(DISTINCT ppsa.PLT_CN) as plot_count
        FROM POP_EVAL pe
        LEFT JOIN POP_PLOT_STRATUM_ASSGN ppsa ON pe.EVALID = ppsa.EVALID
        WHERE pe.EVALID = {evalid}
        GROUP BY pe.EVALID, pe.EVAL_DESCR, pe.STATECD
        """

        result = query_interface.execute_query(validation_query)
        assert len(result) > 0, f"EVALID {evalid} not found in database"

        row = result.row(0, named=True)
        assert row['STATECD'] == 48, f"EVALID {evalid} is not for Texas"
        assert row['plot_count'] > 0, f"EVALID {evalid} has no plots"

        print(f"✅ Selected EVALID: {evalid}")
        print(f"✅ Description: {row['EVAL_DESCR']}")
        print(f"✅ Plot count: {row['plot_count']:,}")
        print(f"✅ Explanation: {explanation}")

        return evalid

    def test_texas_loblolly_count_is_correct(self, fia_instance, query_interface):
        """THE MAIN TEST: Verify Texas loblolly pine count matches expected value."""

        print(f"\n{'='*60}")
        print("🌲 TEXAS LOBLOLLY PINE COUNT VERIFICATION")
        print(f"{'='*60}")

        # Get recommended EVALID
        evalid, explanation = get_recommended_evalid(
            query_interface,
            48,  # Texas
            "tree_count"
        )

        assert evalid is not None, "Must have valid EVALID for counting"
        print(f"📋 Using EVALID: {evalid}")
        print(f"📝 Reason: {explanation}")

        # Set EVALID on FIA instance
        fia_instance.evalid = evalid

        # Count loblolly pine trees using the real function
        print("\n🔢 Counting loblolly pine trees...")
        print("   Species: Loblolly pine (SPCD = 131)")
        print("   Location: Texas (STATECD = 48)")
        print("   Type: Live trees")

        result = tree_count(
            fia_instance,
            tree_domain="SPCD == 131",  # Loblolly pine
            area_domain="STATECD == 48",  # Texas
            tree_type="live",
            by_species=True,
            totals=True
        )

        # Verify we got results
        assert len(result) > 0, "Tree count should return results"

        # Extract the actual count
        row = result.row(0, named=True)
        actual_count = int(row['TREE_COUNT'])
        se = row.get('SE', 0)
        se_percent = row.get('SE_PERCENT', 0)
        species_code = row.get('SPCD')
        common_name = row.get('COMMON_NAME', '')

        # Verify species information
        assert species_code == 131, f"Expected SPCD 131, got {species_code}"
        assert 'loblolly' in common_name.lower(), f"Expected loblolly pine, got '{common_name}'"

        print("\n📊 RESULTS:")
        print(f"   Species: {common_name}")
        print(f"   Total trees: {actual_count:,}")
        print(f"   Standard error: {se:,.0f}")
        print(f"   SE percentage: {se_percent:.1f}%")

        # The critical verification
        expected_count = 1_747_270_660

        print("\n🎯 VERIFICATION:")
        print(f"   Expected: {expected_count:,}")
        print(f"   Actual:   {actual_count:,}")

        difference = abs(actual_count - expected_count)
        percent_diff = (difference / expected_count) * 100

        print(f"   Difference: {difference:,} ({percent_diff:.3f}%)")

        # Set tolerance for statistical estimates
        # With updated logic that prioritizes statewide evaluations, we should get exact match
        # The expected count is based on statewide EVALID 482201
        tolerance_percent = 0.0  # Require exact match

        if difference == 0:
            print("   🎉 PERFECT MATCH!")
            status = "PERFECT"
        elif percent_diff <= tolerance_percent:
            print(f"   ✅ WITHIN TOLERANCE ({tolerance_percent}%)")
            status = "PASS"
        else:
            print(f"   ❌ OUTSIDE TOLERANCE (>{tolerance_percent}%)")
            status = "FAIL"

        print(f"\n{'='*60}")
        print(f"🏁 FINAL RESULT: {status}")

        # The assertion that makes or breaks the test
        assert percent_diff <= tolerance_percent, (
            f"Texas loblolly pine count {actual_count:,} differs from expected "
            f"{expected_count:,} by {percent_diff:.3f}% "
            f"(tolerance: {tolerance_percent}%)\n"
            f"This indicates either:\n"
            f"  1. Wrong EVALID selected (check EVALID selection logic)\n"
            f"  2. Error in tree counting methodology\n"
            f"  3. Database data issue\n"
            f"Selected EVALID: {evalid} - {explanation}"
        )

        print(f"✅ Texas loblolly pine count VERIFIED: {actual_count:,} trees")
        return actual_count

    def test_evalid_prioritizes_statewide(self, query_interface):
        """Verify that EVALID selection prioritizes statewide evaluations."""

        # Get all available Texas EXPVOL evaluations
        all_evals_query = """
        SELECT
            pe.EVALID,
            pe.EVAL_DESCR,
            pe.END_INVYR,
            CASE
                WHEN pe.EVAL_DESCR LIKE '%(%' THEN 'Regional'
                ELSE 'Statewide'
            END as scope
        FROM POP_EVAL pe
        LEFT JOIN POP_EVAL_TYP pet ON pe.CN = pet.EVAL_CN
        WHERE pe.STATECD = 48
          AND pet.EVAL_TYP = 'EXPVOL'
        ORDER BY pe.END_INVYR DESC
        """

        result = query_interface.execute_query(all_evals_query)
        assert len(result) > 0, "Should find Texas EXPVOL evaluations"

        # Get our recommended EVALID
        recommended_evalid, _ = get_recommended_evalid(query_interface, 48, "tree_count")

        # Find the most recent statewide evaluation
        most_recent_statewide = None
        for row in result.iter_rows(named=True):
            if row['scope'] == 'Statewide':
                most_recent_statewide = row
                break

        assert most_recent_statewide is not None, "Should find at least one statewide evaluation"

        # Verify our recommended EVALID matches the most recent statewide
        recommended_row = None
        for row in result.iter_rows(named=True):
            if row['EVALID'] == recommended_evalid:
                recommended_row = row
                break

        assert recommended_row is not None, f"Recommended EVALID {recommended_evalid} not found in results"

        print("\n📅 EVALID Prioritization Check:")
        print(f"   Most recent statewide: {most_recent_statewide['EVALID']} ({most_recent_statewide['END_INVYR']})")
        print(f"   Recommended EVALID: {recommended_evalid} ({recommended_row['END_INVYR']})")
        print(f"   Scope: {recommended_row['scope']}")

        # Should prioritize statewide evaluations
        assert recommended_row['scope'] == 'Statewide', (
            f"EVALID selection should prioritize statewide evaluations. "
            f"Selected {recommended_row['scope']} evaluation."
        )

        assert recommended_evalid == most_recent_statewide['EVALID'], (
            f"Should select most recent statewide evaluation. "
            f"Selected {recommended_evalid}, expected {most_recent_statewide['EVALID']}"
        )

        print("✅ EVALID selection correctly prioritizes most recent evaluation")


# Convenience function to run just the main test
def run_main_test_only():
    """Run just the main tree count test."""

    # Find database
    possible_paths = [
        "fia_datamart.db",
        "data/fia_datamart.db",
        "../fia_datamart.db",
        os.path.expanduser("~/fia_datamart.db"),
    ]

    db_path = None
    for path in possible_paths:
        if Path(path).exists():
            db_path = path
            break

    if not db_path:
        print("❌ No database found for testing")
        return False

    try:
        query_interface = DuckDBQueryInterface(db_path)
        fia_instance = FIA(db_path)

        test_instance = TestTexasLoblollyRealData()
        actual_count = test_instance.test_texas_loblolly_count_is_correct(
            fia_instance, query_interface
        )

        print(f"\n🎉 SUCCESS: Verified {actual_count:,} loblolly pine trees in Texas")
        return True

    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        return False


if __name__ == "__main__":
    success = run_main_test_only()
    exit(0 if success else 1)
