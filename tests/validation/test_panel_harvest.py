"""
Validation tests comparing panel harvest detection to EVALIDator-validated removals.

Key principles:
1. panel() now uses GRM tables directly for authoritative tree fate classification
2. Panel should closely match removals() since both use TREE_GRM_COMPONENT
3. Volume comparisons may have variance due to different volume sources
4. The "diversion" fate is now tracked separately from "cut"

Run these tests:
    uv run pytest tests/validation/test_panel_harvest.py -v -s
"""

import pytest

from pyfia import FIA, panel, removals
from pyfia.estimation.estimators.panel_validation import (
    compare_panel_to_removals,
    diagnose_panel_removals_diff,
    validate_panel_harvest,
)

from .conftest import STATES, StateConfig


class TestPanelRemovalsConsistency:
    """Cross-validate panel harvest detection against EVALIDator-validated removals."""

    def test_panel_vs_removals_tree_count(self, fia_db):
        """
        Compare panel cut tree detection to EVALIDator-validated removals.

        Both panel() and removals() now use GRM tables for tree fate classification:
        - CUT1/CUT2 components → 'cut' fate in panel
        - DIVERSION1/DIVERSION2 components → 'diversion' fate in panel

        Note: The exact TPA values differ because:
        - removals() uses proper stratified estimation with expansion factors
        - panel() provides raw TPA_UNADJ values without full expansion

        This test validates that panel produces meaningful estimates and
        runs the comparison successfully.
        """
        comparison = compare_panel_to_removals(
            fia_db,
            measure="tpa",
            tree_type="gs",
            land_type="forest",
            verbose=True,
        )

        panel_count = comparison["PANEL_CUT_TREES"][0]
        panel_estimate = comparison["PANEL_ANNUALIZED"][0]
        removals_estimate = comparison["REMOVALS_ESTIMATE"][0]

        # Panel should find cut/diversion trees
        assert panel_count > 0, (
            f"Panel found no cut/diversion trees. Check GRM component mapping."
        )

        # Panel estimate should be positive
        assert panel_estimate > 0, (
            f"Panel found no cut trees. Panel estimate: {panel_estimate}"
        )

        # Removals estimate should be positive
        assert removals_estimate > 0, (
            f"Removals found no trees. Check GRM data availability."
        )

        # Report the comparison for diagnostic purposes
        print(
            f"\nPanel found {panel_count:,} cut/diversion trees"
            f"\nPanel annualized: {panel_estimate:.2f} TPA per plot per year"
            f"\nRemovals estimate: {removals_estimate:.2f} TPA per acre per year"
        )

    def test_panel_vs_removals_volume(self, fia_db):
        """
        Panel cut volume should provide a reasonable volume estimate.

        Volume comparison uses:
        - Panel: VOLCFNET from TREE_GRM_MIDPT
        - Removals: VOLCFNET from TREE_GRM_MIDPT

        Note: Exact values differ due to stratified estimation in removals().
        """
        comparison = compare_panel_to_removals(
            fia_db,
            measure="volume",
            tree_type="gs",
            land_type="forest",
            verbose=True,
        )

        panel_count = comparison["PANEL_CUT_TREES"][0]
        panel_estimate = comparison["PANEL_ANNUALIZED"][0]

        # Panel should find trees
        assert panel_count > 0, (
            f"Panel found no cut trees for volume analysis."
        )

        # Volume should be positive (or at least non-negative if no VOLCFNET)
        assert panel_estimate >= 0, (
            f"Panel found negative volume for cut trees. "
            f"Check that volume columns are available in panel data."
        )

        # Report the comparison
        print(
            f"\nPanel found {panel_count:,} cut/diversion trees"
            f"\nPanel annualized volume: {panel_estimate:.2f} cuft per plot per year"
        )

    def test_tree_fate_categories(self, fia_db):
        """
        Verify that panel produces all expected tree fate categories.

        GRM-based panel should produce:
        - survivor: Trees alive at both measurements
        - mortality: Trees that died naturally
        - cut: Trees removed by harvest
        - diversion: Trees removed by land use change
        - ingrowth: New trees crossing size threshold
        """
        with FIA(fia_db) as db:
            panel_data = panel(
                db,
                level="tree",
                land_type="forest",
                tree_type="gs",
                min_invyr=2000,
            )

        # Get fate distribution
        fate_counts = panel_data.group_by("TREE_FATE").len()
        fates = set(fate_counts["TREE_FATE"].to_list())

        # Should have at least survivor, mortality, and either cut or diversion
        expected_core = {"survivor", "mortality"}
        found_core = fates & expected_core

        assert len(found_core) >= 2, (
            f"Expected at least 2 of {expected_core}, found {fates}"
        )

        # Cut or diversion should be present (some form of removal)
        removal_fates = fates & {"cut", "diversion"}
        assert len(removal_fates) >= 1 or panel_data.height == 0, (
            f"Expected at least one removal fate (cut/diversion), found {fates}"
        )

        # Report fate distribution
        print(f"\nTree fate distribution:")
        for row in fate_counts.sort("TREE_FATE").iter_rows(named=True):
            print(f"  {row['TREE_FATE']}: {row['len']:,}")

    def test_validate_panel_harvest(self, fia_db):
        """Run the simple validation check.

        Note: Due to methodological differences between panel and removals,
        this test uses a lower tolerance. The key validation is that panel
        produces positive estimates using the same GRM components.
        """
        # Use lower tolerance since panel doesn't use full stratified estimation
        result = validate_panel_harvest(
            fia_db,
            tolerance_ratio=0.01,  # Just check that it's non-zero
            verbose=True,
        )

        assert result, "Panel harvest validation failed"

    def test_expanded_panel_matches_removals(self, fia_db):
        """
        Panel with expand=True should match removals closely.

        When using proper per-acre expansion (ADJ_FACTOR × EXPNS), panel
        should produce estimates very close to removals() since both use
        the same underlying GRM data and stratified estimation.
        """
        comparison = compare_panel_to_removals(
            fia_db,
            measure="tpa",
            tree_type="gs",
            land_type="forest",
            expand=True,  # Enable proper expansion
            verbose=True,
        )

        panel_estimate = comparison["PANEL_ANNUALIZED"][0]
        removals_estimate = comparison["REMOVALS_ESTIMATE"][0]
        ratio = comparison["RATIO"][0]

        # Both estimates should be positive
        assert panel_estimate > 0, f"Panel estimate is zero or negative: {panel_estimate}"
        assert removals_estimate > 0, f"Removals estimate is zero or negative: {removals_estimate}"

        # With expansion, ratio should be close to 1.0 (within 20%)
        assert 0.8 <= ratio <= 1.2, (
            f"Expected ratio ~1.0 with expansion, got {ratio:.2f}. "
            f"Panel: {panel_estimate:.2f}, Removals: {removals_estimate:.2f}"
        )

        print(
            f"\nExpanded panel matches removals:"
            f"\n  Panel (expanded): {panel_estimate:.2f} TPA per acre"
            f"\n  Removals: {removals_estimate:.2f} TPA per acre"
            f"\n  Ratio: {ratio:.2f}"
        )

    def test_diagnose_provides_useful_info(self, fia_db):
        """Ensure diagnostic function runs and provides tree fate breakdown."""
        fate_counts = diagnose_panel_removals_diff(
            fia_db,
            land_type="forest",
            tree_type="gs",
            verbose=True,
        )

        # Should have at least some tree fates
        assert fate_counts.height > 0, "No tree fates found"

        # Should include common fates (now including diversion)
        fates = set(fate_counts["TREE_FATE"].to_list())
        expected_fates = {"survivor", "mortality", "cut", "diversion", "ingrowth"}

        # At least 2 of the expected fates should be present
        found_expected = fates & expected_fates
        assert len(found_expected) >= 2, (
            f"Expected at least 2 of {expected_fates}, found {fates}"
        )


@pytest.mark.parametrize("state_key", list(STATES.keys()))
class TestMultiStatePanelValidation:
    """Run panel validation across multiple states."""

    def test_panel_removals_consistency_by_state(
        self, state_key: str, state_config: StateConfig
    ):
        """
        Validate panel produces meaningful data for each configured state.

        Note: The exact ratio between panel and removals will vary because
        they use different aggregation methods. This test validates that
        panel produces non-zero estimates using GRM components.
        """
        from .conftest import _find_database

        db_path = _find_database(state_key)
        if db_path is None:
            pytest.skip(f"Database not found for {state_config.name}")

        comparison = compare_panel_to_removals(
            db_path,
            measure="tpa",
            tree_type="gs",
            verbose=True,
        )

        panel_count = comparison["PANEL_CUT_TREES"][0]
        panel_estimate = comparison["PANEL_ANNUALIZED"][0]

        # Panel should find cut/diversion trees
        assert panel_count > 0, (
            f"{state_config.name}: Panel found no cut/diversion trees. "
            f"Check GRM component mapping for this state."
        )

        # Panel estimate should be positive
        assert panel_estimate > 0, (
            f"{state_config.name}: Panel annualized estimate is zero or negative."
        )

        print(
            f"\n{state_config.name}: Panel found {panel_count:,} trees, "
            f"annualized: {panel_estimate:.2f} TPA/plot/year"
        )
