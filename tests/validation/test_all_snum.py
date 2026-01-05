"""
Validation tests for all 752 EVALIDator SNUM values.

This module tests that each of the 752 estimate types (snum values) can be
successfully queried from the EVALIDator API and returns valid response data.

These tests verify API accessibility and response structure, not pyFIA
estimation accuracy. For accuracy validation, see the individual estimate
type test modules (test_volume.py, test_biomass.py, etc.).

Run all snum tests:
    uv run pytest tests/validation/test_all_snum.py -v

Run by category:
    uv run pytest tests/validation/test_all_snum.py -v -k "area"
    uv run pytest tests/validation/test_all_snum.py -v -k "volume"
    uv run pytest tests/validation/test_all_snum.py -v -k "biomass"

Run with parallel workers (recommended for 752 tests):
    uv run pytest tests/validation/test_all_snum.py -v -n auto

Note: The EVALIDator API is notoriously unreliable and may return connection
errors, timeouts, or empty responses. Tests are marked with xfail(strict=False)
to allow for transient API failures. Use --runxfail to run all tests strictly.
"""

import pytest
import requests

from pyfia.evalidator.client import EVALIDatorClient
from pyfia.evalidator.estimate_types import EstimateType, SNUM_DESCRIPTIONS, get_category

# Test configuration
STATE_CODE = 13  # Georgia
YEAR = 2023

# Marker for flaky API tests
flaky_api = pytest.mark.xfail(
    reason="EVALIDator API is unreliable",
    raises=(requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    strict=False,
)


def get_all_snums_by_category():
    """Get all snum values organized by category for parametrization."""
    categories = {}
    for e in EstimateType:
        cat = get_category(e.value)
        if cat not in categories:
            categories[cat] = []
        desc = SNUM_DESCRIPTIONS.get(e.value, "Unknown")
        # Truncate description for test ID readability
        short_desc = desc[:50] + "..." if len(desc) > 50 else desc
        categories[cat].append(
            pytest.param(
                e.value,
                desc,
                id=f"{cat.lower()}_{e.value}_{e.name}",
            )
        )
    return categories


# Pre-compute all snums by category
SNUMS_BY_CATEGORY = get_all_snums_by_category()


# =============================================================================
# AREA Tests (3 estimates)
# =============================================================================


@flaky_api
class TestAreaSnum:
    """Validate AREA estimate types (snum 2, 3, 79)."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("AREA", []))
    def test_area_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate AREA snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="acres",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# AREA_CHANGE Tests (10 estimates)
# =============================================================================


@flaky_api
class TestAreaChangeSnum:
    """Validate AREA_CHANGE estimate types."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("AREA_CHANGE", []))
    def test_area_change_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate AREA_CHANGE snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="acres",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# TREE_COUNT Tests (10 estimates)
# =============================================================================


@flaky_api
class TestTreeCountSnum:
    """Validate TREE_COUNT estimate types."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("TREE_COUNT", []))
    def test_tree_count_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate TREE_COUNT snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="trees",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# BASAL_AREA Tests (4 estimates)
# =============================================================================


@flaky_api
class TestBasalAreaSnum:
    """Validate BASAL_AREA estimate types."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("BASAL_AREA", []))
    def test_basal_area_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate BASAL_AREA snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="sq ft",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# VOLUME Tests (311 estimates)
# =============================================================================


@flaky_api
class TestVolumeSnum:
    """Validate VOLUME estimate types (311 estimates)."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("VOLUME", []))
    def test_volume_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate VOLUME snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="cu ft",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# BIOMASS Tests (327 estimates)
# =============================================================================


@flaky_api
class TestBiomassSnum:
    """Validate BIOMASS estimate types (327 estimates)."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("BIOMASS", []))
    def test_biomass_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate BIOMASS snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="dry tons",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# CARBON Tests (38 estimates)
# =============================================================================


@flaky_api
class TestCarbonSnum:
    """Validate CARBON estimate types (38 estimates)."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("CARBON", []))
    def test_carbon_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate CARBON snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="short tons",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# DOWN_WOODY Tests (1 estimate)
# =============================================================================


@flaky_api
class TestDownWoodySnum:
    """Validate DOWN_WOODY estimate types."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("DOWN_WOODY", []))
    def test_down_woody_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate DOWN_WOODY snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="dry tons",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# TREE_DYNAMICS Tests (48 estimates)
# =============================================================================


@flaky_api
class TestTreeDynamicsSnum:
    """Validate TREE_DYNAMICS estimate types (growth, mortality, removals)."""

    @pytest.mark.parametrize("snum,description", SNUMS_BY_CATEGORY.get("TREE_DYNAMICS", []))
    def test_tree_dynamics_snum(self, evalidator_client: EVALIDatorClient, snum: int, description: str):
        """Validate TREE_DYNAMICS snum returns valid response."""
        result = evalidator_client.get_custom_estimate(
            snum=snum,
            state_code=STATE_CODE,
            year=YEAR,
            units="cu ft/yr",
            estimate_type=description,
        )

        assert result is not None, f"No result for snum {snum}"
        assert result.estimate is not None, f"No estimate for snum {snum}"
        assert result.sampling_error is not None, f"No SE for snum {snum}"
        assert result.plot_count >= 0, f"Invalid plot count for snum {snum}"


# =============================================================================
# Summary Test
# =============================================================================


class TestSnumCoverage:
    """Verify all 752 snum values are covered by tests."""

    def test_all_snums_have_tests(self):
        """Verify all EstimateType values have corresponding test parameters."""
        tested_snums = set()
        for category_params in SNUMS_BY_CATEGORY.values():
            for param in category_params:
                # Extract snum from pytest.param
                tested_snums.add(param.values[0])

        all_snums = {e.value for e in EstimateType}

        missing = all_snums - tested_snums
        assert len(missing) == 0, f"Missing tests for snums: {sorted(missing)}"
        assert len(tested_snums) == 752, f"Expected 752 snums, got {len(tested_snums)}"

    def test_category_counts(self):
        """Verify category counts match expected values."""
        expected = {
            "AREA": 3,
            "AREA_CHANGE": 10,
            "TREE_COUNT": 10,
            "BASAL_AREA": 4,
            "VOLUME": 311,
            "BIOMASS": 327,
            "CARBON": 38,
            "DOWN_WOODY": 1,
            "TREE_DYNAMICS": 48,
        }

        for category, expected_count in expected.items():
            actual_count = len(SNUMS_BY_CATEGORY.get(category, []))
            assert actual_count == expected_count, (
                f"Category {category}: expected {expected_count}, got {actual_count}"
            )
