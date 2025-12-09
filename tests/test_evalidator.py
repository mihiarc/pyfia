"""
Tests for the EVALIDator API client.

These tests validate pyFIA estimates against official USFS EVALIDator values.
Tests require network access to the EVALIDator API.
"""

import pytest
import polars as pl

from pyfia.evalidator import (
    EVALIDatorClient,
    EVALIDatorEstimate,
    EstimateType,
    ValidationResult,
    compare_estimates,
    validate_pyfia_estimate,
)


# Skip all tests if network is unavailable
pytestmark = pytest.mark.network


class TestEVALIDatorClient:
    """Tests for the EVALIDatorClient class."""

    @pytest.fixture
    def client(self):
        """Create an EVALIDator client instance."""
        return EVALIDatorClient(timeout=60)

    def test_client_initialization(self):
        """Test client can be initialized."""
        client = EVALIDatorClient()
        assert client.timeout == 30
        assert client.session is not None

    def test_build_wc(self, client):
        """Test wc (evaluation group code) building."""
        # North Carolina 2023
        assert client._build_wc(37, 2023) == 372023
        # Texas 2022
        assert client._build_wc(48, 2022) == 482022
        # Delaware 2020
        assert client._build_wc(10, 2020) == 102020

    @pytest.mark.slow
    def test_get_forest_area(self, client):
        """Test fetching forest area from EVALIDator."""
        result = client.get_forest_area(state_code=37, year=2023)

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0
        assert result.sampling_error >= 0
        assert result.sampling_error_pct >= 0
        assert result.units == "acres"
        assert result.estimate_type == "forest_area"
        assert result.state_code == 37
        assert result.year == 2023

    @pytest.mark.slow
    def test_get_timberland_area(self, client):
        """Test fetching timberland area from EVALIDator."""
        result = client.get_forest_area(state_code=37, year=2023, land_type="timber")

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0
        assert result.estimate_type == "timber_area"

    @pytest.mark.slow
    def test_get_volume(self, client):
        """Test fetching volume from EVALIDator."""
        result = client.get_volume(state_code=37, year=2023)

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0
        assert result.units == "cubic_feet"
        assert "volume" in result.estimate_type

    @pytest.mark.slow
    def test_get_biomass(self, client):
        """Test fetching biomass from EVALIDator."""
        result = client.get_biomass(state_code=37, year=2023, component="ag")

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0
        assert result.units == "dry_short_tons"
        assert "biomass" in result.estimate_type

    @pytest.mark.slow
    def test_get_carbon(self, client):
        """Test fetching carbon from EVALIDator."""
        result = client.get_carbon(state_code=37, year=2023, pool="total")

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0
        assert result.units == "metric_tonnes"
        assert "carbon" in result.estimate_type

    @pytest.mark.slow
    def test_get_tree_count(self, client):
        """Test fetching tree count from EVALIDator."""
        result = client.get_tree_count(state_code=37, year=2023, min_diameter=5.0)

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0
        assert result.units == "trees"

    @pytest.mark.slow
    def test_get_custom_estimate(self, client):
        """Test fetching custom estimate with arbitrary snum."""
        result = client.get_custom_estimate(
            snum=EstimateType.BASAL_AREA_5INCH,
            state_code=37,
            year=2023,
            units="square_feet",
            estimate_type="basal_area"
        )

        assert isinstance(result, EVALIDatorEstimate)
        assert result.estimate > 0


class TestCompareEstimates:
    """Tests for the compare_estimates function."""

    def test_compare_estimates_passed(self):
        """Test comparison that passes within tolerance."""
        ev_result = EVALIDatorEstimate(
            estimate=1000000,
            sampling_error=20000,
            sampling_error_pct=2.0,
            units="acres",
            estimate_type="forest_area",
            state_code=37,
            year=2023
        )

        result = compare_estimates(
            pyfia_value=1010000,  # 1% difference
            pyfia_se=25000,
            evalidator_result=ev_result,
            tolerance_pct=5.0
        )

        assert isinstance(result, ValidationResult)
        assert result.passed is True
        assert result.pct_diff == pytest.approx(1.0, rel=0.01)
        assert "ACCEPTABLE" in result.message or "EXCELLENT" in result.message or "GOOD" in result.message

    def test_compare_estimates_failed(self):
        """Test comparison that fails tolerance."""
        ev_result = EVALIDatorEstimate(
            estimate=1000000,
            sampling_error=10000,  # Small SE
            sampling_error_pct=1.0,
            units="acres",
            estimate_type="forest_area",
            state_code=37,
            year=2023
        )

        result = compare_estimates(
            pyfia_value=1100000,  # 10% difference - exceeds tolerance and SE
            pyfia_se=10000,
            evalidator_result=ev_result,
            tolerance_pct=5.0
        )

        assert result.passed is False
        assert result.pct_diff == pytest.approx(10.0, rel=0.01)
        assert "FAILED" in result.message

    def test_compare_within_2se(self):
        """Test comparison that passes because within 2 standard errors."""
        ev_result = EVALIDatorEstimate(
            estimate=1000000,
            sampling_error=50000,  # Large SE
            sampling_error_pct=5.0,
            units="acres",
            estimate_type="forest_area",
            state_code=37,
            year=2023
        )

        result = compare_estimates(
            pyfia_value=1080000,  # 8% difference but within 2 SE
            pyfia_se=50000,
            evalidator_result=ev_result,
            tolerance_pct=5.0  # Would fail on tolerance alone
        )

        # Should pass because combined 2*SE > 80000
        assert result.within_2se is True
        assert result.passed is True


class TestValidatePyfiaEstimate:
    """Tests for the validate_pyfia_estimate function."""

    @pytest.fixture
    def mock_pyfia_result(self):
        """Create a mock pyFIA result DataFrame."""
        return pl.DataFrame({
            "TOTAL_AREA": [18500000.0],
            "AREA_SE": [350000.0],
            "AREA_SE_PCT": [1.89]
        })

    @pytest.mark.slow
    def test_validate_extracts_columns(self, mock_pyfia_result):
        """Test that validation extracts correct columns from result."""
        # This test makes a real API call to validate the workflow
        result = validate_pyfia_estimate(
            mock_pyfia_result,
            state_code=37,
            year=2023,
            estimate_type="area"
        )

        # Should return a ValidationResult
        assert isinstance(result, ValidationResult)
        assert result.pyfia_estimate == 18500000.0
        assert result.pyfia_se == 350000.0
        assert result.state_code == 37
        assert result.year == 2023


class TestEstimateType:
    """Tests for EstimateType constants."""

    def test_area_constants(self):
        """Test area estimate type constants."""
        assert EstimateType.AREA_FOREST == 2
        assert EstimateType.AREA_TIMBERLAND == 3

    def test_volume_constants(self):
        """Test volume estimate type constants."""
        assert EstimateType.VOLUME_NET_GROWINGSTOCK == 15
        assert EstimateType.VOLUME_SAWLOG_INTERNATIONAL == 20

    def test_biomass_constants(self):
        """Test biomass estimate type constants."""
        assert EstimateType.BIOMASS_AG_LIVE == 10
        assert EstimateType.BIOMASS_BG_LIVE == 59

    def test_carbon_constants(self):
        """Test carbon estimate type constants."""
        assert EstimateType.CARBON_AG_LIVE == 53000
        assert EstimateType.CARBON_POOL_TOTAL == 103


@pytest.mark.integration
@pytest.mark.slow
class TestPyFIAValidation:
    """
    Integration tests that validate pyFIA estimates against EVALIDator.

    These tests require:
    1. Network access to EVALIDator API
    2. A pyFIA database with data for the test state/year
    """

    @pytest.fixture
    def client(self):
        return EVALIDatorClient(timeout=60)

    @pytest.fixture
    def fia_db(self, sample_fia_instance):
        """Get a configured FIA instance for testing."""
        return sample_fia_instance

    def test_validate_area_estimate(self, client, fia_db):
        """Validate pyFIA area estimate against EVALIDator."""
        from pyfia import area

        # Get pyFIA estimate
        fia_db.clip_by_state(13)  # Georgia
        fia_db.clip_most_recent(eval_type="EXPALL")
        pyfia_result = area(fia_db, land_type="forest")

        # Get EVALIDator estimate
        ev_result = client.get_forest_area(state_code=13, year=2023)

        # Extract pyFIA values
        total_col = [c for c in pyfia_result.columns if "TOTAL" in c.upper()][0]
        se_col = [c for c in pyfia_result.columns if "SE" in c.upper() and "PCT" not in c.upper()][0]

        # Compare
        validation = compare_estimates(
            pyfia_value=pyfia_result[total_col][0],
            pyfia_se=pyfia_result[se_col][0],
            evalidator_result=ev_result,
            tolerance_pct=10.0  # Allow 10% for methodology differences
        )

        # Log result for debugging
        print(f"\nArea validation for Georgia:")
        print(f"  pyFIA: {validation.pyfia_estimate:,.0f} acres (SE: {validation.pyfia_se:,.0f})")
        print(f"  EVALIDator: {validation.evalidator_estimate:,.0f} acres (SE: {validation.evalidator_se:,.0f})")
        print(f"  Difference: {validation.pct_diff:.2f}%")
        print(f"  Result: {validation.message}")

        assert validation.passed, f"Area validation failed: {validation.message}"

    def test_validate_volume_estimate(self, client, fia_db):
        """Validate pyFIA volume estimate against EVALIDator."""
        from pyfia import volume

        # Get pyFIA estimate
        fia_db.clip_by_state(13)  # Georgia
        fia_db.clip_most_recent(eval_type="EXPVOL")
        pyfia_result = volume(fia_db, land_type="forest", vol_type="net")

        # Get EVALIDator estimate
        ev_result = client.get_volume(state_code=13, year=2023, vol_type="net")

        # Extract pyFIA values
        total_col = [c for c in pyfia_result.columns if "TOTAL" in c.upper()][0]
        se_col = [c for c in pyfia_result.columns if "SE" in c.upper() and "PCT" not in c.upper()][0]

        # Compare
        validation = compare_estimates(
            pyfia_value=pyfia_result[total_col][0],
            pyfia_se=pyfia_result[se_col][0],
            evalidator_result=ev_result,
            tolerance_pct=10.0
        )

        print(f"\nVolume validation for Georgia:")
        print(f"  pyFIA: {validation.pyfia_estimate:,.0f} cu ft (SE: {validation.pyfia_se:,.0f})")
        print(f"  EVALIDator: {validation.evalidator_estimate:,.0f} cu ft (SE: {validation.evalidator_se:,.0f})")
        print(f"  Difference: {validation.pct_diff:.2f}%")
        print(f"  Result: {validation.message}")

        assert validation.passed, f"Volume validation failed: {validation.message}"
