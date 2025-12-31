"""Unit tests for carbon_flux estimator."""

import polars as pl
import pytest

from pyfia.estimation.estimators.carbon_flux import (
    CARBON_FRACTION,
    _calculate_carbon_flux,
    _empty_result,
    _grouped_flux,
    _normalize_group_cols,
    _safe_get,
    _scalar_flux,
    carbon_flux,
)


class TestNormalizeGroupCols:
    """Tests for _normalize_group_cols helper."""

    def test_none_input(self):
        """Test with no grouping."""
        result = _normalize_group_cols(None, by_species=False)
        assert result == []

    def test_string_input(self):
        """Test with single string column."""
        result = _normalize_group_cols("FORTYPCD", by_species=False)
        assert result == ["FORTYPCD"]

    def test_list_input(self):
        """Test with list of columns."""
        result = _normalize_group_cols(["FORTYPCD", "OWNGRPCD"], by_species=False)
        assert result == ["FORTYPCD", "OWNGRPCD"]

    def test_by_species_adds_spcd(self):
        """Test that by_species=True adds SPCD."""
        result = _normalize_group_cols(None, by_species=True)
        assert result == ["SPCD"]

    def test_by_species_with_existing_columns(self):
        """Test by_species with existing grouping columns."""
        result = _normalize_group_cols(["FORTYPCD"], by_species=True)
        assert result == ["FORTYPCD", "SPCD"]

    def test_by_species_no_duplicate(self):
        """Test that SPCD isn't duplicated if already present."""
        result = _normalize_group_cols(["SPCD", "FORTYPCD"], by_species=True)
        assert result == ["SPCD", "FORTYPCD"]
        assert result.count("SPCD") == 1


class TestSafeGet:
    """Tests for _safe_get helper."""

    def test_existing_column(self):
        """Test getting value from existing column."""
        df = pl.DataFrame({"A": [1.5, 2.5]})
        assert _safe_get(df, "A") == 1.5

    def test_missing_column(self):
        """Test with missing column returns default."""
        df = pl.DataFrame({"A": [1.5]})
        assert _safe_get(df, "B") == 0.0
        assert _safe_get(df, "B", default=99.0) == 99.0

    def test_empty_dataframe(self):
        """Test with empty DataFrame returns default."""
        df = pl.DataFrame({"A": []}).cast({"A": pl.Float64})
        assert _safe_get(df, "A") == 0.0

    def test_null_value(self):
        """Test with null value returns default."""
        df = pl.DataFrame({"A": [None]})
        assert _safe_get(df, "A") == 0.0

    def test_integer_value(self):
        """Test that integer values are converted to float."""
        df = pl.DataFrame({"A": [42]})
        result = _safe_get(df, "A")
        assert result == 42.0
        assert isinstance(result, float)


class TestEmptyResult:
    """Tests for _empty_result helper."""

    def test_basic_empty_result(self):
        """Test basic empty result structure."""
        result = _empty_result([], totals=True, variance=True, include_components=False)
        assert result.is_empty()
        assert "NET_CARBON_FLUX_ACRE" in result.columns
        assert "AREA_TOTAL" in result.columns
        assert "NET_CARBON_FLUX_TOTAL" in result.columns
        assert "N_PLOTS" in result.columns
        assert "YEAR" in result.columns

    def test_empty_result_with_grouping(self):
        """Test empty result includes grouping columns."""
        result = _empty_result(
            ["FORTYPCD", "OWNGRPCD"],
            totals=True,
            variance=True,
            include_components=False,
        )
        assert "FORTYPCD" in result.columns
        assert "OWNGRPCD" in result.columns

    def test_empty_result_no_totals(self):
        """Test empty result without totals."""
        result = _empty_result([], totals=False, variance=False, include_components=False)
        assert "NET_CARBON_FLUX_TOTAL" not in result.columns

    def test_empty_result_with_components(self):
        """Test empty result with component columns."""
        result = _empty_result([], totals=True, variance=False, include_components=True)
        assert "GROWTH_CARBON_TOTAL" in result.columns
        assert "MORT_CARBON_TOTAL" in result.columns
        assert "REMV_CARBON_TOTAL" in result.columns

    def test_empty_result_variance_columns(self):
        """Test empty result has variance columns when requested."""
        result = _empty_result([], totals=True, variance=True, include_components=False)
        assert "NET_CARBON_FLUX_ACRE_SE" in result.columns
        assert "NET_CARBON_FLUX_CV" in result.columns
        assert "NET_CARBON_FLUX_TOTAL_SE" in result.columns


class TestScalarFlux:
    """Tests for _scalar_flux helper."""

    def test_basic_calculation(self):
        """Test basic carbon flux calculation."""
        area_result = pl.DataFrame({"AREA": [1000000.0]})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [1000000.0],
            "GROWTH_TOTAL_SE": [50000.0],
            "N_PLOTS": [100],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({
            "MORT_TOTAL": [200000.0],
            "MORT_TOTAL_SE": [20000.0],
        })
        remv_result = pl.DataFrame({
            "REMOVALS_TOTAL": [300000.0],
            "REMOVALS_TOTAL_SE": [30000.0],
        })

        result = _scalar_flux(
            area_result, growth_result, mort_result, remv_result,
            totals=True, variance=True, include_components=True
        )

        # Net = (1M - 0.2M - 0.3M) * 0.47 = 0.5M * 0.47 = 235,000
        expected_total = (1000000 - 200000 - 300000) * CARBON_FRACTION
        assert abs(result["NET_CARBON_FLUX_TOTAL"][0] - expected_total) < 1

        # Per acre = 235,000 / 1,000,000 = 0.235
        expected_acre = expected_total / 1000000
        assert abs(result["NET_CARBON_FLUX_ACRE"][0] - expected_acre) < 0.001

    def test_zero_area(self):
        """Test handling of zero area."""
        area_result = pl.DataFrame({"AREA": [0.0]})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [1000.0],
            "N_PLOTS": [10],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({"MORT_TOTAL": [200.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [300.0]})

        result = _scalar_flux(
            area_result, growth_result, mort_result, remv_result,
            totals=True, variance=False, include_components=False
        )

        # Per acre should be 0 when area is 0
        assert result["NET_CARBON_FLUX_ACRE"][0] == 0.0
        assert result["AREA_TOTAL"][0] == 0.0

    def test_carbon_source(self):
        """Test carbon source (negative flux) scenario."""
        area_result = pl.DataFrame({"AREA": [1000000.0]})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [100000.0],
            "N_PLOTS": [100],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({"MORT_TOTAL": [200000.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [300000.0]})

        result = _scalar_flux(
            area_result, growth_result, mort_result, remv_result,
            totals=True, variance=False, include_components=False
        )

        # Net = (0.1M - 0.2M - 0.3M) * 0.47 = -0.4M * 0.47 < 0
        assert result["NET_CARBON_FLUX_TOTAL"][0] < 0
        assert result["NET_CARBON_FLUX_ACRE"][0] < 0

    def test_components_output(self):
        """Test component columns are included when requested."""
        area_result = pl.DataFrame({"AREA": [1000000.0]})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [1000000.0],
            "N_PLOTS": [100],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({"MORT_TOTAL": [200000.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [300000.0]})

        result = _scalar_flux(
            area_result, growth_result, mort_result, remv_result,
            totals=True, variance=False, include_components=True
        )

        assert "GROWTH_CARBON_TOTAL" in result.columns
        assert "MORT_CARBON_TOTAL" in result.columns
        assert "REMV_CARBON_TOTAL" in result.columns

        # Verify carbon calculation
        assert abs(result["GROWTH_CARBON_TOTAL"][0] - 1000000 * CARBON_FRACTION) < 1
        assert abs(result["MORT_CARBON_TOTAL"][0] - 200000 * CARBON_FRACTION) < 1
        assert abs(result["REMV_CARBON_TOTAL"][0] - 300000 * CARBON_FRACTION) < 1


class TestGroupedFlux:
    """Tests for _grouped_flux helper."""

    def test_basic_grouped_calculation(self):
        """Test grouped carbon flux calculation."""
        area_result = pl.DataFrame({
            "OWNGRPCD": [10, 20, 40],
            "AREA": [100000.0, 200000.0, 500000.0],
        })
        growth_result = pl.DataFrame({
            "OWNGRPCD": [10, 20, 40],
            "GROWTH_TOTAL": [50000.0, 80000.0, 200000.0],
            "GROWTH_TOTAL_SE": [5000.0, 8000.0, 20000.0],
            "N_PLOTS": [50, 80, 200],
            "YEAR": [2023, 2023, 2023],
        })
        mort_result = pl.DataFrame({
            "OWNGRPCD": [10, 20, 40],
            "MORT_TOTAL": [10000.0, 30000.0, 50000.0],
            "MORT_TOTAL_SE": [1000.0, 3000.0, 5000.0],
        })
        remv_result = pl.DataFrame({
            "OWNGRPCD": [10, 20, 40],
            "REMOVALS_TOTAL": [20000.0, 60000.0, 100000.0],
            "REMOVALS_TOTAL_SE": [2000.0, 6000.0, 10000.0],
        })

        result = _grouped_flux(
            area_result, growth_result, mort_result, remv_result,
            group_cols=["OWNGRPCD"],
            totals=True, variance=True, include_components=False
        )

        assert len(result) == 3
        assert "OWNGRPCD" in result.columns
        assert "NET_CARBON_FLUX_ACRE" in result.columns
        assert "NET_CARBON_FLUX_TOTAL" in result.columns
        assert "AREA_TOTAL" in result.columns

    def test_partial_data(self):
        """Test grouped calculation with partial data (some groups missing)."""
        area_result = pl.DataFrame({
            "OWNGRPCD": [10, 20, 40],
            "AREA": [100000.0, 200000.0, 500000.0],
        })
        growth_result = pl.DataFrame({
            "OWNGRPCD": [10, 40],  # Missing 20
            "GROWTH_TOTAL": [50000.0, 200000.0],
            "N_PLOTS": [50, 200],
            "YEAR": [2023, 2023],
        })
        mort_result = pl.DataFrame({
            "OWNGRPCD": [10, 20, 40],
            "MORT_TOTAL": [10000.0, 30000.0, 50000.0],
        })
        remv_result = pl.DataFrame({
            "OWNGRPCD": [10, 40],  # Missing 20
            "REMOVALS_TOTAL": [20000.0, 100000.0],
        })

        result = _grouped_flux(
            area_result, growth_result, mort_result, remv_result,
            group_cols=["OWNGRPCD"],
            totals=True, variance=False, include_components=False
        )

        # Should have results for groups with growth data
        assert len(result) == 2


class TestCalculateCarbonFlux:
    """Tests for _calculate_carbon_flux dispatcher."""

    def test_empty_growth_result(self):
        """Test handling of empty growth result."""
        area_result = pl.DataFrame({"AREA": [1000000.0]})
        growth_result = pl.DataFrame(schema={"GROWTH_TOTAL": pl.Float64})
        mort_result = pl.DataFrame({"MORT_TOTAL": [200000.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [300000.0]})

        result = _calculate_carbon_flux(
            area_result, growth_result, mort_result, remv_result,
            group_cols=[], totals=True, variance=False, include_components=False
        )

        assert result.is_empty()

    def test_empty_area_result(self):
        """Test handling of empty area result."""
        area_result = pl.DataFrame(schema={"AREA": pl.Float64})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [1000000.0],
            "N_PLOTS": [100],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({"MORT_TOTAL": [200000.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [300000.0]})

        result = _calculate_carbon_flux(
            area_result, growth_result, mort_result, remv_result,
            group_cols=[], totals=True, variance=False, include_components=False
        )

        assert result.is_empty()

    def test_dispatches_to_scalar(self):
        """Test that empty group_cols dispatches to scalar calculation."""
        area_result = pl.DataFrame({"AREA": [1000000.0]})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [1000000.0],
            "N_PLOTS": [100],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({"MORT_TOTAL": [200000.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [300000.0]})

        result = _calculate_carbon_flux(
            area_result, growth_result, mort_result, remv_result,
            group_cols=[], totals=True, variance=False, include_components=False
        )

        assert len(result) == 1
        assert "NET_CARBON_FLUX_TOTAL" in result.columns

    def test_dispatches_to_grouped(self):
        """Test that non-empty group_cols dispatches to grouped calculation."""
        area_result = pl.DataFrame({
            "OWNGRPCD": [10, 40],
            "AREA": [100000.0, 500000.0],
        })
        growth_result = pl.DataFrame({
            "OWNGRPCD": [10, 40],
            "GROWTH_TOTAL": [50000.0, 200000.0],
            "N_PLOTS": [50, 200],
            "YEAR": [2023, 2023],
        })
        mort_result = pl.DataFrame({
            "OWNGRPCD": [10, 40],
            "MORT_TOTAL": [10000.0, 50000.0],
        })
        remv_result = pl.DataFrame({
            "OWNGRPCD": [10, 40],
            "REMOVALS_TOTAL": [20000.0, 100000.0],
        })

        result = _calculate_carbon_flux(
            area_result, growth_result, mort_result, remv_result,
            group_cols=["OWNGRPCD"], totals=True, variance=False, include_components=False
        )

        assert len(result) == 2
        assert "OWNGRPCD" in result.columns


class TestCarbonFraction:
    """Tests for carbon fraction constant."""

    def test_carbon_fraction_value(self):
        """Test IPCC standard carbon fraction is 0.47."""
        assert CARBON_FRACTION == 0.47

    def test_carbon_fraction_applied(self):
        """Test carbon fraction is correctly applied in calculations."""
        area_result = pl.DataFrame({"AREA": [1000000.0]})
        growth_result = pl.DataFrame({
            "GROWTH_TOTAL": [1000000.0],  # 1M tons biomass
            "N_PLOTS": [100],
            "YEAR": [2023],
        })
        mort_result = pl.DataFrame({"MORT_TOTAL": [0.0]})
        remv_result = pl.DataFrame({"REMOVALS_TOTAL": [0.0]})

        result = _scalar_flux(
            area_result, growth_result, mort_result, remv_result,
            totals=True, variance=False, include_components=True
        )

        # Growth carbon should be biomass * 0.47
        expected_carbon = 1000000 * 0.47
        assert abs(result["GROWTH_CARBON_TOTAL"][0] - expected_carbon) < 1
        assert abs(result["NET_CARBON_FLUX_TOTAL"][0] - expected_carbon) < 1


class TestCarbonFluxIntegration:
    """Integration tests for carbon_flux function (requires database)."""

    @pytest.fixture
    def fia_db(self):
        """Get FIA database path."""
        import os
        from pathlib import Path

        # Try environment variable first
        env_path = os.getenv("PYFIA_DATABASE_PATH")
        if env_path:
            # MotherDuck connection strings don't need file existence check
            if env_path.startswith("md:") or env_path.startswith("motherduck:"):
                return env_path
            if Path(env_path).exists():
                return env_path

        # Try default location
        default_path = Path("data/georgia.duckdb")
        if default_path.exists():
            return str(default_path)

        pytest.skip("No FIA database found")

    def test_carbon_flux_basic(self, fia_db):
        """Test basic carbon_flux call."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = carbon_flux(db)

        assert not result.is_empty()
        assert "NET_CARBON_FLUX_ACRE" in result.columns
        assert "NET_CARBON_FLUX_TOTAL" in result.columns
        assert "AREA_TOTAL" in result.columns

    def test_carbon_flux_with_grouping(self, fia_db):
        """Test carbon_flux with grouping."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = carbon_flux(db, grp_by="OWNGRPCD")

        assert not result.is_empty()
        assert "OWNGRPCD" in result.columns
        assert len(result) > 1

    def test_carbon_flux_with_components(self, fia_db):
        """Test carbon_flux with component breakdown."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = carbon_flux(db, include_components=True)

        assert "GROWTH_CARBON_TOTAL" in result.columns
        assert "MORT_CARBON_TOTAL" in result.columns
        assert "REMV_CARBON_TOTAL" in result.columns

        # Verify net = growth - mortality - removals
        net = result["NET_CARBON_FLUX_TOTAL"][0]
        growth = result["GROWTH_CARBON_TOTAL"][0]
        mort = result["MORT_CARBON_TOTAL"][0]
        remv = result["REMV_CARBON_TOTAL"][0]

        expected_net = growth - mort - remv
        assert abs(net - expected_net) < 1  # Allow small rounding

    def test_per_acre_equals_total_over_area(self, fia_db):
        """Test that per-acre = total / area."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = carbon_flux(db)

        net_acre = result["NET_CARBON_FLUX_ACRE"][0]
        net_total = result["NET_CARBON_FLUX_TOTAL"][0]
        area = result["AREA_TOTAL"][0]

        if area > 0:
            expected_acre = net_total / area
            assert abs(net_acre - expected_acre) < 0.0001

    def test_variance_output(self, fia_db):
        """Test variance columns are present."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = carbon_flux(db, variance=True)

        assert "NET_CARBON_FLUX_TOTAL_SE" in result.columns
        assert "NET_CARBON_FLUX_ACRE_SE" in result.columns

    def test_no_variance_output(self, fia_db):
        """Test variance columns are absent when not requested."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = carbon_flux(db, variance=False)

        # SE columns should not be present
        se_cols = [c for c in result.columns if "SE" in c]
        assert len(se_cols) == 0

    def test_fia_class_method(self, fia_db):
        """Test carbon_flux as FIA class method."""
        from pyfia import FIA

        db = FIA(fia_db)
        db.clip_most_recent("GROW")

        result = db.carbon_flux()

        assert not result.is_empty()
        assert "NET_CARBON_FLUX_TOTAL" in result.columns
