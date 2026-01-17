"""Unit tests for GRM size class grouping functionality."""

import polars as pl
import pytest

from pyfia.filtering.utils import create_size_class_expr


class TestCreateSizeClassExpr:
    """Test size class expression creation for different types."""

    def test_standard_size_class_boundaries(self):
        """Test standard FIA size class boundaries."""
        data = pl.DataFrame(
            {"DIA_MIDPT": [3.0, 5.0, 7.5, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0]}
        )

        expr = create_size_class_expr(dia_col="DIA_MIDPT", size_class_type="standard")
        result = data.with_columns(expr)

        expected = [
            "1.0-4.9",  # 3.0
            "5.0-9.9",  # 5.0
            "5.0-9.9",  # 7.5
            "10.0-19.9",  # 10.0
            "10.0-19.9",  # 15.0
            "20.0-29.9",  # 20.0
            "20.0-29.9",  # 25.0
            "30.0+",  # 30.0
            "30.0+",  # 35.0
        ]
        assert result["SIZE_CLASS"].to_list() == expected

    def test_descriptive_size_class_labels(self):
        """Test descriptive size class labels."""
        data = pl.DataFrame({"DIA_MIDPT": [3.0, 7.0, 15.0, 25.0]})

        expr = create_size_class_expr(
            dia_col="DIA_MIDPT", size_class_type="descriptive"
        )
        result = data.with_columns(expr)

        expected = ["Saplings", "Small", "Medium", "Large"]
        assert result["SIZE_CLASS"].to_list() == expected

    def test_market_size_class_pine(self):
        """Test market size classes for pine/softwood (SPCD < 300)."""
        data = pl.DataFrame(
            {
                "DIA_MIDPT": [6.0, 8.5, 9.0, 10.5, 12.0, 15.0],
                "SPCD": [131, 131, 131, 131, 131, 131],  # Loblolly pine
            }
        )

        expr = create_size_class_expr(dia_col="DIA_MIDPT", size_class_type="market")
        result = data.with_columns(expr)

        expected = [
            "Pulpwood",  # 6.0 < 9.0
            "Pulpwood",  # 8.5 < 9.0
            "Chip-n-Saw",  # 9.0 < 12.0
            "Chip-n-Saw",  # 10.5 < 12.0
            "Sawtimber",  # 12.0 >= 12.0
            "Sawtimber",  # 15.0 >= 12.0
        ]
        assert result["SIZE_CLASS"].to_list() == expected

    def test_market_size_class_hardwood(self):
        """Test market size classes for hardwood (SPCD >= 300)."""
        data = pl.DataFrame(
            {
                "DIA_MIDPT": [6.0, 10.0, 11.0, 15.0],
                "SPCD": [802, 802, 802, 802],  # White oak
            }
        )

        expr = create_size_class_expr(dia_col="DIA_MIDPT", size_class_type="market")
        result = data.with_columns(expr)

        # Hardwood has no Chip-n-Saw category
        expected = [
            "Pulpwood",  # 6.0 < 11.0
            "Pulpwood",  # 10.0 < 11.0
            "Sawtimber",  # 11.0 >= 11.0
            "Sawtimber",  # 15.0 >= 11.0
        ]
        assert result["SIZE_CLASS"].to_list() == expected

    def test_market_size_class_mixed_species(self):
        """Test market size classes with mixed pine and hardwood."""
        data = pl.DataFrame(
            {
                "DIA_MIDPT": [10.0, 10.0, 12.0, 12.0],
                "SPCD": [131, 802, 131, 802],  # Pine, Oak, Pine, Oak
            }
        )

        expr = create_size_class_expr(dia_col="DIA_MIDPT", size_class_type="market")
        result = data.with_columns(expr)

        expected = [
            "Chip-n-Saw",  # Pine 10.0: 9.0 <= 10.0 < 12.0
            "Pulpwood",  # Oak 10.0: 10.0 < 11.0
            "Sawtimber",  # Pine 12.0: >= 12.0
            "Sawtimber",  # Oak 12.0: >= 11.0
        ]
        assert result["SIZE_CLASS"].to_list() == expected

    def test_invalid_size_class_type(self):
        """Test that invalid size_class_type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid size_class_type"):
            create_size_class_expr(dia_col="DIA", size_class_type="invalid")


class TestGRMSizeClassConfig:
    """Test size_class_type validation in GRM functions."""

    def test_mortality_invalid_size_class_type(self):
        """Test that mortality rejects invalid size_class_type."""
        from pyfia.estimation.estimators.mortality import mortality
        from unittest.mock import MagicMock

        mock_db = MagicMock()

        with pytest.raises(ValueError, match="size_class_type must be one of"):
            mortality(mock_db, by_size_class=True, size_class_type="invalid")

    def test_growth_invalid_size_class_type(self):
        """Test that growth rejects invalid size_class_type."""
        from pyfia.estimation.estimators.growth import growth
        from unittest.mock import MagicMock

        mock_db = MagicMock()

        with pytest.raises(ValueError, match="size_class_type must be one of"):
            growth(mock_db, by_size_class=True, size_class_type="invalid")

    def test_removals_invalid_size_class_type(self):
        """Test that removals rejects invalid size_class_type."""
        from pyfia.estimation.estimators.removals import removals
        from unittest.mock import MagicMock

        mock_db = MagicMock()

        with pytest.raises(ValueError, match="size_class_type must be one of"):
            removals(mock_db, by_size_class=True, size_class_type="invalid")


class TestMarketSizeClassConstants:
    """Test market size class constant definitions."""

    def test_pine_market_classes_exist(self):
        """Test that pine market size classes are defined."""
        from pyfia.constants.plot_design import MARKET_SIZE_CLASSES_PINE

        assert "Pulpwood" in MARKET_SIZE_CLASSES_PINE
        assert "Chip-n-Saw" in MARKET_SIZE_CLASSES_PINE
        assert "Sawtimber" in MARKET_SIZE_CLASSES_PINE

    def test_hardwood_market_classes_exist(self):
        """Test that hardwood market size classes are defined."""
        from pyfia.constants.plot_design import MARKET_SIZE_CLASSES_HARDWOOD

        assert "Pulpwood" in MARKET_SIZE_CLASSES_HARDWOOD
        assert "Sawtimber" in MARKET_SIZE_CLASSES_HARDWOOD
        # Hardwood should NOT have Chip-n-Saw
        assert "Chip-n-Saw" not in MARKET_SIZE_CLASSES_HARDWOOD

    def test_pine_boundaries_correct(self):
        """Test that pine size class boundaries match TimberMart-South specs."""
        from pyfia.constants.plot_design import MARKET_SIZE_CLASSES_PINE

        # Pulpwood: 5.0-8.9"
        assert MARKET_SIZE_CLASSES_PINE["Pulpwood"] == (5.0, 9.0)
        # Chip-n-Saw: 9.0-11.9"
        assert MARKET_SIZE_CLASSES_PINE["Chip-n-Saw"] == (9.0, 12.0)
        # Sawtimber: 12.0"+
        assert MARKET_SIZE_CLASSES_PINE["Sawtimber"][0] == 12.0

    def test_hardwood_boundaries_correct(self):
        """Test that hardwood size class boundaries match industry specs."""
        from pyfia.constants.plot_design import MARKET_SIZE_CLASSES_HARDWOOD

        # Pulpwood: 5.0-10.9"
        assert MARKET_SIZE_CLASSES_HARDWOOD["Pulpwood"] == (5.0, 11.0)
        # Sawtimber: 11.0"+
        assert MARKET_SIZE_CLASSES_HARDWOOD["Sawtimber"][0] == 11.0
